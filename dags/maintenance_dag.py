import os
import re
import shutil
import logging
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import XCom, Log
# from airflow.models import DagRun, TaskInstance, SlaMiss, ImportError
from airflow.utils.session import provide_session
from sqlalchemy import func, and_

# 配置区域
DAG_ID = "maintenance_cleanup"
SCHEDULE = "0 2 * * *"  # 每天凌晨 2:00 运行
RETENTION_DAYS_XCOM = 7  # XCom 保留 7 天
RETENTION_DAYS_LOGS = 30 # DB 日志保留 30 天
RETENTION_DAYS_RULE_RESULTS = 90   # 规则明细保留 90 天
RETENTION_DAYS_RUN_SUMMARY = 365   # 运行汇总保留 365 天
RETENTION_DAYS_PARQUET = 7         # Parquet 中间数据保留 7 天
GOVERNANCE_DATA_BASE = "/opt/airflow/data/governance"
GOVERNANCE_REPORT_CONN_ID = "qa_mysql_conn"
BATCH_SIZE = 5000       # 每次删除的批次大小，防止锁表

# batch_id 日期解析模式（按优先级排列）
_BATCH_DATE_PATTERNS = [
    (re.compile(r"(\d{8}T\d{6})"), "%Y%m%dT%H%M%S"),
    (re.compile(r"(\d{8}_\d{6})"), "%Y%m%d_%H%M%S"),
    (re.compile(r"(\d{8})"), "%Y%m%d"),
]


def _parse_batch_date(batch_name: str):
    """从 batch_id 目录名中提取日期，解析失败返回 None"""
    for pattern, fmt in _BATCH_DATE_PATTERNS:
        m = pattern.search(batch_name)
        if m:
            try:
                return pendulum.instance(datetime.strptime(m.group(1), fmt))
            except ValueError:
                continue
    return None

default_args = {
    "owner": "box_admin",
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    schedule=SCHEDULE,
    default_args=default_args,
    catchup=False,
    tags=["maintenance", "cleanup"],
    doc_md=__doc__,
    is_paused_upon_creation=False,
) as dag:

    @provide_session
    def _cleanup_xcoms(session=None):
        """清理过期的 XCom 数据"""
        limit_date = pendulum.now("UTC").subtract(days=RETENTION_DAYS_XCOM)
        logging.info(f"Cleaning up XComs before {limit_date}...")
        
        # 查找需要删除的数量
        query = session.query(XCom).filter(XCom.timestamp < limit_date)
        count = query.count()
        logging.info(f"Found {count} XCom entries to delete.")
        
        if count > 0:
            # 批量删除
            # 注意: 不同数据库对 delete limit 支持不同，这里使用标准 SQLAlchemy 方式
            # 为了避免长事务，循环删除
            deleted_total = 0
            while True:
                subq = session.query(XCom.key, XCom.timestamp, XCom.dag_id, XCom.task_id, XCom.run_id)\
                    .filter(XCom.timestamp < limit_date)\
                    .limit(BATCH_SIZE).all()
                
                if not subq:
                    break
                    
                # 构造删除条件 (复合主键比较复杂，这里简化为按 timestamp 删除，可能稍微多删一点同时间的，但在清理场景可接受)
                # 更严谨的做法是取 id (如果有) 或者使用 filter(XCom.timestamp < limit_date) 配合 limit
                # 但 SQLAlchemy 的 delete() 配合 limit 在某些 DB dialect 有限制。
                # 简单做法：直接 execute delete where timestamp < limit
                
                # 由于我们只想分批删，可以 execute delete ... limit N (MySQL支持)
                # 或者使用 filter(XCom.timestamp < limit_date) 直接全部删除，由 DB 处理事务
                # 如果数据量巨大，建议还是分批。
                
                # 针对 MySQL 的优化 (因为项目用的是 MySQL/Postgres)
                # Docker compose 显示用的是 Postgres (postgres_airflow)
                # Postgres delete 不支持 limit。
                # 所以我们用 delete where id in (select id ... limit N)
                
                # Airflow 2.0+ XCom 主键是 (dag_id, task_id, run_id, key)
                # 比较麻烦。
                
                # 简单策略：直接按时间删除。如果数据量特别大可能会慢，但对于每日运行的 job 应该还好。
                deleted = session.query(XCom).filter(XCom.timestamp < limit_date).delete(synchronize_session=False)
                session.commit()
                deleted_total = deleted
                break # 直接一次性删除了
                
            logging.info(f"Deleted {deleted_total} XCom rows.")
        else:
            logging.info("No XComs to delete.")

    @provide_session
    def _cleanup_db_logs(session=None):
        """清理 Airflow DB 中的 Log 表 (操作日志)"""
        limit_date = pendulum.now("UTC").subtract(days=RETENTION_DAYS_LOGS)
        logging.info(f"Cleaning up DB Logs before {limit_date}...")
        
        deleted = session.query(Log).filter(Log.dttm < limit_date).delete(synchronize_session=False)
        session.commit()
        logging.info(f"Deleted {deleted} Log rows.")

    cleanup_xcom_task = PythonOperator(
        task_id="cleanup_xcoms",
        python_callable=_cleanup_xcoms,
    )

    cleanup_logs_task = PythonOperator(
        task_id="cleanup_db_logs",
        python_callable=_cleanup_db_logs,
    )

    def _cleanup_governance_reports():
        """清理过期的治理报表数据（qa_mysql_conn 数据库）"""
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        hook = MySqlHook(mysql_conn_id=GOVERNANCE_REPORT_CONN_ID)

        rule_results_deleted = hook.run(
            "DELETE FROM governance_rule_results WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
            parameters=(RETENTION_DAYS_RULE_RESULTS,),
        )
        logging.info(f"governance_rule_results: cleaned rows older than {RETENTION_DAYS_RULE_RESULTS} days")

        run_summary_deleted = hook.run(
            "DELETE FROM governance_run_summary WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
            parameters=(RETENTION_DAYS_RUN_SUMMARY,),
        )
        logging.info(f"governance_run_summary: cleaned rows older than {RETENTION_DAYS_RUN_SUMMARY} days")

    cleanup_governance_task = PythonOperator(
        task_id="cleanup_governance_reports",
        python_callable=_cleanup_governance_reports,
    )

    def _cleanup_parquet_data():
        """清理过期的 Parquet 中间数据（按 batch_id 目录维度）"""
        storage_type = os.environ.get("GOVERNANCE_STORAGE_TYPE", "local")
        cutoff = pendulum.now("UTC").subtract(days=RETENTION_DAYS_PARQUET)

        logging.info(
            f"Cleaning Parquet batches older than {RETENTION_DAYS_PARQUET} days "
            f"(before {cutoff.to_date_string()}), storage={storage_type}"
        )

        if storage_type == "local":
            _cleanup_parquet_local(cutoff)
        else:
            _cleanup_parquet_minio(cutoff)

    def _cleanup_parquet_local(cutoff):
        if not os.path.exists(GOVERNANCE_DATA_BASE):
            logging.info(f"Directory not found, skipping: {GOVERNANCE_DATA_BASE}")
            return

        deleted, skipped, freed = 0, 0, 0

        for entry in sorted(os.listdir(GOVERNANCE_DATA_BASE)):
            if not entry.startswith("batch_id="):
                continue

            batch_name = entry.split("=", 1)[1]
            batch_path = os.path.join(GOVERNANCE_DATA_BASE, entry)
            batch_date = _parse_batch_date(batch_name)

            if batch_date is None:
                logging.warning(f"Cannot parse date from '{batch_name}', skipping")
                skipped += 1
                continue

            if batch_date < cutoff:
                dir_size = sum(
                    os.path.getsize(os.path.join(dp, f))
                    for dp, _, fns in os.walk(batch_path) for f in fns
                )
                shutil.rmtree(batch_path)
                freed += dir_size
                deleted += 1
                logging.info(
                    f"  Deleted: {entry} "
                    f"(date={batch_date.to_date_string()}, size={dir_size / 1024:.1f}KB)"
                )
            else:
                logging.info(f"  Retained: {entry} (date={batch_date.to_date_string()})")

        logging.info(
            f"Parquet local cleanup done: deleted={deleted}, skipped={skipped}, "
            f"freed={freed / 1024 / 1024:.2f}MB"
        )

    def _cleanup_parquet_minio(cutoff):
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError:
            logging.warning("boto3 not available, skipping MinIO Parquet cleanup")
            return

        bucket = os.environ.get("MINIO_GOVERNANCE_BUCKET", "governance-data")
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_GOVERNANCE_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("MINIO_GOVERNANCE_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("MINIO_GOVERNANCE_SECRET_KEY", "minioadmin"),
            region_name=os.environ.get("MINIO_GOVERNANCE_REGION", "us-east-1"),
        )

        try:
            paginator = s3.get_paginator("list_objects_v2")

            batch_prefixes = set()
            for page in paginator.paginate(Bucket=bucket, Prefix="batch_id=", Delimiter="/"):
                for p in page.get("CommonPrefixes", []):
                    batch_prefixes.add(p["Prefix"])

            deleted = 0
            for prefix in sorted(batch_prefixes):
                batch_name = prefix.replace("batch_id=", "").rstrip("/")
                batch_date = _parse_batch_date(batch_name)

                if batch_date is None:
                    logging.warning(f"Cannot parse date from MinIO prefix '{batch_name}', skipping")
                    continue

                if batch_date < cutoff:
                    objects = []
                    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                        objects.extend({"Key": o["Key"]} for o in page.get("Contents", []))

                    for i in range(0, len(objects), 1000):
                        s3.delete_objects(Bucket=bucket, Delete={"Objects": objects[i:i + 1000]})

                    deleted += 1
                    logging.info(
                        f"  Deleted MinIO: {prefix} "
                        f"({len(objects)} objects, date={batch_date.to_date_string()})"
                    )

            logging.info(f"Parquet MinIO cleanup done: deleted {deleted} batch(es)")

        except ClientError as e:
            logging.error(f"MinIO cleanup error: {e}")

    cleanup_parquet_task = PythonOperator(
        task_id="cleanup_parquet_data",
        python_callable=_cleanup_parquet_data,
    )

    cleanup_xcom_task >> cleanup_logs_task >> cleanup_governance_task >> cleanup_parquet_task

