import os
import logging
import pendulum
from datetime import timedelta
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
BATCH_SIZE = 5000       # 每次删除的批次大小，防止锁表

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

    cleanup_xcom_task >> cleanup_logs_task

