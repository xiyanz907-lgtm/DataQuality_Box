import logging
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

# 尝试导入 ReconciliationRunner
# 假设 plugins 目录已添加到 PYTHONPATH
try:
    from services.reconciliation_runner import ReconciliationRunner
except ImportError:
    # Fallback for local testing or if plugins structure differs
    import sys
    import os
    sys.path.append(os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'plugins'))
    from services.reconciliation_runner import ReconciliationRunner

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False, # 可以根据需求开启
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='reconciliation_worker',
    default_args=default_args,
    description='Worker DAG for data reconciliation, triggered by scanner',
    schedule_interval=None, # 被动触发
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1, # 防止并发冲突
    max_active_tasks=5, # 控制同时运行的 Worker 数量
    tags=['worker', 'reconciliation']
) as dag:

    @task
    def batch_vehicles(**context):
        """
        Step 1: 获取 vehicle_list 并进行分片
        """
        conf = context['dag_run'].conf or {}
        vehicle_list = conf.get('vehicle_list', [])
        
        logging.info(f"Received raw vehicle_list: {vehicle_list}")

        if not vehicle_list:
            logging.warning("No vehicles provided in conf.")
            return []

        # 分片策略: 每 10 辆车一个 Batch
        BATCH_SIZE = 10
        batches = [vehicle_list[i:i + BATCH_SIZE] for i in range(0, len(vehicle_list), BATCH_SIZE)]
        
        logging.info(f"Total vehicles: {len(vehicle_list)}. Created {len(batches)} batches.")
        return batches

    @task(max_active_ti_per_dag=5) # 也可以在 Task 级别控制并发
    def execute_reconciliation(vehicle_batch: list, **context):
        """
        Step 2: 执行单个 Batch 的回填逻辑
        """
        if not vehicle_batch:
            return {'updated': 0, 'inserted': 0, 'status': 'SKIPPED', 'batch_size': 0}

        conf = context['dag_run'].conf or {}
        date_range_str = conf.get('date_range', (None, None))
        
        logging.info(f"Processing batch: {vehicle_batch}")
        logging.info(f"Date range string: {date_range_str}")

        start_str, end_str = date_range_str
        
        # 转换时间字符串为 datetime 对象
        # 兼容多种格式，这里使用 pandas 解析比较稳健
        try:
            start_dt = pd.to_datetime(start_str).to_pydatetime() if start_str else datetime.min
            end_dt = pd.to_datetime(end_str).to_pydatetime() if end_str else datetime.max
        except Exception as e:
            logging.error(f"Failed to parse date range: {e}")
            raise e

        # 实例化 Runner
        # 使用用户指定的 Connection IDs
        runner = ReconciliationRunner(
            cactus_conn_id='cactus_mysql_conn', 
            ngen_conn_id='ngen_mysql_conn'
        )
        
        try:
            # 执行核心逻辑
            result = runner.run_reconciliation(vehicle_batch, (start_dt, end_dt))
            
            # 附加元数据
            result['batch_size'] = len(vehicle_batch)
            result['vehicles'] = vehicle_batch
            return result
            
        except Exception as e:
            logging.error(f"Batch execution failed for {vehicle_batch}: {e}")
            raise e

    @task
    def summarize_and_notify(results: list):
        """
        Step 3: 汇总结果并发送通知
        """
        total_vehicles = 0
        total_updated = 0
        total_inserted = 0
        failed_batches = 0
        
        if not results:
            logging.info("No results to summarize.")
            return "No Data"

        for res in results:
            # handle potential failures/skips passed as None or dict
            if not res: 
                continue
                
            total_updated += res.get('updated', 0)
            total_inserted += res.get('inserted', 0)
            total_vehicles += res.get('batch_size', 0)
            
            if res.get('status') != 'SUCCESS':
                failed_batches += 1
        
        summary_lines = [
            "Reconciliation Worker Report",
            "============================",
            f"Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Vehicles Processed: {total_vehicles}",
            f"Total Records Updated: {total_updated}",
            f"Total Records Inserted: {total_inserted}",
            f"Failed Batches: {failed_batches}",
            "============================"
        ]
        
        summary_text = "\n".join(summary_lines)
        logging.info(summary_text)
        
        # 简单的邮件通知逻辑 (如果 Airflow SMTP 配置就绪)
        # try:
        #     send_email(
        #         to=['data-team@example.com'],
        #         subject=f"Reconciliation Report - {datetime.now().strftime('%Y-%m-%d')}",
        #         html_content=summary_text.replace('\n', '<br>')
        #     )
        # except Exception as e:
        #     logging.warning(f"Could not send email: {e}")
            
        return summary_text

    # 定义 Task 依赖 (Dynamic Task Mapping)
    # 1. 生成分片
    batches = batch_vehicles()
    
    # 2. 动态映射执行 (Map over batches)
    # 注意: execute_reconciliation 接收 vehicle_batch 作为参数
    # 其他参数 (如 date_range) 在 task 内部从 context 获取，不需要 partial 传递
    execution_results = execute_reconciliation.expand(vehicle_batch=batches)
    
    # 3. 汇总结果 (Reducer)
    summarize_and_notify(execution_results)

