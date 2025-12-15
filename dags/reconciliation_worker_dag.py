import logging
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from services.config import CONN_ID_CACTUS, CONN_ID_NGEN

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
    max_active_tasks=1, # [修改] 强制串行执行，避免 MySQL 大事务更新时的锁等待超时 (Lock Wait Timeout)
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

    @task(max_active_tis_per_dag=1) # [修改] 同步限制 Task 级并发度
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
        # 默认值使用 1970-01-01 而不是 datetime.min (0001-01-01) 防止 timedelta 溢出
        DEFAULT_MIN_DATE = datetime(1970, 1, 1)
        DEFAULT_MAX_DATE = datetime(2099, 12, 31)

        try:
            # 检查空字符串
            if start_str and start_str.strip():
                start_dt = pd.to_datetime(start_str).to_pydatetime()
            else:
                start_dt = DEFAULT_MIN_DATE

            if end_str and end_str.strip():
                end_dt = pd.to_datetime(end_str).to_pydatetime()
            else:
                end_dt = DEFAULT_MAX_DATE
                
        except Exception as e:
            logging.error(f"Failed to parse date range: {e}")
            raise e

        # 实例化 Runner
        # 使用统一配置的 Connection IDs
        runner = ReconciliationRunner(
            cactus_conn_id=CONN_ID_CACTUS, 
            ngen_conn_id=CONN_ID_NGEN
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
        failed_batches = 0
        
        # DQ 汇总
        total_matched_pairs = 0
        total_perfect = 0
        time_diff_sum = 0.0
        cntr_stats_agg = {}
        all_errors = []
        
        if not results:
            logging.info("No results to summarize.")
            return "No Data"

        for res in results:
            # handle potential failures/skips passed as None or dict
            if not res: 
                continue
                
            total_updated += res.get('updated', 0)
            total_vehicles += res.get('batch_size', 0)
            
            if res.get('status') != 'SUCCESS':
                failed_batches += 1
                
            # 汇总 DQ Report
            dq = res.get('dq_report', {})
            if dq:
                # 注意: metrics.py 返回的 key 是 total_count, consistency_stats
                # 我们需要适配一下
                matched_cnt = dq.get('total_count', 0)
                total_matched_pairs += matched_cnt
                total_perfect += dq.get('perfect_count', 0)
                
                # 加权平均时间差 (简单累加总秒数，最后除以总数)
                avg_diff = dq.get('avg_time_diff_sec', 0)
                time_diff_sum += avg_diff * matched_cnt
                
                # 累加箱号状态
                stats = dq.get('consistency_stats', {})
                for k, v in stats.items():
                    cntr_stats_agg[k] = cntr_stats_agg.get(k, 0) + v
                    
                # 收集异常 (限制总数以免爆内存/邮件)
                if len(all_errors) < 100:
                    all_errors.extend(dq.get('errors', []))
        
        # 计算全局平均
        global_avg_diff = 0.0
        if total_matched_pairs > 0:
            global_avg_diff = time_diff_sum / total_matched_pairs
            
        perfect_rate = 0.0
        if total_matched_pairs > 0:
            perfect_rate = (total_perfect / total_matched_pairs) * 100
        
        summary_lines = [
            "Reconciliation Worker Report",
            "============================",
            f"Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Vehicles Processed: {total_vehicles}",
            f"Total Records Updated: {total_updated}",
            f"Failed Batches: {failed_batches}",
            "",
            "Data Quality Metrics (Matched Pairs)",
            "----------------------------",
            f"Total Matched Pairs: {total_matched_pairs}",
            f"Perfect Matches: {total_perfect} ({perfect_rate:.2f}%)",
            f"Avg Time Diff: {global_avg_diff:.2f} sec",
            "",
            "Container Consistency Stats:",
            *[f"  - {k}: {v}" for k, v in cntr_stats_agg.items()],
            "",
            "Top Errors (Sample):",
            "----------------------------"
        ]
        
        # 格式化错误列表
        for err in all_errors[:20]: # 只展示前20个
             summary_lines.append(f"ID:{err.get('id')} | Veh:{err.get('vehicle')} | Issue:{err.get('issue')}")
             summary_lines.append(f"  nGen: {err.get('ngen_cnts')}")
             summary_lines.append(f"  Cactus: {err.get('cactus_cnts')}")
             summary_lines.append("-")

        summary_lines.append("============================")
        
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
