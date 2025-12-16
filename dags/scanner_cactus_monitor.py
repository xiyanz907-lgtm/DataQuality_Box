import logging
import pandas as pd
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from services.config import CONN_ID_CACTUS

# 设置默认参数
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 定义 DAG
with DAG(
    'scanner_cactus_monitor',
    default_args=default_args,
    description='Monitor Cactus (cnt_cycles) changes and trigger reconciliation for unmatched data',
    schedule_interval='@hourly',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['monitor', 'cactus', 'reconciliation'],
    is_paused_upon_creation=False,
) as dag:

    def check_changes_and_branch(**context):
        """
        检查 Cactus (cnt_cycles) 表是否有新数据或更新。
        条件: 
        1. last_modified > Watermark
        2. matched != 1 (及 4)
        """
        logger = logging.getLogger("airflow.task")
        
        # 1. 获取水位线
        watermark_key = "watermark_cactus_last_modified"
        # 默认值: '2024-01-01 00:00:00'
        last_watermark_str = Variable.get(watermark_key, default_var='2024-01-01 00:00:00')
        logger.info(f"Current Watermark: {last_watermark_str}")

        # 2. 连接数据库
        hook = MySqlHook(mysql_conn_id=CONN_ID_CACTUS)
        
        # 3. 检查是否有新数据 (Cold Start / Batch Limit 策略)
        # 限制每次处理的时间窗口: 4 小时
        batch_limit_hours = 4
        
        # 查询全局 Max last_modified
        # 假设 last_modified 是 TIMESTAMP 或 DATETIME 类型
        check_max_sql = "SELECT MAX(last_modified_timestamp) FROM cnt_cycles"
        
        max_ts_df = hook.get_pandas_df(check_max_sql)
        current_max_ts = max_ts_df.iloc[0, 0]
        
        if not current_max_ts:
            logger.info("No data found in cnt_cycles table.")
            return 'skip_processing'

        # 转换为 datetime 对象
        if isinstance(current_max_ts, str):
            current_max_dt = pendulum.parse(current_max_ts)
        else:
            # pd.Timestamp or datetime
            current_max_dt = pd.to_datetime(current_max_ts).replace(tzinfo=pendulum.timezone("UTC"))

        last_watermark_dt = pendulum.parse(last_watermark_str)
        if last_watermark_dt.tzinfo is None:
            last_watermark_dt = last_watermark_dt.replace(tzinfo=pendulum.timezone("UTC"))

        if current_max_dt <= last_watermark_dt:
            logger.info("No new data detected (Current Max <= Watermark).")
            return 'skip_processing'
            
        # 4. 确定本次处理的水位线终点 (Batch Control)
        next_window_end = last_watermark_dt.add(hours=batch_limit_hours)
        
        if current_max_dt > next_window_end:
            target_watermark_dt = next_window_end
            logger.info(f"Large data gap detected. Limiting batch to {batch_limit_hours} hours.")
        else:
            target_watermark_dt = current_max_dt

        target_watermark_str = target_watermark_dt.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Target Watermark for this run: {target_watermark_str}")

        # 5. 查询受影响的数据详情
        # 筛选条件: last_modified 在窗口内 AND matched NOT IN (1, 4)
        # matched=1 (Strict), matched=4 (Loose) 都是已完全匹配的状态，无需重试
        # matched=0 (New), matched=2 (Target Only), matched=3 (Insert-Deprecated) 需要处理
        # 注意: 防止死循环 - 只有当 last_modified 真正更新时才处理。
        # MySQL 特性: 如果 update set matched=2 where matched=2，last_modified 不会变。
        
        vehicles_sql = f"""
            SELECT DISTINCT vehicleId 
            FROM cnt_cycles 
            WHERE last_modified_timestamp > '{last_watermark_str}' 
            AND last_modified_timestamp <= '{target_watermark_str}'
            AND (matched IS NULL OR matched NOT IN (1, 4))
        """
        
        # 获取时间范围 (_time) 用于 Worker 拉取数据
        # Worker 会自动 +/- 3小时，所以这里只需要给准确的业务时间范围
        # 增加 WHERE _time IS NOT NULL 且不为空字符串的过滤
        range_sql = f"""
            SELECT MIN(_time) as min_t, MAX(_time) as max_t
            FROM cnt_cycles 
            WHERE last_modified_timestamp > '{last_watermark_str}'
            AND last_modified_timestamp <= '{target_watermark_str}'
            AND _time IS NOT NULL AND _time != ''
        """

        df_vehicles = hook.get_pandas_df(vehicles_sql)
        vehicle_list = df_vehicles['vehicleId'].dropna().unique().tolist()
        
        # 将 XCom push 放在所有 return 分支之前或确保逻辑覆盖
        context['ti'].xcom_push(key='new_watermark', value=target_watermark_str)

        if not vehicle_list:
            logger.info("Watermark advanced, but no vehicles found needing reconciliation (maybe all matched=1?).")
            return 'empty_batch'
        else:
            df_range = hook.get_pandas_df(range_sql)
            min_start = df_range.iloc[0, 0]
            max_end = df_range.iloc[0, 1]
            
            if pd.isna(min_start) or pd.isna(max_end):
                 # 极端情况：有 vehicles 但拿不到时间范围 (比如所有 _time 都是 NULL?)
                 logger.warning("Vehicles found but no valid time range. Skipping this batch but advancing watermark.")
                 return 'empty_batch'

            logger.info(f"Found {len(vehicle_list)} vehicles to reconcile.")
            logger.info(f"Data Time Range: {min_start} - {max_end}")

            trigger_conf = {
                "vehicle_list": vehicle_list,
                "date_range": (str(min_start), str(max_end))
            }

            # Push conf to XCom
            context['ti'].xcom_push(key='trigger_conf', value=trigger_conf)
            return 'trigger_reconciliation_worker'
        
    check_task = BranchPythonOperator(
        task_id='check_cactus_changes',
        python_callable=check_changes_and_branch,
    )

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_reconciliation_worker',
        trigger_dag_id='reconciliation_worker',
        conf=check_task.output['trigger_conf'],
        wait_for_completion=False,
    )

    def update_watermark_func(**context):
        watermark_key = "watermark_cactus_last_modified"
        new_watermark = context['ti'].xcom_pull(task_ids='check_cactus_changes', key='new_watermark')
        
        if new_watermark:
            logger = logging.getLogger("airflow.task")
            logger.info(f"Updating watermark to: {new_watermark}")
            Variable.set(watermark_key, new_watermark)
        else:
            raise ValueError("New watermark not found in XCom.")

    update_watermark_task = PythonOperator(
        task_id='update_watermark',
        python_callable=update_watermark_func,
        trigger_rule=TriggerRule.ONE_SUCCESS # Trigger 或 EmptyBatch 成功都更新
    )

    skip_task = EmptyOperator(task_id='skip_processing')
    empty_batch_task = EmptyOperator(task_id='empty_batch')

    # 定义依赖关系
    check_task >> [trigger_task, skip_task, empty_batch_task]
    trigger_task >> update_watermark_task
    empty_batch_task >> update_watermark_task

