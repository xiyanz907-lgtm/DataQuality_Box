import logging
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable

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
    'scanner_ngen_monitor',
    default_args=default_args,
    description='Monitor nGen data changes and trigger reconciliation',
    schedule_interval='@hourly',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['monitor', 'ngen', 'reconciliation'],
) as dag:

    def check_changes_and_branch(**context):
        """
        检查 nGen 表是否有新数据。
        如果 Current_Max_Last_Modified > Watermark，则分支到触发器，否则跳过。
        """
        logger = logging.getLogger("airflow.task")
        
        # 1. 获取水位线
        watermark_key = "watermark_ngen_last_modified"
        # 默认值: '2024-01-01 00:00:00'
        last_watermark_str = Variable.get(watermark_key, default_var='2024-01-01 00:00:00')
        logger.info(f"Current Watermark: {last_watermark_str}")

        # 2. 连接数据库
        # 假设连接 ID 为 'ngen_db'，根据之前文件的上下文
        hook = MySqlHook(mysql_conn_id='ngen_db')
        
        # 3. 检查是否有新数据 (Cold Start / Batch Limit 策略)
        # 为了防止一次拉取太多，我们限制每次处理的时间窗口。
        # 比如每次最多推进 4 小时 (或者根据实际数据量调整)。
        # 这里为了演示，我们先查询全局 Max，然后决定截断点。
        
        check_max_sql = "SELECT MAX(last_modified_timestamp) FROM hutchisonports.ngen"
        # 注意: 假设 last_modified_timestamp 是 datetime 类型或可比较的格式
        
        max_ts_df = hook.get_pandas_df(check_max_sql)
        current_max_ts = max_ts_df.iloc[0, 0]
        
        if not current_max_ts:
            logger.info("No data found in nGen table.")
            return 'skip_processing'

        # 转换为 datetime 对象以便比较
        # 假设 DB 返回的是 timestamp/datetime 或 string ISO
        if isinstance(current_max_ts, str):
            current_max_dt = pendulum.parse(current_max_ts)
        else:
            # pd.Timestamp or datetime
            current_max_dt = pd.to_datetime(current_max_ts) # type: ignore

        last_watermark_dt = pendulum.parse(last_watermark_str)

        if current_max_dt <= last_watermark_dt:
            logger.info("No new data detected (Current Max <= Watermark).")
            return 'skip_processing'
            
        # 4. 确定本次处理的水位线终点 (Batch Control)
        # 如果落后太多，限制本次只处理 Watermark + 4 Hours
        batch_limit_hours = 4
        next_window_end = last_watermark_dt.add(hours=batch_limit_hours)
        
        # 本次实际的 new_watermark 是 min(GlobalMax, Watermark + 4h)
        if current_max_dt > next_window_end:
            target_watermark_dt = next_window_end
            logger.info(f"Large data gap detected. Limiting batch to {batch_limit_hours} hours.")
        else:
            target_watermark_dt = current_max_dt

        target_watermark_str = target_watermark_dt.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Target Watermark for this run: {target_watermark_str}")

        # 5. 查询受影响的数据详情
        # 获取车号列表
        # last_modified_timestamp 应该在 (last_watermark, target_watermark] 之间
        
        vehicles_sql = f"""
            SELECT DISTINCT Tractor_No 
            FROM hutchisonports.ngen 
            WHERE last_modified_timestamp > '{last_watermark_str}' 
            AND last_modified_timestamp <= '{target_watermark_str}'
            AND Tractor_No LIKE 'AT%'
        """
        
        # 获取时间范围 (基于 On_Chasis / Off_Chasis)
        # 注意: 字段是 varchar "DD/MM/YYYY HH:MM:SS"，需转换
        range_sql = f"""
            SELECT 
                MIN(STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%M:%S')) as min_start,
                MAX(STR_TO_DATE(Off_Chasis_Datetime, '%d/%m/%Y %H:%M:%S')) as max_end
            FROM hutchisonports.ngen 
            WHERE last_modified_timestamp > '{last_watermark_str}'
            AND last_modified_timestamp <= '{target_watermark_str}'
        """

        df_vehicles = hook.get_pandas_df(vehicles_sql)
        vehicle_list = df_vehicles['Tractor_No'].dropna().unique().tolist()
        
        df_range = hook.get_pandas_df(range_sql)
        min_start = df_range.iloc[0]['min_start']
        max_end = df_range.iloc[0]['max_end']

        if not vehicle_list:
            logger.info("New data detected by timestamp, but no valid Tractor_No found. Advancing watermark anyway.")
            # 即使没有车号，也应该更新水位线防止死循环，但不需要触发下游
            # 不过根据逻辑，如果 timestamp 变了，通常应该有数据。
            # 这里选择推送到 Update 任务但不触发 Trigger? 
            # 简化逻辑：直接 Update 水位线即可。
            # 但 BranchPythonOperator 只能选一条路。
            # 让我们仍然触发下游，但传递空列表，下游会处理(skipping)。
            pass

        logger.info(f"Found {len(vehicle_list)} vehicles changed.")
        logger.info(f"Data Time Range: {min_start} - {max_end}")

        # 6. 准备 Trigger Payload
        # 如果 min_start/max_end 为空 (e.g. 只有 last_modified 更新但业务时间字段为空?)，给个默认值或报错
        if pd.isna(min_start) or pd.isna(max_end):
             # Fallback: use watermark window as a proxy for date range if content dates are missing?
             # Better to log warning
             logger.warning("Min/Max business dates are null. Using watermark window as fallback.")
             min_start = last_watermark_dt
             max_end = target_watermark_dt

        trigger_conf = {
            "vehicle_list": vehicle_list,
            "date_range": (str(min_start), str(max_end))
        }

        # Push conf to XCom
        context['ti'].xcom_push(key='trigger_conf', value=trigger_conf)
        context['ti'].xcom_push(key='new_watermark', value=target_watermark_str)

        return 'trigger_reconciliation_worker'

    check_task = BranchPythonOperator(
        task_id='check_ngen_changes',
        python_callable=check_changes_and_branch,
    )

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_reconciliation_worker',
        trigger_dag_id='reconciliation_worker',
        conf="{{ task_instance.xcom_pull(task_ids='check_ngen_changes', key='trigger_conf') }}",
        wait_for_completion=False, # 异步触发，不等待下游完成
    )

    def update_watermark_func(**context):
        watermark_key = "watermark_ngen_last_modified"
        new_watermark = context['ti'].xcom_pull(task_ids='check_ngen_changes', key='new_watermark')
        
        if new_watermark:
            logger = logging.getLogger("airflow.task")
            logger.info(f"Updating watermark to: {new_watermark}")
            Variable.set(watermark_key, new_watermark)
        else:
            raise ValueError("New watermark not found in XCom.")

    update_watermark_task = PythonOperator(
        task_id='update_watermark',
        python_callable=update_watermark_func,
        trigger_rule='all_success' # 只有 Trigger 成功才更新
    )

    skip_task = EmptyOperator(task_id='skip_processing')

    # 定义依赖关系
    check_task >> [trigger_task, skip_task]
    trigger_task >> update_watermark_task

