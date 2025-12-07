from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import pendulum

default_args = {
    'owner': 'box_admin',
    'start_date': pendulum.today('UTC').add(days=-1),
}

def _scan_and_trigger(**context):
    # 获取上次处理到的 ID 水位线
    last_processed_id = int(Variable.get("ngen_last_processed_id", default_var=1000000))
    print(f"上次处理到的 ID: {last_processed_id}")

    # 查询 nGen 当前最大的 ID
    mysql_hook = MySqlHook(mysql_conn_id='ngen_mysql_conn')
    max_id_sql = "SELECT MAX(id) FROM hutchisonports.ngen" 
    current_max_id = mysql_hook.get_first(max_id_sql)[0]
    
    # 没有新数据，直接退出
    if current_max_id is None or current_max_id <= last_processed_id:
        print("没有检测到新 ID，无需触发。")
        return

    print(f"发现新 ID 水位: {current_max_id}")

    incremental_sql = f"""
        SELECT DISTINCT STR_TO_DATE(Off_Chasis_Datetime, '%d/%m/%Y') as dt 
        FROM hutchisonports.ngen
        WHERE id > {last_processed_id} 
          AND id <= {current_max_id}
          AND Off_Chasis_Datetime IS NOT NULL
    """
    
    df_increment = mysql_hook.get_pandas_df(incremental_sql)
    new_dates = sorted([
        str(d) for d in set(df_increment['dt'].astype(str).tolist()) 
        if d and str(d).strip() != 'None'
    ])
    
    print(f"增量数据涉及的日期: {new_dates}")
    
    # 触发 Worker DAG
    if new_dates:
        for date_str in new_dates:
            print(f"正在触发检测任务: {date_str}")
            TriggerDagRunOperator(
                task_id=f'trigger_{date_str.replace("-", "_")}',
                trigger_dag_id='worker_cycle_check',
                conf={"date_filter": date_str},
                wait_for_completion=False
            ).execute(context)
    else:
        print("虽然有新 ID，但没解析出有效日期（可能是脏数据），跳过。")

    # 更新水位线
    Variable.set("ngen_last_processed_id", current_max_id)
    print(f"水位线已更新为: {current_max_id}")

with DAG(
    'scanner_ngen_updates_by_id',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
    tags=['scanner', 'watermark']
) as dag:

    scan_task = PythonOperator(
        task_id='scan_by_id_watermark',
        python_callable=_scan_and_trigger
    )