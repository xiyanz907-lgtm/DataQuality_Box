from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import pendulum

default_args = {
    'owner': 'box_admin',
    'start_date': pendulum.today('UTC').add(days=-1),
}

def _scan_cnt_newcycles(**context):
    # 获取上次处理到的 ID 水位线
    last_id = int(Variable.get("logic_watermark_cnt_newcycles", default_var=0))

    # 查询当前最大的 ID
    hook = MySqlHook(mysql_conn_id='cactus_mysql_conn')
    max_id_sql = "SELECT MAX(id) FROM kpi_data_db.cnt_newcycles"
    max_id = hook.get_first(max_id_sql)[0]

    if max_id is None or max_id <= last_id:
        print("没有检测到新 ID，无需触发。")
        return  
    
    # 有新数据，查询增量日期
    sql = f"""
        SELECT DISTINCT LEFT(_time, 10) as dt
        FROM kpi_data_db.cnt_newcycles
        WHERE id > {last_id} AND id <= {max_id}
    """ 

    df = hook.get_pandas_df(sql)
    new_dates = sorted([
        str(d) for d in set(df['dt'].astype(str).tolist()) 
        if d and str(d).strip() != 'None'
    ])

    print(f"增量数据涉及的日期: {new_dates}")

    # 触发 Worker DAG
    for date_str in new_dates:
        TriggerDagRunOperator(
            task_id=f'trigger_{date_str}',
            trigger_dag_id='worker_newcycle_check',
            conf ={"date_filter": date_str},
            wait_for_completion=False
        ).execute(context)

    # 更新水位线
    Variable.set("logic_watermark_cnt_newcycles", max_id)

with DAG('scanner_internal_newcycles',
         default_args=default_args,
         schedule='@hourly',
         catchup=False) as dag:
    PythonOperator(task_id='scan',
                   python_callable=_scan_cnt_newcycles)