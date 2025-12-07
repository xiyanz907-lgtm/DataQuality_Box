import imp
from operator import imod
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
plugins_dir = os.path.join(project_root, "plugins")
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

from services.logic_runner import run_logic_check

default_args = {
    "owner": "box_admin",
    "start_date": pendulum.today("UTC").add(days=-1),
}


def _run_new_check(**context):
    print(f"RAW PARAMS: {context.get('params')}") 
    target_date = context['params'].get('date_filter')
        
    print(f"FINAL TARGET DATE: {target_date}")    
    result = run_logic_check(
        cactus_conn_id='cactus_mysql_conn', 
        ngen_conn_id='ngen_mysql_conn', # 占位符，虽然没用到
        ngen_table_name='None',         # 占位符
        date_filter=target_date,
        
        target_table_type="cnt_newcycles" 
    )

    print(f"Check Result: {result}")

    # 结果推送到xcom
    context['ti'].xcom_push(key='qa_result', value=result)
    if result['status'] == 'FAILED':
        raise ValueError(f"Found errors: {result.get('violation_count')}")
    return result


with DAG(
    "worker_newcycle_check",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["kpi", "ngen","new_cycles"]
) as dag:

    check_task = PythonOperator(
        task_id="check_new_cycles",
        python_callable=_run_new_check,
    )

    # 发邮件
    recipients = os.getenv("ALTER_EMAIL_TO", "xiyan.zhou@westwell-lab.com")
    send_email = EmailOperator(
        task_id="send_report_email",
        to=recipients,
        subject='Cactus数据质量检测报告 (单表自测{{ dag_run.conf.get("date_filter", ds) }})',
        html_content="""
        <h3>数据质量检测运行完成</h3>
        <pre>{{ task_instance.xcom_pull(task_ids='check_hutchisonports_ngen', key='qa_result').get('report_text', '暂无详细报告内容 (可能暂无数据更新)') }}</pre>
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 设置依赖
    check_task >> send_email
