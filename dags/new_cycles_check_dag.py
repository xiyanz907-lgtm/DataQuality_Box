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

from services.logic_runner import LogicRunner

default_args = {
    "owner": "box_admin",
    "start_date": pendulum.today("UTC").add(days=-1),
}


def _run_new_check(**context):
    # 步骤：解析日期参数 -> 实例化 Runner -> 调用 run_checks -> 推送结果
    print(f"RAW PARAMS: {context.get('params')}") 
    target_date = context['params'].get('date_filter') or context.get('ds')
    print(f"FINAL TARGET DATE: {target_date}")

    # 旧调用方式（已废弃，保留注释以便追溯）
    # result = LogicRunner(
    #     cactus_conn_id='cactus_mysql_conn', 
    #     ngen_conn_id='ngen_mysql_conn', # 占位符，虽然没用到
    #     ngen_table_name='None',         # 占位符
    #     date_filter=target_date,
    #     target_table_type="cnt_newcycles" 
    # )

    runner = LogicRunner(
        cactus_conn_id='cactus_mysql_conn',
        ngen_conn_id='ngen_mysql_conn'
    )

    try:
        result = runner.run_checks(
            table_name='cnt_newcycles',
            date_filter=target_date
        )
    except Exception as e:
        # 防御性处理：捕获执行器异常
        raise RuntimeError(f"cnt_newcycles run_checks failed: {e}")

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
        {% set r = task_instance.xcom_pull(task_ids='check_new_cycles', key='qa_result') or {} %}
        <h3>数据质量检测运行完成</h3>
        <p>状态: {{ r.get('status', 'UNKNOWN') }} | 失败规则数: {{ (r.get('details', []) | selectattr('passed', 'equalto', False) | list | length) if r.get('details') else 'N/A' }}</p>
        <pre>{{ r.get('report_text', r) }}</pre>
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 设置依赖
    check_task >> send_email
