import imp
from operator import imod
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.email import EmailOperator
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
    from services.report_generator import ReportGenerator
    
    print(f"RAW PARAMS: {context.get('params')}") 
    target_date = context['params'].get('date_filter') or context.get('ds')
    print(f"FINAL TARGET DATE: {target_date}")

    runner = LogicRunner(
        cactus_conn_id='cactus_mysql_conn',
        ngen_conn_id='ngen_mysql_conn'
    )

    try:
        raw_result = runner.run_checks(
            table_name='cnt_newcycles',
            date_filter=target_date
        )
        
        # 即使是单次运行，也要封装成 list 传给 ReportGenerator
        summary_dict = ReportGenerator.generate_html_report([raw_result], title="Cactus NewCycles 检测报告")
        
    except Exception as e:
        # 防御性处理：捕获执行器异常
        raise RuntimeError(f"cnt_newcycles run_checks failed: {e}")

    print(f"Check Result: {summary_dict['report_text']}")

    # 结果推送到xcom
    context['ti'].xcom_push(key='qa_result', value=summary_dict)
    
    # 注意：generate_html_report 返回的是汇总 dict，我们需要检查内部 status
    if summary_dict['status'] == 'FAILED':
        raise ValueError(f"Found errors: {summary_dict.get('violation_count')}")
    return summary_dict


with DAG(
    "worker_newcycle_check",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5,
    tags=["kpi", "ngen","new_cycles"]
) as dag:

    check_task = PythonOperator(
        task_id="check_new_cycles",
        python_callable=_run_new_check,
    )

    # 发邮件 (已移至 global_alert_reporter)
    # recipients = os.getenv("ALERT_EMAIL_TO", "xiyan.zhou@westwell-lab.com")
    # send_email = EmailOperator(
    #     task_id="send_report_email",
    #     to=recipients,
    #     subject='[Quality] Cactus 数据质量检测报告 ({{ dag_run.conf.get("date_filter", ds) }})',
    #     html_content="""
    #     {% set r = task_instance.xcom_pull(task_ids='check_new_cycles', key='qa_result') or {} %}
    #     {{ r.get('html_report', 'Error generating report') }}
    #     """,
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )

    # 设置依赖
    # check_task >> send_email
