import os
import sys
import pendulum
import logging
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.email import send_email
from airflow.models import TaskInstance, XCom
from airflow.utils.state import State
from airflow.utils.session import provide_session

# 确保 plugins 路径在 sys.path 中
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
plugins_dir = os.path.join(project_root, "plugins")
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

# 配置
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "xiyan.zhou@westwell-lab.com")
LOOKBACK_MINUTES = 65  # 稍微多一点以防边界遗漏

default_args = {
    "owner": "box_admin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="global_alert_reporter",
    schedule="0 * * * *",  # 每小时运行
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    tags=["monitor", "alert"],
    default_args=default_args,
)
def global_alert_reporter():

    @task
    @provide_session
    def scan_failed_tasks(session=None):
        """
        第一步：扫描过去一小时内失败的任务。
        如果是业务逻辑检查失败（即 XCom 中包含 qa_result），则提取报告。
        """
        now = pendulum.now("UTC")
        start_time = now.subtract(minutes=LOOKBACK_MINUTES)
        
        logging.info(f"Scanning for failed tasks since {start_time}")

        # 1. 查找最近失败的任务实例
        failed_tis = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.end_date >= start_time,
                TaskInstance.state == State.FAILED
            )
            .all()
        )

        failures_found = []

        for ti in failed_tis:
            # 2. 尝试从 XCom 获取 qa_result
            # 我们直接查询 XCom 表以确保能精确匹配到对应的 run_id 和 task_id
            xcom_query = session.query(XCom).filter(
                XCom.dag_id == ti.dag_id,
                XCom.task_id == ti.task_id,
                XCom.run_id == ti.run_id,
                XCom.map_index == ti.map_index,
                XCom.key == "qa_result"
            )
            
            xcom_entry = xcom_query.first()

            if xcom_entry:
                logging.info(f"Found QA failure in {ti.dag_id}.{ti.task_id}")
                
                # 反序列化值
                try:
                    if isinstance(xcom_entry.value, dict):
                        qa_result = xcom_entry.value
                    else:
                        qa_result = XCom.deserialize_value(xcom_entry)
                except Exception as e:
                    logging.error(f"Failed to deserialize XCom for {ti.dag_id}.{ti.task_id}: {e}")
                    # Try to see if it is just json
                    try:
                        import json
                        if isinstance(xcom_entry.value, str):
                             qa_result = json.loads(xcom_entry.value)
                        elif isinstance(xcom_entry.value, bytes):
                             qa_result = json.loads(xcom_entry.value.decode('utf-8'))
                        else:
                             continue
                    except:
                        continue

                # 提取关键信息
                # 如果不是字典（例如 None），则跳过
                if not isinstance(qa_result, dict):
                    continue
                    
                failures_found.append({
                    "dag_id": ti.dag_id,
                    "task_id": ti.task_id,
                    "execution_date": str(ti.execution_date),
                    "log_url": ti.log_url,
                    "html_report": qa_result.get("html_report", "No HTML Content"),
                    "violation_count": qa_result.get("violation_count", "N/A")
                })
        
        logging.info(f"Found {len(failures_found)} actionable failures.")
        return failures_found

    @task
    def send_consolidated_alert(failures):
        """
        第二步：如果有失败记录，发送汇总邮件。
        """
        if not failures:
            print("No failures found. Skipping email.")
            return

        print(f"Preparing email for {len(failures)} failures...")
        
        subject = f"[Quality Alert] {len(failures)} Data Quality Checks Failed"
        
        # 构建 HTML 邮件正文
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .alert-box {{ border: 1px solid #d9534f; background-color: #fdf7f7; padding: 15px; margin-bottom: 20px; border-radius: 4px; }}
                .header {{ color: #d9534f; }}
                .meta {{ color: #666; font-size: 0.9em; }}
                summary {{ cursor: pointer; color: #007bff; font-weight: bold; }}
            </style>
        </head>
        <body>
            <h2>⚠️ Global Quality Alert Report</h2>
            <p>During the last hour scan, <b>{len(failures)}</b> quality check task(s) failed.</p>
            <hr>
        """
        
        for fail in failures:
            html_content += f"""
            <div class="alert-box">
                <h3 class="header">🔴 {fail['dag_id']}</h3>
                <p class="meta">
                    <b>Task:</b> {fail['task_id']}<br>
                    <b>Time:</b> {fail['execution_date']}<br>
                    <b>Violations:</b> {fail['violation_count']}
                </p>
                <p><a href="{fail['log_url']}">View Airflow Logs</a></p>
                
                <details>
                    <summary>View Full Report</summary>
                    <div style="margin-top: 10px; border-top: 1px dashed #ccc; padding-top: 10px;">
                        {fail['html_report']}
                    </div>
                </details>
            </div>
            """
            
        html_content += """
            <hr>
            <p style="color: #999; font-size: 0.8em;">Generated by global_alert_reporter DAG</p>
        </body>
        </html>
        """
        
        # 发送邮件
        send_email(
            to=ALERT_EMAIL_TO,
            subject=subject,
            html_content=html_content
        )
        print("Email sent successfully.")

    # 定义流程
    failures = scan_failed_tasks()
    send_consolidated_alert(failures)

# 实例化 DAG
global_alert_reporter()

