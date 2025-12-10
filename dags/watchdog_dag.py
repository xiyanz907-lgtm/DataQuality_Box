import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable, DagRun
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

# -------------------------------------------------------------------------
# 配置区域 (Configuration)
# -------------------------------------------------------------------------
DAG_ID = "watchdog_dag"
SCHEDULE = "0 8,20 * * *"  # 每天早晚 8:00 和 20:00 执行

# 变量名称 (Variable Names)
VAR_NGEN_ID = "watermark_cnt_cycles_ngen_id"
VAR_CACTUS_TIME = "watermark_cnt_cycles_cactus_time"

# 阈值配置 (Thresholds)
THRESHOLD_NGEN_HOURS = 30   # nGen (T+1) 容忍 30小时
THRESHOLD_CACTUS_HOURS = 12 # Cactus (T+0) 容忍 12小时

# 数据库连接配置 (Database Configuration)
CONN_CACTUS = "cactus_mysql_conn"
CONN_NGEN = "ngen_mysql_conn"
TABLE_CACTUS = "kpi_data_db.cnt_cycles"
TABLE_NGEN = "hutchisonports.ngen"
TARGET_DAG_ID = "scanner_universal"

# 邮件配置 (Email Configuration)
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "xiyan.zhou@westwell-lab.com")

default_args = {
    "owner": "box_admin",
    "start_date": pendulum.today("UTC").add(days=-1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    schedule=SCHEDULE,
    default_args=default_args,
    catchup=False,
    tags=["monitoring", "watchdog", "health_check"],
    doc_md=__doc__,
) as dag:

    def _check_staleness(**context):
        """
        检查数据流的滞后状态 (Health Check)。
        如果发现水位线长期未更新且源端有数据，则抛出异常。
        """
        errors = []
        now = pendulum.now("UTC")

        # ---------------------------------------------------------
        # 1. 检查 Cactus (基于时间 / Time-based)
        # ---------------------------------------------------------
        watermark_time_str = Variable.get(VAR_CACTUS_TIME, default_var=None)
        
        if watermark_time_str:
            try:
                # 尝试解析时间 (处理各类 ISO 格式)
                watermark_time = pendulum.parse(watermark_time_str)
                # 确保时区感知 (通常存储为 UTC，若无时区则假设为 UTC)
                if watermark_time.tzinfo is None:
                    watermark_time = pendulum.instance(watermark_time, tz="UTC")
                
                gap_hours = (now - watermark_time).total_hours()
                
                if gap_hours > THRESHOLD_CACTUS_HOURS:
                    print(f"[Check Cactus] 检测到滞后。Gap: {gap_hours:.2f}h > {THRESHOLD_CACTUS_HOURS}h。正在检查数据库...")
                    
                    # 检查数据库中实际的最新时间戳
                    hook = MySqlHook(mysql_conn_id=CONN_CACTUS)
                    sql = f"SELECT MAX(last_modified_timestamp) FROM {TABLE_CACTUS}"
                    db_max_time = hook.get_first(sql)[0]
                    
                    if db_max_time:
                        db_max_time = pendulum.instance(db_max_time)
                        if db_max_time.tzinfo is None:
                            db_max_time = pendulum.instance(db_max_time, tz="UTC")
                            
                        if db_max_time > watermark_time:
                            errors.append(
                                f"[严重故障/CRITICAL] Cactus 服务中断 (Service Down):\n"
                                f"  - 水位线 (Watermark): {watermark_time}\n"
                                f"  - 数据库最新 (DB_Max): {db_max_time}\n"
                                f"  - 滞后时长 (Gap): {gap_hours:.2f}h\n"
                                f"  - 原因: 源端数据库有新数据，但 Box 未处理 (DB has newer data but Box not processing)."
                            )
                        elif db_max_time == watermark_time:
                            errors.append(
                                f"[业务警告/WARNING] Cactus 业务停滞 (Business Stagnation):\n"
                                f"  - 水位线 (Watermark): {watermark_time} (Matches DB Max)\n"
                                f"  - 滞后时长 (Gap): {gap_hours:.2f}h\n"
                                f"  - 原因: 源端在过去 {gap_hours:.2f} 小时内无新数据产生 (No new data in source)."
                            )
                    else:
                        print("[Check Cactus] 数据库表为空或返回 None。")
            except Exception as e:
                errors.append(f"[错误/ERROR] Cactus 检查失败 (Check Failed): {str(e)}")
        else:
            errors.append(f"[错误/ERROR] 变量 {VAR_CACTUS_TIME} 未找到 (Variable Not Found)。")

        # ---------------------------------------------------------
        # 2. 检查 nGen (基于 ID / ID-based)
        # ---------------------------------------------------------
        watermark_id_str = Variable.get(VAR_NGEN_ID, default_var=None)
        
        if watermark_id_str:
            try:
                watermark_id = int(watermark_id_str)
                
                hook = MySqlHook(mysql_conn_id=CONN_NGEN)
                sql = f"SELECT MAX(id) FROM {TABLE_NGEN}"
                db_max_id = hook.get_first(sql)[0]
                
                if db_max_id is not None:
                    db_max_id = int(db_max_id)
                    
                    if db_max_id > watermark_id:
                        print(f"[Check nGen] 检测到落后。DB_ID={db_max_id} > Watermark={watermark_id}。正在检查 Scanner DAG 历史...")
                        
                        # 检查 scanner DAG 最近一次的成功运行
                        # 注意: DagRun.find 如果记录很多可能会慢，这里我们假设最近有运行
                        last_runs = DagRun.find(dag_id=TARGET_DAG_ID, state=State.SUCCESS)
                        # 按 end_date 倒序排列
                        last_runs.sort(key=lambda x: x.end_date or pendulum.datetime(1970,1,1), reverse=True)
                        
                        is_dag_stalled = False
                        if not last_runs:
                            is_dag_stalled = True
                            last_run_info = "从未成功运行 (Never)"
                        else:
                            last_run = last_runs[0]
                            time_since_last_run = (now - last_run.end_date).total_hours()
                            last_run_info = f"{last_run.end_date} ({time_since_last_run:.2f}h ago)"
                            if time_since_last_run > THRESHOLD_NGEN_HOURS:
                                is_dag_stalled = True
                        
                        if is_dag_stalled:
                            errors.append(
                                f"[严重故障/CRITICAL] nGen 管道卡顿 (Pipeline Stalled):\n"
                                f"  - DB ID: {db_max_id} (Watermark: {watermark_id})\n"
                                f"  - 上次成功扫描: {last_run_info}\n"
                                f"  - 阈值 (Threshold): {THRESHOLD_NGEN_HOURS}h\n"
                                f"  - 原因: 源端有新 ID，但扫描任务长期未成功运行。"
                            )
            except Exception as e:
                errors.append(f"[错误/ERROR] nGen 检查失败 (Check Failed): {str(e)}")
        else:
            errors.append(f"[错误/ERROR] 变量 {VAR_NGEN_ID} 未找到 (Variable Not Found)。")

        # ---------------------------------------------------------
        # 汇报结果 (Report)
        # ---------------------------------------------------------
        if errors:
            report_text = "\n".join(errors)
            # 将详情推送到 XCom 供邮件任务使用
            context['ti'].xcom_push(key="health_report", value=report_text)
            
            # 抛出异常以触发任务失败状态
            raise ValueError(f"Watchdog 检测到系统异常 (System Anomalies):\n{report_text}")
        
        print("系统健康。未检测到异常。 (System appears healthy.)")

    check_staleness_task = PythonOperator(
        task_id="check_staleness",
        python_callable=_check_staleness,
    )

    send_alert = EmailOperator(
        task_id="send_alert",
        to=ALERT_EMAIL_TO,
        subject="[告警/ALARM] Port Quality Box 健康检查失败 (Health Check Failed)",
        html_content="""
        <h3>系统健康告警 / System Health Alert</h3>
        <p>看门狗 (Watchdog DAG) 检测到以下异常：</p>
        <p>The watchdog DAG detected the following anomalies:</p>
        <pre style="background-color: #f8d7da; padding: 10px; border: 1px solid #f5c6cb; white-space: pre-wrap;">
{{ task_instance.xcom_pull(task_ids='check_staleness', key='health_report') }}
        </pre>
        <p>请立即检查 Airflow 日志及系统状态。</p>
        <p>Please check the Airflow logs and system status immediately.</p>
        <hr/>
        <p><i>Generated by watchdog_dag</i></p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    check_staleness_task >> send_alert
