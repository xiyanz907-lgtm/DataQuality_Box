"""
DQ v1 Worker DAG

职责：
- 接收参数 target_shift_date, vehicle_list
- 实例化 DataQualityRunner 并执行 run_validation（严格参数签名：run_validation(shift_date, vehicle_list)）

约束：
- 仅执行 NGen vs DataLog 对账
- 所有不匹配只记录 WARN，不抛异常
"""

import os
import sys
import logging
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task

# 确保 plugins 路径可导入（与项目现有 DAG 保持一致）
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
plugins_dir = os.path.join(project_root, "plugins")
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

try:
    from services.data_quality_runner import DataQualityRunner
    from services.config import CONN_ID_DATALOG, CONN_ID_NGEN
except ImportError:
    # fallback：某些环境下 AIRFLOW_HOME 不同
    sys.path.append(os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "plugins"))
    from services.data_quality_runner import DataQualityRunner
    from services.config import CONN_ID_DATALOG, CONN_ID_NGEN


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="dq_v1_worker",
    schedule_interval=None,  # 被动触发
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["dq", "worker", "ngen_vs_datalog"],
    is_paused_upon_creation=False,
) as dag:

    @task
    def run_dq(**context):
        """
        从 dag_run.conf 读取参数：
        - target_shift_date
        - vehicle_list
        """
        conf = context.get("dag_run").conf or {}
        target_shift_date = conf.get("target_shift_date")
        vehicle_list = conf.get("vehicle_list", [])

        logger = logging.getLogger("airflow.task")
        logger.info(f"[DQ-Worker] target_shift_date={target_shift_date}, vehicles={len(vehicle_list)}")

        if not target_shift_date or not vehicle_list:
            logger.warning("[DQ-Worker] 参数不足，跳过本次执行。")
            return {"status": "SKIPPED", "target_shift_date": target_shift_date, "vehicles": vehicle_list}

        # 连接 ID 统一来自 services.config；如需覆盖，可用环境变量。
        ngen_conn_id = os.getenv("NGEN_CONN_ID", CONN_ID_NGEN)
        datalog_conn_id = os.getenv("DATALOG_CONN_ID", CONN_ID_DATALOG)

        runner = DataQualityRunner(ngen_conn_id=ngen_conn_id, datalog_conn_id=datalog_conn_id)

        # 严格按需求：run_validation(self, shift_date, vehicle_list)
        result = runner.run_validation(target_shift_date, vehicle_list)

        # 供 global_alert_reporter 等后续能力使用
        context["ti"].xcom_push(key="qa_result", value=result)
        return result

    run_dq()


