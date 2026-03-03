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
    dag_id="DatalogNgenCheck_worker",
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
        logger = logging.getLogger("airflow.task")
        
        try:
            conf = context.get("dag_run").conf or {}
            target_shift_date = conf.get("target_shift_date")
            vehicle_list = conf.get("vehicle_list", [])

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
            
            logger.info(f"[DQ-Worker] Validation completed: status={result.get('status')}, written={result.get('written')}")
            
            # 简化 XCom 内容，避免序列化问题
            simplified_result = {
                "status": result.get("status"),
                "shift_date": result.get("shift_date"),
                "vehicles": result.get("vehicles"),
                "written": result.get("written"),
                # 不包含 results 字段，因为它可能很大
            }
            
            # 供 global_alert_reporter 等后续能力使用
            try:
                context["ti"].xcom_push(key="qa_result", value=simplified_result)
                logger.info(f"[DQ-Worker] XCom pushed successfully")
            except Exception as xcom_error:
                logger.warning(f"[DQ-Worker] Failed to push XCom: {xcom_error}")
                # XCom 失败不影响任务成功，因为数据已写入数据库
            
            logger.info(f"[DQ-Worker] Task completed successfully")
            return simplified_result
            
        except Exception as e:
            logger.error(f"[DQ-Worker] Validation failed with exception: {e}", exc_info=True)
            raise

    run_dq()


