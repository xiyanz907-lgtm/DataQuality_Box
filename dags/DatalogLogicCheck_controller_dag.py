"""
DQ v1 Controller DAG - Ground Truth Validation

职责：
1) 定时轮询（每 30 分钟）
2) 数据就绪检测（只检查 subtarget_vehicle_cycle 表）：
   - 读取 Variable: last_processed_ground_truth_shift_date
   - 查询 subtarget_vehicle_cycle 表的 MAX(shift_date)
   - 只有表中数据 > watermark 时，才视为数据就绪
3) 动态分片：
   - 从 subtarget_vehicle_cycle 获取该 shift_date 下所有 DISTINCT vehicle_id
   - 按每 5~10 辆车一组切片
4) 分发：
   - 触发 Worker DAG（dq_v1_worker_ground_truth）
5) 更新：
   - inflight 机制：
     - 触发后立刻返回（wait_for_completion=False）
     - 后续轮询时通过结果表 `datalog_logic_check_result` 的完成度（done/expected）判断已全部落盘，
       再推进 `last_processed_ground_truth_shift_date`

注意：
- 本 DAG 仅负责"就绪判断+分片+调度"，不包含任何验证计算逻辑
- 与 dq_v1_controller 完全独立，互不影响
"""

import os
import sys
import logging
import pendulum
from datetime import timedelta
from typing import Dict, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.empty import EmptyOperator

# 统一复用项目内的 Connection ID 配置
from services.config import CONN_ID_DATALOG as CONFIG_CONN_ID_DATALOG

# 确保 plugins 路径可导入（与项目现有 DAG 保持一致）
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
plugins_dir = os.path.join(project_root, "plugins")
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

# -----------------------------
# 配置区
# -----------------------------
DAG_ID = "DatalogLogicCheck_controller"
SCHEDULE = "*/30 * * * *"  # 每 30 分钟轮询一次

# Watermark Variable（独立的，不与原 Controller 冲突）
VAR_LAST_PROCESSED = "DatalogLogicCheck_last_processed_shift_date"
VAR_INFLIGHT = "DatalogLogicCheck_inflight_shift_date"

# DataLog DB Connection
CONN_ID_DATALOG = os.getenv("DATALOG_CONN_ID", CONFIG_CONN_ID_DATALOG)

# DataLog 表（Ground Truth 只需要 subtarget 表）
TBL_SUBTARGET = "dagster_pipelines.subtarget_vehicle_cycle"
TBL_RESULT = "dagster_pipelines.datalog_logic_check_result"

# Worker DAG
WORKER_DAG_ID = "DatalogLogicCheck_worker"

# 冷启动默认起始日期（可通过环境变量配置）
DEFAULT_START_DATE = os.getenv("DATALOG_LOGIC_CHECK_START_DATE", "2025-12-18")

# 分片大小：严格要求 5~10
try:
    DEFAULT_SHARD_SIZE = int(os.getenv("DQ_VEHICLE_SHARD_SIZE", "8") or "8")
except (ValueError, TypeError):
    DEFAULT_SHARD_SIZE = 8
SHARD_SIZE = max(5, min(10, DEFAULT_SHARD_SIZE))

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE,
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    default_args=default_args,
    tags=["dq", "controller", "ground_truth"],
    is_paused_upon_creation=False,
) as dag:

    @task
    def compute_plan() -> Dict:
        """
        计划器（inflight 机制）：
        - 若存在 inflight：
          - 若已完成（结果表覆盖该 shift_date 的所有车辆），返回 mode=finalize
          - 若未完成，返回 mode=wait
        - 若不存在 inflight：
          - 检查 subtarget_vehicle_cycle 表，得到下一天 target，返回 mode=trigger（否则 mode=skip）

        Returns:
            dict: {mode: trigger|finalize|wait|skip, target_shift_date: str|None}
        """
        logger = logging.getLogger("airflow.task")
        hook = MySqlHook(mysql_conn_id=CONN_ID_DATALOG)

        watermark = Variable.get(VAR_LAST_PROCESSED, default_var=None)
        if not watermark:
            watermark_dt = None
        else:
            watermark_dt = pendulum.parse(str(watermark)).date()

        # 检查 inflight
        inflight = Variable.get(VAR_INFLIGHT, default_var=None)
        if inflight:
            inflight_date = str(inflight).strip()
            if inflight_date:
                logger.info(f"[GT-Controller] inflight detected: {inflight_date}")
                try:
                    # 期望车辆数（从 subtarget 表）
                    expected_sql = f"""
                        SELECT COUNT(DISTINCT vehicle_id) AS cnt
                        FROM {TBL_SUBTARGET}
                        WHERE shift_date = '{inflight_date}'
                          AND vehicle_id IS NOT NULL AND vehicle_id != ''
                    """
                    # 已完成车辆数（从结果表）
                    done_sql = f"""
                        SELECT COUNT(DISTINCT vehicle_id) AS cnt
                        FROM {TBL_RESULT}
                        WHERE shift_date = '{inflight_date}'
                          AND vehicle_id IS NOT NULL AND vehicle_id != ''
                    """
                    expected_cnt = int(hook.get_first(expected_sql)[0] or 0)
                    done_cnt = int(hook.get_first(done_sql)[0] or 0)
                    logger.info(f"[GT-Controller] inflight progress: done={done_cnt} / expected={expected_cnt}")

                    # 兜底：如果该班次没有任何车辆，直接 finalize
                    if expected_cnt == 0:
                        logger.warning(
                            f"[GT-Controller] inflight expected_cnt=0 for shift_date={inflight_date}. "
                            f"Will finalize to avoid deadlock."
                        )
                        return {"mode": "finalize", "target_shift_date": inflight_date, "reason": "expected_cnt_zero"}

                    if expected_cnt > 0 and done_cnt >= expected_cnt:
                        return {"mode": "finalize", "target_shift_date": inflight_date}
                    return {"mode": "wait", "target_shift_date": inflight_date}
                except Exception as e:
                    logger.warning(f"[GT-Controller] inflight completion check failed: {e}")
                    return {"mode": "wait", "target_shift_date": inflight_date}

        # 查询 subtarget 表的最大日期
        def _max_shift_date() -> Optional[pendulum.Date]:
            sql = f"SELECT MAX(shift_date) FROM {TBL_SUBTARGET}"
            v = hook.get_first(sql)[0]
            if v is None:
                return None
            return pendulum.parse(str(v)).date()

        max_subtarget = _max_shift_date()
        logger.info(f"[GT-Controller] max shift_date in subtarget: {max_subtarget}")

        if max_subtarget is None:
            logger.warning("[GT-Controller] subtarget 表无数据，暂不推进 watermark。")
            return {"mode": "skip", "target_shift_date": None}

        if watermark_dt is None:
            # 冷启动：从配置的起始日期开始
            default_start_date = pendulum.parse(DEFAULT_START_DATE).date()
            
            # 如果默认起始日期大于数据库最大日期，等待数据就绪
            if default_start_date > max_subtarget:
                logger.warning(
                    f"[GT-Controller] 默认起始日期 {default_start_date} 大于数据库最大日期 {max_subtarget}，"
                    f"等待数据就绪。"
                )
                return {"mode": "skip", "target_shift_date": None}
            
            target = default_start_date.to_date_string()
            logger.info(f"[GT-Controller] 冷启动，从 {target} 开始处理")
            return {"mode": "trigger", "target_shift_date": target}

        # 只有当 max_subtarget > watermark 才推进
        if max_subtarget <= watermark_dt:
            logger.info("[GT-Controller] subtarget 最新日期未超过 watermark，数据尚未就绪。")
            return {"mode": "skip", "target_shift_date": None}

        # 处理"下一天"，确保顺序消费
        next_dt = watermark_dt.add(days=1)
        if next_dt > max_subtarget:
            logger.info("[GT-Controller] 下一天尚未就绪，等待下一轮。")
            return {"mode": "skip", "target_shift_date": None}

        target = next_dt.to_date_string()
        logger.info(f"[GT-Controller] target_shift_date={target}")
        return {"mode": "trigger", "target_shift_date": target}

    @task
    def shard_vehicles(plan: Dict) -> List[Dict]:
        """
        分片：
        - 查询 subtarget_vehicle_cycle 获取该 shift_date 下所有 DISTINCT vehicle_id
        - 按每 5~10 辆车一组进行切片

        Returns:
            list[dict]: 每个 dict 会作为 TriggerDagRunOperator 的 conf
        """
        logger = logging.getLogger("airflow.task")
        if plan.get("mode") != "trigger":
            return []
        target_shift_date = plan.get("target_shift_date")
        if not target_shift_date:
            return []

        hook = MySqlHook(mysql_conn_id=CONN_ID_DATALOG)
        sql = f"""
            SELECT DISTINCT vehicle_id
            FROM {TBL_SUBTARGET}
            WHERE shift_date = '{target_shift_date}'
              AND vehicle_id IS NOT NULL
              AND vehicle_id != ''
        """
        df = hook.get_pandas_df(sql)
        if df is None or df.empty:
            logger.warning(f"[GT-Controller] shift_date={target_shift_date} 未找到任何 vehicle_id，跳过。")
            return []

        vehicles = sorted(list({str(v).strip() for v in df["vehicle_id"].tolist() if str(v).strip()}))
        if not vehicles:
            return []

        # 分片（严格每片 5~10 辆车，默认 8）
        shards = [vehicles[i : i + SHARD_SIZE] for i in range(0, len(vehicles), SHARD_SIZE)]
        logger.info(f"[GT-Controller] target={target_shift_date}, vehicles={len(vehicles)}, shards={len(shards)}, shard_size={SHARD_SIZE}")

        return [
            {"target_shift_date": target_shift_date, "vehicle_list": shard}
            for shard in shards
        ]

    @task
    def mark_inflight(plan: Dict, conf_list: List[Dict]) -> Optional[str]:
        """
        当需要 trigger 新批次时，先写入 inflight，防止 controller 在下一轮重复触发。
        """
        logger = logging.getLogger("airflow.task")
        if plan.get("mode") != "trigger":
            return None
        target = plan.get("target_shift_date")
        if not target:
            logger.warning("[GT-Controller] plan 缺少 target_shift_date，不写 inflight。")
            return None

        # 兜底：空批次（没有任何可分片车辆）
        if not conf_list:
            logger.warning(
                f"[GT-Controller] Empty batch (no vehicles) for shift_date={target}. "
                f"Will advance watermark and skip triggering workers."
            )
            Variable.set(VAR_LAST_PROCESSED, target)
            try:
                Variable.delete(VAR_INFLIGHT)
            except Exception:
                Variable.set(VAR_INFLIGHT, "")
            return None

        Variable.set(VAR_INFLIGHT, target)
        logger.info(f"[GT-Controller] inflight set: {target}")
        return target

    @task
    def finalize_if_ready(plan: Dict) -> None:
        """
        inflight 完成后推进 watermark，并清理 inflight。
        """
        logger = logging.getLogger("airflow.task")
        if plan.get("mode") != "finalize":
            return
        target = plan.get("target_shift_date")
        if not target:
            return
        Variable.set(VAR_LAST_PROCESSED, target)
        try:
            Variable.delete(VAR_INFLIGHT)
        except Exception:
            Variable.set(VAR_INFLIGHT, "")
        logger.info(f"[GT-Controller] finalized: watermark={target}, inflight cleared")

    # 任务流
    plan = compute_plan()
    conf_list = shard_vehicles(plan)
    inflight_date = mark_inflight(plan, conf_list)

    trigger_workers = TriggerDagRunOperator.partial(
        task_id="trigger_workers",
        trigger_dag_id=WORKER_DAG_ID,
        wait_for_completion=False,
        reset_dag_run=False,
    ).expand(conf=conf_list)

    inflight_date >> trigger_workers
    finalize_if_ready(plan)

    done = EmptyOperator(task_id="done")
    [trigger_workers, done]

