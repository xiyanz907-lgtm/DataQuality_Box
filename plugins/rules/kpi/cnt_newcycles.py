import os
import polars as pl
from dq_lib.consistency import ConsistencyChecks

# 迁移自 rules/single/cnt_newcycles.py，保持与 cnt_cycles 规则结构一致：
# - 提供 CONFIG 让 LogicRunner 识别为“单表模式”
# - get_logic_rules 直接接受已清洗的 df_self，执行链路连续性检查

# Runner 读取此配置，知晓无需参考表
# 提示：中央配置在 services/config.py 会覆盖同名键
CONFIG = {
    "need_reference": False,
    "reference_source": None,
    # 单表过滤日期字段（供 LogicRunner 读取）
    "date_filter_column": "_time",
    # 作业链容差（秒），默认 1，允许中央配置覆盖
    "gap_threshold_seconds": int(os.getenv("THRESHOLD_CHAIN_GAP", 1)),
}

def get_logic_rules(df_self: pl.DataFrame, df_ref: pl.DataFrame = None):
    results = []

    # 防御性校验
    if df_self is None or df_self.height == 0:
        return [{
            "type": "meta_check",
            "passed": False,
            "msg": "未获取到 cnt_newcycles 数据，跳过规则。"
        }]

    required_cols = {"vehicleId", "_time", "_time_end", "id"}
    missing = [c for c in required_cols if c not in df_self.columns]
    if missing:
        return [{
            "type": "meta_check",
            "passed": False,
            "msg": f"缺少必要列: {missing}"
        }]

    # 统一时间类型：varchar -> datetime（截取前19位，忽略毫秒）
    try:
        df_self = df_self.with_columns(
            pl.col("_time").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False),
            pl.col("_time_end").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False),
        ).drop_nulls(subset=["_time", "_time_end"])
    except Exception as e:
        return [{
            "type": "meta_check",
            "passed": False,
            "msg": f"时间字段解析失败: {e}"
        }]

    # 车辆作业链连续性检查（容差由 CONFIG 控制，默认 1 秒）
    continuity_res = ConsistencyChecks.check_chain_continuity(
        df=df_self,
        group_col="vehicleId",
        sort_col="_time",
        current_time_col="_time",
        prev_time_col="_time_end",
        id_col="id",
        gap_threshold_seconds=CONFIG.get("gap_threshold_seconds", 1),
    )
    results.append(continuity_res)

    return results

