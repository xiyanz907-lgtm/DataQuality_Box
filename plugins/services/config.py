import os

# 伪代码：
# 1. 集中存放表级配置，便于按表扩展数据源/阈值/字段。
# 2. 支持通过环境变量覆盖阈值，保持现有默认行为。
# 3. LogicRunner 会读取此配置并与规则模块内的 CONFIG 合并（后者可保留为兜底）。

# 数据库连接 ID 配置 (Single Source of Truth)
CONN_ID_NGEN = "ngen_mysql_conn"
CONN_ID_CACTUS = "cactus_mysql_conn"
# DataLog (开发库) 连接 ID
# 用于 NGen vs DataLog 对账（DQ v1），库名通常为 dagster_pipelines
CONN_ID_DATALOG = "datalog_mysql_conn"

# 表级配置中心（可按需扩展新表）
TABLE_CONFIGS = {
    "cnt_cycles": {
        "need_reference": True,
        "reference_source": "ngen",
        "reference_table": "ngen",
        "join_key_target": "Tractor_Cycle_Id",
        "join_key_source": "cycleId",
        "reference_id_col": "id",
        # 阈值支持环境变量覆盖，保持默认 300 秒
        "threshold_time_diff": int(os.getenv("THRESHOLD_TIME_DIFF", 300)),
        # 单表日期过滤列（单表模式备用）
        "date_filter_column": "_time_begin",
        # SQL 端计算 duration，并可限制样本条数
        "select_sql_template": (
            "SELECT cycleId, _time_begin, _time_end, "
            "TIMESTAMPDIFF(SECOND, _time_begin, _time_end) AS duration_sec "
            "FROM kpi_data_db.{table_name} "
            "WHERE DATE({date_col}) = '{{date_filter}}'{{limit_clause}}"
        ),
        "sql_limit": os.getenv("CNT_CYCLES_SQL_LIMIT"),  # 可选限制行数
        # 动态窗口缓冲时间（小时），覆盖时区差异/时钟偏差，默认 3 小时
        "time_window_buffer_hours": int(os.getenv("TIME_WINDOW_BUFFER_HOURS", 3)),
    },
    "cnt_newcycles": {
        "need_reference": False,
        "reference_source": None,
        "date_filter_column": "_time",
        # 作业链容差（秒），默认 1s，允许 ENV 覆盖
        "gap_threshold_seconds": int(os.getenv("THRESHOLD_CHAIN_GAP", 1)),
    },
}


def get_table_config(table_name: str) -> dict:
    """返回表级配置副本，未配置返回空 dict。"""
    cfg = TABLE_CONFIGS.get(table_name, {})
    return dict(cfg)
