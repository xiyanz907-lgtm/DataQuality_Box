import polars as pl
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging

logger = logging.getLogger("airflow.task")

def check_cycle_time_diff(cactus_conn_id, ngen_conn_id, ngen_table_name, date_filter=None):

    # local_tz = _get_timezone_by_table(ngen_table_name)
    # 读取时间配置，默认 UTC
    local_tz = os.getenv("SITE_TIMEZONE", "UTC")
    logger.info(f"正在校验表: {ngen_table_name}, 识别时区为: {local_tz}")

    # 读取阈值配置
    try:
        threshold = int(os.getenv("THREAD_TIME_DIFF", "300"))
    except ValueError:
        threshold = 300

    logger.info(f"正在校验表: {ngen_table_name}, 配置时区：{local_tz}, 报警阈值: {threshold} 秒")

    # 初始化两个 Hook
    hook_kpi = MySqlHook(mysql_conn_id=cactus_conn_id)
    hook_ngen = MySqlHook(mysql_conn_id=ngen_conn_id)
    
    # ==========================================
    # 1. 读取 KPI 数据 (Cactus / UTC)
    # ==========================================
    sql_kpi = f"""
        SELECT cycleId, _time_begin, _time_end 
        FROM kpi_data_db.cnt_cycles
        {f"WHERE DATE(_time_begin) = '{date_filter}'" if date_filter else ""}
    """
    # 逻辑：强制转 Int64 -> 截取前19位去除毫秒 -> 转时间 -> 设为 UTC
    df_kpi = pl.from_pandas(hook_kpi.get_pandas_df(sql_kpi)).with_columns(
        pl.col("cycleId").cast(pl.Int64),
        pl.col("_time_begin").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False).dt.replace_time_zone("UTC"),
        pl.col("_time_end").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False).dt.replace_time_zone("UTC")
    )
    
    if df_kpi.height == 0: return {"status": "SKIPPED", "msg": "No KPI data"}

    # ==========================================
    # 2. 读取 nGen 数据 
    # ==========================================
    target_ids = df_kpi["cycleId"].unique().to_list()
    ids_str = ",".join([f"'{str(id)}'" for id in target_ids])
    
    sql_ngen = f"""
        SELECT Tractor_Cycle_Id, On_Chasis_Datetime, Off_Chasis_Datetime 
        FROM hutchisonports.{ngen_table_name}
        WHERE Tractor_Cycle_Id IN ({ids_str})
    """

    df_ngen_pd = hook_ngen.get_pandas_df(sql_ngen)
    df_ngen = pl.from_pandas(df_ngen_pd) 
    
    if df_ngen.height == 0: return {"status": "SKIPPED", "msg": "No nGen data found"}

    logger.info(f"聚合前 nGen 原始行数: {df_ngen.height}")

    try:
        df_ngen = df_ngen.with_columns(
            pl.col("Tractor_Cycle_Id").cast(pl.Int64),

            # 解析开始时间 -> 转 UTC
            pl.col("On_Chasis_Datetime")
            .str.to_datetime(format="%d/%m/%Y %H:%M:%S", strict=False)
            .dt.replace_time_zone(local_tz, ambiguous="earliest").dt.convert_time_zone("UTC"),
            
            # 解析结束时间 -> 转 UTC
            pl.col("Off_Chasis_Datetime")
            .str.to_datetime(format="%d/%m/%Y %H:%M:%S", strict=False)
            .dt.replace_time_zone(local_tz, ambiguous="earliest").dt.convert_time_zone("UTC")
        )

        df_ngen = df_ngen.group_by("Tractor_Cycle_Id").agg([
            pl.col("On_Chasis_Datetime").min(),
            pl.col("Off_Chasis_Datetime").max()
        ])
        
        logger.info(f"聚合后 nGen 唯一 ID 数: {df_ngen.height}")

    except Exception as e:
        return {"status": "ERROR", "msg": f"nGen data parsing error: {str(e)}"}

    # ==========================================
    # 3. Join 与 统计
    # ==========================================
    joined_raw = df_kpi.join(
        df_ngen, left_on="cycleId", right_on="Tractor_Cycle_Id", how="inner", suffix="_ngen"
    )
    
    total_matched_count = joined_raw.height

    # 过滤解析失败数据
    joined_clean = joined_raw.drop_nulls(subset=["_time_begin", "_time_end", "On_Chasis_Datetime", "Off_Chasis_Datetime"])
    
    valid_check_count = joined_clean.height
    filtered_count = total_matched_count - valid_check_count

    # ==========================================
    # 4. 核心计算 (年份修正 + 结束时间检测)
    # ==========================================
    result_df = joined_clean.with_columns([
        # 计算开始时间差 (仅参考)
        (pl.col("_time_begin") - pl.col("On_Chasis_Datetime").dt.replace(year=pl.col("_time_begin").dt.year()))
        .dt.total_seconds().abs().alias("diff_begin_sec"),
        
        # 计算结束时间差 (核心指标)
        (pl.col("_time_end") - pl.col("Off_Chasis_Datetime").dt.replace(year=pl.col("_time_end").dt.year()))
        .dt.total_seconds().abs().alias("diff_end_sec")
    ])
    
    violations = result_df.filter(
        pl.col("diff_end_sec") > threshold
    )

    failed_count = violations.height
    passed_count = valid_check_count - failed_count
    failed_ids = violations["cycleId"].to_list()

    # ==========================================
    # 5. 日志与返回
    # ==========================================
    log_msg = f"""
    ==========================================================
    [数据质量检测报告] - 表名: {ngen_table_name}
    ==========================================================
    匹配的ID总数 : {total_matched_count}
    过滤无效时间数据 : {filtered_count}
    实际检验总数 : {valid_check_count}
    ----------------------------------------------------------
     检验通过  : {passed_count}
     检验异常  : {failed_count}
    ==========================================================
    """
    logger.info(log_msg)

    if failed_count > 0:
        logger.error("异常样本详情 (Top 20):")
        # 打印关键列
        sample_df = violations.select([
            "cycleId", 
            "diff_end_sec",
            "diff_begin_sec",
            "_time_end",
            "Off_Chasis_Datetime"# nGen时间(转UTC版)
        ]).head(20)
        logger.error(f"\n{sample_df}")

    return {
        "status": "FAILED" if failed_count > 0 else "SUCCESS",
        "total_matched": total_matched_count,
        "filtered": filtered_count,
        "passed": passed_count,
        "violation_count": failed_count,
        "failed_ids": failed_ids,
        "report_text": log_msg
    }