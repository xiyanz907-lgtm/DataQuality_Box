import polars as pl
from airflow.providers.mysql.hooks.mysql import MySqlHook
import importlib
import logging
import traceback
import os

logger = logging.getLogger("airflow.task")

class LogicRunner:
    def __init__(self, cactus_conn_id, ngen_conn_id):
        self.hook_kpi = MySqlHook(mysql_conn_id=cactus_conn_id)
        self.hook_ngen = MySqlHook(mysql_conn_id=ngen_conn_id)

    def run_checks(self, table_name, date_filter):
        """
        V2版执行器：基于 CONFIG 字典驱动数据加载
        """
        logger.info(f"🚀 [LogicRunner V2] 开始处理表: {table_name}, 日期: {date_filter}")

        # 1. 动态加载规则模块
        try:
            module = importlib.import_module(f"rules.kpi.{table_name}")
        except ImportError:
            return {"status": "ERROR", "msg": f"Rule file rules/kpi/{table_name}.py not found", "report_text": "规则文件缺失"}

        # 读取配置 (这就是 V2 的核心)
        config = getattr(module, 'CONFIG', {"need_reference": False})
        
        df_self = None
        df_ref = None

        # =========================================================
        # 场景 A: 需要 nGen 参考数据 (跨库)
        # =========================================================
        if config.get("need_reference") and config.get("reference_source") == 'ngen':
            ref_table_name = config.get("reference_table", "ngen")
            target_key = config.get("join_key_target") # Tractor_Cycle_Id
            source_key = config.get("join_key_source") # cycleId

            # --- 拉取 nGen 数据 (SQL 下推) ---
            # 这里的日期格式必须匹配 nGen 数据库的实际存储格式
            # 假设数据库里存的是 '11/12/2024...' 这种字符串
            sql_ngen = f"""
                SELECT {target_key}, On_Chasis_Datetime, Off_Chasis_Datetime
                FROM hutchisonports.{ref_table_name}
                WHERE STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y') = STR_TO_DATE('{date_filter}', '%Y-%m-%d')
                AND Tractor_No LIKE 'AT%'
            """
            logger.info("正在拉取 nGen 数据(Filter:AT%)...")
            
            try:
                df_ref_pd = self.hook_ngen.get_pandas_df(sql_ngen)
                df_ref_raw = pl.from_pandas(df_ref_pd)
                
                # ---nGen 数据清洗 ---
                if df_ref_raw.height > 0:
                    site_tz = os.getenv("SITE_TIMEZONE", "UTC")
                    
                    df_ref_clean = df_ref_raw.with_columns(
                        pl.col(target_key).cast(pl.Int64),
                        
                        # 清洗 nGen 时间: 指定格式 -> 转时区 -> 转UTC
                        pl.col("On_Chasis_Datetime")
                        .str.to_datetime(format="%d/%m/%Y %H:%M:%S", strict=False)
                        .dt.replace_time_zone(site_tz, ambiguous="earliest").dt.convert_time_zone("UTC"),
                        
                        pl.col("Off_Chasis_Datetime")
                        .str.to_datetime(format="%d/%m/%Y %H:%M:%S", strict=False)
                        .dt.replace_time_zone(site_tz, ambiguous="earliest").dt.convert_time_zone("UTC")
                    )
                    
                    # 聚合去重 (取最早开始，最晚结束)
                    df_ref = df_ref_clean.group_by(target_key).agg([
                        pl.col("On_Chasis_Datetime").min(),
                        pl.col("Off_Chasis_Datetime").max(),
                    ])
                    logger.info(f"nGen 数据准备完毕: {df_ref.height} 条")
                else:
                    df_ref = pl.DataFrame() # 空表
                    
            except Exception as e:
                err_msg = f"nGen 数据读取/清洗失败: {str(e)}"
                logger.error(err_msg)
                return {"status": "ERROR", "msg": err_msg, "report_text": err_msg}

            # --- A4. 拉取 Cactus 数据 (根据 nGen ID 过滤) ---
            if df_ref is not None and df_ref.height > 0:
                ids = df_ref[target_key].unique().to_list()
                ids_str = ",".join([f"'{i}'" for i in ids])
                
                sql_kpi = f"SELECT * FROM kpi_data_db.{table_name} WHERE {source_key} IN ({ids_str})"
                logger.info(f"正在拉取 Cactus 数据 (过滤 {len(ids)} 个ID)...")
                df_self = pl.from_pandas(self.hook_kpi.get_pandas_df(sql_kpi))
                
                # Cactus 数据清洗 (截取前19位)
                if df_self.height > 0:
                    df_self = df_self.with_columns(
                         pl.col(source_key).cast(pl.Int64),
                         pl.col("_time_end").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False).dt.replace_time_zone("UTC"),
                         pl.col("_time_begin").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False).dt.replace_time_zone("UTC")
                    )

        # =========================================================
        # 场景 B: 单表模式 (不需要 nGen)
        # =========================================================
        else:
            logger.info("单表模式: 仅拉取自采数据...")
            # 兼容不同表的日期字段，默认 _time_begin，允许在 CONFIG 中通过 date_filter_column 覆盖
            date_col = config.get("date_filter_column", "_time_begin")
            sql_kpi = f"SELECT * FROM kpi_data_db.{table_name} WHERE DATE({date_col}) = '{date_filter}'"
            df_self = pl.from_pandas(self.hook_kpi.get_pandas_df(sql_kpi))
            # 可以在这里补充单表的时间清洗逻辑...

        # =========================================================
        # 3. 调用规则 & 生成报告
        # =========================================================
        
        # 容错：如果数据没拉到
        if df_self is None or df_self.height == 0:
             msg = f"未找到 Cactus 数据 (日期: {date_filter})"
             return {"status": "SKIPPED", "msg": msg, "report_text": msg}

        # 【核心】调用 V2 版本的入口函数
        report_list = module.get_logic_rules(df_self, df_ref)
        
        # 统计结果
        failed_count = sum([1 for r in report_list if not r.get('passed', True)])
        final_status = "FAILED" if failed_count > 0 else "SUCCESS"
        
        # 生成报告文本（通过场景也打印关键统计信息）
        report_str = f"检测表: {table_name}\n日期: {date_filter}\n状态: {final_status}\n\n"
        for r in report_list:
            status_icon = "✅" if r.get('passed') else "❌"
            report_str += f"{status_icon} [{r.get('type', 'Check')}]\n"

            # 若规则返回统计信息（均值/标准差/上下界），通过时也打印
            stats_keys = ["mean", "std_dev", "upper_limit", "lower_limit", "total_samples", "outlier_count", "outlier_ratio"]
            stats_items = {k: r[k] for k in stats_keys if k in r}
            if stats_items:
                report_str += f"   统计: {stats_items}\n"

            if not r.get('passed'):
                report_str += f"   详情: {r}\n"  # 打印失败样本等

            report_str += "-" * 20 + "\n"

        return {
            "status": final_status,
            "violation_count": failed_count,
            "details": report_list,
            "report_text": report_str
        }