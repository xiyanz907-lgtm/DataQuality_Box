import polars as pl
import importlib
import logging
import traceback
import os
import pendulum
from datetime import datetime, timedelta
from services.datasource import get_datasource
from services.config import get_table_config

logger = logging.getLogger("airflow.task")

class LogicRunner:
    def __init__(self, cactus_conn_id, ngen_conn_id):
        """初始化数据源。

        Args:
            cactus_conn_id: KPI (cactus) 数据库的 Airflow 连接 ID。
            ngen_conn_id: nGen 数据库的 Airflow 连接 ID。
        """
        # 数据源工厂：目前仅 mysql，后续可扩展
        self.ds_kpi = get_datasource("mysql", cactus_conn_id)
        self.ds_ngen = get_datasource("mysql", ngen_conn_id)

    def _get_local_time_range_sql(self, date_filter_utc, col_name="On_Chasis_Datetime"):
        """
        根据 UTC 日期生成对应 Local Time 的 SQL 过滤条件。
        解决 nGen (Local Time) 与 Cactus (UTC) 的时区对齐问题。
        """
        try:
            site_tz = os.getenv("SITE_TIMEZONE", "UTC")
            # 构造 UTC 时间范围
            utc_start = pendulum.from_format(date_filter_utc, "YYYY-MM-DD", tz="UTC").start_of("day")
            utc_end = pendulum.from_format(date_filter_utc, "YYYY-MM-DD", tz="UTC").end_of("day")
            
            # 转换为 Site Local Time
            local_start = utc_start.in_timezone(site_tz)
            local_end = utc_end.in_timezone(site_tz)
            
            # 格式化为 SQL 字符串 (假设 nGen 存储格式可被 STR_TO_DATE 解析)
            fmt = "%Y-%m-%d %H:%M:%S"
            s_str = local_start.strftime(fmt)
            e_str = local_end.strftime(fmt)
            
            # 构造 BETWEEN 子句
            # nGen 字段通常是字符串 'DD/MM/YYYY HH:MM:SS'
            return f"STR_TO_DATE({col_name}, '%d/%m/%Y %H:%M:%S') BETWEEN '{s_str}' AND '{e_str}'"
        except Exception as e:
            logger.error(f"时区转换失败: {e}. 回退到简单日期匹配。")
            return f"STR_TO_DATE({col_name}, '%d/%m/%Y') = STR_TO_DATE('{date_filter_utc}', '%Y-%m-%d')"

    def get_id_boundaries(self, table_name, date_filter):
        """获取指定日期 nGen 数据的 ID 范围 (Min/Max ID)。

        Args:
            table_name: 配置中的表名 key (如 cnt_cycles)
            date_filter: 日期字符串 (YYYY-MM-DD)

        Returns:
            tuple: (min_id, max_id) 或 (None, None)
        """
        logger.info(f"🔍 [LogicRunner] 获取 ID 边界: {table_name}, 日期: {date_filter}")

        # 1. 加载配置
        try:
            module = importlib.import_module(f"rules.kpi.{table_name}")
        except ImportError:
            logger.error(f"规则文件 rules/kpi/{table_name}.py 未找到")
            return None, None

        config_module = getattr(module, 'CONFIG', {"need_reference": False})
        config_center = get_table_config(table_name)
        config = {**config_module, **config_center}

        if not config.get("need_reference") or config.get("reference_source") != 'ngen':
            logger.info("非 nGen 参考模式，跳过 ID 边界查询")
            return None, None

        ref_table = config.get("reference_table", "ngen")
        # 默认 ID 列为 id，可配置覆盖
        id_col = config.get("reference_id_col", "id") 
        
        # 构造 SQL
        # 修正: 使用时区转换后的时间范围，而非直接匹配日期字符串
        where_clause = self._get_local_time_range_sql(date_filter, "On_Chasis_Datetime")
        
        sql = f"""
            SELECT MIN({id_col}), MAX({id_col})
            FROM hutchisonports.{ref_table}
            WHERE {where_clause}
            AND Tractor_No LIKE 'AT%'
        """
        
        try:
            df = self.ds_ngen.get_pandas_df(sql)
            if not df.empty and df.iloc[0, 0] is not None:
                min_id = int(df.iloc[0, 0])
                max_id = int(df.iloc[0, 1])
                logger.info(f"✅ ID 范围获取成功: {min_id} - {max_id}")
                return min_id, max_id
            else:
                logger.warning(f"⚠️ 未查询到 ID 范围 (可能该日期无数据)")
                return None, None
        except Exception as e:
            logger.error(f"❌ 获取 ID 边界失败: {e}")
            return None, None

    def run_checks(self, table_name, date_filter, id_range=None):
        """Run data quality checks for a specific table.

        Args:
            table_name: Target table name under kpi_data_db.
            date_filter: Date string used for partition filtering (YYYY-MM-DD).
            id_range: Optional tuple (start_id, end_id) for sharding.

        Returns:
            dict: Aggregated result including status, violation_count, details, and report_text.
        """
        range_info = f" [ID Range: {id_range}]" if id_range else ""
        logger.info(f"🚀 [LogicRunner V2] 开始处理表: {table_name}, 日期: {date_filter}{range_info}")

        # 1. 动态加载规则模块
        try:
            module = importlib.import_module(f"rules.kpi.{table_name}")
        except ImportError:
            return {"status": "ERROR", "msg": f"Rule file rules/kpi/{table_name}.py not found", "report_text": "规则文件缺失"}

        # 读取配置 (规则内配置 + 中央配置合并)
        config_module = getattr(module, 'CONFIG', {"need_reference": False})
        config_center = get_table_config(table_name)
        # 中央配置优先覆盖
        config = {**config_module, **config_center}
        
        df_self = None
        df_ref = None

        # =========================================================
        # 场景 A: 需要参考数据 (跨库)
        # =========================================================
        if config.get("need_reference") and config.get("reference_source") == 'ngen':
            ref_table_name = config.get("reference_table", "ngen")
            target_key = config.get("join_key_target") # Tractor_Cycle_Id
            source_key = config.get("join_key_source") # cycleId
            id_col = config.get("reference_id_col", "id")

            # --- 拉取 nGen 数据 (SQL 下推) ---
            # 这里的日期格式必须匹配 nGen 数据库的实际存储格式
            # 假设数据库里存的是 '11/12/2024...' 这种字符串
            
            # 动态构建 WHERE 子句
            if id_range:
                start_id, end_id = id_range
                # 策略: 物理 ID 分片模式 (针对新数据)
                # 直接使用 ID 范围圈定一批数据，不叠加日期过滤。
                # 理由: nGen 数据的 ID 是物理连续的导入批次，但业务日期可能是乱序的。
                # 使用 ID 范围可以保证该批次数据被完整检测，不漏掉任何一条。
                where_clause = f"{id_col} BETWEEN {start_id} AND {end_id}"
                logger.info(f"使用 ID 分片模式: {where_clause}")
            else:
                # 策略: 业务日期模式 (针对旧数据更新)
                # 修正: 考虑到时区差异，将 UTC Date 转换为 Local Time Range
                where_clause = self._get_local_time_range_sql(date_filter, "On_Chasis_Datetime")
                logger.info(f"使用日期过滤模式 (TZ Adjusted): {where_clause}")

            sql_ngen = f"""
                SELECT {target_key}, On_Chasis_Datetime, Off_Chasis_Datetime
                FROM hutchisonports.{ref_table_name}
                WHERE {where_clause}
                AND Tractor_No LIKE 'AT%'
            """
            logger.info("正在拉取 nGen 数据...")
            
            try:
                df_ref_pd = self.ds_ngen.get_pandas_df(sql_ngen)
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
                
                # 核心优化: Dynamic Precise Window (动态精准窗口)
                min_time = df_ref["On_Chasis_Datetime"].min()
                max_time = df_ref["Off_Chasis_Datetime"].max()
                
                # 读取缓冲配置，默认 3 小时
                buffer_hours = config.get("time_window_buffer_hours", 3)
                buffer = timedelta(hours=buffer_hours)
                
                # 转换为 SQL 友好的字符串格式 (UTC)
                time_filter_clause = ""
                if min_time and max_time:
                    window_start = (min_time - buffer).strftime('%Y-%m-%d %H:%M:%S')
                    window_end = (max_time + buffer).strftime('%Y-%m-%d %H:%M:%S')
                    time_filter_clause = f"AND _time_begin BETWEEN '{window_start}' AND '{window_end}'"
                    logger.info(f"⏱️ 启用动态精准窗口: {window_start} ~ {window_end}")
                
                sql_kpi = f"SELECT * FROM kpi_data_db.{table_name} WHERE {source_key} IN ({ids_str}) {time_filter_clause}"
                
                logger.info(f"正在拉取 Cactus 数据 (过滤 {len(ids)} 个ID)...")
                df_self = pl.from_pandas(self.ds_kpi.get_pandas_df(sql_kpi))
                
                # Cactus 数据清洗 (截取前19位)
                if df_self.height > 0:
                    df_self = df_self.with_columns(
                         pl.col(source_key).cast(pl.Int64),
                         pl.col("_time_end").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False).dt.replace_time_zone("UTC"),
                         pl.col("_time_begin").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False).dt.replace_time_zone("UTC")
                    )

        # =========================================================
        # 场景 B: 单表模式 (不需要参考库)
        # =========================================================
        else:
            logger.info("单表模式: 仅拉取自采数据...")
            # 兼容不同表的日期字段，默认 _time_begin，允许配置覆盖
            date_col = config.get("date_filter_column", "_time_begin")
            limit_clause = ""
            if config.get("sql_limit"):
                limit_clause = f" LIMIT {config['sql_limit']}"

            sql_tpl = config.get("select_sql_template")
            if sql_tpl:
                sql_kpi = sql_tpl.format(
                    table_name=table_name,
                    date_filter=date_filter,
                    date_col=date_col,
                    limit_clause=limit_clause
                )
            else:
                sql_kpi = f"SELECT * FROM kpi_data_db.{table_name} WHERE DATE({date_col}) = '{date_filter}'{limit_clause}"
            df_self = pl.from_pandas(self.ds_kpi.get_pandas_df(sql_kpi))
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
        
        # 提取时间范围供报告使用 (如果存在)
        time_range = "N/A"
        if df_ref is not None and df_ref.height > 0:
             try:
                min_t = df_ref["On_Chasis_Datetime"].min().strftime("%m-%d %H:%M")
                max_t = df_ref["On_Chasis_Datetime"].max().strftime("%m-%d %H:%M")
                time_range = f"{min_t} ~ {max_t}"
             except:
                 pass

        # 提取失败规则详情
        failed_rules = []
        for r in report_list:
            if not r.get('passed', True):
                # 尝试提取样本数据 (标准化字段: missing_samples, failed_samples, outlier_samples)
                samples = []
                for sample_key in ['missing_samples', 'failed_samples', 'outlier_samples']:
                    raw_samples = r.get(sample_key)
                    if raw_samples and isinstance(raw_samples, list):
                        # 增强版提取：保留 Key 名称，例如 "Tractor_Cycle_Id: 12345"
                        extracted_info = []
                        for s in raw_samples:
                            if isinstance(s, dict) and s:
                                # 尝试提取第一个键值对，或者提取所有键值对
                                # 这里为了简洁，取第一个 Key-Value
                                first_k, first_v = list(s.items())[0]
                                extracted_info.append(f"{first_k}: {first_v}")
                        
                        if extracted_info:
                            samples.extend(extracted_info)
                            break # 找到一种样本格式就够了

                failed_rules.append({
                    "rule": r.get('type', 'Unknown'),
                    "msg": str(r.get('msg', 'No message'))[:100], # 截断过长信息
                    "samples": samples[:50] # 增加到50个样本，满足"完整信息"需求
                })

        # 生成报告文本（通过场景也打印关键统计信息）
        report_str = f"检测表: {table_name}\n日期: {date_filter}\n时间范围: {time_range}\n状态: {final_status}\n\n"
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
            "report_text": report_str,
            "meta_time_range": time_range,
            "meta_failed_rules": failed_rules
        }