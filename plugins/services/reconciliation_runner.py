import logging
import pandas as pd
import polars as pl
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import os
import re
import pendulum
from services.config import CONN_ID_CACTUS, CONN_ID_NGEN
from dq_lib.reconciliation import ReconciliationEngine
from dq_lib.metrics import MetricsEngine
from dq_lib.metrics import MetricsEngine

class ReconciliationRunner:
    """
    负责调度 nGen 与 Cactus 之间的数据对齐 (Reconciliation) 流程。
    包含：数据提取 (Extract)、调用核心算法 (Transform)、数据库回写 (Load)。
    """

    def __init__(self, cactus_conn_id: str = CONN_ID_CACTUS, ngen_conn_id: str = CONN_ID_NGEN):
        """
        Args:
            cactus_conn_id: Cactus (KPI) 数据库连接 ID
            ngen_conn_id: nGen (Source) 数据库连接 ID
        """
        self.cactus_conn_id = cactus_conn_id
        self.ngen_conn_id = ngen_conn_id
        self.logger = logging.getLogger("airflow.task")

    def run_reconciliation(self, vehicle_list: list, date_range: tuple) -> dict:
        """
        执行对齐流程的核心入口。

        Args:
            vehicle_list: 需要处理的车号列表 (e.g. ['T101', 'T102'])
            date_range: 时间范围元组 (start_dt, end_dt) - datetime 对象

        Returns:
            dict: 统计结果 {'updated': count, 'inserted': count, 'status': 'SUCCESS'}
        """
        start_dt, end_dt = date_range
        self.logger.info(f"Starting reconciliation for {len(vehicle_list)} vehicles from {start_dt} to {end_dt}")

        if not vehicle_list:
            self.logger.warning("Vehicle list is empty. Skipping reconciliation.")
            return {'updated': 0, 'inserted': 0, 'status': 'SKIPPED'}

        try:
            # --- Step 1: Extract nGen & Clean ---
            self.logger.info("Step 1: Extracting data from nGen...")
            
            df_ngen_pl = self._extract_ngen_data(vehicle_list, start_dt, end_dt)
            self.logger.info(f"Extracted nGen rows: {df_ngen_pl.height}")

            # --- Step 2: Transform nGen & Get Normalized Vehicle List ---
            self.logger.info("Step 2: Cleaning nGen data and normalizing Vehicle IDs...")
            
            # 2.1 聚合 nGen 数据 (含清洗逻辑)
            # nGen 的清洗（标准化车号）发生在 aggregate_ngen 内部
            # [Fix] 传入 SITE_TIMEZONE 以便正确转换 Local Time -> UTC
            site_tz_str = os.getenv("SITE_TIMEZONE", "Asia/Hong_Kong")
            df_ngen_agg = ReconciliationEngine.aggregate_ngen(df_ngen_pl, timezone=site_tz_str)
            
            # 2.2 获取清洗后的标准车号列表
            # [新增] 防御性检查: 确保聚合结果不为空且包含 vehicle_id 列
            if df_ngen_agg.is_empty() or "vehicle_id" not in df_ngen_agg.columns:
                self.logger.warning("nGen aggregation returned empty or invalid result. Skipping Cactus extraction.")
                # 即使没有 nGen 数据，如果此调用是由 Cactus 触发的，我们也应该提取 Cactus 看看是否需要标记为 sync_status=2
                # 但如果 vehicle_list 是从 Cactus 来的，我们依然需要提取 Cactus 数据。
                # 假设 vehicle_list 包含归一化后的 ID (例如从 Cactus 来的)
                pass 
                # 这里逻辑有点 tricky：如果 nGen 没数据，aggregat_ngen 返回空。
                # 我们依然需要去查 Cactus，把这些车辆标记为 sync_status=2 (如果它们还没被标记)。
                # 但 _extract_cactus_data 需要 vehicle_list。
                # 如果 df_ngen_agg 为空，我们无法从 nGen 拿到 normalized list。
                # **关键修正**: 我们应该直接使用传入的 vehicle_list 去查 Cactus，前提是传入的就是归一化的 ID。
                # 假设调用方（Scanner）传入的已经是标准 ID (如 T101 -> AT101 ?? 不，Scanner 传的是 DB 里的原始 ID)
                # nGen Scanner 传的是 Tractor_No (AT101). Cactus Scanner 传的是 vehicleId (AT101).
                # 我们假设 vehicle_list 里的已经是标准格式 (AT...)，或者至少 Cactus 能认。

            # 从聚合结果中提取 vehicle_id (已经是 normalized_vehicle_id)
            # 如果 nGen 没数据，尝试使用传入的 vehicle_list (假设它是标准化的，如果不是，_extract_cactus_data 可能会查不到)
            if not df_ngen_agg.is_empty() and "vehicle_id" in df_ngen_agg.columns:
                normalized_vehicle_list = df_ngen_agg.select(pl.col("vehicle_id").unique()).to_series().to_list()
            else:
                 # Fallback: 使用输入的 vehicle_list，假设它们已经在 Cactus 中存在 (由 Cactus Scanner 触发)
                 normalized_vehicle_list = vehicle_list

            if not normalized_vehicle_list:
                self.logger.warning("No valid vehicles found. Skipping Cactus extraction.")
                return {'updated': 0, 'inserted': 0, 'status': 'SKIPPED_NO_VEHICLES'}

            self.logger.info(f"Normalized Vehicle List (Size): {len(normalized_vehicle_list)}")

            # --- Step 3: Extract Cactus Data (Using Normalized IDs) ---
            self.logger.info("Step 3: Extracting data from Cactus using normalized IDs...")
            
            df_cactus_pl = self._extract_cactus_data(normalized_vehicle_list, start_dt, end_dt)
            self.logger.info(f"Extracted Cactus rows: {df_cactus_pl.height}")

            # --- Step 4: Match ---
            self.logger.info("Step 4: Running Reconciliation Matching (2-Tier)...")
            
            match_result = ReconciliationEngine.match_data(df_ngen_agg, df_cactus_pl)
            df_matched = match_result['matched']
            df_orphaned = match_result['orphaned_ngen']

            self.logger.info(f"Match complete. Updates needed: {df_matched.height}, Orphaned nGen: {df_orphaned.height}")

            # --- Step 5: Load ---
            self.logger.info("Step 5: Updating Cactus DB...")

            updated_count = 0

            if df_matched.height > 0:
                updated_count = self._perform_updates(df_matched)
            
            # --- Step 5a: Insert orphaned nGen records ---
            inserted_count = 0
            if df_orphaned.height > 0:
                self.logger.info(f"Step 5a: Inserting {df_orphaned.height} orphaned nGen records into cnt_cycles_check...")
                inserted_count = self._perform_inserts(df_orphaned, df_ngen_agg)

            # --- Step 6: Metrics & Reporting (In-Memory) ---
            self.logger.info("Step 6: Calculating Data Quality Metrics...")
            dq_report = MetricsEngine.calculate_report(df_matched)
            self.logger.info(f"DQ Report Summary: Perfect={dq_report.get('perfect_count')}/{dq_report.get('total_count')}")

            # --- Step 7: Audit ---
            return {
                'updated': updated_count,
                'inserted': inserted_count,
                'status': 'SUCCESS',
                'dq_report': dq_report
            }

        except Exception as e:
            self.logger.error(f"Reconciliation failed: {str(e)}")
            self.logger.error(e, exc_info=True)
            raise e

    def _extract_ngen_data(self, vehicle_list: list, start_dt: datetime, end_dt: datetime) -> pl.DataFrame:
        """从 nGen 提取数据"""
        if not vehicle_list:
            return pl.DataFrame()

        hook = MySqlHook(mysql_conn_id=self.ngen_conn_id)
        
        # [变更] 扩大时间窗口 +/- 3小时，以支持宽松匹配
        buffer_hours = 4
        
        # 获取时区配置 (优先使用 SITE_TIMEZONE，默认 Asia/Hong_Kong)
        # 注意: nGen 数据库存储的是 Local Time (dd/mm/yyyy)，而 start_dt/end_dt 是 UTC。
        # 我们必须把 UTC 时间转为 Local Time 才能正确过滤。
        site_tz_str = os.getenv("SITE_TIMEZONE", "Asia/Hong_Kong")
        local_tz = pendulum.timezone(site_tz_str)
        
        # 转换 UTC -> Local Time
        # 首先确保 start_dt/end_dt 是 pendulum 对象
        start_dt = pendulum.instance(start_dt)
        end_dt = pendulum.instance(end_dt)

        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=pendulum.timezone("UTC"))
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=pendulum.timezone("UTC"))
            
        start_dt_local = start_dt.in_timezone(local_tz)
        end_dt_local = end_dt.in_timezone(local_tz)
        
        # 计算 buffer 后的 Local Time
        search_start_local = start_dt_local - timedelta(hours=buffer_hours)
        search_end_local = end_dt_local + timedelta(hours=buffer_hours)
        
        # 转为 Naive datetime 用于和 Pandas 解析结果比较 (Pandas 默认解析为 Naive)
        search_start_naive = search_start_local.replace(tzinfo=None)
        search_end_naive = search_end_local.replace(tzinfo=None)

        self.logger.info(f"Expanding nGen search window (Local Time {site_tz_str}): {search_start_naive} to {search_end_naive}")
        
        # --- 车号格式处理 ---
        # 输入格式通常为 ATxx (如 AT04, AT10)
        # nGen 格式混乱，包括: AT001, AT002 (3位补0); AT07, AT08 (2位补0); 可能还有纯数字
        # 策略: 提取数字，生成 zfill(2) 和 zfill(3) 的 AT 前缀变体
        
        search_vehicles = set()
        for v in vehicle_list:
            v_str = str(v).strip()
            # 尝试提取数字
            digits = re.sub(r'\D', '', v_str)
            
            if digits:
                # 原始输入
                search_vehicles.add(v_str)
                # 变体
                search_vehicles.add(f"AT{digits.zfill(2)}")
                search_vehicles.add(f"AT{digits.zfill(3)}")
            else:
                search_vehicles.add(v_str)

        vehicles_str = ",".join([f"'{v}'" for v in search_vehicles])
        
        # [修正] 将时间过滤下推到 SQL (Server-side Filter)
        start_str = search_start_naive.strftime('%d/%m/%Y %H:%M:%S')
        end_str = search_end_naive.strftime('%d/%m/%Y %H:%M:%S')
        
        self.logger.info(f"nGen Search Params - Vehicles: {list(search_vehicles)}")
        self.logger.info(f"nGen Search Params - Local Time Range: {start_str} to {end_str}")
        
        sql = f"""
            SELECT 
                Tractor_No, 
                Tractor_Cycle_Id, 
                On_Chasis_Datetime, 
                Off_Chasis_Datetime, 
                Cntr_Id,
                Cntr_Length_In_Feet,
                Movement_Hot_Datetime
            FROM ngen
            WHERE TRIM(Tractor_No) IN ({vehicles_str})
            AND STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%i:%s') >= STR_TO_DATE('{start_str}', '%d/%m/%Y %H:%i:%s')
            AND STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%i:%s') <= STR_TO_DATE('{end_str}', '%d/%m/%Y %H:%i:%s')
        """
        
        self.logger.info(f"Executing nGen SQL (Python Filter Mode): {sql}")
        df_pd = hook.get_pandas_df(sql)
        
        if df_pd.empty:
            self.logger.warning(f"No nGen data found for vehicles: {list(search_vehicles)[:5]}... (SQL stage)")
            return pl.DataFrame()

        # --- Python 端时间过滤 ---
        # 1. 解析时间字段 (使用 Pandas 的容错解析)
        # 注意: 格式为 DD/MM/YYYY
        df_pd['dt_parsed'] = pd.to_datetime(
            df_pd['On_Chasis_Datetime'], 
            format='%d/%m/%Y %H:%M:%S', 
            errors='coerce'
        )
        
        # 执行过滤 (使用扩大的 Local Time 时间窗口)
        mask = (df_pd['dt_parsed'] >= search_start_naive) & (df_pd['dt_parsed'] <= search_end_naive)
        df_filtered = df_pd[mask].copy()
        
        # 丢弃临时列
        df_filtered.drop(columns=['dt_parsed'], inplace=True)
        
        self.logger.info(f"Python Filtered: {len(df_pd)} -> {len(df_filtered)} rows")
        
        return pl.from_pandas(df_filtered)

    def _extract_cactus_data(self, vehicle_list: list, start_dt: datetime, end_dt: datetime) -> pl.DataFrame:
        """从 Cactus 提取数据"""
        if not vehicle_list:
            return pl.DataFrame()

        hook = MySqlHook(mysql_conn_id=self.cactus_conn_id)
        
        vehicles_str = ",".join([f"'{v}'" for v in vehicle_list])
        
        # Cactus 时间格式: YYYY-MM-DD HH:MM:SS.ssssss (varchar)
        # 我们可以按字符串比较 (ISO格式支持)，但为了保险可以使用 STR_TO_DATE 或 LEFT截取
        # 这里假设是字符串 ISO 格式，直接比较字符串即可，或者截取前19位
        
        sql = f"""
            SELECT 
                id,
                vehicleId, 
                cycleId, 
                _time_begin, 
                _time,
                cnt01,
                cnt02,
                cnt03
            FROM cnt_cycles_check
            WHERE vehicleId IN ({vehicles_str})
            AND _time >= '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}'
            AND _time <= '{end_dt.strftime('%Y-%m-%d %H:%M:%S')}.999999'
        """
        
        self.logger.debug(f"Executing Cactus SQL: {sql}")
        df_pd = hook.get_pandas_df(sql)
        return pl.from_pandas(df_pd)

    def _perform_updates(self, df: pl.DataFrame) -> int:
        """
        更新匹配成功的记录：回填 cycleId (nGen ID) 和 sync_status=1，以及箱号 (从 list/str 转换回列)
        """
        hook = MySqlHook(mysql_conn_id=self.cactus_conn_id)
        
        # [修改] 转 Pandas 处理，彻底解决 Polars List 列操作的 IndexOutOfBounds/Ambiguous Truth Value 问题
        df_pd = df.to_pandas()

        def get_safe(lst, idx):
            # 处理 None, NaN, 空列表等情况
            if lst is None:
                return None
            try:
                # 尝试作为列表访问
                if len(lst) > idx:
                    return lst[idx]
            except:
                pass
            return None

        # 应用转换 (仅当 sync_status != 2 时才需要处理箱号，因为 sync_status=2 没有 nGen 数据)
        # 但为了代码统一，我们统一处理，反正 sync_status=2 时 ref_cnt_small_list 是 None
        
        if 'ref_cnt_small_list' in df_pd.columns:
            df_pd['ref_cnt01'] = df_pd['ref_cnt_small_list'].apply(lambda x: get_safe(x, 0))
            df_pd['ref_cnt03'] = df_pd['ref_cnt_small_list'].apply(lambda x: get_safe(x, 1))
        else:
             df_pd['ref_cnt01'] = None
             df_pd['ref_cnt03'] = None

        # 重命名 ref_cnt_large -> ref_cnt02 (如果列存在)
        if 'ref_cnt_large' in df_pd.columns:
            df_pd['ref_cnt02'] = df_pd['ref_cnt_large']
        else:
             df_pd['ref_cnt02'] = None

        # 选取需要的列并转字典
        # 确保列存在，防止 Key Error
        cols_to_keep = ["cycle_id", "id", "ref_cnt01", "ref_cnt02", "ref_cnt03", "matched_status"]
        for col in cols_to_keep:
            if col not in df_pd.columns:
                df_pd[col] = None

        # [修复] 显式处理 NaN -> None，防止 MySQLdb 报错 "nan can not be used with MySQL"
        # 即使使用了 df.where(pd.notnull)，在 to_dict 过程中某些 float 列仍可能保留 nan
        # 最安全的做法是在 list comprehension 中逐个清洗
        
        update_data = df_pd[cols_to_keep].to_dict('records')
        
        def safe_val(v):
            # 处理 pandas 的 nan / nat
            if pd.isna(v):
                return None
            return v

        # Params: (cycle_id, ref_cnt01, ref_cnt02, ref_cnt03, matched_status, id)
        params = [
            (
                safe_val(row['id']), # 注意：调整顺序，ID放在第一位以便插入临时表
                safe_val(row['cycle_id']), 
                safe_val(row['ref_cnt01']), 
                safe_val(row['ref_cnt02']), 
                safe_val(row['ref_cnt03']),
                safe_val(row['matched_status']) # 使用算法返回的 matched_status (1, 2, or 4)
            ) 
            for row in update_data
        ]
        
        self.logger.info(f"Updating {len(params)} rows in Cactus DB (Optimized via Temp Table)...")
        
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            
            # [优化] 设置会话隔离级别为 READ COMMITTED 以减少间隙锁 (Gap Locks)
            # 这对于高并发的 UPDATE ... JOIN 操作至关重要，防止 Lock Wait Timeout
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            
            # 1. 创建临时表 (复制 schema，但不复制数据)
            # 使用 TEMPORARY 关键字，会话结束自动删除，且不产生 binlog (取决于配置)
            # 这里的 LIMIT 0 只是为了复制列结构和类型
            create_temp_sql = """
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_cnt_cycles_check_updates 
            AS SELECT id, cycleId, cnt01, cnt02, cnt03, sync_status FROM cnt_cycles_check LIMIT 0;
            """
            cursor.execute(create_temp_sql)
            
            # 确保临时表是空的 (如果是连接池复用可能残留)
            cursor.execute("TRUNCATE TABLE tmp_cnt_cycles_check_updates")
            
            # 2. 批量插入数据到临时表 (Batch Insert is FAST)
            insert_temp_sql = """
                INSERT INTO tmp_cnt_cycles_check_updates (id, cycleId, cnt01, cnt02, cnt03, sync_status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            # 批量插入通常非常快，5万条几秒钟
            # 依然保留 batch_size 以防内存溢出，但可以大一点
            batch_size = 5000 
            for i in range(0, len(params), batch_size):
                batch = params[i:i + batch_size]
                cursor.executemany(insert_temp_sql, batch)
                conn.commit()
            
            # [关键优化] 给临时表添加索引，防止 UPDATE JOIN 触发全表扫描导致 CPU 100%
            # CREATE TEMPORARY TABLE AS SELECT 不会复制索引，必须手动添加
            self.logger.info("Indexing temporary table...")
            cursor.execute("CREATE INDEX idx_tmp_id ON tmp_cnt_cycles_check_updates(id)")
                
            # 3. 执行 Join Update
            # 利用主键/索引进行关联更新，效率极高
            # 注意：如果 sync_status=2，cycleId/cnt 字段为 NULL，MySQL UPDATE SET x=NULL 是合法的。
            # [修改] 注释掉箱号覆盖逻辑，但保留 mismatch 对比记录用于 DQ 报告
            join_update_sql = """
                UPDATE cnt_cycles_check t
                INNER JOIN tmp_cnt_cycles_check_updates s ON t.id = s.id
                SET 
                    t.cycleId = s.cycleId,
                    -- [注释] 箱号覆盖逻辑已禁用，但报告中仍保留箱号 mismatch 对比记录
                    -- t.cnt01 = s.cnt01,
                    -- t.cnt02 = s.cnt02,
                    -- t.cnt03 = s.cnt03,
                    t.sync_status = s.sync_status
            """
            cursor.execute(join_update_sql)
            affected_rows = cursor.rowcount
            conn.commit()
            
            self.logger.info(f"Bulk Update completed. Affected rows: {affected_rows}")
            
            # 4. 清理 (可选，Connection 关闭也会自动清理)
            cursor.execute("DROP TEMPORARY TABLE IF EXISTS tmp_cnt_cycles_check_updates")
            cursor.close()
            
        except Exception as e:
            self.logger.error(f"DB Bulk Update failed: {e}")
            raise e
        finally:
            conn.close()
            
        return len(params)

    def _compute_time_begin(self, df_orphaned: pl.DataFrame, df_all_ngen: pl.DataFrame) -> pl.DataFrame:
        """
        为 orphaned nGen 记录计算 _time_begin (移植自 NgenNewJobV2.handleTimeBegin)。

        修正逻辑:
        1. 候选值: min_hot_datetime (首选) 或 real_start_time (兜底)
        2. 查找同车辆的上一圈 prevOffTime (前一个 cycle 的 real_end_time)
        3. 修正条件 (任一满足则用 prevOffTime 替代):
           - candidate >= real_start_time (on_chasis，说明 hot 时间不合理)
           - candidate < prevOffTime (跑到上一圈去了)
           - candidate > real_end_time (比结束时间还晚)
        4. 若无 prevOffTime 且 candidate > real_end_time → 丢弃该记录
        """
        if df_orphaned.is_empty():
            return df_orphaned.with_columns(pl.lit(None).cast(pl.Datetime(time_zone="UTC")).alias("computed_time_begin"))

        # 1. 计算每个 cycle 的 prev_off_time (同车辆上一圈的结束时间)
        df_sorted = df_all_ngen.sort(["vehicle_id", "real_end_time"])
        df_with_prev = df_sorted.with_columns(
            pl.col("real_end_time")
              .shift(1)
              .over("vehicle_id")
              .alias("prev_off_time")
        )
        # 只取 cycle_id 和 prev_off_time 用于 join
        df_prev_map = df_with_prev.select(["cycle_id", "prev_off_time"])

        # 2. 将 prev_off_time join 到 orphaned 上
        df = df_orphaned.join(df_prev_map, on="cycle_id", how="left")

        # 3. 确定候选 time_begin
        has_hot = pl.col("min_hot_datetime").is_not_null()
        candidate_expr = pl.when(has_hot).then(pl.col("min_hot_datetime")).otherwise(pl.col("real_start_time"))

        df = df.with_columns(candidate_expr.alias("candidate_begin"))

        # 4. 应用修正条件
        has_prev = pl.col("prev_off_time").is_not_null()
        cand = pl.col("candidate_begin")
        on_chasis = pl.col("real_start_time")
        off_chasis = pl.col("real_end_time")
        prev_off = pl.col("prev_off_time")

        needs_correction = (
            (cand >= on_chasis) |   # candidate 不早于 on_chasis → 不合理
            (cand < prev_off) |     # candidate 跑到上一圈之前
            (cand > off_chasis)     # candidate 比结束时间还晚
        )

        # 有 prevOffTime 时: 不合理则修正为 prevOffTime
        # 无 prevOffTime 时: candidate > off_chasis 则标记为 null (丢弃)
        corrected_expr = (
            pl.when(has_prev & needs_correction)
              .then(prev_off)
            .when(has_prev.not_() & (cand > off_chasis))
              .then(pl.lit(None).cast(pl.Datetime(time_zone="UTC")))
            .otherwise(cand)
        )

        df = df.with_columns(corrected_expr.alias("computed_time_begin"))

        # 5. 过滤掉无法计算 _time_begin 的记录
        before_count = df.height
        df = df.filter(pl.col("computed_time_begin").is_not_null())
        dropped = before_count - df.height
        if dropped > 0:
            self.logger.warning(f"Time begin correction: dropped {dropped} records with invalid time_begin")

        # 清理临时列
        df = df.drop(["prev_off_time", "candidate_begin"])

        return df

    def _perform_inserts(self, df_orphaned: pl.DataFrame, df_all_ngen: pl.DataFrame) -> int:
        """
        将 orphaned nGen 记录插入到 cnt_cycles_check 表，补全 Cactus 数据完整性。

        流程:
        1. 调用 _compute_time_begin 计算 _time_begin (含时间修正)
        2. 构造 INSERT 字段 (vehicleId, cycleId, _time_begin, _time, _time_end, cnt01-03, sync_status=3)
        3. 去重: 查询 cnt_cycles_check 中已存在的 cycleId，排除重复
        4. 批量 INSERT
        """
        if df_orphaned.is_empty():
            self.logger.info("No orphaned nGen records to insert.")
            return 0

        # Step 1: 计算 time_begin (含修正)
        df_with_time = self._compute_time_begin(df_orphaned, df_all_ngen)

        if df_with_time.is_empty():
            self.logger.warning("All orphaned records dropped after time_begin correction.")
            return 0

        self.logger.info(f"Preparing to insert {df_with_time.height} orphaned nGen records")

        # Step 2: 转为 Pandas 处理 (便于逐行提取 list 元素和处理 null)
        df_pd = df_with_time.to_pandas()

        # 获取时区配置 (Cactus 表存储的是本地时间)
        site_tz_str = os.getenv("SITE_TIMEZONE", "Asia/Hong_Kong")
        local_tz = pendulum.timezone(site_tz_str)

        def utc_to_local_str(dt_val):
            """将 UTC datetime 转为本地时间字符串 (Cactus 存储格式)"""
            if pd.isna(dt_val) or dt_val is None:
                return None
            dt = pendulum.instance(dt_val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pendulum.timezone("UTC"))
            return dt.in_timezone(local_tz).format("YYYY-MM-DD HH:mm:ss")

        def get_list_element(lst, idx):
            if lst is None:
                return None
            try:
                if len(lst) > idx:
                    return lst[idx]
            except (TypeError, AttributeError):
                pass
            return None

        insert_params = []
        for _, row in df_pd.iterrows():
            time_begin = utc_to_local_str(row.get('computed_time_begin'))
            time_end = utc_to_local_str(row.get('real_end_time'))

            if not time_begin or not time_end:
                continue

            cycle_id = row.get('cycle_id')
            if pd.isna(cycle_id):
                continue

            vehicle_id = row.get('vehicle_id')
            ref_large = row.get('ref_cnt_large')
            ref_small_list = row.get('ref_cnt_small_list')

            cnt01 = get_list_element(ref_small_list, 0)
            cnt02 = ref_large if (ref_large and not pd.isna(ref_large)) else None
            cnt03 = get_list_element(ref_small_list, 1)

            insert_params.append((
                vehicle_id,         # vehicleId
                int(cycle_id),      # cycleId
                time_begin,         # _time_begin
                time_end,           # _time (= _time_end)
                time_end,           # _time_end
                cnt01,              # cnt01
                cnt02,              # cnt02
                cnt03,              # cnt03
                3,                  # sync_status = 3 (Synthetic Insert)
            ))

        if not insert_params:
            self.logger.warning("No valid insert params after conversion.")
            return 0

        # Step 3: 去重 — 查询 cnt_cycles_check 中已存在的 cycleId
        hook = MySqlHook(mysql_conn_id=self.cactus_conn_id)
        cycle_ids = list({str(p[1]) for p in insert_params})
        cycle_ids_str = ",".join(cycle_ids)

        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            existing_sql = f"SELECT DISTINCT cycleId FROM cnt_cycles_check WHERE cycleId IN ({cycle_ids_str})"
            cursor.execute(existing_sql)
            existing_cycle_ids = {str(r[0]) for r in cursor.fetchall()}

            if existing_cycle_ids:
                before_count = len(insert_params)
                insert_params = [p for p in insert_params if str(p[1]) not in existing_cycle_ids]
                self.logger.info(f"Dedup: {before_count} -> {len(insert_params)} (excluded {before_count - len(insert_params)} existing cycleIds)")

            if not insert_params:
                self.logger.info("All orphaned records already exist in cnt_cycles_check. Nothing to insert.")
                cursor.close()
                return 0

            # Step 4: 批量 INSERT
            insert_sql = """
                INSERT INTO cnt_cycles_check (vehicleId, cycleId, _time_begin, _time, _time_end, cnt01, cnt02, cnt03, sync_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            batch_size = 5000
            total_inserted = 0
            for i in range(0, len(insert_params), batch_size):
                batch = insert_params[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)

            self.logger.info(f"Inserted {total_inserted} synthetic records into cnt_cycles_check (sync_status=3)")
            cursor.close()

        except Exception as e:
            self.logger.error(f"DB Insert failed: {e}")
            raise e
        finally:
            conn.close()

        return total_inserted

