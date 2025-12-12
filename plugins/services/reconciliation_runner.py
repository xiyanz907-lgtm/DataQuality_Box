import logging
import pandas as pd
import polars as pl
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
from dq_lib.reconciliation import ReconciliationEngine

class ReconciliationRunner:
    """
    负责调度 nGen 与 Cactus 之间的数据对齐 (Reconciliation) 流程。
    包含：数据提取 (Extract)、调用核心算法 (Transform)、数据库回写 (Load)。
    """

    def __init__(self, cactus_conn_id: str = "cactus_db", ngen_conn_id: str = "ngen_db"):
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
            df_ngen_agg = ReconciliationEngine.aggregate_ngen(df_ngen_pl)
            
            # 2.2 获取清洗后的标准车号列表
            # 从聚合结果中提取 vehicle_id (已经是 normalized_vehicle_id)
            normalized_vehicle_list = df_ngen_agg.select(pl.col("vehicle_id").unique()).to_series().to_list()
            
            if not normalized_vehicle_list:
                self.logger.warning("No valid vehicles found after nGen cleaning. Skipping Cactus extraction.")
                return {'updated': 0, 'inserted': 0, 'status': 'SKIPPED_AFTER_CLEAN'}

            self.logger.info(f"Normalized Vehicle List: {normalized_vehicle_list}")

            # --- Step 3: Extract Cactus Data (Using Normalized IDs) ---
            self.logger.info("Step 3: Extracting data from Cactus using normalized IDs...")
            
            df_cactus_pl = self._extract_cactus_data(normalized_vehicle_list, start_dt, end_dt)
            self.logger.info(f"Extracted Cactus rows: {df_cactus_pl.height}")

            # --- Step 4: Match ---
            self.logger.info("Step 4: Running Reconciliation Matching...")
            
            match_result = ReconciliationEngine.match_data(df_ngen_agg, df_cactus_pl)
            df_matched = match_result['matched']
            df_unmatched = match_result['unmatched']

            self.logger.info(f"Match complete. Matched: {df_matched.height}, Unmatched: {df_unmatched.height}")

            # --- Step 5: Load ---
            self.logger.info("Step 5: Loading data back to Cactus DB...")

            updated_count = 0
            inserted_count = 0

            if df_matched.height > 0:
                updated_count = self._perform_updates(df_matched)
            
            if df_unmatched.height > 0:
                inserted_count = self._perform_inserts(df_unmatched)

            # --- Step 6: Audit ---
            return {
                'updated': updated_count,
                'inserted': inserted_count,
                'status': 'SUCCESS'
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
        
        # 格式化列表为 SQL IN 子句字符串
        vehicles_str = ",".join([f"'{v}'" for v in vehicle_list])
        
        # nGen 时间格式: DD/MM/YYYY HH:MM:SS
        # 既然是 varchar，我们需要在 SQL 中转换以便比较，或者由业务逻辑保证拉取范围稍微大一点，
        # 并在 Python 端精确过滤。这里选择 SQL 端转换。
        
        sql = f"""
            SELECT 
                Tractor_No, 
                Tractor_Cycle_Id, 
                On_Chasis_Datetime, 
                Off_Chasis_Datetime, 
                Container_No
            FROM ngen
            WHERE Tractor_No IN ({vehicles_str})
            AND STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%M:%S') 
                BETWEEN '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}' 
                AND '{end_dt.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        
        self.logger.debug(f"Executing nGen SQL: {sql}")
        df_pd = hook.get_pandas_df(sql)
        return pl.from_pandas(df_pd)

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
                _time
            FROM cnt_cycles
            WHERE vehicleId IN ({vehicles_str})
            AND _time >= '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}'
            AND _time <= '{end_dt.strftime('%Y-%m-%d %H:%M:%S')}.999999'
        """
        
        self.logger.debug(f"Executing Cactus SQL: {sql}")
        df_pd = hook.get_pandas_df(sql)
        return pl.from_pandas(df_pd)

    def _perform_updates(self, df: pl.DataFrame) -> int:
        """
        更新匹配成功的记录：回填 cycleId (nGen ID) 和 matched=1
        """
        hook = MySqlHook(mysql_conn_id=self.cactus_conn_id)
        
        # 需要的字段: cycle_id (nGen), id (Cactus PK)
        # df 中: cycle_id 来自 nGen (聚合后), id 来自 Cactus
        
        # 转换为 Python List of Tuples for executemany
        # SQL: UPDATE cnt_cycles SET cycleId = %s, matched = 1 WHERE id = %s
        # Params: (cycle_id, id)
        
        # 确保 cycle_id 为 Int (Python int)
        update_data = df.select(["cycle_id", "id"]).to_dicts()
        
        # 转换 generator
        params = [(row['cycle_id'], row['id']) for row in update_data]
        
        sql = "UPDATE cnt_cycles SET cycleId = %s, matched = 1 WHERE id = %s"
        
        self.logger.info(f"Updating {len(params)} rows in Cactus DB...")
        
        # 分批执行，防止包过大 (按 1000 条)
        batch_size = 1000
        for i in range(0, len(params), batch_size):
            batch = params[i:i + batch_size]
            hook.run(sql, parameters=batch, executemany=True)
            
        return len(params)

    def _perform_inserts(self, df: pl.DataFrame) -> int:
        """
        补录未匹配的 nGen 记录：插入新行，matched=3
        """
        hook = MySqlHook(mysql_conn_id=self.cactus_conn_id)
        
        # 需要的字段: cycle_id (nGen), vehicle_id, real_start_time, real_end_time
        # 注意: DB 字段是 _time_begin, _time
        # matched = 3
        
        # 处理时间格式: Datetime -> String (ISO)
        # 假设 Cactus 接受 'YYYY-MM-DD HH:MM:SS' 格式写入 varchar 字段
        
        insert_data = df.select([
            pl.col("cycle_id"),
            pl.col("vehicle_id"),
            pl.col("real_start_time").dt.strftime("%Y-%m-%d %H:%M:%S"),
            pl.col("real_end_time").dt.strftime("%Y-%m-%d %H:%M:%S")
        ]).to_dicts()
        
        params = [
            (
                row['cycle_id'],
                row['vehicle_id'],
                row['real_start_time'],
                row['real_end_time'],
                3  # matched status
            )
            for row in insert_data
        ]
        
        sql = """
            INSERT INTO cnt_cycles 
            (cycleId, vehicleId, _time_begin, _time, matched) 
            VALUES (%s, %s, %s, %s, %s)
        """
        
        self.logger.info(f"Inserting {len(params)} rows into Cactus DB...")
        
        batch_size = 1000
        for i in range(0, len(params), batch_size):
            batch = params[i:i + batch_size]
            hook.run(sql, parameters=batch, executemany=True)
            
        return len(params)

