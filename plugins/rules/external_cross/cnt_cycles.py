import polars as pl
from airflow.providers.mysql.hooks.mysql import MySqlHook
from dq_lib.consistency import ConsistencyChecks


class CntCyclesLogicRule:

    @staticmethod
    def _get_timezone(table_name):
        if "AQCT" in table_name.upper() or "NGEN" in table_name.upper():
            return "Africa/Cairo"
        elif "ICAVE" in table_name.upper():
            return "America/Mexico_City"
        return "UTC"

    @staticmethod
    def load_data(cactus_conn_id, ngen_conn_id, ngen_table_name, date_filter=None):
        """
        数据读取+清洗+聚合
        """
        # 读取Cactus(kpi_data)
        hook_kpi = MySqlHook(mysql_conn_id=cactus_conn_id)
        sql_kpi = f"""
            SELECT cycleId, _time_begin, _time_end 
            FROM kpi_data_db.cnt_cycles
            {f"WHERE DATE(_time_begin) = '{date_filter}'" if date_filter else ""}
        """
        df_kpi = pl.from_pandas(hook_kpi.get_pandas_df(sql_kpi)).with_columns(
            pl.col("cycleId").cast(pl.Int64),
            # pl.col("_time_begin").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False).dt.replace_time_zone("UTC"),
            pl.col("_time_end").cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False).dt.replace_time_zone("UTC"),
        )

        if df_kpi.height == 0:
            return None, None
        
        # 读取nGen
        hook_ngen = MySqlHook(mysql_conn_id=ngen_conn_id)
        target_ids = df_kpi["cycleId"].unique().to_list()
        ids_str = ",".join([f"'{str(id)}'" for id in target_ids])
        sql_ngen = f"""
            SELECT Tractor_Cycle_Id, On_Chasis_Datetime, Off_Chasis_Datetime 
            FROM hutchisonports.{ngen_table_name}
            WHERE Tractor_Cycle_Id IN ({ids_str})
        """
        df_ngen_raw = pl.from_pandas(hook_ngen.get_pandas_df(sql_ngen))

        if df_ngen_raw.height == 0:
            return df_kpi, None
        
        # 清洗nGen数据
        local_tz = CntCyclesLogicRule._get_timezone(ngen_table_name)
        try:
            df_ngen_clean = df_ngen_raw.with_columns(
                # pl.col("Tractor_Cycle_Id").cast(pl.Int64),
                # pl.col("On_Chasis_Datetime").str.to_datetime(format="%d/%m/%Y %H:%M:%S", strict=False)
                # .dt.replace_time_zone(local_tz,ambiguous="earliest").dt.convert_time_zone("UTC"),
                pl.col("Tractor_Cycle_Id").cast(pl.Int64),
                pl.col("Off_Chasis_Datetime").str.to_datetime(format="%d/%m/%Y %H:%M:%S", strict=False)
                .dt.replace_time_zone(local_tz,ambiguous="earliest").dt.convert_time_zone("UTC"),
            )
            df_ngen_agg = df_ngen_clean.group_by("Tractor_Cycle_Id").agg([
                # pl.col("On_Chasis_Datetime").min().alias("On_Chasis_Datetime"),
                pl.col("Off_Chasis_Datetime").max().alias("Off_Chasis_Datetime")
            ])

        except Exception as e:
            print(f"nGen数据清洗出错: {e}")
            return df_kpi, None
        return df_kpi, df_ngen_agg
    
    @staticmethod
    def run_checks(df_self, df_ngen):
        """"
        执行校验
        """

        results = []
        if df_ngen is not None:
            result_time_diff = ConsistencyChecks.check_time_consistency(
                df_left=df_self,
                df_right=df_ngen,
                left_key="cycleId",
                right_key="Tractor_Cycle_Id",
                left_col="_time_end",
                right_col="Off_Chasis_Datetime",
                threshold=300,
            )
            results.append(result_time_diff)

        return results
