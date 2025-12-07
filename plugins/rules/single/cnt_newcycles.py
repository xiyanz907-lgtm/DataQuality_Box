import polars as pl
from airflow.providers.mysql.hooks.mysql import MySqlHook
from dq_lib.consistency import ConsistencyChecks

class CntNewCyclesLogicRule:
    
    @staticmethod
    def load_data(cactus_conn_id, ngen_conn_id, ngen_table_name, date_filter=None):
        """
        读取 cnt_newcycles 表，并进行数据清洗
        """
        hook_kpi = MySqlHook(mysql_conn_id=cactus_conn_id)
        
        sql = f"""
            SELECT id, vehicleId, _time, _time_end
            FROM kpi_data_db.cnt_newcycles
            {f"WHERE _time LIKE '{date_filter}%'" if date_filter else ""}
        """
        
        df_raw = pl.from_pandas(hook_kpi.get_pandas_df(sql))
        
        if df_raw.height == 0: return None, None

        # 数据清洗
        try:
            df_clean = df_raw.with_columns(
                pl.col("id").cast(pl.Int64),
                
                # varchar -> datetime
                # 截取前19位，忽略毫秒，转 UTC
                pl.col("_time")
                  .cast(pl.String) 
                  .str.slice(0, 19) 
                  .str.to_datetime(strict=False), 
                  
                pl.col("_time_end")
                  .cast(pl.String)
                  .str.slice(0, 19)
                  .str.to_datetime(strict=False)
            )
            
            # 过滤掉格式错误导致解析失败的行
            df_clean = df_clean.drop_nulls(subset=["_time", "_time_end"])
            
        except Exception as e:
            print(f"cnt_newcycles 数据清洗失败: {e}")
            return None, None
        
        return df_clean, None

    @staticmethod
    def run_checks(df_self, df_ngen=None):
        results = []
        
        if df_self is not None and df_self.height > 0:
            # 调用刚才写的通用检查
            res = ConsistencyChecks.check_chain_continuity(
                df=df_self,
                group_col="vehicleId",      # 按车分组
                sort_col="_time",           # 按开始时间排序
                current_time_col="_time",          # 本行开始
                prev_time_col="_time_end",         # 本行结束 (要去比上一行的结束)
                id_col="id"                 # 记录 ID (可选)
            )
            results.append(res)
            
        return results