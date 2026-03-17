"""
infra/extractors/mysql_extractor.py
MySQL 数据提取器

从当前 loader.py 中的 _extract_from_mysql 重构而来
策略: MySqlHook → Pandas → Polars
"""
import polars as pl
from airflow.providers.mysql.hooks.mysql import MySqlHook
from plugins.infra.extractors.base import BaseExtractor


class MySQLExtractor(BaseExtractor):

    def extract_single(self, rendered_query: str, task_config: dict) -> pl.DataFrame:
        """
        执行一条 SQL 查询

        Args:
            rendered_query: 渲染后的 SQL
            task_config: 任务配置（未使用，预留）

        Returns:
            Polars DataFrame
        """
        try:
            hook = MySqlHook(mysql_conn_id=self.conn_id)
            self.logger.info(f"🔌 MySQL [{self.conn_id}]: executing query...")

            pandas_df = hook.get_pandas_df(sql=rendered_query)
            self.logger.info(f"✅ Fetched {len(pandas_df)} rows from MySQL")

            if pandas_df.empty:
                return pl.from_pandas(pandas_df)

            polars_df = pl.from_pandas(pandas_df)
            self.logger.info(
                f"✅ Converted: {polars_df.height} rows × {polars_df.width} columns"
            )
            return polars_df

        except Exception as e:
            self.logger.error(f"❌ MySQL extraction failed: {e}")
            self.logger.error(f"SQL preview: {rendered_query[:500]}...")
            raise
