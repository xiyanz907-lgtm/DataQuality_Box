"""
infra/extractors/influxdb_extractor.py
InfluxDB 2.x 数据提取器

使用 influxdb-client 官方 SDK 执行 Flux 查询
Airflow Connection 配置约定:
    - host:     InfluxDB URL (如 http://influxdb:8086)
    - login:    Organization
    - password: API Token
    - schema:   默认 Bucket（可被 extraction YAML 中的 bucket 覆盖）
"""
import polars as pl
from plugins.infra.extractors.base import BaseExtractor


class InfluxDBExtractor(BaseExtractor):

    def extract_single(self, rendered_query: str, task_config: dict) -> pl.DataFrame:
        """
        执行一条 Flux 查询

        Args:
            rendered_query: 渲染后的 Flux 查询
            task_config: 任务配置（可包含 bucket 覆盖）

        Returns:
            Polars DataFrame
        """
        try:
            from influxdb_client import InfluxDBClient
        except ImportError:
            raise ImportError(
                "influxdb-client is required for InfluxDB extraction. "
                "Install with: pip install influxdb-client"
            )

        url, token, org = self._get_connection_params()

        self.logger.info(f"🔌 InfluxDB [{self.conn_id}]: executing Flux query...")
        self.logger.info(f"Flux preview: {rendered_query[:300]}...")

        try:
            client = InfluxDBClient(url=url, token=token, org=org)
            query_api = client.query_api()

            tables = query_api.query(rendered_query, org=org)

            records = []
            for table in tables:
                for record in table.records:
                    records.append(record.values)

            client.close()

            if not records:
                self.logger.warning("⚠️ Flux query returned 0 records")
                return pl.DataFrame()

            df = pl.DataFrame(records)

            # 丢弃 InfluxDB 内部元数据列（可选）
            drop_cols = [c for c in ['result', 'table', '_start', '_stop'] if c in df.columns]
            if drop_cols:
                df = df.drop(drop_cols)

            self.logger.info(f"✅ InfluxDB: {df.height} rows × {df.width} columns")
            return df

        except Exception as e:
            self.logger.error(f"❌ InfluxDB extraction failed: {e}")
            raise

    def _get_connection_params(self):
        """从 Airflow Connection 获取 InfluxDB 连接参数"""
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(self.conn_id)

        url = conn.host
        if conn.port:
            url = f"{conn.host}:{conn.port}"
        if not url.startswith('http'):
            url = f"http://{url}"

        token = conn.password
        org = conn.login

        if not token:
            raise ValueError(
                f"InfluxDB connection '{self.conn_id}' missing token (password field)"
            )
        if not org:
            raise ValueError(
                f"InfluxDB connection '{self.conn_id}' missing org (login field)"
            )

        return url, token, org
