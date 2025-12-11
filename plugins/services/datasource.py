from abc import ABC, abstractmethod
from airflow.providers.mysql.hooks.mysql import MySqlHook


class DataSource(ABC):
    """抽象数据源类"""

    @abstractmethod
    def get_pandas_df(self, sql: str):
        """执行 SQL 并返回一个 pandas Pandas DataFrame。"""


class MySQLDataSource(DataSource):
    """MySQL 实现，基于 Airflow MySqlHook。"""

    def __init__(self, conn_id: str):
        """
        Args:
            conn_id: Airflow MySQL 连接 ID。
        """
        self.conn_id = conn_id
        self.hook = MySqlHook(mysql_conn_id=conn_id)

    def get_pandas_df(self, sql: str):
        """
        Args:
            sql: 要执行的 SQL 字符串。

        Returns:
            pandas.DataFrame: 查询结果。
        """
        return self.hook.get_pandas_df(sql)


def get_datasource(kind: str, conn_id: str) -> DataSource:
    """工厂方法，获取具体的 DataSource 实例。

    Args:
        kind: 数据源类型，例如 'mysql'。
        conn_id: Airflow 连接 ID。

    Returns:
        DataSource: 具体的 DataSource 实例。

    Raises:
        ValueError: 当类型不支持时抛出。
    """
    if kind.lower() == "mysql":
        return MySQLDataSource(conn_id)
    raise ValueError(f"不支持的数据源类型: {kind}")

