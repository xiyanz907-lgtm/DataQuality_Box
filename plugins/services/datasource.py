from abc import ABC, abstractmethod
from airflow.providers.mysql.hooks.mysql import MySqlHook


class DataSource(ABC):
    """Abstract base for data sources."""

    @abstractmethod
    def get_pandas_df(self, sql: str):
        """Execute SQL and return a pandas DataFrame."""


class MySQLDataSource(DataSource):
    """MySQL implementation backed by Airflow MySqlHook."""

    def __init__(self, conn_id: str):
        """
        Args:
            conn_id: Airflow connection id for MySQL.
        """
        self.conn_id = conn_id
        self.hook = MySqlHook(mysql_conn_id=conn_id)

    def get_pandas_df(self, sql: str):
        """
        Args:
            sql: SQL string to execute.

        Returns:
            pandas.DataFrame: query result.
        """
        return self.hook.get_pandas_df(sql)


def get_datasource(kind: str, conn_id: str) -> DataSource:
    """Factory to obtain a concrete DataSource.

    Args:
        kind: datasource type, e.g., 'mysql'.
        conn_id: Airflow connection id.

    Returns:
        DataSource: concrete datasource instance.

    Raises:
        ValueError: when type is unsupported.
    """
    if kind.lower() == "mysql":
        return MySQLDataSource(conn_id)
    raise ValueError(f"Unsupported datasource type: {kind}")

