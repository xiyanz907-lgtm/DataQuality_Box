"""
infra/extractors
数据源提取器注册中心

根据 source_type 返回对应的 Extractor 实例
"""
from typing import Dict, Type
from plugins.infra.extractors.base import BaseExtractor
from plugins.infra.extractors.mysql_extractor import MySQLExtractor
from plugins.infra.extractors.influxdb_extractor import InfluxDBExtractor


_REGISTRY: Dict[str, Type[BaseExtractor]] = {
    'mysql': MySQLExtractor,
    'postgresql': MySQLExtractor,  # 暂时复用 MySQL（Hook 不同，后续可拆分）
    'influxdb': InfluxDBExtractor,
}


def get_extractor(source_type: str, conn_id: str, logger) -> BaseExtractor:
    """
    工厂方法：根据 source_type 创建对应的 Extractor

    Args:
        source_type: 数据源类型
        conn_id: Airflow Connection ID
        logger: 日志对象

    Returns:
        BaseExtractor 实例
    """
    cls = _REGISTRY.get(source_type)
    if cls is None:
        raise ValueError(
            f"Unsupported source_type: '{source_type}'. "
            f"Available: {list(_REGISTRY.keys())}"
        )
    return cls(conn_id=conn_id, logger=logger)
