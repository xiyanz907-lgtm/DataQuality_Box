"""
infra/extractors/base.py
提取器基类 + 上游数据引用助手

BaseExtractor: 所有数据源提取器的抽象基类
UpstreamRef:   batch 模式下的模板变量助手，封装上游 DataFrame
"""
from abc import ABC, abstractmethod
from typing import Optional, List
import polars as pl


class UpstreamRef:
    """
    上游 DataFrame 引用（用于 batch 模式模板渲染）

    在 Jinja 模板中以 `ref` 变量暴露:
        {{ ref.values('vehicle_id') }}       → 'V001','V002','V003'
        {{ ref.values('id', quote=False) }}  → 1,2,3
        {{ ref.count }}                      → 42
        {{ ref.min('shift_date') }}          → '2026-01-01'
        {{ ref.max('shift_date') }}          → '2026-01-31'
        {{ ref.distinct('vehicle_id') }}     → ['V001','V002','V003']
    """

    def __init__(self, df: pl.DataFrame):
        self._df = df

    @property
    def count(self) -> int:
        return self._df.height

    @property
    def empty(self) -> bool:
        return self._df.height == 0

    def values(self, column: str, quote: bool = True, distinct: bool = True) -> str:
        """逗号分隔值（适合 SQL IN 子句）"""
        series = self._df[column]
        vals = series.unique().to_list() if distinct else series.to_list()
        if quote:
            return ','.join(f"'{v}'" for v in vals)
        return ','.join(str(v) for v in vals)

    def distinct(self, column: str) -> list:
        """去重值列表"""
        return self._df[column].unique().to_list()

    def min(self, column: str) -> str:
        return str(self._df[column].min())

    def max(self, column: str) -> str:
        return str(self._df[column].max())

    def column(self, column: str) -> list:
        """整列值"""
        return self._df[column].to_list()


class BaseExtractor(ABC):
    """
    数据源提取器基类

    子类只需实现 extract_single()——执行一条已渲染的查询。
    batch / per_row 迭代逻辑由 Loader 统一编排。
    """

    def __init__(self, conn_id: str, logger):
        self.conn_id = conn_id
        self.logger = logger

    @abstractmethod
    def extract_single(self, rendered_query: str, task_config: dict) -> pl.DataFrame:
        """
        执行单条已渲染的查询

        Args:
            rendered_query: 渲染后的查询字符串
            task_config: 完整的 extraction 任务配置

        Returns:
            查询结果 DataFrame
        """
        ...
