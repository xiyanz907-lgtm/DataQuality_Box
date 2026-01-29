"""
operators/adapter.py
领域适配器

职责：
- 将 Raw Parquet 转换为标准化的 Entity Parquet
- 执行链式 Join
- 应用字段映射和计算
- 支持低代码表达式（Polars Expression）
"""
import polars as pl
from polars import col, lit, when
from typing import Dict, Any

from plugins.infra.operators import BaseGovernanceOperator
from plugins.domian.context import GovernanceContext


class DomainAdapterOperator(BaseGovernanceOperator):
    """
    领域适配器
    
    配置示例 (configs/adapters/cycle_adapter.yaml):
    ```yaml
    target_entity: "Cycle"
    output_key: "entity_cycle"
    
    input_schema:
      primary_source: "raw_summary"
      joins:
        - join_source: "raw_subtarget"
          type: "left"
          left_on: "cycle_id"
          right_on: "TRACTOR_CYCLE_ID"
          suffix: "_sub"
    
    fields:
      - target: "cycle_id"
        source_expr: "col('cycle_id')"
      
      - target: "vehicle_id"
        source_expr: "col('vehicle_id')"
      
      - target: "start_time"
        source_expr: "col('cycle_start_time').cast(pl.Datetime)"
      
      - target: "end_time"
        source_expr: "col('cycle_end_time').cast(pl.Datetime)"
      
      - target: "duration"
        source_expr: "(col('cycle_end_time') - col('cycle_start_time')).dt.total_seconds()"
      
      - target: "is_twin_lift"
        source_expr: "col('TWIN_LIFT_INDICATOR') == 1"
    ```
    
    使用示例:
    ```python
    adapter = DomainAdapterOperator(
        task_id='adapt_cycle_entity',
        config_path='configs/adapters/cycle_adapter.yaml',
        upstream_task_id='load_raw_data',
        dag=dag
    )
    ```
    """
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        适配逻辑
        
        流程：
        1. 加载主表（primary_source）
        2. 执行链式 Join
        3. 应用字段映射和计算
        4. 写入标准化实体
        """
        # 验证配置
        self._validate_config(['target_entity', 'output_key', 'input_schema', 'fields'])
        
        target_entity = self._config['target_entity']
        output_key = self._config['output_key']
        
        self.log.info(f"🔄 Adapting to entity: {target_entity}")
        
        # 1. 加载主表
        df = self._load_primary_source(ctx)
        self.log.info(f"Loaded primary source: {df.height} rows, {len(df.columns)} columns")
        
        # 2. 执行链式 Join
        df = self._apply_joins(df, ctx)
        self.log.info(f"After joins: {df.height} rows, {len(df.columns)} columns")
        
        # 3. 应用字段映射
        df_entity = self._apply_field_mapping(df)
        self.log.info(f"After mapping: {df_entity.height} rows, {len(df_entity.columns)} columns")
        
        # 4. 清理旧数据
        self._clean_partition(ctx, stage="ENTITY", key=output_key)
        
        # 5. 写入 Context
        ctx.put_dataframe(
            key=output_key,
            df=df_entity,
            stage="ENTITY",
            alt_key=self._config.get('alt_key', target_entity)
        )
        
        self.log.info(f"✅ Entity [{target_entity}] adapted successfully")
    
    def _load_primary_source(self, ctx: GovernanceContext) -> pl.DataFrame:
        """
        加载主表
        
        Args:
            ctx: 治理上下文
        
        Returns:
            主表 DataFrame
        """
        primary_key = self._config['input_schema']['primary_source']
        
        # 使用 alt_key 查找（支持别名）
        df = ctx.get_dataframe(key=primary_key, use_alt_key=True)
        
        return df
    
    def _apply_joins(self, df: pl.DataFrame, ctx: GovernanceContext) -> pl.DataFrame:
        """
        应用链式 Join
        
        Args:
            df: 主表 DataFrame
            ctx: 治理上下文
        
        Returns:
            Join 后的 DataFrame
        """
        joins = self._config['input_schema'].get('joins', [])
        
        if not joins:
            self.log.info("No joins configured, skipping...")
            return df
        
        for idx, join_config in enumerate(joins):
            join_source = join_config['join_source']
            join_type = join_config['type']
            left_on = join_config['left_on']
            right_on = join_config['right_on']
            suffix = join_config.get('suffix', '_right')
            
            self.log.info(
                f"Join {idx + 1}/{len(joins)}: "
                f"{join_source} ({join_type}) on {left_on}={right_on}"
            )
            
            # 加载右表
            df_right = ctx.get_dataframe(key=join_source, use_alt_key=True)
            
            # 执行 Join
            df = df.join(
                df_right,
                left_on=left_on,
                right_on=right_on,
                how=join_type,
                suffix=suffix
            )
        
        return df
    
    def _apply_field_mapping(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        应用字段映射和计算
        
        Args:
            df: 输入 DataFrame
        
        Returns:
            映射后的 DataFrame（只包含目标字段）
        """
        fields = self._config['fields']
        
        # 构建选择表达式
        select_exprs = []
        
        for field_map in fields:
            target = field_map['target']
            source_expr_str = field_map['source_expr']
            
            try:
                # 解析表达式
                expr = self._parse_expression(source_expr_str).alias(target)
                select_exprs.append(expr)
                
            except Exception as e:
                self.log.error(f"Failed to parse expression for field '{target}': {e}")
                self.log.error(f"Expression: {source_expr_str}")
                raise
        
        # 执行投影（只保留目标字段）
        df_mapped = df.select(select_exprs)
        
        return df_mapped
    
    def _parse_expression(self, expr_str: str):
        """
        安全解析 Polars 表达式
        
        Args:
            expr_str: 表达式字符串（如 "col('name').cast(pl.Utf8)"）
        
        Returns:
            Polars Expression 对象
        
        安全性：
        - 限制 eval 作用域，只允许 Polars 函数
        - 禁止访问 __builtins__
        """
        # 定义安全上下文（只允许 Polars 相关函数）
        safe_context = {
            'col': col,
            'lit': lit,
            'when': when,
            'pl': pl,
            '__builtins__': {}  # 禁止内置函数
        }
        
        # 解析表达式
        try:
            expr = eval(expr_str, safe_context)
            return expr
        except Exception as e:
            raise ValueError(f"Invalid expression: {expr_str}. Error: {e}")
