"""
operators/rule_engine.py
通用规则执行器

职责：
- 读取标准化实体数据
- 执行规则过滤逻辑
- 写入命中数据到 Result Parquet
- 添加 severity 常量列
- 区分业务错误和系统错误
"""
import time
import polars as pl
from polars import col, lit, when
from typing import Dict, Any

from plugins.infra.operators import BaseGovernanceOperator
from plugins.domian.context import GovernanceContext


class GenericRuleOperator(BaseGovernanceOperator):
    """
    通用规则执行器
    
    配置示例 (configs/rules/p0_time_check.yaml):
    ```yaml
    meta:
      rule_id: "rule_p0_time_inversion"
      severity: "P0"
      description: "End time is earlier than Start time"
    
    target_entity: "entity_cycle"
    
    logic:
      filter_expr: "col('end_time') < col('start_time')"
    
    output:
      action: "ALERT"
      title: "[P0] 物理时间倒挂"
      content: "Cycle {{ cycle_id }} 时间倒挂"
    ```
    
    使用示例:
    ```python
    rule = GenericRuleOperator(
        task_id='rule_p0_time_check',
        config_path='configs/rules/p0_time_check.yaml',
        upstream_task_id='adapt_cycle_entity',
        dag=dag
    )
    ```
    """
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        规则执行逻辑
        
        流程：
        1. 读取实体数据
        2. 执行过滤逻辑
        3. 添加 severity 列
        4. 写入结果（如有命中）
        5. 注册规则输出（成功或失败）
        
        异常处理：
        - 业务逻辑错误（如除零）：捕获，生成 ErrorResult，不阻断流程
        - 系统级错误（如 MinIO 连不上）：抛出，触发 Airflow Retry
        """
        # 验证配置
        self._validate_config(['meta', 'target_entity', 'logic'])
        
        rule_id = self._config['meta']['rule_id']
        severity = self._config['meta']['severity']
        
        self.log.info(f"🔍 Executing rule: {rule_id} ({severity})")
        
        # 开始计时
        start_time = time.time()
        
        try:
            # 1. 读取实体数据
            entity_key = self._config['target_entity']
            df = ctx.get_dataframe(key=entity_key, use_alt_key=True)
            
            self.log.info(f"Loaded entity: {df.height} rows")
            
            # 2. 执行过滤逻辑
            filter_expr_str = self._config['logic']['filter_expr']
            predicate = self._parse_expression(filter_expr_str)
            
            hit_df = df.filter(predicate)
            
            self.log.info(f"Rule result: {hit_df.height} hits")
            
            # 3. 如果有命中，写入结果
            if hit_df.height > 0:
                # 3.1 添加 severity 常量列（重要！供 Aggregator 使用）
                hit_df = hit_df.with_columns(
                    pl.lit(severity).alias('_severity')
                )
                
                # 3.2 清理旧结果
                result_key = f"{rule_id}_hits"
                self._clean_partition(ctx, stage="RESULT", key=result_key)
                
                # 3.3 写入结果 Parquet
                ctx.put_dataframe(
                    key=result_key,
                    df=hit_df,
                    stage="RESULT"
                )
                
                # 3.4 注册规则输出（成功）
                execution_time = time.time() - start_time
                ctx.register_rule_output(
                    rule_id=rule_id,
                    status="SUCCESS",
                    output_uri=ctx.get_data_uri(result_key),
                    hit_count=hit_df.height,
                    execution_time=execution_time
                )
                
                self.log.info(f"✅ Rule [{rule_id}] completed: {hit_df.height} hits in {execution_time:.2f}s")
            else:
                # 无命中
                execution_time = time.time() - start_time
                ctx.register_rule_output(
                    rule_id=rule_id,
                    status="SUCCESS",
                    hit_count=0,
                    execution_time=execution_time
                )
                
                self.log.info(f"✅ Rule [{rule_id}] passed: 0 hits in {execution_time:.2f}s")
        
        except Exception as e:
            # 异常处理：区分业务错误和系统错误
            execution_time = time.time() - start_time
            
            if self._is_system_error(e):
                # 系统级错误：抛出，触发 Airflow Retry
                self.log.error(f"🔴 System error in rule [{rule_id}]: {e}")
                raise
            else:
                # 业务逻辑错误：捕获，生成 ErrorResult
                self.log.error(f"⚠️ Logic error in rule [{rule_id}]: {e}")
                
                # 注册规则输出（失败）
                ctx.register_rule_output(
                    rule_id=rule_id,
                    status="FAILED",
                    error_message=str(e),
                    hit_count=0,
                    execution_time=execution_time
                )
                
                # Task 仍标记为 Success（不阻断流程）
                self.log.info(f"⚠️ Rule [{rule_id}] marked as FAILED but task continues")
    
    def _parse_expression(self, expr_str: str):
        """
        安全解析 Polars 表达式
        
        Args:
            expr_str: 表达式字符串
        
        Returns:
            Polars Expression 对象
        """
        # 定义安全上下文
        safe_context = {
            'col': col,
            'lit': lit,
            'when': when,
            'pl': pl,
            '__builtins__': {}
        }
        
        try:
            expr = eval(expr_str, safe_context)
            return expr
        except Exception as e:
            raise ValueError(f"Invalid filter expression: {expr_str}. Error: {e}")
    
    def _is_system_error(self, error: Exception) -> bool:
        """
        判断是否为系统级错误
        
        系统级错误：
        - 网络错误（ConnectionError, TimeoutError）
        - IO 错误（IOError, OSError）
        - 数据库连接错误
        - MinIO 连接错误
        
        业务逻辑错误：
        - 除零错误（ZeroDivisionError）
        - 类型错误（TypeError）
        - 值错误（ValueError）
        - 键错误（KeyError）
        
        Args:
            error: 异常对象
        
        Returns:
            是否为系统级错误
        """
        # 定义系统级错误类型
        system_error_types = (
            ConnectionError,
            IOError,
            OSError,
            TimeoutError,
            # 可以根据实际情况添加更多类型
        )
        
        return isinstance(error, system_error_types)
