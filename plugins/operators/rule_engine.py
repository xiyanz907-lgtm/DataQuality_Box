"""
operators/rule_engine.py
通用规则执行器 V3

职责：
- 读取标准化实体数据
- 支持多种规则逻辑类型（filter / aggregate / cross_entity）
- 支持预计算步骤（pre_compute）
- 写入命中数据到 Result Parquet
- 添加 severity 常量列
- 区分业务错误和系统错误

V2 新增：
- logic.type: filter（默认） | aggregate
- logic.pre_compute: 预计算列（可选，在主逻辑前添加计算列）
- logic.group_by / agg_exprs / having_expr: 聚合型规则
- input.entity_key: 明确的实体读取键（可选，兜底用 target_entity）

V3 新增：
- logic.type: cross_entity（跨表校验）
- logic.operation: anti_join | compare
- input.entity_keys: {left: ..., right: ...}
- logic.join_on: {left: [...], right: [...]}
- logic.left_pre_agg / right_pre_agg: 跨表前的可选预聚合
- logic.assertion: compare 操作的断言表达式
"""
import time
import polars as pl
from polars import col, lit, when
from typing import Dict, Any, List, Optional

from plugins.infra.operators import BaseGovernanceOperator
from plugins.domian.context import GovernanceContext


class GenericRuleOperator(BaseGovernanceOperator):
    """
    通用规则执行器 V2
    
    配置示例 - filter 类型 (configs/rules/p0_time_check.yaml):
    ```yaml
    meta:
      rule_id: "rule_p0_time_inversion"
      severity: "P0"
    
    target_entity: "Cycle"
    
    input:
      entity_key: "entity_cycle"
    
    logic:
      type: filter
      pre_compute:
        - name: "efficiency"
          expr: "col('box') / col('duration')"
      filter_expr: "col('end_time') < col('start_time')"
    
    alert:
      enabled: true
      title: "[P0] 物理时间倒挂"
      trigger_id_field: "cycle_id"
      content_template: "发现 {{ hit_count }} 条时间倒挂数据"
    ```
    
    配置示例 - aggregate 类型:
    ```yaml
    meta:
      rule_id: "rule_p0_daily_box_zero"
      severity: "P0"
    
    target_entity: "Cycle"
    
    input:
      entity_key: "entity_cycle"
    
    logic:
      type: aggregate
      group_by: ["vehicle_id", "shift_date"]
      agg_exprs:
        - name: "total_box"
          expr: "col('box').sum()"
      having_expr: "col('total_box') == 0"
    
    alert:
      enabled: true
      title: "[P0] 车辆整日零箱"
      trigger_id_field: "vehicle_id"
    ```
    """
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        规则执行逻辑 V2
        
        流程：
        1. 读取实体数据（支持 input.entity_key）
        2. 应用预计算步骤（pre_compute）
        3. 根据 logic.type 分发执行
        4. 添加 severity 列
        5. 写入结果（如有命中）
        6. 注册规则输出
        
        异常处理：
        - 业务逻辑错误（如除零）：捕获，生成 ErrorResult，不阻断流程
        - 系统级错误（如 MinIO 连不上）：抛出，触发 Airflow Retry
        """
        # 验证配置
        self._validate_config(['meta', 'target_entity', 'logic'])
        
        rule_id = self._config['meta']['rule_id']
        severity = self._config['meta']['severity']
        alert_title = self._config.get('alert', {}).get('title', '')
        logic_type = self._config['logic'].get('type', 'filter')
        
        self.log.info(f"🔍 Executing rule: {rule_id} ({severity}, type={logic_type})")
        
        # 开始计时
        start_time = time.time()
        
        try:
            # 根据逻辑类型分发执行
            if logic_type == 'cross_entity':
                # 跨表校验：不走单表加载流程
                hit_df = self._execute_cross_entity(ctx)
            else:
                # 单表规则：加载 → 预计算 → 过滤/聚合
                # 1. 读取实体数据
                df = self._load_entity(ctx)
                self.log.info(f"Loaded entity: {df.height} rows, {df.width} columns")
                
                # 2. 应用预计算步骤（可选）
                df = self._apply_pre_compute(df)
                
                # 3. 执行规则
                if logic_type == 'filter':
                    hit_df = self._execute_filter(df)
                elif logic_type == 'aggregate':
                    hit_df = self._execute_aggregate(df)
                else:
                    raise ValueError(f"Unknown logic type: {logic_type}")
            
            self.log.info(f"Rule result: {hit_df.height} hits")
            
            # 4. 如果有命中，写入结果
            if hit_df.height > 0:
                # 4.1 添加 severity 常量列
                hit_df = hit_df.with_columns(
                    pl.lit(severity).alias('_severity')
                )
                
                # 4.2 清理旧结果
                result_key = f"{rule_id}_hits"
                self._clean_partition(ctx, stage="RESULT", key=result_key)
                
                # 4.3 写入结果 Parquet
                ctx.put_dataframe(
                    key=result_key,
                    df=hit_df,
                    stage="RESULT"
                )
                
                # 4.4 注册规则输出（成功）
                execution_time = time.time() - start_time
                ctx.register_rule_output(
                    rule_id=rule_id,
                    status="SUCCESS",
                    output_uri=ctx.get_data_uri(result_key),
                    hit_count=hit_df.height,
                    execution_time=execution_time,
                    alert_title=alert_title
                )
                
                self.log.info(f"✅ Rule [{rule_id}] completed: {hit_df.height} hits in {execution_time:.2f}s")
            else:
                # 无命中
                execution_time = time.time() - start_time
                ctx.register_rule_output(
                    rule_id=rule_id,
                    status="SUCCESS",
                    hit_count=0,
                    execution_time=execution_time,
                    alert_title=alert_title
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
                    execution_time=execution_time,
                    alert_title=alert_title
                )
                
                # Task 仍标记为 Success（不阻断流程）
                self.log.info(f"⚠️ Rule [{rule_id}] marked as FAILED but task continues")
    
    # ========================================================================
    # 实体加载
    # ========================================================================
    
    def _load_entity(self, ctx: GovernanceContext) -> pl.DataFrame:
        """
        加载实体数据
        
        优先级：
        1. input.entity_key（V2 明确指定）
        2. target_entity（V1 兼容，通过 alt_key 查找）
        
        Args:
            ctx: 治理上下文
        
        Returns:
            Polars DataFrame
        """
        # V2：优先读 input.entity_key
        entity_key = self._config.get('input', {}).get('entity_key')
        
        if entity_key:
            self.log.info(f"Loading entity via input.entity_key: {entity_key}")
            return ctx.get_dataframe(key=entity_key, use_alt_key=True)
        
        # V1 兼容：使用 target_entity 作为 alt_key
        target_entity = self._config['target_entity']
        self.log.info(f"Loading entity via target_entity (alt_key): {target_entity}")
        return ctx.get_dataframe(key=target_entity, use_alt_key=True)
    
    # ========================================================================
    # 预计算步骤
    # ========================================================================
    
    def _apply_pre_compute(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        应用预计算步骤（在主逻辑前添加计算列）
        
        配置示例:
        ```yaml
        logic:
          pre_compute:
            - name: "efficiency"
              expr: "col('box') / col('duration')"
            - name: "power_z_score"
              expr: "(col('power_consumption') - col('power_consumption').mean()) / col('power_consumption').std()"
        ```
        
        Args:
            df: 输入 DataFrame
        
        Returns:
            添加计算列后的 DataFrame
        """
        pre_computes = self._config['logic'].get('pre_compute', [])
        
        if not pre_computes:
            return df
        
        self.log.info(f"Applying {len(pre_computes)} pre_compute steps...")
        
        for step in pre_computes:
            name = step['name']
            expr_str = step['expr']
            
            try:
                expr = self._parse_expression(expr_str).alias(name)
                df = df.with_columns(expr)
                self.log.info(f"  ✅ Pre-computed: {name}")
            except Exception as e:
                self.log.error(f"  ❌ Failed to pre-compute '{name}': {e}")
                raise ValueError(f"Pre-compute '{name}' failed: {e}")
        
        return df
    
    # ========================================================================
    # 逻辑类型：Filter
    # ========================================================================
    
    def _execute_filter(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        执行过滤型规则
        
        逻辑：df.filter(filter_expr) → 命中的行
        
        Args:
            df: 输入 DataFrame（可能已经过 pre_compute）
        
        Returns:
            命中的 DataFrame
        """
        filter_expr_str = self._config['logic']['filter_expr']
        
        self.log.info(f"Filter expression: {filter_expr_str[:200]}")
        
        predicate = self._parse_expression(filter_expr_str)
        hit_df = df.filter(predicate)
        
        return hit_df
    
    # ========================================================================
    # 逻辑类型：Aggregate
    # ========================================================================
    
    def _execute_aggregate(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        执行聚合型规则
        
        逻辑：df.group_by(cols).agg(exprs).filter(having) → 命中的分组
        
        配置示例:
        ```yaml
        logic:
          type: aggregate
          group_by: ["vehicle_id", "shift_date"]
          agg_exprs:
            - name: "total_box"
              expr: "col('box').sum()"
            - name: "cycle_count"
              expr: "col('cycle_id').count()"
          having_expr: "col('total_box') == 0"
        ```
        
        Args:
            df: 输入 DataFrame
        
        Returns:
            命中的聚合结果 DataFrame（列 = group_by cols + agg cols）
        """
        logic = self._config['logic']
        group_cols = logic['group_by']
        
        self.log.info(f"Aggregate: group_by={group_cols}")
        
        # 1. 构建聚合表达式
        agg_exprs = []
        for agg in logic['agg_exprs']:
            expr = self._parse_expression(agg['expr']).alias(agg['name'])
            agg_exprs.append(expr)
            self.log.info(f"  Agg: {agg['name']} = {agg['expr'][:100]}")
        
        # 2. 分组聚合
        agg_df = df.group_by(group_cols).agg(agg_exprs)
        self.log.info(f"  After group_by: {agg_df.height} groups")
        
        # 3. HAVING 过滤
        having_expr_str = logic['having_expr']
        having_predicate = self._parse_expression(having_expr_str)
        hit_df = agg_df.filter(having_predicate)
        
        self.log.info(f"  After having: {hit_df.height} groups match")
        
        return hit_df
    
    # ========================================================================
    # 逻辑类型：Cross-Entity（跨表校验）
    # ========================================================================
    
    def _execute_cross_entity(self, ctx: GovernanceContext) -> pl.DataFrame:
        """
        执行跨表校验规则
        
        支持操作：
        - anti_join: 左表中在右表找不到匹配的行（如：cycle 表有但 mileage 表没有的车辆）
        - compare:  左右表 join 后根据断言表达式过滤出不一致的行
        
        可选预聚合：
        - left_pre_agg / right_pre_agg: 在 join 前对单侧进行 group_by + agg
          适用于两表粒度不同的场景（如 cycle 按条记录 vs mileage 按天汇总）
        
        配置示例（compare + 左侧预聚合）：
        ```yaml
        input:
          entity_keys:
            left: entity_cycle
            right: entity_vehicle_mileage
        
        logic:
          type: cross_entity
          operation: compare
          left_pre_agg:
            group_by: [vehicle_id, shift_date]
            agg_exprs:
              - name: total_mileage
                expr: "col('mileage').sum()"
          join_on:
            left: [vehicle_id, shift_date]
            right: [vehicle_id, shift_date]
          assertion: "col('total_mileage') != col('mileage_distance')"
        ```
        
        Args:
            ctx: 治理上下文
        
        Returns:
            命中的 DataFrame
        """
        logic = self._config['logic']
        operation = logic['operation']
        entity_keys = self._config['input']['entity_keys']
        
        # 1. 加载左右表
        left_key = entity_keys['left']
        right_key = entity_keys['right']
        
        left_df = ctx.get_dataframe(key=left_key, use_alt_key=True)
        right_df = ctx.get_dataframe(key=right_key, use_alt_key=True)
        
        self.log.info(
            f"Cross-entity [{operation}]: "
            f"left={left_key} ({left_df.height} rows), "
            f"right={right_key} ({right_df.height} rows)"
        )
        
        # 2. 可选预聚合
        if 'left_pre_agg' in logic:
            left_df = self._pre_aggregate(left_df, logic['left_pre_agg'], 'left')
        if 'right_pre_agg' in logic:
            right_df = self._pre_aggregate(right_df, logic['right_pre_agg'], 'right')
        
        # 3. 获取 join 键
        join_on = logic['join_on']
        left_on = join_on['left']
        right_on = join_on['right']
        
        # 4. 根据操作类型执行
        if operation == 'anti_join':
            hit_df = left_df.join(
                right_df,
                left_on=left_on,
                right_on=right_on,
                how='anti'
            )
            self.log.info(f"  Anti-join result: {hit_df.height} unmatched rows in left")
        
        elif operation == 'compare':
            suffix = logic.get('suffix', '_right')
            joined_df = left_df.join(
                right_df,
                left_on=left_on,
                right_on=right_on,
                how='left',
                suffix=suffix
            )
            self.log.info(f"  Joined: {joined_df.height} rows, {joined_df.width} columns")
            
            assertion_str = logic['assertion']
            self.log.info(f"  Assertion: {assertion_str[:200]}")
            
            assertion_expr = self._parse_expression(assertion_str)
            hit_df = joined_df.filter(assertion_expr)
            
            self.log.info(f"  Compare result: {hit_df.height} mismatched rows")
        
        else:
            raise ValueError(f"Unknown cross_entity operation: {operation}")
        
        return hit_df
    
    def _pre_aggregate(self, df: pl.DataFrame, agg_config: dict, side: str) -> pl.DataFrame:
        """
        跨表操作前的预聚合
        
        Args:
            df: 输入 DataFrame
            agg_config: {'group_by': [...], 'agg_exprs': [{'name': ..., 'expr': ...}]}
            side: 'left' 或 'right'（用于日志）
        
        Returns:
            聚合后的 DataFrame
        """
        group_cols = agg_config['group_by']
        
        agg_exprs = []
        for agg in agg_config['agg_exprs']:
            expr = self._parse_expression(agg['expr']).alias(agg['name'])
            agg_exprs.append(expr)
        
        result = df.group_by(group_cols).agg(agg_exprs)
        
        self.log.info(
            f"  Pre-agg ({side}): group_by={group_cols} → "
            f"{result.height} groups (from {df.height} rows)"
        )
        
        return result
    
    # ========================================================================
    # 表达式解析
    # ========================================================================
    
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
            raise ValueError(f"Invalid expression: {expr_str}. Error: {e}")
    
    # ========================================================================
    # 错误分类
    # ========================================================================
    
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
        )
        
        return isinstance(error, system_error_types)
