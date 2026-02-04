"""
operators/aggregator.py
上下文聚合器（Reduce 阶段）

职责：
- 合并多个 Rule Task 的 Context
- 读取所有 Rule Result Parquet
- 处理 P0/P1/P2 逻辑和白名单豁免
- 生成最终的 Alerts 和 Assets
"""
import polars as pl
from typing import Dict, Any, List

from plugins.infra.operators import BaseGovernanceOperator, get_multiple_upstream_contexts
from plugins.domian.context import GovernanceContext


class ContextAggregatorOperator(BaseGovernanceOperator):
    """
    上下文聚合器
    
    职责：
    1. 合并多个 Rule Task 的 Context（rule_outputs）
    2. 读取所有命中的 Result Parquet
    3. 根据 _severity 列处理不同等级的规则
    4. P0 → 直接生成 Alert
    5. P1 → 生成 Asset（标记白名单）
    6. P2 → 检查白名单后生成 Alert
    
    配置示例:
    ```yaml
    # 可选配置（如 Asset 打包路径模板）
    asset_path_template: "corner_case/{batch_id}/{cycle_id}/"
    ```
    
    使用示例:
    ```python
    aggregator = ContextAggregatorOperator(
        task_id='aggregate_context',
        rule_task_ids=['rule_p0', 'rule_p1', 'rule_p2'],  # 所有上游 Rule Task
        dag=dag
    )
    ```
    """
    
    def __init__(self, upstream_task_ids: List[str] = None, rule_task_ids: List[str] = None, **kwargs):
        """
        初始化
        
        Args:
            upstream_task_ids: 上游 Task 的 ID 列表（推荐使用）
            rule_task_ids: 上游 Rule Task 的 ID 列表（兼容旧参数名）
        """
        # 兼容两种参数名
        if upstream_task_ids is not None:
            self.rule_task_ids = upstream_task_ids
        elif rule_task_ids is not None:
            self.rule_task_ids = rule_task_ids
        else:
            raise ValueError("Must provide either 'upstream_task_ids' or 'rule_task_ids'")
        
        super().__init__(**kwargs)
    
    def _restore_context(self, context: Dict[str, Any]) -> GovernanceContext:
        """
        重写：支持多上游合并
        
        Args:
            context: Airflow context 字典
        
        Returns:
            合并后的 GovernanceContext
        """
        ti = context['task_instance']
        
        # 拉取所有上游 Context
        contexts = get_multiple_upstream_contexts(ti, self.rule_task_ids)
        
        if not contexts:
            raise ValueError("No upstream contexts found! Check rule_task_ids.")
        
        self.log.info(f"Merging {len(contexts)} upstream contexts")
        
        # 合并 Context（取第一个作为基础）
        merged_ctx = contexts[0]
        
        # 合并其他 Context 的 rule_outputs、data_registry 和 audit_logs
        for ctx in contexts[1:]:
            merged_ctx.rule_outputs.update(ctx.rule_outputs)
            merged_ctx.data_registry.update(ctx.data_registry)  # ⭐ 修复：合并 data_registry
            merged_ctx.audit_logs.extend(ctx.audit_logs)
        
        self.log.info(f"✅ Merged context: {len(merged_ctx.rule_outputs)} rule outputs, {len(merged_ctx.data_registry)} data refs")
        
        return merged_ctx
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        聚合逻辑
        
        流程：
        1. 处理 P0 告警（直接生成）
        2. 处理 P1 资产（生成 Assets）
        3. 处理 P2 违规（白名单豁免）
        4. 打印统计信息
        """
        self.log.info("🔄 Starting context aggregation...")
        
        # 统计规则执行结果
        total_rules = len(ctx.rule_outputs)
        success_rules = sum(1 for r in ctx.rule_outputs.values() if r.status == "SUCCESS")
        failed_rules = sum(1 for r in ctx.rule_outputs.values() if r.status == "FAILED")
        total_hits = sum(r.hit_count for r in ctx.rule_outputs.values())
        
        self.log.info(
            f"Rule execution summary: "
            f"{total_rules} total, {success_rules} success, {failed_rules} failed, {total_hits} hits"
        )
        
        # 1. 处理 P0 告警
        self._process_p0_alerts(ctx)
        
        # 2. 处理 P1 资产
        self._process_p1_assets(ctx)
        
        # 3. 处理 P2 违规
        self._process_p2_violations(ctx)
        
        # 4. 打印最终统计
        self.log.info(f"✅ Aggregation completed: {len(ctx.alerts)} alerts, {len(ctx.assets)} assets")
        
        # 5. 推送 GovernanceContext 到 XCom（供 save_assets_to_queue 使用）
        ctx_json = ctx.to_json()
        context['ti'].xcom_push(key='governance_context', value=ctx_json)
        self.log.info(f"📤 Pushed GovernanceContext to XCom (key='governance_context')")
    
    def _process_p0_alerts(self, ctx: GovernanceContext) -> None:
        """
        处理 P0 告警（直接生成）
        
        Args:
            ctx: 治理上下文
        """
        self.log.info("📋 Processing P0 alerts...")
        
        p0_count = 0
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # ✅ 使用 _severity 列过滤（不解析字符串）
                    p0_df = hit_df.filter(pl.col('_severity') == 'P0')
                    
                    if p0_df.height > 0:
                        # 提取 cycle_id 列表
                        cycle_ids = p0_df['cycle_id'].to_list()
                        
                        # 生成告警
                        ctx.add_alert(
                            rule_id=rule_id,
                            severity="P0",
                            title=f"[P0] 数据质量异常 - {rule_id}",
                            content=f"发现 {len(cycle_ids)} 条异常数据",
                            trigger_cycle_ids=cycle_ids
                        )
                        
                        p0_count += 1
                        self.log.info(f"  ✅ Generated P0 alert: {rule_id} ({len(cycle_ids)} hits)")
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"📋 P0 alerts processed: {p0_count} alerts generated")
    
    def _process_p1_assets(self, ctx: GovernanceContext) -> None:
        """
        处理 P1 资产（生成 Assets）
        
        Args:
            ctx: 治理上下文
        """
        self.log.info("📦 Processing P1 assets...")
        
        asset_count = 0
        asset_path_template = self._config.get('asset_path_template', 'corner_case/{batch_id}/{cycle_id}/')
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # ✅ 使用 _severity 列过滤
                    p1_df = hit_df.filter(pl.col('_severity') == 'P1')
                    
                    if p1_df.height > 0:
                        # 生成 Assets
                        for row in p1_df.iter_rows(named=True):
                            # 构建目标路径
                            target_path = asset_path_template.format(
                                batch_id=ctx.batch_id,
                                cycle_id=row['cycle_id']
                            )
                            
                            ctx.add_asset(
                                asset_id=row['cycle_id'],
                                asset_type="HIGH_VALUE_SCENARIO",  # 可从配置读取
                                rule_id=rule_id,  # 添加规则ID
                                vehicle_id=row['vehicle_id'],
                                start_ts=str(row['start_time']),
                                end_ts=str(row['end_time']),
                                tags=["VALID_TWIN_LIFT"],  # 标记为白名单
                                target_path=target_path
                            )
                        
                        asset_count += p1_df.height
                        self.log.info(f"  ✅ Generated P1 assets: {rule_id} ({p1_df.height} assets)")
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"📦 P1 assets processed: {asset_count} assets generated")
    
    def _process_p2_violations(self, ctx: GovernanceContext) -> None:
        """
        处理 P2 违规（白名单豁免）
        
        逻辑：
        1. 提取 P1 资产的 cycle_id 作为白名单
        2. 读取 P2 规则的命中数据
        3. 过滤掉白名单中的 cycle_id
        4. 生成告警
        
        Args:
            ctx: 治理上下文
        """
        self.log.info("⚠️ Processing P2 violations...")
        
        # 1. 提取白名单（P1 资产的 cycle_id）
        exempted_ids = set(asset.asset_id for asset in ctx.assets)
        
        if exempted_ids:
            self.log.info(f"  白名单: {len(exempted_ids)} cycle_ids")
        
        p2_count = 0
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # ✅ 使用 _severity 列过滤
                    p2_df = hit_df.filter(pl.col('_severity') == 'P2')
                    
                    if p2_df.height > 0:
                        # 2. 过滤白名单
                        if exempted_ids:
                            final_df = p2_df.filter(
                                ~pl.col('cycle_id').is_in(list(exempted_ids))
                            )
                        else:
                            final_df = p2_df
                        
                        # 3. 生成告警
                        if final_df.height > 0:
                            cycle_ids = final_df['cycle_id'].to_list()
                            
                            ctx.add_alert(
                                rule_id=rule_id,
                                severity="P2",
                                title=f"[P2] SLA 违规 - {rule_id}",
                                content=f"发现 {len(cycle_ids)} 条超时数据（已排除白名单 {len(exempted_ids)} 条）",
                                trigger_cycle_ids=cycle_ids
                            )
                            
                            p2_count += 1
                            self.log.info(
                                f"  ✅ Generated P2 alert: {rule_id} "
                                f"({len(cycle_ids)} violations after exemption)"
                            )
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"⚠️ P2 violations processed: {p2_count} alerts generated")
