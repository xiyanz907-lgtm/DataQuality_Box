"""
operators/aggregator.py
上下文聚合器 V2（Reduce 阶段）

职责：
- 合并多个 Rule Task 的 Context
- 读取所有 Rule Result Parquet
- 数据驱动的 P0/P1/P2 处理（从规则 YAML 读取配置）
- 生成最终的 Alerts 和 Assets

V2 改进（方案B）：
- Aggregator 自己扫描 rules/ 目录加载规则配置
- Alert 内容、trigger_id_field 从 rule YAML 读取
- Asset 字段映射从 rule YAML 的 asset.field_mapping 读取
- P2 白名单豁免字段可配置
- 不再硬编码字段名
"""
import polars as pl
from typing import Dict, Any, List, Optional
from jinja2 import Template

from plugins.infra.operators import BaseGovernanceOperator, get_multiple_upstream_contexts
from plugins.domain.context import GovernanceContext
from plugins.orchestration.rule_scanner import RuleScanner


class ContextAggregatorOperator(BaseGovernanceOperator):
    """
    上下文聚合器 V2
    
    核心改进：
    1. 通过 RuleScanner（方案B）自动加载规则配置
    2. 所有 alert/asset 处理逻辑由规则 YAML 配置驱动
    3. 不再硬编码字段名、asset_type、tags
    
    使用示例:
    ```python
    aggregator = ContextAggregatorOperator(
        task_id='context_aggregator',
        rule_task_ids=['rule_tasks.rule_p0', 'rule_tasks.rule_p1'],
        rules_dir='configs/rules',  # 方案B：自己扫描规则目录
        dag=dag
    )
    ```
    """
    
    def __init__(
        self,
        upstream_task_ids: List[str] = None,
        rule_task_ids: List[str] = None,
        rules_dir: str = None,
        **kwargs
    ):
        """
        初始化
        
        Args:
            upstream_task_ids: 上游 Task 的 ID 列表（推荐使用）
            rule_task_ids: 上游 Rule Task 的 ID 列表（兼容旧参数名）
            rules_dir: 规则目录路径（方案B，用于加载规则配置）
        """
        # 兼容两种参数名
        if upstream_task_ids is not None:
            self.rule_task_ids = upstream_task_ids
        elif rule_task_ids is not None:
            self.rule_task_ids = rule_task_ids
        else:
            raise ValueError("Must provide either 'upstream_task_ids' or 'rule_task_ids'")
        
        # 方案B：Aggregator 自己加载规则配置
        self.rules_dir = rules_dir or 'configs/rules'
        
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
            merged_ctx.data_registry.update(ctx.data_registry)
            merged_ctx.audit_logs.extend(ctx.audit_logs)
        
        self.log.info(
            f"✅ Merged context: {len(merged_ctx.rule_outputs)} rule outputs, "
            f"{len(merged_ctx.data_registry)} data refs"
        )
        
        return merged_ctx
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        聚合逻辑 V2
        
        流程：
        1. 加载所有规则配置（方案B：自己扫描目录）
        2. 处理 P0 告警
        3. 处理 P1 资产（数据驱动的字段映射）
        4. 处理 P2 违规（可配置的白名单豁免）
        5. 打印统计信息
        """
        self.log.info("🔄 Starting context aggregation V2...")
        
        # 0. 加载规则配置（方案B）
        rule_configs = self._load_rule_configs()
        self.log.info(f"📚 Loaded {len(rule_configs)} rule configs from {self.rules_dir}")
        
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
        self._process_p0_alerts(ctx, rule_configs)
        
        # 2. 处理 P1 资产
        self._process_p1_assets(ctx, rule_configs)
        
        # 3. 处理 P2 违规
        self._process_p2_violations(ctx, rule_configs)
        
        # 4. 打印最终统计
        self.log.info(f"✅ Aggregation completed: {len(ctx.alerts)} alerts, {len(ctx.assets)} assets")
        
        # 5. 推送 GovernanceContext 到 XCom（供 save_assets_to_queue 使用）
        ctx_json = ctx.to_json()
        context['ti'].xcom_push(key='governance_context', value=ctx_json)
        self.log.info(f"📤 Pushed GovernanceContext to XCom (key='governance_context')")
    
    # ========================================================================
    # 方案B：自己加载规则配置
    # ========================================================================
    
    def _load_rule_configs(self) -> Dict[str, Dict]:
        """
        加载所有规则的完整配置（方案B）
        
        Returns:
            {rule_id: full_yaml_config}
        """
        try:
            scanner = RuleScanner(rules_dir=self.rules_dir)
            return scanner.get_rule_configs_by_id()
        except Exception as e:
            self.log.warning(f"⚠️ Failed to load rule configs from {self.rules_dir}: {e}")
            return {}
    
    # ========================================================================
    # P0 处理：数据驱动的告警生成
    # ========================================================================
    
    def _process_p0_alerts(self, ctx: GovernanceContext, rule_configs: Dict[str, Dict]) -> None:
        """
        处理 P0 告警（数据驱动）
        
        改进：
        - trigger_id_field 从 rule YAML 的 alert.trigger_id_field 读取
        - 告警内容支持 Jinja 模板渲染
        - 不再硬编码 'cycle_id'
        
        Args:
            ctx: 治理上下文
            rule_configs: 规则配置字典
        """
        self.log.info("📋 Processing P0 alerts...")
        
        p0_count = 0
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # 使用 _severity 列过滤
                    p0_df = hit_df.filter(pl.col('_severity') == 'P0')
                    
                    if p0_df.height > 0:
                        # 获取规则配置
                        rule_cfg = rule_configs.get(rule_id, {})
                        alert_cfg = rule_cfg.get('alert', {})
                        
                        # 从配置读取 trigger_id_field（默认 cycle_id）
                        trigger_id_field = alert_cfg.get('trigger_id_field', 'cycle_id')
                        
                        # 安全提取 trigger IDs
                        trigger_ids = self._safe_extract_ids(p0_df, trigger_id_field)
                        
                        # 渲染告警内容
                        title = rule_output.alert_title or alert_cfg.get('title', f"[P0] 数据质量异常 - {rule_id}")
                        content = self._render_alert_content(
                            rule_cfg=rule_cfg,
                            trigger_ids=trigger_ids,
                            ctx=ctx,
                            hit_count=p0_df.height,
                            default_content=f"发现 {len(trigger_ids)} 条异常数据"
                        )
                        
                        # 生成告警
                        ctx.add_alert(
                            rule_id=rule_id,
                            severity="P0",
                            title=title,
                            content=content,
                            trigger_cycle_ids=[str(t) for t in trigger_ids]
                        )
                        
                        p0_count += 1
                        self.log.info(f"  ✅ Generated P0 alert: {rule_id} ({p0_df.height} hits)")
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"📋 P0 alerts processed: {p0_count} alerts generated")
    
    # ========================================================================
    # P1 处理：数据驱动的资产生成
    # ========================================================================
    
    def _process_p1_assets(self, ctx: GovernanceContext, rule_configs: Dict[str, Dict]) -> None:
        """
        处理 P1 资产（数据驱动的字段映射）
        
        改进：
        - 字段映射从 rule YAML 的 asset.field_mapping 读取
        - asset_type、tags 从 rule YAML 的 asset 配置读取
        - 不再硬编码 'cycle_id', 'vehicle_id' 等字段名
        
        Args:
            ctx: 治理上下文
            rule_configs: 规则配置字典
        """
        self.log.info("📦 Processing P1 assets...")
        
        asset_count = 0
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # 使用 _severity 列过滤
                    p1_df = hit_df.filter(pl.col('_severity') == 'P1')
                    
                    if p1_df.height > 0:
                        # 获取规则配置
                        rule_cfg = rule_configs.get(rule_id, {})
                        asset_cfg = rule_cfg.get('asset', {})
                        
                        # 检查 asset 是否启用（P1 默认启用）
                        if not asset_cfg.get('enabled', True):
                            self.log.info(f"  ⏭️ Asset disabled for {rule_id}, skipping")
                            continue
                        
                        # 获取字段映射（有默认值兜底）
                        field_map = asset_cfg.get('field_mapping', {
                            'asset_id': 'cycle_id',
                            'vehicle_id': 'vehicle_id',
                            'start_ts': 'start_time',
                            'end_ts': 'end_time'
                        })
                        
                        asset_type = asset_cfg.get('asset_type', 'HIGH_VALUE_SCENARIO')
                        tags = asset_cfg.get('tags', [])
                        path_template = asset_cfg.get(
                            'target_path_template',
                            'corner_case/{batch_id}/{asset_id}/'
                        )
                        
                        # 生成 Assets
                        for row in p1_df.iter_rows(named=True):
                            # 安全读取映射字段
                            asset_id_val = str(row.get(field_map.get('asset_id', 'cycle_id'), 'UNKNOWN'))
                            vehicle_id_val = str(row.get(field_map.get('vehicle_id', 'vehicle_id'), 'UNKNOWN'))
                            start_ts_val = str(row.get(field_map.get('start_ts', 'start_time'), ''))
                            end_ts_val = str(row.get(field_map.get('end_ts', 'end_time'), ''))
                            
                            # 构建目标路径
                            try:
                                target_path = path_template.format(
                                    batch_id=ctx.batch_id,
                                    asset_id=asset_id_val,
                                    vehicle_id=vehicle_id_val,
                                    rule_id=rule_id
                                )
                            except KeyError:
                                target_path = f"corner_case/{ctx.batch_id}/{asset_id_val}/"
                            
                            ctx.add_asset(
                                asset_id=asset_id_val,
                                asset_type=asset_type,
                                rule_id=rule_id,
                                vehicle_id=vehicle_id_val,
                                start_ts=start_ts_val,
                                end_ts=end_ts_val,
                                tags=tags,
                                target_path=target_path
                            )
                        
                        asset_count += p1_df.height
                        self.log.info(f"  ✅ Generated P1 assets: {rule_id} ({p1_df.height} assets)")
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"📦 P1 assets processed: {asset_count} assets generated")
    
    # ========================================================================
    # P2 处理：可配置的白名单豁免
    # ========================================================================
    
    def _process_p2_violations(self, ctx: GovernanceContext, rule_configs: Dict[str, Dict]) -> None:
        """
        处理 P2 违规（可配置的白名单豁免）
        
        改进：
        - 白名单豁免字段可通过 exemption.exempt_field 配置
        - 告警内容支持模板渲染
        - trigger_id_field 可配置
        
        逻辑：
        1. 提取 P1 资产的 asset_id 作为白名单
        2. 读取 P2 规则的命中数据
        3. 按 exempt_field 过滤掉白名单
        4. 生成告警
        
        Args:
            ctx: 治理上下文
            rule_configs: 规则配置字典
        """
        self.log.info("⚠️ Processing P2 violations...")
        
        # 1. 提取白名单（P1 资产的 asset_id）
        exempted_ids = set(asset.asset_id for asset in ctx.assets)
        
        if exempted_ids:
            self.log.info(f"  白名单: {len(exempted_ids)} asset_ids")
        
        p2_count = 0
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # 使用 _severity 列过滤
                    p2_df = hit_df.filter(pl.col('_severity') == 'P2')
                    
                    if p2_df.height > 0:
                        # 获取规则配置
                        rule_cfg = rule_configs.get(rule_id, {})
                        alert_cfg = rule_cfg.get('alert', {})
                        exemption_cfg = rule_cfg.get('exemption', {})
                        
                        # 可配置的豁免字段（默认 cycle_id）
                        exempt_field = exemption_cfg.get('exempt_field', 'cycle_id')
                        
                        # 2. 过滤白名单
                        if exempted_ids and exempt_field in p2_df.columns:
                            final_df = p2_df.filter(
                                ~pl.col(exempt_field).cast(pl.Utf8).is_in(
                                    [str(eid) for eid in exempted_ids]
                                )
                            )
                        else:
                            final_df = p2_df
                        
                        # 3. 生成告警
                        if final_df.height > 0:
                            trigger_id_field = alert_cfg.get('trigger_id_field', 'cycle_id')
                            trigger_ids = self._safe_extract_ids(final_df, trigger_id_field)
                            
                            title = rule_output.alert_title or alert_cfg.get(
                                'title', f"[P2] SLA 违规 - {rule_id}"
                            )
                            content = self._render_alert_content(
                                rule_cfg=rule_cfg,
                                trigger_ids=trigger_ids,
                                ctx=ctx,
                                hit_count=final_df.height,
                                exempted_count=len(exempted_ids),
                                default_content=(
                                    f"发现 {len(trigger_ids)} 条违规数据"
                                    f"（已排除白名单 {len(exempted_ids)} 条）"
                                )
                            )
                            
                            ctx.add_alert(
                                rule_id=rule_id,
                                severity="P2",
                                title=title,
                                content=content,
                                trigger_cycle_ids=[str(t) for t in trigger_ids]
                            )
                            
                            p2_count += 1
                            self.log.info(
                                f"  ✅ Generated P2 alert: {rule_id} "
                                f"({len(trigger_ids)} violations after exemption)"
                            )
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"⚠️ P2 violations processed: {p2_count} alerts generated")
    
    # ========================================================================
    # 工具方法
    # ========================================================================
    
    def _safe_extract_ids(self, df: pl.DataFrame, field: str) -> list:
        """
        安全提取 ID 列表（字段不存在时返回空列表）
        
        Args:
            df: DataFrame
            field: 字段名
        
        Returns:
            ID 列表
        """
        if field in df.columns:
            return df[field].to_list()
        else:
            self.log.warning(f"⚠️ Field '{field}' not found in columns: {df.columns}")
            return []
    
    def _render_alert_content(
        self,
        rule_cfg: Dict,
        trigger_ids: list,
        ctx: GovernanceContext,
        hit_count: int = 0,
        exempted_count: int = 0,
        default_content: str = ""
    ) -> str:
        """
        渲染告警内容（支持 Jinja 模板）
        
        如果规则配置中有 alert.content_template，使用 Jinja 渲染；
        否则使用 default_content。
        
        可用模板变量：
        - hit_count: 命中行数
        - trigger_ids: 触发 ID 列表
        - run_date: 运行日期
        - batch_id: 批次 ID
        - exempted_count: 白名单豁免数量
        
        Args:
            rule_cfg: 规则完整配置
            trigger_ids: 触发 ID 列表
            ctx: 治理上下文
            hit_count: 命中数量
            exempted_count: 豁免数量
            default_content: 默认内容（无模板时使用）
        
        Returns:
            渲染后的告警内容字符串
        """
        template_str = rule_cfg.get('alert', {}).get('content_template')
        
        if template_str:
            try:
                template = Template(template_str)
                return template.render(
                    hit_count=hit_count,
                    trigger_ids=trigger_ids,
                    trigger_ids_preview=trigger_ids[:10],
                    run_date=ctx.run_date,
                    batch_id=ctx.batch_id,
                    exempted_count=exempted_count,
                )
            except Exception as e:
                self.log.warning(f"⚠️ Failed to render content_template: {e}")
                return default_content
        
        return default_content