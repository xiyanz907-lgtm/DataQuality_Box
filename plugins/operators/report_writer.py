"""
operators/report_writer.py
结果持久化算子

职责：
- 将 GovernanceContext 中的规则执行结果写入 MySQL
- governance_run_summary: 批次级汇总（每次 DAG 运行一条记录）
- governance_rule_results: 规则级明细（每条规则一条记录）
- 支持幂等写入（ON DUPLICATE KEY UPDATE）

位置：Aggregator → [ReportWriter] → Dispatcher
"""
import json
from typing import Dict, Any, List
from datetime import datetime, timezone

from airflow.providers.mysql.hooks.mysql import MySqlHook

from plugins.infra.operators import BaseGovernanceOperator
from plugins.domian.context import GovernanceContext
from plugins.orchestration.rule_scanner import RuleScanner


class ReportWriterOperator(BaseGovernanceOperator):
    """
    结果持久化算子

    从 GovernanceContext 中提取执行结果，写入 MySQL 的两张表：
    - governance_run_summary: 批次级汇总
    - governance_rule_results: 规则级明细

    使用示例:
    ```python
    writer = ReportWriterOperator(
        task_id='report_writer',
        conn_id='qa_mysql_conn',
        rules_dir='configs/rules',
        upstream_task_id='context_aggregator',
        dag=dag
    )
    ```
    """

    def __init__(
        self,
        conn_id: str = "qa_mysql_conn",
        rules_dir: str = None,
        fail_on_error: bool = False,
        **kwargs
    ):
        """
        Args:
            conn_id: MySQL 连接 ID（Airflow Connection）
            rules_dir: 规则目录路径（用于加载 severity/logic_type 等元数据）
            fail_on_error: 写入失败时是否中断 pipeline
        """
        self.conn_id = conn_id
        self.rules_dir = rules_dir or 'configs/rules'
        self.fail_on_error = fail_on_error
        super().__init__(**kwargs)

    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        持久化逻辑

        流程：
        1. 加载规则配置（获取 severity/logic_type/description）
        2. 写入 governance_rule_results
        3. 写入 governance_run_summary
        """
        self.log.info("📊 Starting report persistence...")

        try:
            rule_configs = self._load_rule_configs()
            hook = MySqlHook(mysql_conn_id=self.conn_id)

            rule_count = self._write_rule_results(hook, ctx, rule_configs)
            self._write_run_summary(hook, ctx, context)

            self.log.info(
                f"✅ Report persisted: {rule_count} rule results + 1 run summary"
            )

        except Exception as e:
            self.log.error(f"❌ Failed to persist report: {e}")
            if self.fail_on_error:
                raise
            self.log.warning("⚠️ Continuing pipeline despite report write failure")

    # ========================================================================
    # 规则配置加载
    # ========================================================================

    def _load_rule_configs(self) -> Dict[str, Dict]:
        try:
            scanner = RuleScanner(rules_dir=self.rules_dir)
            return scanner.get_rule_configs_by_id()
        except Exception as e:
            self.log.warning(f"⚠️ Failed to load rule configs: {e}")
            return {}

    # ========================================================================
    # governance_rule_results 写入
    # ========================================================================

    def _write_rule_results(
        self,
        hook: MySqlHook,
        ctx: GovernanceContext,
        rule_configs: Dict[str, Dict]
    ) -> int:
        """写入规则级结果明细（幂等 UPSERT）"""

        alert_lookup = self._build_alert_lookup(ctx)

        sql = """
            INSERT INTO governance_rule_results (
                batch_id, run_date, rule_id, severity, logic_type,
                description, status, hit_count, execution_time_sec,
                error_message, alert_title, trigger_ids
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                severity = VALUES(severity),
                logic_type = VALUES(logic_type),
                description = VALUES(description),
                status = VALUES(status),
                hit_count = VALUES(hit_count),
                execution_time_sec = VALUES(execution_time_sec),
                error_message = VALUES(error_message),
                alert_title = VALUES(alert_title),
                trigger_ids = VALUES(trigger_ids)
        """

        count = 0
        for rule_id, rule_output in ctx.rule_outputs.items():
            rule_cfg = rule_configs.get(rule_id, {})
            meta = rule_cfg.get('meta', {})
            logic = rule_cfg.get('logic', {})

            severity = meta.get('severity', 'INFO')
            logic_type = logic.get('type', 'filter')
            description = meta.get('description', '')

            alert_info = alert_lookup.get(rule_id, {})
            alert_title = alert_info.get('title', rule_output.alert_title or '')
            trigger_ids = alert_info.get('trigger_ids', [])
            trigger_ids_json = (
                json.dumps(trigger_ids[:1000], default=str) if trigger_ids else None
            )

            try:
                hook.run(sql, parameters=(
                    ctx.batch_id,
                    ctx.run_date,
                    rule_id,
                    severity,
                    logic_type,
                    description[:512] if description else None,
                    rule_output.status,
                    rule_output.hit_count,
                    rule_output.execution_time_sec,
                    rule_output.error_message or None,
                    alert_title[:256] if alert_title else None,
                    trigger_ids_json,
                ))
                count += 1
            except Exception as e:
                self.log.warning(f"⚠️ Failed to write result for {rule_id}: {e}")

        self.log.info(f"📊 Written {count}/{len(ctx.rule_outputs)} rule results")
        return count

    # ========================================================================
    # governance_run_summary 写入
    # ========================================================================

    def _write_run_summary(
        self,
        hook: MySqlHook,
        ctx: GovernanceContext,
        context: Dict[str, Any]
    ) -> None:
        """写入批次级运行汇总（幂等 UPSERT）"""

        dag_id = context['dag'].dag_id
        dag_run = context['dag_run']

        total_rules = len(ctx.rule_outputs)
        success_rules = sum(
            1 for r in ctx.rule_outputs.values() if r.status == "SUCCESS"
        )
        failed_rules = sum(
            1 for r in ctx.rule_outputs.values() if r.status == "FAILED"
        )
        skipped_rules = sum(
            1 for r in ctx.rule_outputs.values() if r.status == "SKIPPED"
        )
        total_hits = sum(r.hit_count for r in ctx.rule_outputs.values())

        entity_refs = [
            ref for ref in ctx.data_registry.values() if ref.stage == "ENTITY"
        ]
        entity_names = [ref.alt_key or ref.key for ref in entity_refs]
        entity_name = ','.join(entity_names) if entity_names else None
        total_rows = sum(ref.row_count for ref in entity_refs)

        started_at = dag_run.start_date
        completed_at = datetime.now(timezone.utc)
        if started_at:
            if started_at.tzinfo is None:
                duration_sec = (completed_at.replace(tzinfo=None) - started_at).total_seconds()
            else:
                duration_sec = (completed_at - started_at).total_seconds()
        else:
            duration_sec = 0

        if failed_rules == total_rules and total_rules > 0:
            status = 'FAILED'
        elif failed_rules > 0:
            status = 'PARTIAL_FAILURE'
        else:
            status = 'SUCCESS'

        sql = """
            INSERT INTO governance_run_summary (
                batch_id, dag_id, run_date, entity_name, total_rows,
                total_rules, success_rules, failed_rules, skipped_rules,
                total_hits, alert_count, asset_count,
                started_at, completed_at, duration_sec, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                dag_id = VALUES(dag_id),
                entity_name = VALUES(entity_name),
                total_rows = VALUES(total_rows),
                total_rules = VALUES(total_rules),
                success_rules = VALUES(success_rules),
                failed_rules = VALUES(failed_rules),
                skipped_rules = VALUES(skipped_rules),
                total_hits = VALUES(total_hits),
                alert_count = VALUES(alert_count),
                asset_count = VALUES(asset_count),
                started_at = VALUES(started_at),
                completed_at = VALUES(completed_at),
                duration_sec = VALUES(duration_sec),
                status = VALUES(status)
        """

        hook.run(sql, parameters=(
            ctx.batch_id,
            dag_id,
            ctx.run_date,
            entity_name,
            total_rows,
            total_rules,
            success_rules,
            failed_rules,
            skipped_rules,
            total_hits,
            len(ctx.alerts),
            len(ctx.assets),
            started_at,
            completed_at,
            duration_sec,
            status,
        ))

        self.log.info(
            f"📊 Written run summary: {status} "
            f"({total_rules} rules, {total_hits} hits, {duration_sec:.1f}s)"
        )

    # ========================================================================
    # 工具方法
    # ========================================================================

    def _build_alert_lookup(self, ctx: GovernanceContext) -> Dict[str, Dict]:
        """构建 rule_id -> alert 信息的索引"""
        lookup: Dict[str, Dict] = {}
        for alert in ctx.alerts:
            if alert.rule_id not in lookup:
                lookup[alert.rule_id] = {
                    'title': alert.title,
                    'trigger_ids': list(alert.trigger_cycle_ids)
                }
            else:
                lookup[alert.rule_id]['trigger_ids'].extend(
                    alert.trigger_cycle_ids
                )
        return lookup
