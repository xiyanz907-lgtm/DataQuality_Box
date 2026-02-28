import os
"""
operators/dispatcher.py
通知分发器

职责：
- 读取 Context 的 Alerts
- 按严重等级分组（P0 即时，P2 批量）
- 发送邮件通知
- 预留其他通知渠道接口
"""
from typing import Dict, Any, List
from datetime import datetime

from airflow.utils.email import send_email
from plugins.infra.operators import BaseGovernanceOperator
from plugins.domian.context import GovernanceContext, AlertItem


class NotificationDispatcherOperator(BaseGovernanceOperator):
    """
    通知分发器
    
    配置示例:
    ```yaml
    email_to: "admin@example.com,team@example.com"
    email_cc: "manager@example.com"
    
    # 可选：邮件模板配置
    templates:
      p0_subject: "[紧急] {batch_id} - 数据质量告警"
      p2_subject: "{batch_id} - SLA 违规汇总"
    ```
    
    使用示例:
    ```python
    dispatcher = NotificationDispatcherOperator(
        task_id='dispatch_notifications',
        config_dict={'email_to': 'admin@example.com'},
        upstream_task_id='aggregate_context',
        dag=dag
    )
    ```
    """
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        分发逻辑
        
        流程：
        1. 检查是否有告警
        2. 按严重等级分组
        3. P0 告警即时发送
        4. P2 告警批量汇总发送
        """
        if not ctx.alerts:
            self.log.info("✅ No alerts to dispatch")
            return
        
        self.log.info(f"📧 Dispatching {len(ctx.alerts)} alerts...")
        
        # 1. 按严重等级分组
        p0_alerts = [a for a in ctx.alerts if a.severity == "P0"]
        p2_alerts = [a for a in ctx.alerts if a.severity == "P2"]
        other_alerts = [a for a in ctx.alerts if a.severity not in ("P0", "P2")]
        
        self.log.info(f"  P0: {len(p0_alerts)}, P2: {len(p2_alerts)}, Other: {len(other_alerts)}")
        
        # 2. 发送 P0 告警（即时）
        if p0_alerts:
            self._send_p0_alerts(p0_alerts, ctx)
        
        # 3. 发送 P2 告警（批量汇总）
        if p2_alerts:
            self._send_p2_batch_alert(p2_alerts, ctx)
        
        # 4. 发送其他告警
        if other_alerts:
            self._send_other_alerts(other_alerts, ctx)
        
        self.log.info("📧 ✅ All alerts dispatched successfully")
    
    def _send_p0_alerts(self, alerts: List[AlertItem], ctx: GovernanceContext) -> None:
        """
        发送 P0 告警（批量汇总为一封邮件）
        
        Args:
            alerts: P0 告警列表
            ctx: 治理上下文
        """
        self.log.info(f"📧 Sending P0 batch alert ({len(alerts)} rules)...")
        
        try:
            subject = self._build_subject("P0", ctx, len(alerts))
            body = self._build_p0_batch_body(alerts, ctx)
            self._send_email(subject, body)
            self.log.info(f"  ✅ Sent P0 batch alert: {len(alerts)} rules")
        except Exception as e:
            self.log.error(f"  ❌ Failed to send P0 batch alert: {e}")
    
    def _send_p2_batch_alert(self, alerts: List[AlertItem], ctx: GovernanceContext) -> None:
        """
        发送 P2 告警（批量汇总）
        
        Args:
            alerts: P2 告警列表
            ctx: 治理上下文
        """
        self.log.info(f"📧 Sending P2 batch alert ({len(alerts)} rules)...")
        
        try:
            # 构建汇总邮件
            subject = self._build_subject("P2", ctx, len(alerts))
            body = self._build_p2_batch_body(alerts, ctx)
            
            # 发送
            self._send_email(subject, body)
            
            self.log.info(f"  ✅ Sent P2 batch alert: {len(alerts)} rules")
        
        except Exception as e:
            self.log.error(f"  ❌ Failed to send P2 batch alert: {e}")
    
    def _send_other_alerts(self, alerts: List[AlertItem], ctx: GovernanceContext) -> None:
        """
        发送其他告警（P1/INFO 等）
        
        Args:
            alerts: 告警列表
            ctx: 治理上下文
        """
        self.log.info(f"📧 Sending {len(alerts)} other alerts...")
        
        for alert in alerts:
            try:
                self._send_single_alert(alert, ctx, urgent=False)
                self.log.info(f"  ✅ Sent {alert.severity} alert: {alert.rule_id}")
            except Exception as e:
                self.log.error(f"  ❌ Failed to send alert {alert.rule_id}: {e}")
    
    def _send_single_alert(
        self, 
        alert: AlertItem, 
        ctx: GovernanceContext, 
        urgent: bool = False
    ) -> None:
        """
        发送单个告警邮件
        
        Args:
            alert: 告警对象
            ctx: 治理上下文
            urgent: 是否紧急
        """
        # 构建邮件
        subject = self._build_subject(alert.severity, ctx, 1, alert.rule_id)
        body = self._build_single_alert_body(alert, ctx, urgent)
        
        # 发送
        self._send_email(subject, body)
    
    def _build_subject(
        self, 
        severity: str, 
        ctx: GovernanceContext, 
        count: int = 1,
        rule_id: str = ""
    ) -> str:
        """
        构建邮件主题
        
        Args:
            severity: 严重等级
            ctx: 治理上下文
            count: 告警数量
            rule_id: 规则ID（可选）
        
        Returns:
            邮件主题字符串
        """
        # 从配置读取模板（如果有）
        templates = self._config.get('templates', {})
        template_key = f"{severity.lower()}_subject"
        
        if template_key in templates:
            return templates[template_key].format(batch_id=ctx.batch_id, count=count)
        
        # 默认模板
        prefix = "🔴 [紧急]" if severity == "P0" else "⚠️"
        
        if rule_id:
            return f"{prefix} [{ctx.batch_id}] {severity} 告警 - {rule_id}"
        else:
            return f"{prefix} [{ctx.batch_id}] {severity} 告警汇总 ({count} 条)"
    
    def _build_single_alert_body(
        self, 
        alert: AlertItem, 
        ctx: GovernanceContext,
        urgent: bool
    ) -> str:
        """
        构建单个告警的邮件正文
        
        Args:
            alert: 告警对象
            ctx: 治理上下文
            urgent: 是否紧急
        
        Returns:
            HTML 格式的邮件正文
        """
        # 紧急标记
        urgent_mark = "🔴 <strong>紧急告警</strong>" if urgent else ""
        
        # 截取部分 cycle_id（邮件不要太长）
        trigger_ids_preview = alert.trigger_cycle_ids[:10]
        has_more = len(alert.trigger_cycle_ids) > 10
        
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #f44336; color: white; padding: 15px; }}
                .content {{ padding: 20px; }}
                .info {{ background-color: #f5f5f5; padding: 10px; margin: 10px 0; }}
                .cycle-ids {{ color: #666; font-size: 0.9em; }}
            </style>
        </head>
        <body>
            <div class="header">
                {urgent_mark}
                <h2>{alert.title}</h2>
            </div>
            <div class="content">
                <p><strong>批次:</strong> {ctx.batch_id}</p>
                <p><strong>日期:</strong> {ctx.run_date}</p>
                <p><strong>规则:</strong> {alert.rule_id}</p>
                <p><strong>严重等级:</strong> {alert.severity}</p>
                
                <div class="info">
                    <h3>告警详情</h3>
                    <p>{alert.content}</p>
                </div>
                
                <div class="cycle-ids">
                    <h4>触发数据 (Cycle IDs):</h4>
                    <p>{', '.join(trigger_ids_preview)}</p>
                    {'<p><em>... 还有 {} 条</em></p>'.format(len(alert.trigger_cycle_ids) - 10) if has_more else ''}
                </div>
                
                <p><em>生成时间: {alert.created_at}</em></p>
            </div>
        </body>
        </html>
        """
        
        return body
    
    def _build_p0_batch_body(
        self, 
        alerts: List[AlertItem], 
        ctx: GovernanceContext
    ) -> str:
        """
        构建 P0 批量告警的邮件正文
        
        Args:
            alerts: P0 告警列表
            ctx: 治理上下文
        
        Returns:
            HTML 格式的邮件正文
        """
        total_hits = sum(len(alert.trigger_cycle_ids) for alert in alerts)
        
        # 构建规则汇总表格
        rule_rows = ""
        for alert in alerts:
            ids_preview = alert.trigger_cycle_ids[:5]
            ids_text = ", ".join(str(cid) for cid in ids_preview)
            if len(alert.trigger_cycle_ids) > 5:
                ids_text += f" ... (+{len(alert.trigger_cycle_ids) - 5})"
            
            rule_rows += f"""
            <tr>
                <td>{alert.rule_id}</td>
                <td>{alert.title}</td>
                <td style="text-align:center;"><strong>{len(alert.trigger_cycle_ids)}</strong></td>
                <td class="cycle-ids">{ids_text}</td>
            </tr>
            """
        
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #d32f2f; color: white; padding: 15px; }}
                .summary {{ background-color: #ffebee; padding: 15px; margin: 15px 0; border-left: 4px solid #d32f2f; }}
                .content {{ padding: 20px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #ffcdd2; }}
                .cycle-ids {{ color: #666; font-size: 0.85em; max-width: 300px; word-break: break-all; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🔴 P0 数据质量告警汇总</h2>
            </div>
            <div class="content">
                <div class="summary">
                    <p><strong>批次:</strong> {ctx.batch_id}</p>
                    <p><strong>日期:</strong> {ctx.run_date}</p>
                    <p><strong>命中规则数:</strong> {len(alerts)}</p>
                    <p><strong>异常数据总数:</strong> {total_hits} 行</p>
                </div>
                
                <h3>📋 规则命中明细</h3>
                <table>
                    <tr>
                        <th>规则 ID</th>
                        <th>说明</th>
                        <th>命中数</th>
                        <th>Cycle IDs (前5条)</th>
                    </tr>
                    {rule_rows}
                </table>
                
                <p><em>生成时间: {datetime.utcnow().isoformat()}</em></p>
            </div>
        </body>
        </html>
        """
        
        return body
    
    def _build_p2_batch_body(
        self, 
        alerts: List[AlertItem], 
        ctx: GovernanceContext
    ) -> str:
        """
        构建 P2 批量告警的邮件正文
        
        Args:
            alerts: P2 告警列表
            ctx: 治理上下文
        
        Returns:
            HTML 格式的邮件正文
        """
        # 统计总数
        total_violations = sum(len(alert.trigger_cycle_ids) for alert in alerts)
        
        # 构建告警列表
        alert_items = ""
        for alert in alerts:
            alert_items += f"""
            <tr>
                <td>{alert.rule_id}</td>
                <td>{len(alert.trigger_cycle_ids)}</td>
                <td>{alert.title}</td>
            </tr>
            """
        
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #ff9800; color: white; padding: 15px; }}
                .content {{ padding: 20px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #f5f5f5; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>⚠️ P2 SLA 违规汇总</h2>
            </div>
            <div class="content">
                <p><strong>批次:</strong> {ctx.batch_id}</p>
                <p><strong>日期:</strong> {ctx.run_date}</p>
                <p><strong>违规规则数:</strong> {len(alerts)}</p>
                <p><strong>违规数据总数:</strong> {total_violations}</p>
                
                <h3>详细列表</h3>
                <table>
                    <tr>
                        <th>规则 ID</th>
                        <th>违规数量</th>
                        <th>描述</th>
                    </tr>
                    {alert_items}
                </table>
                
                <p><em>生成时间: {datetime.utcnow().isoformat()}</em></p>
            </div>
        </body>
        </html>
        """
        
        return body
    
    def _send_email(self, subject: str, body: str) -> None:
        """
        发送邮件（调用 Airflow 邮件工具）
        
        Args:
            subject: 邮件主题
            body: 邮件正文（HTML）
        """
        # 从配置读取收件人
        email_to = self._config.get('email_to', os.getenv('ALERT_EMAIL_TO', 'xiyan.zhou@westwell-lab.com'))
        email_cc = self._config.get('email_cc')
        
        # 转换为列表
        if isinstance(email_to, str):
            email_to = [e.strip() for e in email_to.split(',')]
        
        # 发送邮件
        send_email(
            to=email_to,
            subject=subject,
            html_content=body,
            cc=email_cc
        )
