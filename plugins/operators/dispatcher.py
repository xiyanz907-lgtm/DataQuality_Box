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
from plugins.domain.context import GovernanceContext, AlertItem


class NotificationDispatcherOperator(BaseGovernanceOperator):
    """
    通知分发器
    
    配置示例:
    ```yaml
    email_to: "admin@example.com,team@example.com"
    email_cc: "manager@example.com"
    
    # 可选：邮件模板配置
    templates:
      p0_subject: "{batch_id} - SLA 违规汇总"
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
        3. P0 告警批量汇总发送
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
        site = os.getenv('SITE_NAME', '')
        site_tag = f"[{site}] " if site else ""
        prefix = "🔴" if severity == "P0" else "⚠️"
        
        if rule_id:
            return f"{site_tag}{prefix} [{ctx.batch_id}] {severity} 告警 - {rule_id}"
        else:
            return f"{site_tag}{prefix} [{ctx.batch_id}] {severity} 告警汇总 ({count} 条)"
    
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
                body {{ font-family: -apple-system, 'Segoe UI', Arial, sans-serif; color: #333; margin: 0; padding: 0; background-color: #f9f9f9; }}
                .wrapper {{ max-width: 640px; margin: 20px auto; background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; overflow: hidden; }}
                .header {{ padding: 16px 24px; border-bottom: 1px solid #e0e0e0; }}
                .header h2 {{ margin: 0; font-size: 17px; font-weight: 600; color: #1a1a1a; }}
                .severity-tag {{ display: inline-block; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 3px; margin-right: 8px; background-color: #fdecea; color: #b71c1c; }}
                .content {{ padding: 20px 24px; }}
                .meta {{ font-size: 13px; color: #666; line-height: 1.8; }}
                .detail-box {{ background-color: #fafafa; border: 1px solid #eee; border-radius: 4px; padding: 12px 16px; margin: 16px 0; font-size: 13px; }}
                .cycle-ids {{ font-size: 12px; color: #888; line-height: 1.6; }}
                .footer {{ padding: 12px 24px; border-top: 1px solid #f0f0f0; font-size: 11px; color: #aaa; }}
            </style>
        </head>
        <body>
            <div class="wrapper">
                <div class="header">
                    <h2><span class="severity-tag">{alert.severity}</span>{alert.title}</h2>
                </div>
                <div class="content">
                    <div class="meta">
                        <p><strong>批次:</strong> {ctx.batch_id}</p>
                        <p><strong>日期:</strong> {ctx.run_date}</p>
                        <p><strong>规则:</strong> {alert.rule_id}</p>
                    </div>
                    
                    <div class="detail-box">
                        <strong>告警详情</strong>
                        <p style="margin:6px 0 0 0;">{alert.content}</p>
                    </div>
                    
                    <div class="cycle-ids">
                        <strong>触发数据 ({self._get_trigger_id_label(alert.trigger_id_field)}):</strong><br>
                        {', '.join(str(cid) for cid in trigger_ids_preview)}
                        {'<br><em>… 还有 {} 条</em>'.format(len(alert.trigger_cycle_ids) - 10) if has_more else ''}
                    </div>
                </div>
                <div class="footer">生成时间: {alert.created_at}</div>
            </div>
        </body>
        </html>
        """
        
        return body
    
    _TRIGGER_ID_LABELS = {
        "cycle_id": "作业号",
        "vehicle_id": "车号",
        "container_id": "箱号",
        "task_id": "任务号",
    }

    @classmethod
    def _get_trigger_id_label(cls, field_name: str) -> str:
        """将 trigger_id_field 转为可读的中文标签"""
        return cls._TRIGGER_ID_LABELS.get(field_name, field_name)

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
            
            id_label = self._get_trigger_id_label(alert.trigger_id_field)
            
            rule_rows += f"""
            <tr>
                <td>{alert.rule_id}</td>
                <td>{alert.title}</td>
                <td class="num">{len(alert.trigger_cycle_ids)}</td>
                <td class="id-type">{id_label}</td>
                <td class="trigger-ids">{ids_text}</td>
            </tr>
            """
        
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, 'Segoe UI', Arial, sans-serif; color: #333; margin: 0; padding: 0; background-color: #f9f9f9; }}
                .wrapper {{ max-width: 800px; margin: 20px auto; background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; overflow: hidden; }}
                .header {{ padding: 16px 24px; border-bottom: 1px solid #e0e0e0; }}
                .header h2 {{ margin: 0; font-size: 17px; font-weight: 600; color: #1a1a1a; }}
                .severity-tag {{ display: inline-block; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 3px; margin-right: 8px; background-color: #fdecea; color: #b71c1c; }}
                .content {{ padding: 20px 24px; }}
                .summary {{ display: flex; gap: 24px; flex-wrap: wrap; font-size: 13px; color: #555; margin-bottom: 20px; padding: 12px 16px; background-color: #fafafa; border: 1px solid #eee; border-radius: 4px; }}
                .summary-item {{ }}
                .summary-item .label {{ color: #999; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
                .summary-item .value {{ font-size: 18px; font-weight: 600; color: #1a1a1a; }}
                table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
                th {{ text-align: left; padding: 8px 12px; background-color: #f5f5f5; border-bottom: 2px solid #e0e0e0; font-weight: 600; color: #555; font-size: 11px; text-transform: uppercase; letter-spacing: 0.3px; }}
                td {{ padding: 10px 12px; border-bottom: 1px solid #f0f0f0; }}
                tr:last-child td {{ border-bottom: none; }}
                .num {{ text-align: center; font-weight: 600; }}
                .id-type {{ color: #666; font-size: 12px; white-space: nowrap; }}
                .trigger-ids {{ color: #999; font-size: 11px; max-width: 260px; word-break: break-all; }}
                .footer {{ padding: 12px 24px; border-top: 1px solid #f0f0f0; font-size: 11px; color: #aaa; }}
            </style>
        </head>
        <body>
            <div class="wrapper">
                <div class="header">
                    <h2><span class="severity-tag">P0</span>数据质量告警汇总</h2>
                </div>
                <div class="content">
                    <div class="summary">
                        <div class="summary-item">
                            <div class="label">批次</div>
                            <div class="value" style="font-size:13px;">{ctx.batch_id}</div>
                        </div>
                        <div class="summary-item">
                            <div class="label">日期</div>
                            <div class="value" style="font-size:13px;">{ctx.run_date}</div>
                        </div>
                        <div class="summary-item">
                            <div class="label">命中规则</div>
                            <div class="value">{len(alerts)}</div>
                        </div>
                        <div class="summary-item">
                            <div class="label">异常数据</div>
                            <div class="value">{total_hits}</div>
                        </div>
                    </div>
                    
                    <table>
                        <tr>
                            <th>规则 ID</th>
                            <th>说明</th>
                            <th style="text-align:center;">命中数</th>
                            <th>标识类型</th>
                            <th>触发标识</th>
                        </tr>
                        {rule_rows}
                    </table>
                </div>
                <div class="footer">生成时间: {datetime.utcnow().isoformat()}</div>
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
                <td class="num">{len(alert.trigger_cycle_ids)}</td>
                <td>{alert.title}</td>
            </tr>
            """
        
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, 'Segoe UI', Arial, sans-serif; color: #333; margin: 0; padding: 0; background-color: #f9f9f9; }}
                .wrapper {{ max-width: 640px; margin: 20px auto; background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; overflow: hidden; }}
                .header {{ padding: 16px 24px; border-bottom: 1px solid #e0e0e0; }}
                .header h2 {{ margin: 0; font-size: 17px; font-weight: 600; color: #1a1a1a; }}
                .severity-tag {{ display: inline-block; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 3px; margin-right: 8px; background-color: #fff3e0; color: #e65100; }}
                .content {{ padding: 20px 24px; }}
                .meta {{ font-size: 13px; color: #666; line-height: 1.8; margin-bottom: 16px; }}
                table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
                th {{ text-align: left; padding: 8px 12px; background-color: #f5f5f5; border-bottom: 2px solid #e0e0e0; font-weight: 600; color: #555; font-size: 11px; text-transform: uppercase; letter-spacing: 0.3px; }}
                td {{ padding: 10px 12px; border-bottom: 1px solid #f0f0f0; }}
                tr:last-child td {{ border-bottom: none; }}
                .num {{ text-align: center; font-weight: 600; }}
                .footer {{ padding: 12px 24px; border-top: 1px solid #f0f0f0; font-size: 11px; color: #aaa; }}
            </style>
        </head>
        <body>
            <div class="wrapper">
                <div class="header">
                    <h2><span class="severity-tag">P2</span>SLA 违规汇总</h2>
                </div>
                <div class="content">
                    <div class="meta">
                        <strong>批次:</strong> {ctx.batch_id} &nbsp;&middot;&nbsp;
                        <strong>日期:</strong> {ctx.run_date} &nbsp;&middot;&nbsp;
                        <strong>违规规则:</strong> {len(alerts)} &nbsp;&middot;&nbsp;
                        <strong>违规数据:</strong> {total_violations}
                    </div>
                    
                    <table>
                        <tr>
                            <th>规则 ID</th>
                            <th style="text-align:center;">违规数量</th>
                            <th>描述</th>
                        </tr>
                        {alert_items}
                    </table>
                </div>
                <div class="footer">生成时间: {datetime.utcnow().isoformat()}</div>
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
