"""
services/notification.py
通知服务

职责：
- 渲染告警邮件正文
- 调用 Airflow 的 send_email
- 降级处理（模板渲染失败时发送纯文本 JSON）

运行时机：
- NotificationDispatcherOperator 内部调用
"""
import json
from dataclasses import asdict
from typing import List, Any
from jinja2 import Template
from airflow.utils.email import send_email
from airflow.utils.log.logging_mixin import LoggingMixin

from plugins.domian.context import GovernanceContext, AlertItem


class NotificationService(LoggingMixin):
    """
    通知服务
    
    特性：
    - 支持 P0 (紧急) 和 P2 (批量) 两种模式
    - Jinja2 模板渲染
    - 降级处理：模板渲染失败时发送纯文本 JSON（确保告警不丢失）
    """
    
    def __init__(self, default_recipients: List[str] = None):
        """
        初始化
        
        Args:
            default_recipients: 默认收件人列表
        """
        self.default_recipients = default_recipients or ['alerts@example.com']
    
    def send_urgent_alert(self, alert: AlertItem, context: GovernanceContext) -> None:
        """
        发送紧急告警（P0）
        
        Args:
            alert: 告警项
            context: 治理上下文
        
        Raises:
            Exception: 邮件发送失败（由 Airflow 重试机制处理）
        """
        self.log.info(f"📧 Sending urgent alert: {alert.rule_id}")
        
        # 渲染邮件内容（带降级处理）
        subject = self._render_subject(alert, urgent=True)
        body = self._render_body(alert, context, urgent=True)
        
        # 发送邮件
        try:
            send_email(
                to=self.default_recipients,
                subject=subject,
                html_content=body
            )
            self.log.info(f"✅ Urgent alert sent: {alert.rule_id}")
        except Exception as e:
            self.log.error(f"❌ Failed to send urgent alert: {e}")
            raise  # 抛出异常，由 Operator 的重试机制处理
    
    def send_batch_summary(self, alerts: List[AlertItem], context: GovernanceContext) -> None:
        """
        发送批量汇总告警（P2）
        
        Args:
            alerts: 告警项列表
            context: 治理上下文
        
        Raises:
            Exception: 邮件发送失败（由 Airflow 重试机制处理）
        """
        if not alerts:
            self.log.info("No P2 alerts to send")
            return
        
        self.log.info(f"📧 Sending batch summary: {len(alerts)} alerts")
        
        # 渲染邮件内容（带降级处理）
        subject = self._render_batch_subject(alerts, context)
        body = self._render_batch_body(alerts, context)
        
        # 发送邮件
        try:
            send_email(
                to=self.default_recipients,
                subject=subject,
                html_content=body
            )
            self.log.info(f"✅ Batch summary sent: {len(alerts)} alerts")
        except Exception as e:
            self.log.error(f"❌ Failed to send batch summary: {e}")
            raise  # 抛出异常，由 Operator 的重试机制处理
    
    # ============================================================
    # 渲染逻辑（带降级处理）
    # ============================================================
    
    def _render_subject(self, alert: AlertItem, urgent: bool = False) -> str:
        """
        渲染邮件主题
        
        Args:
            alert: 告警项
            urgent: 是否紧急
        
        Returns:
            邮件主题
        """
        prefix = "🚨 [URGENT]" if urgent else "⚠️"
        return f"{prefix} Data Governance Alert - {alert.rule_id}"
    
    def _render_body(self, alert: AlertItem, context: GovernanceContext, urgent: bool = False) -> str:
        """
        渲染单个告警的邮件正文（带降级处理）
        
        Args:
            alert: 告警项
            context: 治理上下文
            urgent: 是否紧急
        
        Returns:
            HTML 格式邮件正文
        """
        # 准备渲染数据
        payload = {
            'alert': asdict(alert),
            'context': {
                'batch_id': context.batch_id,
                'run_date': context.run_date,
            },
            'urgent': urgent
        }
        
        # 尝试渲染模板
        try:
            template_str = self._get_single_alert_template()
            template = Template(template_str)
            return template.render(**payload)
        except Exception as e:
            # 降级：发送纯文本 JSON
            self.log.error(f"⚠️ Template render failed: {e}, falling back to plain JSON")
            return self._render_fallback_body(alert, context)
    
    def _render_batch_subject(self, alerts: List[AlertItem], context: GovernanceContext) -> str:
        """
        渲染批量汇总邮件主题
        
        Args:
            alerts: 告警项列表
            context: 治理上下文
        
        Returns:
            邮件主题
        """
        return f"📊 Data Governance Summary - {context.batch_id} ({len(alerts)} alerts)"
    
    def _render_batch_body(self, alerts: List[AlertItem], context: GovernanceContext) -> str:
        """
        渲染批量汇总邮件正文（带降级处理）
        
        Args:
            alerts: 告警项列表
            context: 治理上下文
        
        Returns:
            HTML 格式邮件正文
        """
        # 准备渲染数据
        payload = {
            'alerts': [asdict(alert) for alert in alerts],
            'context': {
                'batch_id': context.batch_id,
                'run_date': context.run_date,
            },
            'total': len(alerts)
        }
        
        # 尝试渲染模板
        try:
            template_str = self._get_batch_summary_template()
            template = Template(template_str)
            return template.render(**payload)
        except Exception as e:
            # 降级：发送纯文本 JSON
            self.log.error(f"⚠️ Template render failed: {e}, falling back to plain JSON")
            return self._render_fallback_batch_body(alerts, context)
    
    # ============================================================
    # 模板定义
    # ============================================================
    
    def _get_single_alert_template(self) -> str:
        """
        单个告警的邮件模板
        
        可用变量：
        - alert: AlertItem (dict)
        - context: {batch_id, run_date}
        - urgent: bool
        """
        return """
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; }
        .header { background-color: #d32f2f; color: white; padding: 10px; }
        .content { padding: 20px; }
        .footer { font-size: 12px; color: #666; }
    </style>
</head>
<body>
    <div class="header">
        <h2>{{ '🚨 URGENT' if urgent else '⚠️' }} Data Governance Alert</h2>
    </div>
    <div class="content">
        <p><strong>Rule ID:</strong> {{ alert.rule_id }}</p>
        <p><strong>Severity:</strong> {{ alert.severity }}</p>
        <p><strong>Title:</strong> {{ alert.title }}</p>
        <p><strong>Content:</strong> {{ alert.content }}</p>
        <p><strong>Trigger Cycle IDs:</strong> {{ alert.trigger_cycle_ids | join(', ') if alert.trigger_cycle_ids else 'N/A' }}</p>
        <p><strong>Batch ID:</strong> {{ context.batch_id }}</p>
        <p><strong>Run Date:</strong> {{ context.run_date }}</p>
    </div>
    <div class="footer">
        <p>This is an automated message from Data Governance Platform.</p>
    </div>
</body>
</html>
        """
    
    def _get_batch_summary_template(self) -> str:
        """
        批量汇总的邮件模板
        
        可用变量：
        - alerts: List[AlertItem] (list of dict)
        - context: {batch_id, run_date}
        - total: int
        """
        return """
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; }
        .header { background-color: #1976d2; color: white; padding: 10px; }
        .content { padding: 20px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .footer { font-size: 12px; color: #666; }
    </style>
</head>
<body>
    <div class="header">
        <h2>📊 Data Governance Batch Summary</h2>
    </div>
    <div class="content">
        <p><strong>Batch ID:</strong> {{ context.batch_id }}</p>
        <p><strong>Run Date:</strong> {{ context.run_date }}</p>
        <p><strong>Total Alerts:</strong> {{ total }}</p>
        
        <h3>Alert Details</h3>
        <table>
            <tr>
                <th>Rule ID</th>
                <th>Severity</th>
                <th>Title</th>
                <th>Content</th>
            </tr>
            {% for alert in alerts %}
            <tr>
                <td>{{ alert.rule_id }}</td>
                <td>{{ alert.severity }}</td>
                <td>{{ alert.title }}</td>
                <td>{{ alert.content }}</td>
            </tr>
            {% endfor %}
        </table>
    </div>
    <div class="footer">
        <p>This is an automated message from Data Governance Platform.</p>
    </div>
</body>
</html>
        """
    
    # ============================================================
    # 降级渲染（Fallback）
    # ============================================================
    
    def _render_fallback_body(self, alert: AlertItem, context: GovernanceContext) -> str:
        """
        降级渲染：纯文本 JSON（单个告警）
        
        当 Jinja2 模板渲染失败时使用，确保告警无论多丑都能发出去
        
        Args:
            alert: 告警项
            context: 治理上下文
        
        Returns:
            HTML 格式的纯文本 JSON
        """
        alert_json = json.dumps(asdict(alert), indent=2, default=str, ensure_ascii=False)
        
        return f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: monospace; }}
        .warning {{ background-color: #fff3cd; padding: 10px; border: 1px solid #ffc107; }}
        pre {{ background-color: #f5f5f5; padding: 10px; overflow: auto; }}
    </style>
</head>
<body>
    <div class="warning">
        <h2>⚠️ 告警（模板渲染失败，显示原始数据）</h2>
        <p>由于邮件模板渲染失败，系统降级为发送原始 JSON 数据。</p>
    </div>
    
    <h3>Alert Data:</h3>
    <pre>{alert_json}</pre>
    
    <h3>Context:</h3>
    <p><strong>Batch ID:</strong> {context.batch_id}</p>
    <p><strong>Run Date:</strong> {context.run_date}</p>
    
    <hr>
    <p style="font-size: 12px; color: #666;">
        This is a fallback message from Data Governance Platform.
    </p>
</body>
</html>
        """
    
    def _render_fallback_batch_body(self, alerts: List[AlertItem], context: GovernanceContext) -> str:
        """
        降级渲染：纯文本 JSON（批量汇总）
        
        Args:
            alerts: 告警项列表
            context: 治理上下文
        
        Returns:
            HTML 格式的纯文本 JSON
        """
        alerts_json = json.dumps(
            [asdict(alert) for alert in alerts],
            indent=2,
            default=str,
            ensure_ascii=False
        )
        
        return f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: monospace; }}
        .warning {{ background-color: #fff3cd; padding: 10px; border: 1px solid #ffc107; }}
        pre {{ background-color: #f5f5f5; padding: 10px; overflow: auto; }}
    </style>
</head>
<body>
    <div class="warning">
        <h2>⚠️ 批量汇总（模板渲染失败，显示原始数据）</h2>
        <p>由于邮件模板渲染失败，系统降级为发送原始 JSON 数据。</p>
    </div>
    
    <h3>Alerts Data ({len(alerts)} alerts):</h3>
    <pre>{alerts_json}</pre>
    
    <h3>Context:</h3>
    <p><strong>Batch ID:</strong> {context.batch_id}</p>
    <p><strong>Run Date:</strong> {context.run_date}</p>
    
    <hr>
    <p style="font-size: 12px; color: #666;">
        This is a fallback message from Data Governance Platform.
    </p>
</body>
</html>
        """
