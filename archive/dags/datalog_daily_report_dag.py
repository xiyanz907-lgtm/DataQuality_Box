"""
数据质量检查日报 DAG
====================
功能：每天 UTC 08:00 发送 HTML 格式的数据质量检查汇总报告

报告内容：
1. NGen vs DataLog 对账检查统计
2. Ground Truth 逻辑验证统计
3. 失败记录明细（Top 10）
4. 失败类型分布

数据来源：
- dagster_pipelines.datalog_ngen_check_result
- dagster_pipelines.datalog_logic_check_result
- dagster_pipelines.datalog_logic_check_failed_detail

调度：每天 UTC 12:00（cron: 0 12 * * *）
收件人：env.ALERT_EMAIL_TO
"""

import os
import json
import logging
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.utils.email import send_email
from airflow.providers.mysql.hooks.mysql import MySqlHook

# ============================================================================
# 配置
# ============================================================================
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "xiyan.zhou@westwell-lab.com")
DATALOG_CONN_ID = "datalog_mysql_conn"

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================================
# DAG 定义
# ============================================================================
@dag(
    dag_id="datalog_daily_report",
    schedule="0 12 * * *",  # 每天 UTC 12:00
    start_date=pendulum.datetime(2025, 12, 18, tz="UTC"),
    catchup=False,
    tags=["report", "datalog", "daily"],
    default_args=default_args,
    description="数据质量检查日报（HTML 邮件）",
)
def datalog_daily_report():
    """
    每日数据质量检查报告生成和发送流程
    """

    @task
    def query_ngen_check_stats(**context):
        """
        查询 NGen vs DataLog 对账检查统计
        
        Returns:
            dict: {
                'vehicle_count': int,  # 检查车辆数
                'record_count': int,   # 记录数
                'has_data': bool       # 是否有数据
            }
        """
        logger = logging.getLogger("airflow.task")
        
        # 获取昨天的日期
        execution_date = context['execution_date']
        target_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"[Report] Querying NGen check stats for {target_date}")
        
        hook = MySqlHook(mysql_conn_id=DATALOG_CONN_ID)
        
        # 查询统计信息
        sql = f"""
            SELECT 
                COUNT(DISTINCT vehicle_id) as vehicle_count,
                COUNT(*) as record_count
            FROM dagster_pipelines.datalog_ngen_check_result
            WHERE shift_date = '{target_date}'
        """
        
        result = hook.get_first(sql)
        
        stats = {
            'vehicle_count': int(result[0] or 0),
            'record_count': int(result[1] or 0),
            'has_data': (result[0] or 0) > 0,
            'target_date': target_date
        }
        
        logger.info(f"[Report] NGen stats: {stats}")
        return stats

    @task
    def query_logic_check_stats(**context):
        """
        查询 Ground Truth 逻辑验证统计
        
        Returns:
            dict: {
                'vehicle_count': int,
                'total_records': int,
                'passed_records': int,
                'failed_records': int,
                'pass_rate': float,
                'has_data': bool
            }
        """
        logger = logging.getLogger("airflow.task")
        
        # 获取昨天的日期
        execution_date = context['execution_date']
        target_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"[Report] Querying Logic check stats for {target_date}")
        
        hook = MySqlHook(mysql_conn_id=DATALOG_CONN_ID)
        
        # 查询统计信息
        sql = f"""
            SELECT 
                COUNT(DISTINCT vehicle_id) as vehicle_count,
                SUM(total_records) as total_records,
                SUM(passed_records) as passed_records,
                SUM(failed_records) as failed_records
            FROM dagster_pipelines.datalog_logic_check_result
            WHERE shift_date = '{target_date}'
        """
        
        result = hook.get_first(sql)
        
        vehicle_count = int(result[0] or 0)
        total_records = int(result[1] or 0)
        passed_records = int(result[2] or 0)
        failed_records = int(result[3] or 0)
        
        # 计算通过率
        pass_rate = (passed_records / total_records * 100) if total_records > 0 else 0
        
        stats = {
            'vehicle_count': vehicle_count,
            'total_records': total_records,
            'passed_records': passed_records,
            'failed_records': failed_records,
            'pass_rate': round(pass_rate, 2),
            'has_data': vehicle_count > 0,
            'target_date': target_date
        }
        
        logger.info(f"[Report] Logic stats: {stats}")
        return stats

    @task
    def query_failure_details(**context):
        """
        查询失败记录明细
        
        Returns:
            dict: {
                'failure_count': int,
                'top_failures': list[dict],  # Top 10 失败记录
                'failure_type_stats': dict,  # 按类型统计
                'has_data': bool
            }
        """
        logger = logging.getLogger("airflow.task")
        
        # 获取昨天的日期
        execution_date = context['execution_date']
        target_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"[Report] Querying failure details for {target_date}")
        
        hook = MySqlHook(mysql_conn_id=DATALOG_CONN_ID)
        
        # 查询失败总数
        count_sql = f"""
            SELECT COUNT(*) 
            FROM dagster_pipelines.datalog_logic_check_failed_detail
            WHERE shift_date = '{target_date}'
        """
        failure_count = int(hook.get_first(count_sql)[0] or 0)
        
        # 查询 Top 10 失败记录
        top_failures = []
        if failure_count > 0:
            top_sql = f"""
                SELECT 
                    vehicle_id,
                    cycle_id,
                    failure_type,
                    expected_condition,
                    actual_value,
                    FROM_UNIXTIME(unix_timestamp) as failure_time
                FROM dagster_pipelines.datalog_logic_check_failed_detail
                WHERE shift_date = '{target_date}'
                ORDER BY unix_timestamp DESC
                LIMIT 10
            """
            
            df = hook.get_pandas_df(top_sql)
            top_failures = df.to_dict('records')
        
        # 查询按失败类型统计
        type_stats = {}
        if failure_count > 0:
            type_sql = f"""
                SELECT 
                    failure_type,
                    COUNT(*) as count
                FROM dagster_pipelines.datalog_logic_check_failed_detail
                WHERE shift_date = '{target_date}'
                GROUP BY failure_type
                ORDER BY count DESC
            """
            
            df = hook.get_pandas_df(type_sql)
            type_stats = dict(zip(df['failure_type'], df['count']))
        
        stats = {
            'failure_count': failure_count,
            'top_failures': top_failures,
            'failure_type_stats': type_stats,
            'has_data': failure_count > 0,
            'target_date': target_date
        }
        
        logger.info(f"[Report] Failure stats: failure_count={failure_count}, types={len(type_stats)}")
        return stats

    @task
    def generate_and_send_report(ngen_stats, logic_stats, failure_stats):
        """
        生成 HTML 报告并发送邮件
        """
        logger = logging.getLogger("airflow.task")
        
        target_date = ngen_stats.get('target_date', 'N/A')
        
        # 判断是否有数据
        has_any_data = (
            ngen_stats.get('has_data', False) or 
            logic_stats.get('has_data', False) or 
            failure_stats.get('has_data', False)
        )
        
        # 生成 HTML
        html_content = _generate_html_report(
            target_date, 
            ngen_stats, 
            logic_stats, 
            failure_stats,
            has_any_data
        )
        
        # 发送邮件
        subject = f"📊 数据质量检查日报 - {target_date}"
        
        if not has_any_data:
            subject = f"ℹ️  数据质量检查日报 - {target_date} (无数据)"
        elif failure_stats.get('failure_count', 0) > 0:
            subject = f"⚠️  数据质量检查日报 - {target_date} (发现 {failure_stats['failure_count']} 条异常)"
        
        logger.info(f"[Report] Sending email to {ALERT_EMAIL_TO}")
        logger.info(f"[Report] Subject: {subject}")
        
        try:
            send_email(
                to=ALERT_EMAIL_TO,
                subject=subject,
                html_content=html_content
            )
            logger.info("[Report] Email sent successfully")
        except Exception as e:
            logger.error(f"[Report] Failed to send email: {e}", exc_info=True)
            raise

    def _generate_html_report(target_date, ngen_stats, logic_stats, failure_stats, has_data):
        """
        生成 HTML 报告
        """
        
        # CSS 样式
        css = """
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .container {
                background-color: white;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                padding: 30px;
            }
            .header {
                text-align: center;
                padding-bottom: 20px;
                border-bottom: 3px solid #4CAF50;
                margin-bottom: 30px;
            }
            .header h1 {
                margin: 0;
                color: #2c3e50;
            }
            .date {
                color: #7f8c8d;
                font-size: 14px;
                margin-top: 5px;
            }
            .section {
                margin-bottom: 30px;
                padding: 20px;
                background-color: #f9f9f9;
                border-radius: 8px;
                border-left: 4px solid #3498db;
            }
            .section-title {
                font-size: 18px;
                font-weight: bold;
                color: #2c3e50;
                margin-bottom: 15px;
                display: flex;
                align-items: center;
            }
            .section-title::before {
                content: "📊";
                margin-right: 10px;
                font-size: 24px;
            }
            .stat-row {
                display: flex;
                justify-content: space-between;
                padding: 10px 0;
                border-bottom: 1px solid #ecf0f1;
            }
            .stat-row:last-child {
                border-bottom: none;
            }
            .stat-label {
                color: #7f8c8d;
            }
            .stat-value {
                font-weight: bold;
                color: #2c3e50;
            }
            .stat-value.success {
                color: #27ae60;
            }
            .stat-value.warning {
                color: #f39c12;
            }
            .stat-value.danger {
                color: #e74c3c;
            }
            .failure-table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 15px;
                font-size: 13px;
            }
            .failure-table th {
                background-color: #34495e;
                color: white;
                padding: 10px;
                text-align: left;
            }
            .failure-table td {
                padding: 8px;
                border-bottom: 1px solid #ecf0f1;
            }
            .failure-table tr:hover {
                background-color: #f5f5f5;
            }
            .badge {
                display: inline-block;
                padding: 3px 8px;
                border-radius: 12px;
                font-size: 11px;
                font-weight: bold;
            }
            .badge.speed {
                background-color: #e74c3c;
                color: white;
            }
            .badge.road {
                background-color: #f39c12;
                color: white;
            }
            .no-data {
                text-align: center;
                padding: 40px;
                color: #95a5a6;
                font-style: italic;
            }
            .footer {
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #ecf0f1;
                text-align: center;
                color: #95a5a6;
                font-size: 12px;
            }
            .progress-bar {
                height: 20px;
                background-color: #ecf0f1;
                border-radius: 10px;
                overflow: hidden;
                margin-top: 5px;
            }
            .progress-fill {
                height: 100%;
                background: linear-gradient(90deg, #27ae60, #2ecc71);
                transition: width 0.3s ease;
                display: flex;
                align-items: center;
                justify-content: center;
                color: white;
                font-size: 12px;
                font-weight: bold;
            }
        </style>
        """
        
        # 无数据提示
        if not has_data:
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                {css}
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>🎯 数据质量检查日报</h1>
                        <div class="date">{target_date}</div>
                    </div>
                    <div class="no-data">
                        <h2>ℹ️  暂无数据</h2>
                        <p>该日期没有数据质量检查记录</p>
                    </div>
                    <div class="footer">
                        Generated by Airflow DAG: datalog_daily_report
                    </div>
                </div>
            </body>
            </html>
            """
            return html
        
        # NGen 检查部分
        ngen_section = f"""
        <div class="section">
            <div class="section-title">NGen vs DataLog 对账检查</div>
            <div class="stat-row">
                <span class="stat-label">检查车辆数</span>
                <span class="stat-value">{ngen_stats.get('vehicle_count', 0)}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">检查记录数</span>
                <span class="stat-value">{ngen_stats.get('record_count', 0)}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">数据状态</span>
                <span class="stat-value success">✓ 正常</span>
            </div>
        </div>
        """
        
        # Logic 检查部分
        pass_rate = logic_stats.get('pass_rate', 0)
        pass_rate_class = 'success' if pass_rate >= 95 else ('warning' if pass_rate >= 90 else 'danger')
        
        logic_section = f"""
        <div class="section">
            <div class="section-title">Ground Truth 逻辑验证</div>
            <div class="stat-row">
                <span class="stat-label">检查车辆数</span>
                <span class="stat-value">{logic_stats.get('vehicle_count', 0)}</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">总验证记录</span>
                <span class="stat-value">{logic_stats.get('total_records', 0)} 条</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">通过记录</span>
                <span class="stat-value success">{logic_stats.get('passed_records', 0)} 条</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">失败记录</span>
                <span class="stat-value {pass_rate_class}">{logic_stats.get('failed_records', 0)} 条</span>
            </div>
            <div class="stat-row">
                <span class="stat-label">通过率</span>
                <span class="stat-value {pass_rate_class}">{pass_rate}%</span>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {pass_rate}%;">
                    {pass_rate}%
                </div>
            </div>
        </div>
        """
        
        # 失败明细部分
        failure_section = ""
        if failure_stats.get('has_data', False):
            # 失败类型统计
            type_stats_html = ""
            for failure_type, count in failure_stats.get('failure_type_stats', {}).items():
                badge_class = "speed" if "speed" in failure_type else "road"
                type_label = "速度异常" if "speed" in failure_type else "路段不匹配"
                type_stats_html += f'<div class="stat-row"><span class="stat-label">{type_label}</span><span class="stat-value"><span class="badge {badge_class}">{count} 条</span></span></div>'
            
            # Top 失败记录表格
            top_failures_html = ""
            for idx, failure in enumerate(failure_stats.get('top_failures', [])[:10], 1):
                badge_class = "speed" if "speed" in failure.get('failure_type', '') else "road"
                type_label = "速度异常" if "speed" in failure.get('failure_type', '') else "路段不匹配"
                
                top_failures_html += f"""
                <tr>
                    <td>{idx}</td>
                    <td>{failure.get('vehicle_id', 'N/A')}</td>
                    <td><span class="badge {badge_class}">{type_label}</span></td>
                    <td>{failure.get('failure_time', 'N/A')}</td>
                    <td>{failure.get('expected_condition', 'N/A')[:30]}...</td>
                </tr>
                """
            
            failure_section = f"""
            <div class="section" style="border-left-color: #e74c3c;">
                <div class="section-title" style="color: #e74c3c;">⚠️  失败明细</div>
                <div class="stat-row">
                    <span class="stat-label">失败记录总数</span>
                    <span class="stat-value danger">{failure_stats.get('failure_count', 0)} 条</span>
                </div>
                {type_stats_html}
                
                <h4 style="margin-top: 20px; color: #2c3e50;">Top 10 失败记录</h4>
                <table class="failure-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>车辆</th>
                            <th>失败类型</th>
                            <th>时间</th>
                            <th>期望条件</th>
                        </tr>
                    </thead>
                    <tbody>
                        {top_failures_html}
                    </tbody>
                </table>
            </div>
            """
        
        # 完整 HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            {css}
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🎯 数据质量检查日报</h1>
                    <div class="date">{target_date}</div>
                </div>
                
                {ngen_section}
                {logic_section}
                {failure_section}
                
                <div class="footer">
                    Generated by Airflow DAG: datalog_daily_report<br>
                    Report Time: {pendulum.now('UTC').format('YYYY-MM-DD HH:mm:ss')} UTC
                </div>
            </div>
        </body>
        </html>
        """
        
        return html

    # ========================================================================
    # DAG 流程
    # ========================================================================
    ngen_stats = query_ngen_check_stats()
    logic_stats = query_logic_check_stats()
    failure_stats = query_failure_details()
    
    generate_and_send_report(ngen_stats, logic_stats, failure_stats)


# 实例化 DAG
datalog_daily_report()

