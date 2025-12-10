
import os

class ReportGenerator:
    """
    通用 HTML 报告生成器
    用于统一各个 DAG 的邮件报警样式
    """
    
    @staticmethod
    def generate_html_report(results, title="数据质量检测报告"):
        """
        根据 LogicRunner 的结果列表生成完整的 HTML 报告
        :param results: list of dict (LogicRunner 结果)
        :param title: 报告标题
        :return: (summary_dict, html_string)
        """
        # 确保 results 是列表
        if not isinstance(results, list):
            results = [results]

        total_shards = len(results)
        failed_shards = 0
        total_violations = 0
        status = "SUCCESS"
        
        aggregated_failures = {} # {rule_name: count}
        report_details_html = []
        stats_rows_html = []
        
        for res in results:
            if res.get("status") == "FAILED":
                failed_shards += 1
                status = "FAILED"
            
            total_violations += res.get("violation_count", 0)
            
            # 聚合失败规则
            if res.get("meta_failed_rules"):
                for rule_info in res.get("meta_failed_rules", []):
                    rule_name = rule_info.get("rule", "Unknown")
                    aggregated_failures[rule_name] = aggregated_failures.get(rule_name, 0) + 1

            # 收集详细报告行 (Failed Details)
            shard_info = res.get("shard_info", "Single Batch")
            time_range = res.get("meta_time_range", "N/A")
            shard_status = res.get("status", "UNKNOWN")
            shard_violations = res.get("violation_count", 0)
            
            # 1. 收集统计指标 (无论 Passed 与否)
            if res.get("details"):
                for rule_res in res.get("details", []):
                    # 只要包含统计字段，就提取出来
                    if "mean" in rule_res and "total_samples" in rule_res:
                         rule_type = rule_res.get("type", "Stats")
                         samples_count = rule_res.get("total_samples", 0)
                         mean_val = rule_res.get("mean", "N/A")
                         std_val = rule_res.get("std_dev", "N/A")
                         
                         stats_row = f"""
                         <tr>
                            <td>{shard_info}</td>
                            <td>{rule_type}</td>
                            <td>{samples_count}</td>
                            <td>{mean_val}</td>
                            <td>{std_val}</td>
                         </tr>
                         """
                         stats_rows_html.append(stats_row)

            # 2. 生成异常详情行
            if shard_status == "FAILED" or shard_violations > 0:
                errors_html_list = []
                all_failed_rules = res.get("meta_failed_rules", [])
                
                # 前3个直接显示
                for f in all_failed_rules[:3]:
                    rule_display = f.get('rule', 'Unknown')
                    samples = f.get('samples', [])
                    if samples:
                        sample_str = ", ".join(samples).replace("<", "&lt;").replace(">", "&gt;")
                        rule_display += f" <span style='color:#777; font-size:0.85em'>[{sample_str}]</span>"
                    errors_html_list.append(f"<div>{rule_display}</div>")

                # 超过3个或是想看完整信息，放入折叠块
                if len(all_failed_rules) > 3 or any(len(f.get('samples', [])) >= 5 for f in all_failed_rules):
                    remaining_html = []
                    # 从第4个开始，或者重新列出所有（为了完整性，这里我们把所有的都放进折叠区域，或者只放剩下的）
                    # 策略：折叠区域显示"完整列表"
                    
                    full_list_html = []
                    for f in all_failed_rules:
                        r_name = f.get('rule', 'Unknown')
                        r_samples = f.get('samples', [])
                        r_s_str = ", ".join(r_samples).replace("<", "&lt;").replace(">", "&gt;")
                        full_list_html.append(f"<li><b>{r_name}</b>: {r_s_str}</li>")
                    
                    details_block = f"""
                    <details>
                        <summary>View All Violations ({len(all_failed_rules)} rules)</summary>
                        <ul style="margin:5px 0 0 15px; padding:0; font-size:0.85em; color:#555;">
                            {"".join(full_list_html)}
                        </ul>
                    </details>
                    """
                    errors_html_list.append(details_block)

                row = f"""
                <tr>
                    <td>{shard_info}</td>
                    <td>{time_range}</td>
                    <td style="color:red"><b>{shard_status}</b></td>
                    <td>{shard_violations}</td>
                    <td>{"".join(errors_html_list)}</td>
                </tr>
                """
                report_details_html.append(row)
        
        # 构造 Top Failures 摘要
        top_failures_text = "None"
        if aggregated_failures:
            top_failures_text = ", ".join([f"{k}: {v}" for k, v in aggregated_failures.items()])
        
        # 构造纯文本摘要 (用于日志或简报)
        summary_text = (
            f"Total Shards: {total_shards}\n"
            f"Failed Shards: {failed_shards}\n"
            f"Total Violations: {total_violations}\n"
            f"Top Failures: {top_failures_text}\n"
            f"Overall Status: {status}"
        )

        # --- 构造 HTML 组件 ---
        
        # A. 异常详情表
        details_table = ""
        if report_details_html:
            details_table = f"""
            <h3 style="margin-top: 20px;">异常详情 (Failures)</h3>
            <table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse; width:100%;">
                <tr style="background-color:#f2f2f2;">
                    <th>Shard/Batch</th>
                    <th>Time Range (nGen)</th>
                    <th>Status</th>
                    <th>Violations</th>
                    <th>Top Errors</th>
                </tr>
                {"".join(report_details_html)}
            </table>
            """
        else:
            details_table = "<p style='color:green; margin-top:15px;'>✅ 所有检测项均已通过 (All checks passed).</p>"
            
        # B. 统计指标表
        stats_table = ""
        if stats_rows_html:
            stats_table = f"""
            <h3 style="margin-top: 20px;">关键统计指标 (Key Statistics)</h3>
            <table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse; width:100%;">
                <tr style="background-color:#e8f4f8;">
                    <th>Shard/Batch</th>
                    <th>Metric Rule</th>
                    <th>Samples</th>
                    <th>Mean</th>
                    <th>Std Dev</th>
                </tr>
                {"".join(stats_rows_html)}
            </table>
            """

        # C. 完整 HTML 模板
        status_color = 'red' if status == 'FAILED' else 'green'
        
        full_html = f"""
        <div style="font-family: Arial, sans-serif; color: #333;">
            <h2 style="border-bottom: 2px solid {status_color}; padding-bottom: 10px;">{title}</h2>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-left: 5px solid {status_color}; margin-bottom: 20px;">
                <p><b>最终状态:</b> <span style="color: {status_color}; font-weight: bold; font-size: 1.1em;">{status}</span></p>
                <table style="width: 100%; max-width: 600px; border: none;">
                    <tr>
                        <td style="padding: 5px 0;"><b>总异常数:</b> {total_violations}</td>
                        <td style="padding: 5px 0;"><b>批次/分片:</b> {total_shards} (失败: {failed_shards})</td>
                    </tr>
                    <tr>
                        <td colspan="2" style="padding: 5px 0;"><b>高频错误:</b> {top_failures_text}</td>
                    </tr>
                </table>
            </div>

            {details_table}
            
            {stats_table}
            
            <p style="margin-top: 30px; border-top: 1px solid #eee; padding-top: 10px; color: #999; font-size: 12px;">
                Generated by Port Quality Box (Airflow) | {os.getenv('ENV', 'Production')} Environment
            </p>
        </div>
        """
        
        # 返回结构化数据供 XCom 使用
        summary_dict = {
            "status": status,
            "total_shards": total_shards,
            "failed_shards": failed_shards,
            "violation_count": total_violations,
            "top_failures": top_failures_text,
            "report_text": summary_text,
            "html_report": full_html
        }
        
        return summary_dict

