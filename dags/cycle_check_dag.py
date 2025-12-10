import os
import sys
import pendulum
from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# 确保 plugins 路径在 sys.path 中
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
plugins_dir = os.path.join(project_root, "plugins")
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

from services.logic_runner import LogicRunner

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50000))

@dag(
    dag_id="worker_cycle_check",
    schedule=None,
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    max_active_runs=1,
    tags=["kpi", "ngen", "sharding"],
    default_args={"owner": "box_admin"},
)
def worker_cycle_check():

    @task
    def get_batches(**context):
        """
        任务 1: 确定用于处理的 ID 批次。
        返回包含 'id_range' 的字典列表。
        """
        target_date = context["dag_run"].conf.get("date_filter", context["ds"])
        table_name = "cnt_cycles"
        
        runner = LogicRunner(
            cactus_conn_id="cactus_mysql_conn",
            ngen_conn_id="ngen_mysql_conn"
        )
        
        # 尝试获取 ID 边界
        min_id, max_id = runner.get_id_boundaries(table_name, target_date)
        
        batches = []
        if min_id is not None and max_id is not None:
            print(f"Found ID boundaries: {min_id} - {max_id}. Generating shards...")
            current_start = min_id
            while current_start <= max_id:
                current_end = min(current_start + BATCH_SIZE - 1, max_id)
                batches.append({"id_range": (current_start, current_end)})
                current_start += BATCH_SIZE
        else:
            # 单表模式或未找到边界（如源表为空）的兜底逻辑
            # LogicRunner 将使用日期过滤来处理 id_range 为 None 的情况
            print("No ID boundaries found or single-table mode. Using full date range.")
            batches.append({"id_range": None})
            
        print(f"Generated {len(batches)} batches.")
        return batches

    @task
    def run_check_shard(shard_config, **context):
        """
        任务 2: 对特定分片运行检查。
        """
        target_date = context["dag_run"].conf.get("date_filter", context["ds"])
        table_name = "cnt_cycles"
        id_range = shard_config.get("id_range")
        
        runner = LogicRunner(
            cactus_conn_id="cactus_mysql_conn",
            ngen_conn_id="ngen_mysql_conn"
        )
        
        result = runner.run_checks(
            table_name=table_name,
            date_filter=target_date,
            id_range=id_range
        )
        
        # 将 id_range 注入结果以便 debug
        result["shard_info"] = str(id_range)
        return result

    @task
    def summarize_results(results, **context):
        """
        任务 3: 聚合所有分片的结果。
        """
        # 显式将 LazyXComAccess 转换为 list 以避免 JSON 序列化错误
        results = list(results)
        
        total_shards = len(results)
        failed_shards = 0
        total_violations = 0
        status = "SUCCESS"
        
        report_lines = []
        
        for res in results:
            if res.get("status") == "FAILED":
                failed_shards += 1
                status = "FAILED"
            
            total_violations += res.get("violation_count", 0)
            
            # 收集每个分片的简要报告
            shard_info = res.get("shard_info", "Full Date")
            shard_status = res.get("status", "UNKNOWN")
            shard_violations = res.get("violation_count", 0)
            
            if shard_status == "FAILED" or shard_violations > 0:
                report_lines.append(f"Shard {shard_info}: {shard_status} ({shard_violations} violations)")
        
        summary_text = (
            f"Total Shards: {total_shards}\n"
            f"Failed Shards: {failed_shards}\n"
            f"Total Violations: {total_violations}\n"
            f"Overall Status: {status}\n\n"
            "--- Details ---\n" +
            "\n".join(report_lines)
        )
        
        print(summary_text)
        
        final_result = {
            "status": status,
            "total_shards": total_shards,
            "failed_shards": failed_shards,
            "violation_count": total_violations,
            "report_text": summary_text,
            "details": results # 可选: 如果分片太多，可能会导致 XCom 过大
        }
        
        # 存入 XCom 供 EmailOperator 使用
        context["ti"].xcom_push(key="qa_result", value=final_result)
        
        if status == "FAILED":
             # 仅作为标记，不阻断 Email 发送
             pass
             
        return final_result

    # 定义 DAG 结构
    batches = get_batches()
    results = run_check_shard.expand(shard_config=batches)
    summary = summarize_results(results)
    
    # 任务 4: 邮件通知
    recipients = os.getenv("ALERT_EMAIL_TO", "xiyan.zhou@westwell-lab.com")
    send_email = EmailOperator(
        task_id="send_report_email",
        to=recipients,
        subject='Cactus数据质量检测报告 ({{ dag_run.conf.get("date_filter", ds) }})',
        html_content="""
        {% set r = task_instance.xcom_pull(task_ids='summarize_results', key='qa_result') or {} %}
        <h3>数据质量检测运行完成 (Sharded)</h3>
        <p><b>状态:</b> {{ r.get('status', 'UNKNOWN') }}</p>
        <p><b>分片统计:</b> 总计 {{ r.get('total_shards') }} | 失败 {{ r.get('failed_shards') }}</p>
        <p><b>总异常数:</b> {{ r.get('violation_count') }}</p>
        <hr/>
        <pre>{{ r.get('report_text', 'No report text') }}</pre>
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    summary >> send_email

# 实例化 DAG
worker_cycle_check()
