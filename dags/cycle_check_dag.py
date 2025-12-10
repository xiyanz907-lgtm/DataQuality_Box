import os
import sys
import pendulum
from airflow.decorators import dag, task
# from airflow.operators.email import EmailOperator
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
        支持两种模式:
        1. ID Range Mode (Source A Trigger): 直接使用传入的 ID 范围
        2. Date Mode (Source B Trigger / Manual): 根据日期查询 ID 范围
        """
        conf = context["dag_run"].conf
        mode = conf.get("mode")
        batches = []

        # --- 模式 1: ID Range Mode ---
        if mode == "id_range":
            start_id = conf.get("start_id")
            end_id = conf.get("end_id")
            print(f"🚀 Running in ID Range Mode: {start_id} - {end_id}")
            
            if start_id is not None and end_id is not None:
                current_start = int(start_id)
                final_end = int(end_id)
                while current_start <= final_end:
                    current_end = min(current_start + BATCH_SIZE - 1, final_end)
                    batches.append({"id_range": (current_start, current_end)})
                    current_start += BATCH_SIZE
            else:
                print("❌ ID Range mode missing start_id or end_id")
            
            print(f"Generated {len(batches)} batches from ID range.")
            return batches

        # --- 模式 2: Date Mode ---
        target_date = conf.get("date_filter", context["ds"])
        print(f"📅 Running in Date Mode: {target_date}")
        table_name = "cnt_cycles"
        
        runner = LogicRunner(
            cactus_conn_id="cactus_mysql_conn",
            ngen_conn_id="ngen_mysql_conn"
        )
        
        # 尝试获取 ID 边界
        min_id, max_id = runner.get_id_boundaries(table_name, target_date)
        
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
        使用 ReportGenerator 生成统一的 HTML 报告
        """
        from services.report_generator import ReportGenerator

        # 显式将 LazyXComAccess 转换为 list
        results = list(results)
        
        # 调用通用生成器
        summary_dict = ReportGenerator.generate_html_report(results, title="Cactus CycleCheck 质量检测报告")
        
        print(summary_dict["report_text"])
        
        # 存入 XCom
        context["ti"].xcom_push(key="qa_result", value=summary_dict)

        # 抛出异常以便将任务状态标记为 failed，供 global_alert_reporter 轮询
        if summary_dict.get("status") == "FAILED":
             raise ValueError(f"Data Quality Checks Failed: {summary_dict.get('violation_count')} violations found.")

        return summary_dict

    # 定义 DAG 结构
    batches = get_batches()
    results = run_check_shard.expand(shard_config=batches)
    summary = summarize_results(results)
    
    # 任务 4: 邮件通知 (已移至 global_alert_reporter)
    # recipients = os.getenv("ALERT_EMAIL_TO", "xiyan.zhou@westwell-lab.com")
    # send_email = EmailOperator(
    #     task_id="send_report_email",
    #     to=recipients,
    #     subject='[Quality] Cactus 数据质量检测报告 ({{ dag_run.conf.get("date_filter", ds) }})',
    #     html_content="""
    #     {% set r = task_instance.xcom_pull(task_ids='summarize_results', key='qa_result') or {} %}
    #     {{ r.get('html_report', 'Error generating report') }}
    #     """,
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )

    # summary >> send_email

# 实例化 DAG
worker_cycle_check()
