"""
governance_main_dag.py
数据治理主 DAG

职责：
- 动态生成 Loader、Adapter、Rule、Aggregator、Dispatcher 任务
- 根据规则配置自动创建任务依赖关系

DAG 结构：
    Loader -> Adapter -> [Rule Tasks] -> Aggregator -> Dispatcher
                            ↓(并行)
                       (根据依赖关系排序)

运行时机：
- 手动触发 / 定时调度
- 传入参数: batch_id, run_date

配置文件：
- plugins/configs/sources.yaml: 数据源配置
- plugins/configs/adapters/*.yaml: 适配器配置
- plugins/configs/rules/*.yaml: 规则配置
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup

# 导入自定义算子
from plugins.operators.loader import UniversalLoaderOperator
from plugins.operators.adapter import DomainAdapterOperator
from plugins.operators.rule_engine import GenericRuleOperator
from plugins.operators.aggregator import ContextAggregatorOperator
from plugins.operators.dispatcher import NotificationDispatcherOperator

# 导入规则扫描器
from plugins.orchestration.rule_scanner import RuleScanner

# ============================================================
# DAG 默认配置
# ============================================================
default_args = {
    'owner': 'data-governance',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================
# 扫描规则（DAG 顶层代码，仅在解析时执行一次）
# ============================================================
try:
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    print(f"✅ Loaded {len(rules)} rules for governance_main_dag")
    print(scanner.get_rule_summary(rules))
except Exception as e:
    print(f"❌ Failed to scan rules: {e}")
    # 如果规则扫描失败，使用空列表（DAG 仍然可以运行 Loader 和 Adapter）
    rules = []

# ============================================================
# DAG 定义
# ============================================================
with DAG(
    dag_id='governance_main_dag',
    default_args=default_args,
    description='Port Autonomous Driving Data Governance Platform - Main DAG',
    schedule_interval=None,  # 手动触发或外部触发
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['governance', 'data-quality', 'main'],
    max_active_runs=1,  # 同时只允许一个实例运行
) as dag:
    
    # ========================================================================
    # Task 1: Universal Loader
    # 从多元异构数据源提取数据
    # ========================================================================
    loader_task = UniversalLoaderOperator(
        task_id='universal_loader',
        config_path='configs/sources.yaml',
    )
    
    # ========================================================================
    # Task 2: Domain Adapter
    # 将原始数据映射为领域实体（Cycle）
    # ========================================================================
    adapter_task = DomainAdapterOperator(
        task_id='domain_adapter',
        config_path='configs/adapters/cycle_adapter.yaml',
    )
    
    # ========================================================================
    # Task 3: Rule Tasks（动态生成）
    # 根据规则配置动态创建任务
    # ========================================================================
    if rules:
        # 使用 TaskGroup 组织规则任务
        with TaskGroup(group_id='rule_tasks') as rule_task_group:
            rule_tasks = {}
            
            # 3.1 为每个规则创建一个 Task
            for rule in rules:
                rule_task = GenericRuleOperator(
                    task_id=rule['rule_id'],
                    config_path=rule['config_path'],
                )
                rule_tasks[rule['rule_id']] = rule_task
            
            # 3.2 设置规则之间的依赖关系
            for rule in rules:
                rule_id = rule['rule_id']
                depends_on = rule.get('depends_on', [])
                
                if depends_on:
                    # 如果规则有依赖，设置上游任务
                    for dep_rule_id in depends_on:
                        if dep_rule_id in rule_tasks:
                            rule_tasks[dep_rule_id] >> rule_tasks[rule_id]
                        else:
                            print(f"⚠️ Warning: Rule '{rule_id}' depends on '{dep_rule_id}', but '{dep_rule_id}' not found!")
        
        # 3.3 设置 Adapter -> Rule Tasks 的依赖关系
        # TaskGroup 会自动处理"入口任务"的依赖
        adapter_task >> rule_task_group
        
    else:
        # 如果没有规则，创建一个占位符任务（防止 DAG 结构错误）
        print("⚠️ No rules found, skipping rule tasks")
        rule_task_group = None
    
    # ========================================================================
    # Task 4: Context Aggregator
    # 聚合所有规则执行结果
    # ========================================================================
    if rule_task_group:
        # 如果有规则任务，聚合所有规则的结果
        rule_task_ids = [rule['rule_id'] for rule in rules]
        
        aggregator_task = ContextAggregatorOperator(
            task_id='context_aggregator',
            upstream_task_ids=rule_task_ids,  # 传入所有规则任务的 task_id
        )
        
        # 设置依赖：Rule Tasks -> Aggregator
        rule_task_group >> aggregator_task
    else:
        # 如果没有规则任务，Aggregator 直接依赖 Adapter
        aggregator_task = ContextAggregatorOperator(
            task_id='context_aggregator',
            upstream_task_ids=['domain_adapter'],  # 只聚合 Adapter 的结果
        )
        
        adapter_task >> aggregator_task
    
    # ========================================================================
    # Task 5: Notification Dispatcher
    # 分发通知（P0 紧急告警 + P2 批量汇总）
    # ========================================================================
    dispatcher_task = NotificationDispatcherOperator(
        task_id='notification_dispatcher',
    )
    
    # 设置依赖：Aggregator -> Dispatcher
    aggregator_task >> dispatcher_task
    
    # ========================================================================
    # 线性依赖关系（已在上面设置）
    # Loader -> Adapter -> [Rule Tasks] -> Aggregator -> Dispatcher
    # ========================================================================
    loader_task >> adapter_task

# ============================================================
# DAG 文档
# ============================================================
dag.doc_md = """
# 数据治理主 DAG

## 功能概述
本 DAG 负责港口自动驾驶数据的全流程治理：
1. **数据提取**：从 MySQL/InfluxDB/S3 等多元异构数据源提取数据
2. **领域映射**：将原始数据映射为标准领域实体（Cycle）
3. **规则引擎**：动态执行治理规则（P0/P1/P2）
4. **结果聚合**：汇总所有规则执行结果
5. **通知分发**：发送告警和资产清单

## 运行参数
- `batch_id`: 批次标识（可选，默认自动生成）
- `run_date`: 运行日期（可选，默认为 ds）

## 配置文件
- `plugins/configs/sources.yaml`: 数据源配置
- `plugins/configs/adapters/cycle_adapter.yaml`: 领域适配器配置
- `plugins/configs/rules/*.yaml`: 规则配置

## 治理等级
- **P0 (Blocker)**: 物理/逻辑错误，立即告警
- **P1 (Asset)**: 高价值特征，打标记录
- **P2 (Violation)**: SLA 违规，批量告警

## 曳光弹场景
- P0: 时间倒挂校验 (end < start)
- P1: 双箱作业挖掘 (twin_lift == 1)
- P2: 作业超时监控 (duration > 2h)，结合 P1 豁免

## 联系方式
Owner: data-governance@example.com
"""
