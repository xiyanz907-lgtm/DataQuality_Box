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
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 导入自定义算子
from plugins.operators.loader import UniversalLoaderOperator
from plugins.operators.adapter import DomainAdapterOperator
from plugins.operators.rule_engine import GenericRuleOperator
from plugins.operators.aggregator import ContextAggregatorOperator
from plugins.operators.dispatcher import NotificationDispatcherOperator

# 导入规则扫描器
from plugins.orchestration.rule_scanner import RuleScanner

# 导入 Dataset（用于触发 DAG B）
from plugins.datasets import GOVERNANCE_ASSET_DATASET

# ============================================================
# DAG 默认配置
# ============================================================
default_args = {
    'owner': 'box_admin',
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
        # ⚠️ 注意：规则任务在 TaskGroup 中，需要添加前缀
        rule_task_ids = [f"rule_tasks.{rule['rule_id']}" for rule in rules]
        
        aggregator_task = ContextAggregatorOperator(
            task_id='context_aggregator',
            upstream_task_ids=rule_task_ids,  # 传入所有规则任务的 task_id（带 TaskGroup 前缀）
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
    # Task 6: Save Assets to Queue
    # 将 P1 资产写入打包队列，触发 DAG B
    # ========================================================================
    def save_assets_to_queue(**context):
        """
        将 P1 资产标记为待打包（单表方案）
        
        逻辑：
        1. 从 Context 读取 AssetItem 列表
        2. 写入 auto_test_case_catalog 表，状态为 PENDING
        3. 触发 Dataset，启动 DAG B
        """
        import logging
        from plugins.domian.context import GovernanceContext
        
        logger = logging.getLogger(__name__)
        
        # 1. 恢复 GovernanceContext
        try:
            # 优先从 'governance_context' key 读取，fallback 到 'return_value'
            ctx_json = context['ti'].xcom_pull(task_ids='context_aggregator', key='governance_context')
            
            if not ctx_json:
                # Fallback: 尝试从 return_value 读取
                ctx_json = context['ti'].xcom_pull(task_ids='context_aggregator')
                if ctx_json:
                    logger.info("📥 Retrieved GovernanceContext from return_value (fallback)")
            else:
                logger.info("📥 Retrieved GovernanceContext from 'governance_context' key")
            
            if not ctx_json:
                logger.warning("⚠️ No GovernanceContext found in XCom, skipping asset queue write")
                logger.info("ℹ️  This is normal if no P1 assets were identified in this run")
                return
            
            ctx = GovernanceContext.from_json(ctx_json)
            assets = ctx.assets
            
            # 调试信息
            logger.info(f"📊 Context Summary: batch_id={ctx.batch_id}, "
                       f"alerts={len(ctx.alerts)}, assets={len(ctx.assets)}, "
                       f"rules_executed={len(ctx.rule_outputs)}")
            
            if not assets:
                logger.info("✅ No P1 assets identified in this run, skipping queue write")
                logger.info("ℹ️  Check rule execution logs to verify P1 rules ran successfully")
                return
            
            logger.info(f"📦 Found {len(assets)} P1 assets to save to queue:")
            for i, asset in enumerate(assets, 1):
                logger.info(f"   {i}. asset_id={asset.asset_id}, rule={asset.rule_id}, vehicle={asset.vehicle_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to restore GovernanceContext: {str(e)}")
            raise
        
        # 2. 写入数据库（单表方案：直接写入 meta 表，状态为 PENDING）
        hook = MySqlHook(mysql_conn_id='qa_mysql_conn')
        
        # SQL: 写入元数据表（初始状态 IDENTIFIED）并标记为 PENDING
        meta_insert_sql = """
            INSERT INTO auto_test_case_catalog 
            (batch_id, cycle_id, vehicle_id, shift_date, rule_version, 
             category, case_tags, severity, 
             trigger_timestamp, time_window_start, time_window_end, 
             triggered_rule_id, process_status, pack_base_path, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', %s, NOW())
            ON DUPLICATE KEY UPDATE 
                process_status = CASE 
                    WHEN process_status IN ('IDENTIFIED', 'ABANDONED') THEN 'PENDING'  -- 重置为待打包
                    ELSE process_status  -- 保持原状态（不覆盖 PACKAGED 等后续状态）
                END,
                triggered_rule_id = VALUES(triggered_rule_id),
                pack_base_path = VALUES(pack_base_path),
                pack_retry_count = 0,
                pack_error_message = NULL,
                updated_at = NOW()
        """
        
        batch_id = ctx.batch_id
        meta_success = 0
        
        for asset in assets:
            try:
                # 从 AssetItem 提取字段
                asset_id = asset.asset_id
                rule_id = asset.rule_id
                vehicle_id = asset.vehicle_id
                asset_type = asset.asset_type
                
                # 从 time_window 提取时间范围
                time_window = asset.time_window or {}
                start_time_str = time_window.get('start', datetime.now().isoformat())
                end_time_str = time_window.get('end', datetime.now().isoformat())
                
                # 解析时间字符串为 datetime 对象
                if isinstance(start_time_str, str):
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                else:
                    start_time = start_time_str
                    
                if isinstance(end_time_str, str):
                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                else:
                    end_time = end_time_str
                
                # 使用 AssetItem 中定义的 target_storage_path
                base_path = asset.target_storage_path or f'/data/assets/{rule_id}/'
                
                # 插入元数据表（单表方案：直接写入 PENDING 状态）
                import json
                hook.run(meta_insert_sql, parameters=(
                    batch_id,                           # batch_id
                    asset_id,                           # cycle_id
                    vehicle_id,                         # vehicle_id
                    start_time.date(),                  # shift_date
                    'v1.0',                             # rule_version
                    'CornerCase',                       # category
                    json.dumps(asset.tags),             # case_tags (JSON)
                    'P1',                               # severity (P1资产)
                    start_time,                         # trigger_timestamp
                    start_time,                         # time_window_start
                    end_time,                           # time_window_end
                    rule_id,                            # triggered_rule_id
                    base_path,                          # pack_base_path
                ))
                meta_success += 1
                
            except Exception as e:
                logger.error(f"❌ Failed to save asset {asset.asset_id}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                continue
        
        logger.info(f"✅ Saved to meta table (PENDING status): {meta_success}/{len(assets)} assets")
        
        # 3. 推送统计信息到 XCom
        context['ti'].xcom_push(key='assets_marked_pending', value=meta_success)
    
    save_assets_task = PythonOperator(
        task_id='save_assets_to_queue',
        python_callable=save_assets_to_queue,
        outlets=[GOVERNANCE_ASSET_DATASET],  # 声明 Dataset 输出，触发 DAG B
    )
    
    # 设置依赖：Dispatcher -> Save Assets
    dispatcher_task >> save_assets_task
    
    # ========================================================================
    # 线性依赖关系（已在上面设置）
    # Loader -> Adapter -> [Rule Tasks] -> Aggregator -> Dispatcher -> Save Assets
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
