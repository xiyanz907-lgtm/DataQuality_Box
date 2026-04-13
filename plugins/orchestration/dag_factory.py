"""
DAG Factory - 动态生成治理 DAG
根据 plugins/configs/sources/*.yaml 配置自动生成完整的数据治理 DAG
"""
import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.mysql.hooks.mysql import MySqlHook

from plugins.schemas.source_config_schema import SourceYAMLConfig
from plugins.operators.loader import UniversalLoaderOperator
from plugins.operators.adapter import DomainAdapterOperator
from plugins.operators.rule_engine import GenericRuleOperator
from plugins.operators.aggregator import ContextAggregatorOperator
from plugins.operators.report_writer import ReportWriterOperator
from plugins.operators.dispatcher import NotificationDispatcherOperator
from plugins.orchestration.rule_scanner import RuleScanner
from plugins.datasets import GOVERNANCE_ASSET_DATASET

logger = logging.getLogger(__name__)

# ============= 全局默认参数 =============
GLOBAL_DEFAULT_ARGS = {
    'owner': 'box_admin',
    'depends_on_past': False,
    'email': ['xiyan.zhou@westwell-lab.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


class DAGFactory:
    """DAG 工厂类"""
    
    def __init__(self, 
                 sources_dir: str = "/opt/airflow/plugins/configs/sources",
                 adapters_dir: str = "/opt/airflow/plugins/configs/adapters",
                 rules_dir: str = "/opt/airflow/plugins/configs/rules"):
        self.sources_dir = Path(sources_dir)
        self.adapters_dir = Path(adapters_dir)
        self.rules_dir = Path(rules_dir)
        self.rule_scanner = RuleScanner(rules_dir=str(self.rules_dir))
    
    def scan_and_generate_dags(self) -> Dict[str, DAG]:
        """
        扫描 sources/*.yaml 并生成所有 DAG
        
        Returns:
            Dict[dag_id, DAG]: 生成的 DAG 字典
        """
        generated_dags = {}
        
        if not self.sources_dir.exists():
            logger.warning(f"⚠️ Sources directory not found: {self.sources_dir}")
            return generated_dags
        
        yaml_files = list(self.sources_dir.glob("*.yaml")) + list(self.sources_dir.glob("*.yml"))
        logger.info(f"📂 Found {len(yaml_files)} source config files")
        
        for yaml_file in yaml_files:
            try:
                dag = self._create_dag_from_yaml(yaml_file)
                if dag:
                    generated_dags[dag.dag_id] = dag
                    logger.info(f"✅ Generated DAG: {dag.dag_id} from {yaml_file.name}")
            except Exception as e:
                logger.error(f"⚠️ Skip invalid YAML [{yaml_file.name}]: {e}", exc_info=True)
                continue
        
        logger.info(f"✅ Successfully generated {len(generated_dags)} DAGs")
        return generated_dags
    
    def _create_dag_from_yaml(self, yaml_file: Path) -> Optional[DAG]:
        """
        从单个 YAML 文件创建 DAG
        
        Args:
            yaml_file: YAML 配置文件路径
            
        Returns:
            DAG 对象或 None (如果校验失败)
        """
        # 1. 加载并校验 YAML
        with open(yaml_file, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)
        
        # Pydantic 严格校验
        config = SourceYAMLConfig(**raw_config)
        
        # 2. 构建 DAG ID 和 default_args
        dag_id = f"gov_{config.source_meta.id}"
        default_args = self._build_default_args(config)
        
        # 3. 构建调度配置
        schedule_config = self._build_schedule_config(config.scheduling)
        
        # 4. 创建 DAG 对象
        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=config.source_meta.description or f"Data governance for {config.source_meta.name}",
            schedule=schedule_config['schedule'],
            start_date=datetime(2026, 1, 1),
            catchup=False,
            tags=config.source_meta.tags + ['auto-generated', 'governance'],
            max_active_runs=1,
        )
        
        with dag:
            # 5. 创建任务流
            self._build_task_pipeline(
                config=config,
                dag=dag,
                sensor_config=schedule_config.get('sensor_config')
            )
        
        return dag
    
    def _build_default_args(self, config: SourceYAMLConfig) -> Dict[str, Any]:
        """合并全局和 YAML 中的 default_args"""
        args = GLOBAL_DEFAULT_ARGS.copy()
        
        if config.default_args:
            if config.default_args.owner:
                args['owner'] = config.default_args.owner
            if config.default_args.email:
                args['email'] = config.default_args.email
            if config.default_args.email_on_failure is not None:
                args['email_on_failure'] = config.default_args.email_on_failure
            if config.default_args.email_on_retry is not None:
                args['email_on_retry'] = config.default_args.email_on_retry
            if config.default_args.retries is not None:
                args['retries'] = config.default_args.retries
            if config.default_args.retry_delay_minutes:
                args['retry_delay'] = timedelta(minutes=config.default_args.retry_delay_minutes)
        
        # 覆盖 owner（优先级：YAML default_args > source_meta.owner > GLOBAL）
        if config.source_meta.owner:
            args['owner'] = config.source_meta.owner
        
        return args
    
    def _build_schedule_config(self, scheduling: Any) -> Dict[str, Any]:
        """构建调度配置"""
        result = {'sensor_config': None}
        
        if scheduling.trigger_mode == 'CRON':
            result['schedule'] = scheduling.cron_expression
        elif scheduling.trigger_mode == 'DATASET':
            result['schedule'] = [Dataset(scheduling.dataset_uri)]
        elif scheduling.trigger_mode == 'MANUAL':
            result['schedule'] = None
        
        # 保存 sensor 配置用于后续创建 Sensor Task
        if scheduling.sensor and scheduling.sensor.enabled:
            result['sensor_config'] = scheduling.sensor
        
        return result
    
    def _resolve_adapters(self, config: SourceYAMLConfig) -> List[Dict[str, str]]:
        """
        解析 Adapter 配置列表
        
        策略：
        1. 如果 YAML 中显式声明了 adapters，使用声明列表
        2. 否则按 target_entity 自动推断单 Adapter（向后兼容）
        
        Returns:
            [{'id': 'cycle', 'config_path': '/abs/path/to/cycle_adapter.yaml'}, ...]
        """
        if config.adapters:
            # 显式声明：解析相对路径为绝对路径
            result = []
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            for adapter in config.adapters:
                if os.path.isabs(adapter.config_path):
                    abs_path = adapter.config_path
                else:
                    abs_path = os.path.join(airflow_home, 'plugins', adapter.config_path)
                result.append({'id': adapter.id, 'config_path': abs_path})
            return result
        
        # 向后兼容：自动推断
        entity_name = config.source_meta.target_entity.lower()
        adapter_path = self.adapters_dir / f"{entity_name}_adapter.yaml"
        return [{'id': entity_name, 'config_path': str(adapter_path)}]
    
    def _collect_pipeline_entities(self, adapter_configs: List[Dict[str, str]]) -> set:
        """
        从 Adapter YAML 中收集 pipeline 所产出的 target_entity 集合
        
        Returns:
            {'Cycle', 'VehicleMileage', ...}
        """
        entities = set()
        for adapter_cfg in adapter_configs:
            try:
                with open(adapter_cfg['config_path'], 'r', encoding='utf-8') as f:
                    adapter_yaml = yaml.safe_load(f)
                entity_name = adapter_yaml.get('target_entity')
                if entity_name:
                    entities.add(entity_name)
            except Exception as e:
                logger.warning(f"⚠️ Cannot read adapter {adapter_cfg['config_path']}: {e}")
        return entities

    def _build_task_pipeline(self, config: SourceYAMLConfig, dag: DAG, sensor_config: Any):
        """
        构建完整的任务流水线
        
        单 Adapter:  [Sensor] -> Loader -> Adapter -> Rules -> Aggregator -> [ReportWriter] -> Dispatcher
        多 Adapter:  [Sensor] -> Loader -> [Adapter1, Adapter2] -> merge_entities -> Rules -> Aggregator -> [ReportWriter] -> Dispatcher
        """
        # ========== Step 1: 可选 Sensor ==========
        prev_task = None
        if sensor_config:
            sensor_task = self._create_sensor_task(sensor_config, task_id="data_ready_sensor")
            prev_task = sensor_task
        
        # ========== Step 2: Universal Loader ==========
        loader = UniversalLoaderOperator(
            task_id="universal_loader",
            config_dict={
                'source_meta': config.source_meta.dict(),
                'extractions': [e.dict() for e in config.extractions]
            },
            dag=dag
        )
        if prev_task:
            prev_task >> loader
        prev_task = loader
        
        # ========== Step 3: Domain Adapters (支持多 Adapter 并行) ==========
        adapter_configs = self._resolve_adapters(config)
        
        # 验证所有 adapter 配置文件存在
        valid_adapters = []
        for ac in adapter_configs:
            if os.path.exists(ac['config_path']):
                valid_adapters.append(ac)
            else:
                logger.warning(f"⚠️ Adapter config not found: {ac['config_path']}, skipping")
        
        if not valid_adapters:
            logger.warning("⚠️ No valid adapter configs found, cannot build rule pipeline")
            return
        
        # 收集 pipeline 产出的实体集合（用于规则匹配）
        pipeline_entities = self._collect_pipeline_entities(valid_adapters)
        logger.info(f"🏷️ Pipeline entities: {pipeline_entities}")
        
        if len(valid_adapters) == 1:
            # ---- 单 Adapter：向后兼容原有逻辑 ----
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            plugins_dir = os.path.join(airflow_home, 'plugins')
            rel_path = os.path.relpath(valid_adapters[0]['config_path'], plugins_dir)
            
            adapter = DomainAdapterOperator(
                task_id="domain_adapter",
                config_path=rel_path,
                upstream_task_id="universal_loader",
                dag=dag
            )
            prev_task >> adapter
            rule_upstream_task_id = "domain_adapter"
            rule_upstream_task = adapter
        else:
            # ---- 多 Adapter：并行执行 + Context 合并 ----
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            plugins_dir = os.path.join(airflow_home, 'plugins')
            adapter_task_ids = []
            
            with TaskGroup(group_id="adapters", dag=dag) as adapter_group:
                for ac in valid_adapters:
                    rel_path = os.path.relpath(ac['config_path'], plugins_dir)
                    adapter_task = DomainAdapterOperator(
                        task_id=f"adapt_{ac['id']}",
                        config_path=rel_path,
                        upstream_task_id="universal_loader",
                        dag=dag
                    )
                    adapter_task_ids.append(f"adapters.adapt_{ac['id']}")
            
            prev_task >> adapter_group
            
            # Context 合并任务：将多个 Adapter 的 Context 合并为一个
            def merge_entity_contexts(adapter_task_ids_list: List[str], **context):
                """合并多个 Adapter Context 的 data_registry"""
                from plugins.domain.context import GovernanceContext
                from plugins.infra.operators import get_multiple_upstream_contexts
                
                ti = context['task_instance']
                contexts = get_multiple_upstream_contexts(ti, adapter_task_ids_list)
                
                if not contexts:
                    raise ValueError("No upstream adapter contexts found!")
                
                # 以第一个 context 为基础，合并其他 context 的 data_registry 和 alt_key_index
                merged = contexts[0]
                for ctx in contexts[1:]:
                    merged.data_registry.update(ctx.data_registry)
                    merged._alt_key_index.update(ctx._alt_key_index)
                    merged.audit_logs.extend(ctx.audit_logs)
                
                logger.info(
                    f"✅ Merged {len(contexts)} adapter contexts: "
                    f"{list(merged.data_registry.keys())}"
                )
                return merged.to_json()
            
            merge_task = PythonOperator(
                task_id='merge_entity_contexts',
                python_callable=merge_entity_contexts,
                op_kwargs={'adapter_task_ids_list': adapter_task_ids},
                dag=dag
            )
            adapter_group >> merge_task
            rule_upstream_task_id = "merge_entity_contexts"
            rule_upstream_task = merge_task
        
        # ========== Step 4: 动态规则扫描 ==========
        # 扫描所有 target_entity 属于 pipeline_entities 的规则
        all_rules = self.rule_scanner.scan_rules()
        target_rules = [
            rule for rule in all_rules 
            if rule.get('target_entity') in pipeline_entities
        ]
        
        if not target_rules:
            logger.warning(f"⚠️ No rules found for entities: {pipeline_entities}")
            return
        
        logger.info(f"📋 Found {len(target_rules)} rules for {pipeline_entities}")
        
        # 创建规则任务组
        with TaskGroup(group_id="rule_tasks", dag=dag) as rule_group:
            rule_tasks_dict = {}  # 使用字典，方便建立依赖关系
            
            # Step 4.1: 创建所有规则任务
            for rule in target_rules:
                rule_task = GenericRuleOperator(
                    task_id=rule['rule_id'],
                    config_dict=rule['config'],  # 传递完整的 YAML 配置
                    upstream_task_id=rule_upstream_task_id,
                    dag=dag
                )
                rule_tasks_dict[rule['rule_id']] = rule_task
            
            # Step 4.2: 设置规则之间的依赖关系
            for rule in target_rules:
                rule_id = rule['rule_id']
                depends_on = rule.get('depends_on', [])
                
                if depends_on:
                    logger.info(f"📌 Setting dependencies for {rule_id}: {depends_on}")
                    for dep_rule_id in depends_on:
                        if dep_rule_id in rule_tasks_dict:
                            # 建立依赖：依赖规则 >> 当前规则
                            rule_tasks_dict[dep_rule_id] >> rule_tasks_dict[rule_id]
                        else:
                            logger.warning(f"⚠️ Rule '{rule_id}' depends on '{dep_rule_id}', but not found!")
        
        rule_upstream_task >> rule_group
        
        # ========== Step 5: Context Aggregator ==========
        aggregator = ContextAggregatorOperator(
            task_id="context_aggregator",
            rule_task_ids=[f"rule_tasks.{rule['rule_id']}" for rule in target_rules],
            rules_dir='configs/rules',  # 方案B：Aggregator 自己扫描规则目录
            dag=dag
        )
        rule_group >> aggregator
        
        # ========== Step 6: Report Writer (可选) ==========
        dispatcher_upstream_task_id = "context_aggregator"
        prev_task_before_dispatcher = aggregator

        if config.report and config.report.enabled:
            report_writer = ReportWriterOperator(
                task_id="report_writer",
                conn_id=config.report.conn_id,
                rules_dir='configs/rules',
                upstream_task_id="context_aggregator",
                dag=dag
            )
            aggregator >> report_writer
            dispatcher_upstream_task_id = "report_writer"
            prev_task_before_dispatcher = report_writer
            logger.info(f"📊 Report writer enabled: conn_id={config.report.conn_id}")
        else:
            logger.info("📭 Report writer disabled, skipping")

        # ========== Step 7: Notification Dispatcher ==========
        dispatcher_config = {}
        if config.notification:
            dispatcher_config['email_to'] = ','.join(config.notification.email_to)
            if config.notification.email_cc:
                dispatcher_config['email_cc'] = ','.join(config.notification.email_cc)
        
        dispatcher = NotificationDispatcherOperator(
            task_id="notification_dispatcher",
            config_dict=dispatcher_config if dispatcher_config else None,
            upstream_task_id=dispatcher_upstream_task_id,
            dag=dag
        )
        prev_task_before_dispatcher >> dispatcher
        
        # ========== Step 8: Save Assets to Queue (触发 DAG B) ==========
        # 根据 asset_packing 配置决定是否添加此任务
        if config.asset_packing and config.asset_packing.enabled:
            asset_config = config.asset_packing
            logger.info(f"📦 Enabling asset packing: conn_id={asset_config.conn_id}, table={asset_config.table}")
            
            def save_assets_to_queue(conn_id: str, table_name: str, **context):
                """将 P1 资产写入数据库队列，触发 DAG B"""
                from datetime import datetime
                from airflow.providers.mysql.hooks.mysql import MySqlHook
                from plugins.domain.context import GovernanceContext
                import json
                
                logger = context['task_instance'].log
                
                # 1. 从 XCom 恢复 GovernanceContext
                aggregator_ti = context['ti']
                ctx_json = aggregator_ti.xcom_pull(task_ids='context_aggregator', key='governance_context')
                
                if not ctx_json:
                    logger.warning("⚠️ No GovernanceContext found, skipping asset save")
                    return
                
                ctx = GovernanceContext.from_json(ctx_json)
                assets = ctx.assets
                
                if not assets:
                    logger.info("ℹ️ No assets to save")
                    return
                
                logger.info(f"📦 Found {len(assets)} assets to save")
                
                # 2. 连接数据库
                hook = MySqlHook(mysql_conn_id=conn_id)
                
                # SQL: 插入元数据表（单表方案）
                meta_insert_sql = f"""
                    INSERT INTO {table_name} (
                    batch_id, cycle_id, vehicle_id, shift_date, rule_version,
                    category, case_tags, severity, trigger_timestamp,
                    time_window_start, time_window_end, triggered_rule_id,
                    pack_base_path, process_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
                """
                
                # 3. 批量插入
                batch_id = ctx.batch_id or f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                meta_success = 0
                
                for asset in assets:
                    try:
                        asset_id = asset.asset_id
                        vehicle_id = asset.vehicle_id or 'UNKNOWN'
                        rule_id = asset.rule_id or 'UNKNOWN'
                        
                        # 从 time_window 提取时间范围
                        time_window = asset.time_window or {}
                        start_time_str = time_window.get('start', datetime.now().isoformat())
                        end_time_str = time_window.get('end', datetime.now().isoformat())
                        
                        # 解析时间字符串
                        if isinstance(start_time_str, str):
                            start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                        else:
                            start_time = start_time_str
                            
                        if isinstance(end_time_str, str):
                            end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                        else:
                            end_time = end_time_str
                        
                        base_path = asset.target_storage_path or f'/data/assets/{rule_id}/'
                        
                        # 插入数据库
                        hook.run(meta_insert_sql, parameters=(
                            batch_id,
                            asset_id,
                            vehicle_id,
                            start_time.date(),
                            'v1.0',
                            'CornerCase',
                            json.dumps(asset.tags),
                            'P1',
                            start_time,
                            start_time,
                            end_time,
                            rule_id,
                            base_path,
                        ))
                        meta_success += 1
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to save asset {asset.asset_id}: {str(e)}")
                        continue
                
                logger.info(f"✅ Saved {meta_success}/{len(assets)} assets to queue (PENDING status)")
                context['ti'].xcom_push(key='assets_marked_pending', value=meta_success)
            
            # 创建 PythonOperator任务
            save_assets_task = PythonOperator(
                task_id='save_assets_to_queue',
                python_callable=save_assets_to_queue,
                op_kwargs={
                    'conn_id': asset_config.conn_id,
                    'table_name': asset_config.table
                },
                outlets=[GOVERNANCE_ASSET_DATASET],  # 声明 Dataset 输出，触发 DAG B
                dag=dag
            )
            
            # 设置依赖：Dispatcher -> Save Assets
            dispatcher >> save_assets_task
        else:
            logger.info("📭 Asset packing disabled, skipping save_assets_to_queue task")
    
    def _create_sensor_task(self, sensor_config: Any, task_id: str):
        """根据 sensor 配置创建对应的 Sensor Task"""
        common_args = {
            'task_id': task_id,
            'timeout': sensor_config.timeout,
            'poke_interval': sensor_config.poke_interval,
            'mode': sensor_config.mode,
        }

        # 如果配置了 timeout_alert，注入自定义 on_failure_callback 并禁用默认邮件
        if getattr(sensor_config, 'timeout_alert', None) and sensor_config.timeout_alert.enabled:
            alert_cfg = sensor_config.timeout_alert
            common_args['on_failure_callback'] = self._build_sensor_timeout_callback(alert_cfg)
            common_args['email_on_failure'] = False

        if sensor_config.type == 'SQL':
            return SqlSensor(
                sql=sensor_config.sql,
                conn_id=sensor_config.conn_id,
                **common_args
            )

        elif sensor_config.type == 'FILE':
            return FileSensor(
                filepath=sensor_config.path,
                fs_conn_id=sensor_config.fs_conn_id or 'fs_default',
                **common_args
            )

        elif sensor_config.type == 'TIME':
            return TimeDeltaSensor(
                delta=timedelta(seconds=sensor_config.wait_seconds),
                **common_args
            )

        elif sensor_config.type == 'EXTERNAL_TASK':
            return ExternalTaskSensor(
                external_dag_id=sensor_config.external_dag_id,
                external_task_id=sensor_config.external_task_id,
                **common_args
            )

        else:
            raise ValueError(f"❌ Unsupported sensor type: {sensor_config.type}")

    @staticmethod
    def _build_sensor_timeout_callback(alert_cfg):
        """
        构建 Sensor 超时回调函数。

        超时时查询指定表的最近更新时间，发送自定义告警邮件。
        如果当天该表无数据更新，额外抄送现场项目经理。
        """
        # 闭包捕获配置值（避免序列化问题）
        check_conn_id = alert_cfg.check_conn_id
        check_table = alert_cfg.check_table
        check_ts_field = alert_cfg.check_timestamp_field
        no_update_cc_env = alert_cfg.no_update_cc_env

        def _on_sensor_timeout(context):
            from airflow.exceptions import AirflowSensorTimeout
            from airflow.providers.mysql.hooks.mysql import MySqlHook
            from airflow.utils.email import send_email

            # 只处理 Sensor 超时，其他异常不干预
            exception = context.get('exception')
            if not isinstance(exception, AirflowSensorTimeout):
                return

            task_instance = context.get('task_instance')
            dag_id = task_instance.dag_id if task_instance else 'unknown'
            ds = context.get('ds', 'unknown')
            log = task_instance.log if task_instance else logging.getLogger(__name__)

            # 1. 查询目标表的最近更新时间
            try:
                hook = MySqlHook(mysql_conn_id=check_conn_id)
                max_ts_row = hook.get_first(
                    f"SELECT MAX({check_ts_field}) FROM {check_table}"
                )
                last_update_ts = max_ts_row[0] if max_ts_row and max_ts_row[0] else None
            except Exception as e:
                log.error(f"Failed to query {check_table}.{check_ts_field}: {e}")
                last_update_ts = None

            # 2. 判断当天是否有数据更新
            has_today_data = False
            try:
                if last_update_ts is not None:
                    today_count_row = hook.get_first(
                        f"SELECT COUNT(*) FROM {check_table} "
                        f"WHERE DATE({check_ts_field}) = CURDATE()"
                    )
                    has_today_data = (today_count_row[0] or 0) > 0
            except Exception as e:
                log.error(f"Failed to check today's data in {check_table}: {e}")

            # 3. 构建邮件
            site = os.getenv('SITE_NAME', '')
            site_tag = f"[{site}] " if site else ""
            last_update_display = str(last_update_ts) if last_update_ts else "无记录"
            today_status = "有更新" if has_today_data else "当天无更新"

            subject = f"{site_tag}Sensor 超时告警 - {dag_id} ({ds})"

            body = f"""
            <html><body>
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <h3 style="color: #d35400;">Sensor 等待超时</h3>
                <p>DAG <strong>{dag_id}</strong> 的数据就绪传感器在等待 <strong>{ds}</strong> 的数据时超时。</p>

                <table style="border-collapse: collapse; width: 100%; margin: 16px 0;">
                    <tr style="background: #f8f9fa;">
                        <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-weight: bold;">
                            {check_table} 最近更新时间
                        </td>
                        <td style="padding: 8px 12px; border: 1px solid #dee2e6;">
                            {last_update_display}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-weight: bold;">
                            当天数据状态
                        </td>
                        <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: {'#27ae60' if has_today_data else '#e74c3c'};">
                            {today_status}
                        </td>
                    </tr>
                </table>

                {"<p style='color: #e74c3c; font-weight: bold;'>当天无数据更新，请确认是当天没有作业还是数据未上传。</p>" if not has_today_data else ""}

                <hr style="border: none; border-top: 1px solid #eee; margin: 16px 0;"/>
                <p style="color: #999; font-size: 12px;">Generated by {dag_id} / data_ready_sensor</p>
            </div>
            </body></html>
            """

            # 4. 确定收件人和抄送
            email_to = os.getenv('ALERT_EMAIL_TO', 'xiyan.zhou@westwell-lab.com')
            email_to = [e.strip() for e in email_to.split(',')]

            email_cc = None
            if not has_today_data and no_update_cc_env:
                cc_addr = os.getenv(no_update_cc_env, '')
                if cc_addr:
                    email_cc = [e.strip() for e in cc_addr.split(',')]

            # 5. 发送
            try:
                send_email(to=email_to, subject=subject, html_content=body, cc=email_cc)
                cc_info = f", CC: {email_cc}" if email_cc else ""
                log.info(f"Sensor timeout alert sent to {email_to}{cc_info}")
            except Exception as e:
                log.error(f"Failed to send sensor timeout alert: {e}")

        return _on_sensor_timeout


# ============= DAG 注册入口 =============
def register_all_dags() -> Dict[str, DAG]:
    """
    扫描并注册所有动态生成的 DAG
    此函数会被 Airflow Scheduler 调用
    
    Returns:
        Dict[dag_id, DAG]: 生成的所有 DAG
    """
    factory = DAGFactory()
    return factory.scan_and_generate_dags()
