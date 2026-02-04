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

from plugins.schemas.source_config_schema import SourceYAMLConfig
from plugins.operators.loader import UniversalLoaderOperator
from plugins.operators.adapter import DomainAdapterOperator
from plugins.operators.rule_engine import GenericRuleOperator
from plugins.operators.aggregator import ContextAggregatorOperator
from plugins.operators.dispatcher import NotificationDispatcherOperator
from plugins.orchestration.rule_scanner import RuleScanner

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
    
    def _build_task_pipeline(self, config: SourceYAMLConfig, dag: DAG, sensor_config: Any):
        """
        构建完整的任务流水线
        
        Pipeline: [Sensor] -> Loader -> Adapter -> Rules -> Aggregator -> Dispatcher
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
        
        # ========== Step 3: Domain Adapter ==========
        # 根据 target_entity 自动加载 adapter 配置
        adapter_config_path = self.adapters_dir / f"{config.source_meta.target_entity.lower()}_adapter.yaml"
        
        if not adapter_config_path.exists():
            logger.warning(f"⚠️ Adapter config not found: {adapter_config_path}, skipping adapter")
            # 如果没有 adapter，后续无法执行规则，直接返回
            return
        
        adapter = DomainAdapterOperator(
            task_id="domain_adapter",
            config_path=str(adapter_config_path),
            upstream_task_ids=["universal_loader"],
            dag=dag
        )
        prev_task >> adapter
        
        # ========== Step 4: 动态规则扫描 ==========
        # 扫描所有 target_entity 匹配的规则
        all_rules = self.rule_scanner.scan_rules()
        target_rules = [
            rule for rule in all_rules 
            if rule.get('target_entity') == config.source_meta.target_entity
        ]
        
        if not target_rules:
            logger.warning(f"⚠️ No rules found for entity: {config.source_meta.target_entity}")
            return
        
        logger.info(f"📋 Found {len(target_rules)} rules for {config.source_meta.target_entity}")
        
        # 创建规则任务组
        with TaskGroup(group_id="rule_tasks", dag=dag) as rule_group:
            rule_tasks = []
            for rule in target_rules:
                rule_task = GenericRuleOperator(
                    task_id=rule['rule_id'],
                    rule_config=rule,
                    upstream_task_ids=["domain_adapter"],
                    dag=dag
                )
                rule_tasks.append(rule_task)
        
        adapter >> rule_group
        
        # ========== Step 5: Context Aggregator ==========
        aggregator = ContextAggregatorOperator(
            task_id="context_aggregator",
            rule_task_ids=[f"rule_tasks.{rule['rule_id']}" for rule in target_rules],
            dag=dag
        )
        rule_group >> aggregator
        
        # ========== Step 6: Notification Dispatcher ==========
        dispatcher = NotificationDispatcherOperator(
            task_id="notification_dispatcher",
            upstream_task_ids=["context_aggregator"],
            dag=dag
        )
        aggregator >> dispatcher
    
    def _create_sensor_task(self, sensor_config: Any, task_id: str):
        """根据 sensor 配置创建对应的 Sensor Task"""
        common_args = {
            'task_id': task_id,
            'timeout': sensor_config.timeout,
            'poke_interval': sensor_config.poke_interval,
            'mode': sensor_config.mode,
        }
        
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
