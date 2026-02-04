"""
DAG Factory 单元测试
测试动态 DAG 生成功能
"""
import os
import sys
import pytest
import yaml
from pathlib import Path

# 添加项目路径
sys.path.insert(0, '/opt/airflow')

from plugins.schemas.source_config_schema import (
    SourceYAMLConfig, 
    SourceMetaConfig, 
    SchedulingConfig,
    SensorConfig,
    ExtractionConfig
)
from plugins.orchestration.dag_factory import DAGFactory


class TestSourceConfigSchema:
    """测试 Pydantic Schema 校验"""
    
    def test_valid_cron_config(self):
        """测试有效的 CRON 配置"""
        config_dict = {
            'source_meta': {
                'id': 'test_source',
                'name': 'Test Source',
                'target_entity': 'Cycle'
            },
            'scheduling': {
                'trigger_mode': 'CRON',
                'cron_expression': '0 2 * * *'
            },
            'extractions': [
                {
                    'id': 'extract1',
                    'source_type': 'mysql',
                    'conn_id': 'test_conn',
                    'query': 'SELECT * FROM test',
                    'output_key': 'raw_data'
                }
            ]
        }
        
        config = SourceYAMLConfig(**config_dict)
        assert config.source_meta.id == 'test_source'
        assert config.scheduling.trigger_mode == 'CRON'
        assert config.scheduling.cron_expression == '0 2 * * *'
    
    def test_invalid_cron_missing_expression(self):
        """测试 CRON 模式缺少 cron_expression 字段"""
        config_dict = {
            'source_meta': {
                'id': 'test_source',
                'name': 'Test Source',
                'target_entity': 'Cycle'
            },
            'scheduling': {
                'trigger_mode': 'CRON'
                # 缺少 cron_expression
            },
            'extractions': [
                {
                    'id': 'extract1',
                    'source_type': 'mysql',
                    'conn_id': 'test_conn',
                    'query': 'SELECT * FROM test',
                    'output_key': 'raw_data'
                }
            ]
        }
        
        with pytest.raises(ValueError, match="trigger_mode=CRON requires 'cron_expression'"):
            SourceYAMLConfig(**config_dict)
    
    def test_valid_sql_sensor(self):
        """测试有效的 SQL Sensor 配置"""
        sensor_dict = {
            'enabled': True,
            'type': 'SQL',
            'sql': 'SELECT 1',
            'conn_id': 'mysql_conn',
            'timeout': 3600,
            'poke_interval': 60
        }
        
        sensor = SensorConfig(**sensor_dict)
        assert sensor.type == 'SQL'
        assert sensor.sql == 'SELECT 1'
        assert sensor.conn_id == 'mysql_conn'
    
    def test_invalid_sql_sensor_missing_fields(self):
        """测试 SQL Sensor 缺少必填字段"""
        sensor_dict = {
            'enabled': True,
            'type': 'SQL',
            # 缺少 sql 和 conn_id
        }
        
        with pytest.raises(ValueError):
            SensorConfig(**sensor_dict)
    
    def test_valid_dataset_config(self):
        """测试有效的 DATASET 配置"""
        config_dict = {
            'source_meta': {
                'id': 'dataset_test',
                'name': 'Dataset Test',
                'target_entity': 'Cycle'
            },
            'scheduling': {
                'trigger_mode': 'DATASET',
                'dataset_uri': 'mysql://conn/table'
            },
            'extractions': [
                {
                    'id': 'extract1',
                    'source_type': 'mysql',
                    'conn_id': 'test_conn',
                    'query': 'SELECT * FROM test',
                    'output_key': 'raw_data'
                }
            ]
        }
        
        config = SourceYAMLConfig(**config_dict)
        assert config.scheduling.trigger_mode == 'DATASET'
        assert config.scheduling.dataset_uri == 'mysql://conn/table'
    
    def test_extra_fields_forbidden(self):
        """测试禁止未定义字段"""
        config_dict = {
            'source_meta': {
                'id': 'test_source',
                'name': 'Test Source',
                'target_entity': 'Cycle'
            },
            'scheduling': {
                'trigger_mode': 'MANUAL'
            },
            'extractions': [
                {
                    'id': 'extract1',
                    'source_type': 'mysql',
                    'conn_id': 'test_conn',
                    'query': 'SELECT * FROM test',
                    'output_key': 'raw_data'
                }
            ],
            'unknown_field': 'should fail'  # 未定义字段
        }
        
        with pytest.raises(ValueError, match="extra fields not permitted"):
            SourceYAMLConfig(**config_dict)


class TestDAGFactory:
    """测试 DAG Factory 生成逻辑"""
    
    @pytest.fixture
    def factory(self, tmp_path):
        """创建临时 Factory 实例"""
        sources_dir = tmp_path / "sources"
        adapters_dir = tmp_path / "adapters"
        rules_dir = tmp_path / "rules"
        
        sources_dir.mkdir()
        adapters_dir.mkdir()
        rules_dir.mkdir()
        
        return DAGFactory(
            sources_dir=str(sources_dir),
            adapters_dir=str(adapters_dir),
            rules_dir=str(rules_dir)
        )
    
    def test_scan_empty_directory(self, factory):
        """测试扫描空目录"""
        dags = factory.scan_and_generate_dags()
        assert len(dags) == 0
    
    def test_generate_single_dag(self, factory, tmp_path):
        """测试生成单个 DAG"""
        # 创建有效的 source YAML
        source_config = {
            'source_meta': {
                'id': 'test_dag',
                'name': 'Test DAG',
                'target_entity': 'Cycle'
            },
            'scheduling': {
                'trigger_mode': 'MANUAL'
            },
            'extractions': [
                {
                    'id': 'extract1',
                    'source_type': 'mysql',
                    'conn_id': 'test_conn',
                    'query': 'SELECT * FROM test',
                    'output_key': 'raw_data'
                }
            ]
        }
        
        # 写入临时文件
        yaml_file = tmp_path / "sources" / "test_dag.yaml"
        with open(yaml_file, 'w') as f:
            yaml.dump(source_config, f)
        
        # 创建必需的 adapter 文件（否则 DAG 生成会跳过）
        adapter_file = tmp_path / "adapters" / "cycle_adapter.yaml"
        adapter_config = {
            'adapter_meta': {'id': 'cycle_adapter'},
            'inputs': {'primary': 'raw_data'},
            'field_mapping': {}
        }
        with open(adapter_file, 'w') as f:
            yaml.dump(adapter_config, f)
        
        # 生成 DAG
        dags = factory.scan_and_generate_dags()
        
        # 验证
        assert len(dags) == 1
        assert 'gov_test_dag' in dags
        dag = dags['gov_test_dag']
        assert dag.dag_id == 'gov_test_dag'
        assert dag.schedule_interval is None  # MANUAL 模式
    
    def test_skip_invalid_yaml(self, factory, tmp_path):
        """测试跳过无效的 YAML 文件"""
        # 创建一个有效的配置
        valid_config = {
            'source_meta': {
                'id': 'valid_dag',
                'name': 'Valid DAG',
                'target_entity': 'Cycle'
            },
            'scheduling': {
                'trigger_mode': 'MANUAL'
            },
            'extractions': [
                {
                    'id': 'extract1',
                    'source_type': 'mysql',
                    'conn_id': 'test_conn',
                    'query': 'SELECT * FROM test',
                    'output_key': 'raw_data'
                }
            ]
        }
        
        # 创建一个无效的配置（缺少必填字段）
        invalid_config = {
            'source_meta': {
                'id': 'invalid_dag',
                'name': 'Invalid DAG'
                # 缺少 target_entity
            },
            'scheduling': {
                'trigger_mode': 'MANUAL'
            },
            'extractions': []  # 空列表，违反 min_items=1
        }
        
        # 写入文件
        valid_file = tmp_path / "sources" / "valid.yaml"
        with open(valid_file, 'w') as f:
            yaml.dump(valid_config, f)
        
        invalid_file = tmp_path / "sources" / "invalid.yaml"
        with open(invalid_file, 'w') as f:
            yaml.dump(invalid_config, f)
        
        # 创建 adapter
        adapter_file = tmp_path / "adapters" / "cycle_adapter.yaml"
        with open(adapter_file, 'w') as f:
            yaml.dump({'adapter_meta': {'id': 'cycle_adapter'}}, f)
        
        # 生成 DAG（应该只生成有效的，跳过无效的）
        dags = factory.scan_and_generate_dags()
        
        # 验证：只有有效的 DAG 被生成
        assert len(dags) == 1
        assert 'gov_valid_dag' in dags
        assert 'gov_invalid_dag' not in dags


class TestIntegrationWithExamples:
    """测试与示例配置文件的集成"""
    
    def test_load_daily_cycle_etl(self):
        """测试加载 daily_cycle_etl.yaml"""
        yaml_path = Path("/opt/airflow/plugins/configs/sources/daily_cycle_etl.yaml")
        
        if not yaml_path.exists():
            pytest.skip("Example file not found")
        
        with open(yaml_path, 'r') as f:
            raw_config = yaml.safe_load(f)
        
        # Pydantic 校验
        config = SourceYAMLConfig(**raw_config)
        
        assert config.source_meta.id == 'daily_cycle_etl'
        assert config.scheduling.trigger_mode == 'CRON'
        assert config.scheduling.cron_expression == '0 2 * * *'
        assert len(config.extractions) == 2
    
    def test_load_asset_driven_etl(self):
        """测试加载 asset_driven_etl.yaml"""
        yaml_path = Path("/opt/airflow/plugins/configs/sources/asset_driven_etl.yaml")
        
        if not yaml_path.exists():
            pytest.skip("Example file not found")
        
        with open(yaml_path, 'r') as f:
            raw_config = yaml.safe_load(f)
        
        config = SourceYAMLConfig(**raw_config)
        
        assert config.source_meta.id == 'asset_driven_etl'
        assert config.scheduling.trigger_mode == 'DATASET'
        assert 'auto_test_case_catalog' in config.scheduling.dataset_uri


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
