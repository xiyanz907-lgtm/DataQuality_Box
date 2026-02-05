"""
Source Configuration Schema for DAG Factory
使用 Pydantic 对 sources/*.yaml 进行严格校验
"""
from typing import Optional, List, Literal, Dict, Any
from pydantic import BaseModel, Field, validator
from datetime import timedelta


class SensorConfig(BaseModel):
    """Sensor 配置"""
    enabled: bool = True
    type: Literal['SQL', 'FILE', 'TIME', 'EXTERNAL_TASK']
    timeout: int = Field(default=3600, description="Sensor超时时间(秒)")
    poke_interval: int = Field(default=60, description="轮询间隔(秒)")
    mode: Literal['poke', 'reschedule'] = 'reschedule'
    
    # 条件可选参数
    sql: Optional[str] = Field(None, description="SQL查询语句 (type=SQL时必填)")
    conn_id: Optional[str] = Field(None, description="数据库连接ID (type=SQL时必填)")
    
    path: Optional[str] = Field(None, description="文件路径 (type=FILE时必填)")
    fs_conn_id: Optional[str] = Field(None, description="文件系统连接ID (type=FILE)")
    
    wait_seconds: Optional[int] = Field(None, description="等待秒数 (type=TIME时必填)")
    
    external_dag_id: Optional[str] = Field(None, description="上游DAG ID (type=EXTERNAL_TASK时必填)")
    external_task_id: Optional[str] = Field(None, description="上游Task ID (type=EXTERNAL_TASK)")
    
    @validator('sql', always=True)
    def validate_sql_sensor(cls, v, values):
        if values.get('type') == 'SQL' and not v:
            raise ValueError("❌ type=SQL requires 'sql' field")
        return v
    
    @validator('conn_id', always=True)
    def validate_sql_conn(cls, v, values):
        if values.get('type') == 'SQL' and not v:
            raise ValueError("❌ type=SQL requires 'conn_id' field")
        return v
    
    @validator('path', always=True)
    def validate_file_sensor(cls, v, values):
        if values.get('type') == 'FILE' and not v:
            raise ValueError("❌ type=FILE requires 'path' field")
        return v
    
    @validator('wait_seconds', always=True)
    def validate_time_sensor(cls, v, values):
        if values.get('type') == 'TIME' and not v:
            raise ValueError("❌ type=TIME requires 'wait_seconds' field")
        return v
    
    @validator('external_dag_id', always=True)
    def validate_external_sensor(cls, v, values):
        if values.get('type') == 'EXTERNAL_TASK' and not v:
            raise ValueError("❌ type=EXTERNAL_TASK requires 'external_dag_id' field")
        return v


class SchedulingConfig(BaseModel):
    """调度策略配置"""
    trigger_mode: Literal['CRON', 'MANUAL', 'DATASET']
    cron_expression: Optional[str] = Field(None, description="Cron表达式 (trigger_mode=CRON时必填)")
    dataset_uri: Optional[str] = Field(None, description="Dataset URI (trigger_mode=DATASET时必填)")
    sensor: Optional[SensorConfig] = Field(None, description="Sensor配置 (可选前置条件)")
    
    @validator('cron_expression', always=True)
    def validate_cron(cls, v, values):
        if values.get('trigger_mode') == 'CRON' and not v:
            raise ValueError("❌ trigger_mode=CRON requires 'cron_expression' field")
        return v
    
    @validator('dataset_uri', always=True)
    def validate_dataset(cls, v, values):
        if values.get('trigger_mode') == 'DATASET' and not v:
            raise ValueError("❌ trigger_mode=DATASET requires 'dataset_uri' field")
        return v


class SourceMetaConfig(BaseModel):
    """数据源元信息"""
    id: str = Field(..., description="数据源唯一标识")
    name: str = Field(..., description="数据源名称")
    description: Optional[str] = None
    target_entity: str = Field(..., description="目标实体类型 (如 Cycle, Vehicle)")
    owner: Optional[str] = None
    tags: Optional[List[str]] = []


class ExtractionConfig(BaseModel):
    """单个数据提取任务配置"""
    id: str = Field(..., description="提取任务唯一标识")
    source_type: Literal['mysql', 'postgresql', 'influxdb', 's3', 'minio']
    conn_id: str = Field(..., description="Airflow Connection ID")
    query: Optional[str] = Field(None, description="SQL查询 (适用于数据库)")
    table: Optional[str] = Field(None, description="表名 (适用于数据库)")
    bucket: Optional[str] = Field(None, description="桶名 (适用于对象存储)")
    key: Optional[str] = Field(None, description="对象键 (适用于对象存储)")
    output_key: str = Field(..., description="输出到Context的key")
    alt_key: Optional[str] = Field(None, description="备用键名 (可选)")
    alt_key: Optional[str] = Field(None, description="备用key")


class DefaultArgsConfig(BaseModel):
    """DAG默认参数 (可选覆盖全局配置)"""
    owner: Optional[str] = None
    email: Optional[List[str]] = None
    email_on_failure: Optional[bool] = None
    email_on_retry: Optional[bool] = None
    retries: Optional[int] = None
    retry_delay_minutes: Optional[int] = None


class SourceYAMLConfig(BaseModel):
    """完整的 Source YAML 配置"""
    source_meta: SourceMetaConfig
    scheduling: SchedulingConfig
    extractions: List[ExtractionConfig] = Field(..., min_items=1)
    default_args: Optional[DefaultArgsConfig] = None
    
    class Config:
        extra = 'forbid'  # 禁止未定义字段


# ============= 示例配置（用于文档和测试） =============
EXAMPLE_CRON_CONFIG = """
source_meta:
  id: "daily_cycle_etl"
  name: "每日作业周期数据提取"
  description: "从MySQL提取作业周期数据并治理"
  target_entity: "Cycle"
  owner: "box_admin"
  tags: ["production", "daily"]

scheduling:
  trigger_mode: "CRON"
  cron_expression: "0 2 * * *"  # 每天凌晨2点
  sensor:
    enabled: true
    type: "SQL"
    sql: "SELECT COUNT(*) FROM cycle_section_summary WHERE DATE(end_time) = CURDATE()"
    conn_id: "datalog_mysql_conn"
    timeout: 1800
    poke_interval: 300

extractions:
  - id: "raw_cycle_section"
    source_type: "mysql"
    conn_id: "datalog_mysql_conn"
    query: |
      SELECT * FROM cycle_section_summary
      WHERE DATE(end_time) = '{{ ds }}'
    output_key: "raw_cycle_section"

  - id: "raw_vehicle_cycle"
    source_type: "mysql"
    conn_id: "datalog_mysql_conn"
    table: "subtarget_vehicle_cycle"
    output_key: "raw_vehicle_cycle"

default_args:
  retries: 3
  email: ["data-team@example.com"]
"""

EXAMPLE_DATASET_CONFIG = """
source_meta:
  id: "asset_driven_etl"
  name: "资产事件驱动的数据提取"
  target_entity: "Cycle"

scheduling:
  trigger_mode: "DATASET"
  dataset_uri: "mysql://qa_mysql_conn/auto_test_case_catalog"
  sensor:
    enabled: true
    type: "EXTERNAL_TASK"
    external_dag_id: "gov_asset_packing"
    external_task_id: "validate_packing_results"
    timeout: 7200

extractions:
  - id: "raw_packaged_assets"
    source_type: "mysql"
    conn_id: "qa_mysql_conn"
    query: "SELECT * FROM auto_test_case_catalog WHERE process_status = 'PACKAGED'"
    output_key: "raw_assets"
"""
