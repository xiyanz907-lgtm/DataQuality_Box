# DAG Factory 实施指南

## 📋 概述

DAG Factory 是一个动态 DAG 生成系统，通过扫描 `plugins/configs/sources/*.yaml` 配置文件自动生成完整的数据治理 DAG。

### 核心特性

✅ **配置驱动**：通过 YAML 定义数据源、调度策略、Sensor配置  
✅ **类型安全**：使用 Pydantic 进行严格的 Schema 校验  
✅ **自动关联**：基于 `target_entity` 自动加载 Adapter 和 Rules  
✅ **完整流水线**：自动生成 Sensor → Loader → Adapter → Rules → Aggregator → Dispatcher  
✅ **容错机制**：跳过错误配置，不阻塞其他 DAG 加载  

---

## 🏗️ 架构设计

### 1. 核心组件

```
plugins/
├── schemas/
│   └── source_config_schema.py       # Pydantic Schema 定义
├── orchestration/
│   ├── dag_factory.py                # DAG Factory 核心逻辑
│   └── rule_scanner.py               # 规则扫描器（复用）
└── configs/
    ├── sources/                      # 数据源配置目录 ✨ 新增
    │   ├── daily_cycle_etl.yaml
    │   ├── asset_driven_etl.yaml
    │   └── manual_adhoc_analysis.yaml
    ├── adapters/                     # Adapter 配置（已有）
    │   └── cycle_adapter.yaml
    └── rules/                        # 规则配置（已有）
        ├── p0_time_check.yaml
        ├── p1_twin_lift.yaml
        └── p2_timeout.yaml

dags/
└── dynamic_governance_dags.py        # DAG 注册入口 ✨ 新增
```

### 2. 数据流

```
YAML Config → Pydantic Validation → DAG Factory → DAG Object → Airflow Globals
     ↓                ↓                   ↓              ↓            ↓
  Schema 校验      类型检查          Task 编排      依赖关系     Scheduler 识别
```

### 3. 自动关联机制

```yaml
# source YAML
source_meta:
  target_entity: "Cycle"  # 声明目标实体

# 系统自动行为：
# 1. 加载 adapters/cycle_adapter.yaml
# 2. 扫描 rules/*.yaml 中所有 target_entity: "Cycle" 的规则
# 3. 生成完整的 Task 依赖链
```

---

## 📝 配置文件详解

### Source YAML Schema

```yaml
# ===== 数据源元信息 =====
source_meta:
  id: "unique_source_id"              # 必填，会转换为 DAG ID: gov_{id}
  name: "数据源名称"                   # 必填
  description: "描述信息"              # 可选
  target_entity: "Cycle"              # 必填，决定 Adapter 和 Rules 的加载
  owner: "box_admin"                  # 可选，覆盖全局 owner
  tags: ["tag1", "tag2"]              # 可选

# ===== 调度策略 =====
scheduling:
  trigger_mode: "CRON"                # 必填，可选值: CRON/MANUAL/DATASET
  
  # trigger_mode=CRON 时必填
  cron_expression: "0 2 * * *"
  
  # trigger_mode=DATASET 时必填
  dataset_uri: "mysql://conn_id/table_name"
  
  # 可选：前置 Sensor
  sensor:
    enabled: true                     # 是否启用
    type: "SQL"                       # SQL/FILE/TIME/EXTERNAL_TASK
    timeout: 3600                     # 超时时间（秒）
    poke_interval: 60                 # 轮询间隔（秒）
    mode: "reschedule"                # poke/reschedule
    
    # 根据 type 不同，填写对应字段
    sql: "SELECT COUNT(*) > 100 FROM table"  # type=SQL
    conn_id: "mysql_conn"                    # type=SQL
    
    path: "/data/flag.txt"                   # type=FILE
    fs_conn_id: "fs_conn"                    # type=FILE（可选）
    
    wait_seconds: 300                        # type=TIME
    
    external_dag_id: "upstream_dag"          # type=EXTERNAL_TASK
    external_task_id: "task_id"              # type=EXTERNAL_TASK（可选）

# ===== 数据提取配置 =====
extractions:                          # 必填，至少1个
  - id: "raw_data_1"                  # 提取任务唯一标识
    source_type: "mysql"              # mysql/postgresql/influxdb/s3/minio
    conn_id: "mysql_conn_id"          # Airflow Connection ID
    query: "SELECT * FROM table"      # SQL 查询（支持 Jinja2 模板）
    output_key: "raw_table_1"         # 输出到 Context 的 key

  - id: "raw_data_2"
    source_type: "mysql"
    conn_id: "mysql_conn_id"
    table: "table_name"               # 或直接指定表名
    output_key: "raw_table_2"

# ===== 可选：覆盖全局 default_args =====
default_args:
  owner: "custom_owner"               # 覆盖 source_meta.owner
  email: ["team@example.com"]
  email_on_failure: true
  email_on_retry: false
  retries: 3
  retry_delay_minutes: 10
```

### Sensor 类型详解

| Type | 用途 | 必填参数 | 可选参数 |
|------|------|---------|---------|
| **SQL** | 检查数据库条件 | `sql`, `conn_id` | `mode` |
| **FILE** | 检查文件存在性 | `path` | `fs_conn_id`, `mode` |
| **TIME** | 硬等待一段时间 | `wait_seconds` | - |
| **EXTERNAL_TASK** | 等待其他 DAG 完成 | `external_dag_id` | `external_task_id`, `mode` |

---

## 🚀 使用指南

### Step 1: 创建 Source 配置文件

```bash
# 在 plugins/configs/sources/ 下创建新的 YAML 文件
cd /opt/airflow/plugins/configs/sources/
vim my_new_source.yaml
```

参考示例：
- `daily_cycle_etl.yaml` - CRON 定时触发
- `asset_driven_etl.yaml` - DATASET 事件驱动
- `manual_adhoc_analysis.yaml` - MANUAL 手动触发

### Step 2: 配置 Adapter（如果是新实体）

如果你的 `target_entity` 是新类型（如 `Vehicle`），需要创建对应的 Adapter：

```bash
vim /opt/airflow/plugins/configs/adapters/vehicle_adapter.yaml
```

如果是已有的 `Cycle` 实体，则会自动复用 `cycle_adapter.yaml`。

### Step 3: 配置 Rules（可选）

在 `plugins/configs/rules/` 下创建规则文件，指定 `target_entity`:

```yaml
rule_meta:
  rule_id: "rule_vehicle_speed_check"
  target_entity: "Vehicle"  # 匹配 source 的 target_entity
  severity: "P0"

filter_expr: "speed > 100"
# ...
```

### Step 4: 校验配置

```bash
# 运行测试验证配置正确性
docker exec -it cactus_airflow_container pytest /opt/airflow/tests/test_dag_factory.py -v
```

### Step 5: 重启 Airflow Scheduler

```bash
docker-compose restart airflow
```

### Step 6: 验证 DAG 生成

在 Airflow UI 中查看：
- DAG ID: `gov_{source_meta.id}`
- 标签: `auto-generated`, `governance`, `{custom_tags}`

---

## 🧪 测试

### 运行单元测试

```bash
# 测试 Pydantic Schema 校验
pytest tests/test_dag_factory.py::TestSourceConfigSchema -v

# 测试 DAG Factory 生成逻辑
pytest tests/test_dag_factory.py::TestDAGFactory -v

# 测试集成（加载示例配置）
pytest tests/test_dag_factory.py::TestIntegrationWithExamples -v
```

### 手动测试

```python
# 在 Airflow Container 中执行
docker exec -it cactus_airflow_container python

>>> from plugins.orchestration.dag_factory import DAGFactory
>>> factory = DAGFactory()
>>> dags = factory.scan_and_generate_dags()
>>> print(f"Generated {len(dags)} DAGs")
>>> for dag_id, dag in dags.items():
...     print(f"  - {dag_id}: {len(dag.tasks)} tasks")
```

---

## ⚠️ 注意事项

### 1. 配置文件错误处理

- ❌ **错误配置不会阻塞其他 DAG 加载**
- 📋 错误信息会记录到 Airflow Log
- 🔍 在 Airflow UI 的 "Import Errors" 中查看详细错误

### 2. Pydantic 校验规则

```python
# 以下配置会被拒绝：
# 1. trigger_mode=CRON 但缺少 cron_expression
# 2. sensor.type=SQL 但缺少 sql 或 conn_id
# 3. extractions 为空列表
# 4. 包含未定义字段（extra='forbid'）
```

### 3. 命名规范

- **DAG ID**: `gov_{source_meta.id}`
- **Task ID**: 
  - Sensor: `data_ready_sensor`
  - Loader: `universal_loader`
  - Adapter: `domain_adapter`
  - Rules: `rule_tasks.{rule_id}`
  - Aggregator: `context_aggregator`
  - Dispatcher: `notification_dispatcher`

### 4. 性能优化建议

- 📂 **大量 YAML 文件时**：按业务域分组，使用 `tags` 过滤
- ⏱️ **Sensor 配置**：使用 `mode: reschedule` 释放 Worker Slot
- 🔄 **规则数量控制**：每个实体建议不超过 50 个规则

### 5. 与现有 DAG 的关系

#### 方案 A: 完全替代（推荐）✅

```bash
# 删除旧的 governance_main_dag.py
rm /opt/airflow/dags/governance_main_dag.py

# 将 sources.yaml 迁移到 sources/ 目录
mv /opt/airflow/plugins/configs/sources.yaml \
   /opt/airflow/plugins/configs/sources/legacy_governance.yaml
```

#### 方案 B: 共存（过渡期）

- 保留 `governance_main_dag.py` 用于核心业务
- DAG Factory 生成的 DAG 用于新增数据源

---

## 🔧 故障排查

### 问题 1: DAG 没有出现在 UI 中

**检查步骤**：
```bash
# 1. 查看 Airflow Scheduler 日志
docker logs cactus_airflow_container --tail=100 | grep "dag_factory"

# 2. 检查 Import Errors
# 在 Airflow UI: Admin -> Import Errors

# 3. 手动触发加载
docker exec -it cactus_airflow_container python /opt/airflow/dags/dynamic_governance_dags.py
```

### 问题 2: Pydantic 校验失败

**解决方案**：
```bash
# 使用 Python 直接校验 YAML
docker exec -it cactus_airflow_container python

>>> import yaml
>>> from plugins.schemas.source_config_schema import SourceYAMLConfig
>>> with open('/opt/airflow/plugins/configs/sources/my_source.yaml') as f:
...     config = yaml.safe_load(f)
>>> SourceYAMLConfig(**config)  # 会显示详细错误信息
```

### 问题 3: Adapter 或 Rules 未找到

**排查**：
```bash
# 检查 target_entity 是否匹配
grep -r "target_entity" /opt/airflow/plugins/configs/

# 应该在以下文件中一致：
# - sources/{source}.yaml: target_entity: "Cycle"
# - adapters/cycle_adapter.yaml: 文件名与 target_entity 小写一致
# - rules/*.yaml: target_entity: "Cycle"
```

---

## 📊 监控和运维

### 生成的 DAG 数量统计

```sql
-- 在 Airflow Metadata DB 中查询
SELECT 
    COUNT(*) AS total_dags,
    SUM(CASE WHEN dag_id LIKE 'gov_%' THEN 1 ELSE 0 END) AS generated_dags
FROM dag;
```

### 配置文件变更跟踪

```bash
# 建议将 sources/ 目录纳入版本控制
git add plugins/configs/sources/
git commit -m "feat: add new data source config"
```

---

## 📚 附录

### A. 完整示例配置

参见：
- `plugins/configs/sources/daily_cycle_etl.yaml`
- `plugins/configs/sources/asset_driven_etl.yaml`
- `plugins/configs/sources/manual_adhoc_analysis.yaml`

### B. Pydantic Schema 完整定义

参见：`plugins/schemas/source_config_schema.py`

### C. 全局默认参数

```python
# plugins/orchestration/dag_factory.py
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
```

修改全局参数：编辑 `dag_factory.py` 中的 `GLOBAL_DEFAULT_ARGS`

---

## 🎯 下一步行动

### Phase 1: 验证核心功能（本次实施）
- ✅ 创建 Pydantic Schema
- ✅ 实现 DAG Factory
- ✅ 创建示例配置
- ✅ 编写单元测试
- ⏳ **执行测试验证** ← 当前步骤

### Phase 2: 迁移现有 DAG（推荐）
- 将 `governance_main_dag.py` 的配置迁移到 YAML
- 删除硬编码的 DAG 文件
- 验证功能等价性

### Phase 3: 扩展功能（可选）
- 支持更多 Sensor 类型（HttpSensor, S3KeySensor）
- 支持 DAG 级别的 SLA 配置
- 支持动态生成 TaskGroup（多阶段流水线）

---

## 📞 支持

如有问题，请联系：
- **技术负责人**: box_admin
- **邮件**: xiyan.zhou@westwell-lab.com
- **文档**: 本文件
