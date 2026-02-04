# DAG Factory 实施总结

## 🎯 实施目标

构建一个 **DAG Factory** 系统，实现：
1. 基于 YAML 配置自动生成数据治理 DAG
2. 支持多种触发模式（CRON、DATASET、MANUAL）
3. 支持多种 Sensor 类型（SQL、FILE、TIME、EXTERNAL_TASK）
4. 基于 `target_entity` 自动关联 Adapter 和 Rules
5. 使用 Pydantic 进行严格的配置校验
6. 容错机制：跳过错误配置，不影响其他 DAG

---

## ✅ 已完成的工作

### 1. 核心代码实现

#### A. Pydantic Schema (`plugins/schemas/source_config_schema.py`)

**功能**：
- 定义完整的 Source YAML 配置 Schema
- 支持 4 种 Sensor 类型的条件校验
- 支持 3 种触发模式的校验
- 禁止未定义字段（`extra='forbid'`）

**关键类**：
```python
- SourceYAMLConfig      # 顶层配置
- SourceMetaConfig      # 数据源元信息
- SchedulingConfig      # 调度策略
- SensorConfig          # Sensor 配置（带条件校验）
- ExtractionConfig      # 数据提取配置
- DefaultArgsConfig     # DAG 默认参数
```

#### B. DAG Factory (`plugins/orchestration/dag_factory.py`)

**功能**：
- 扫描 `plugins/configs/sources/*.yaml`
- Pydantic 严格校验
- 自动生成完整 DAG 流水线：
  ```
  [Sensor] → Loader → Adapter → Rules → Aggregator → Dispatcher
  ```
- 基于 `target_entity` 自动加载：
  - Adapter: `adapters/{target_entity}_adapter.yaml`
  - Rules: 所有 `target_entity` 匹配的规则
- 错误处理：跳过无效 YAML，记录 log.error

**关键方法**：
```python
- scan_and_generate_dags()      # 扫描并生成所有 DAG
- _create_dag_from_yaml()       # 从单个 YAML 创建 DAG
- _build_task_pipeline()        # 构建完整任务流水线
- _create_sensor_task()         # 根据配置创建 Sensor
```

#### C. DAG 注册入口 (`dags/dynamic_governance_dags.py`)

**功能**：
- 调用 `register_all_dags()` 生成 DAG
- 将 DAG 对象注册到 Airflow 的 `globals()`
- 异常捕获，避免阻塞其他 DAG 文件

**代码示例**：
```python
generated_dags = register_all_dags()
for dag_id, dag_obj in generated_dags.items():
    globals()[dag_id] = dag_obj
```

---

### 2. 配置文件和示例

#### 示例 1: CRON 定时触发 (`daily_cycle_etl.yaml`)

```yaml
source_meta:
  id: "daily_cycle_etl"
  target_entity: "Cycle"

scheduling:
  trigger_mode: "CRON"
  cron_expression: "0 2 * * *"
  sensor:
    enabled: true
    type: "SQL"
    sql: "SELECT COUNT(*) >= 100 FROM ..."
    conn_id: "datalog_mysql_conn"

extractions:
  - id: "raw_cycle_section"
    source_type: "mysql"
    query: "SELECT * FROM cycle_section_summary WHERE ..."
    output_key: "raw_cycle_section"
```

**生成的 DAG**：
- DAG ID: `gov_daily_cycle_etl`
- Schedule: `0 2 * * *`
- Tasks: SqlSensor → Loader → Adapter → 3 Rules → Aggregator → Dispatcher

#### 示例 2: DATASET 事件驱动 (`asset_driven_etl.yaml`)

```yaml
scheduling:
  trigger_mode: "DATASET"
  dataset_uri: "mysql://qa_mysql_conn/auto_test_case_catalog"
  sensor:
    type: "EXTERNAL_TASK"
    external_dag_id: "gov_asset_packing"
```

**生成的 DAG**：
- DAG ID: `gov_asset_driven_etl`
- Schedule: `[Dataset('mysql://qa_mysql_conn/auto_test_case_catalog')]`
- Tasks: ExternalTaskSensor → Loader → ...

#### 示例 3: MANUAL 手动触发 (`manual_adhoc_analysis.yaml`)

```yaml
scheduling:
  trigger_mode: "MANUAL"
```

**生成的 DAG**：
- DAG ID: `gov_manual_adhoc_analysis`
- Schedule: `None`
- Tasks: Loader → Adapter → ...（无 Sensor）

---

### 3. 测试和验证

#### A. 单元测试 (`tests/test_dag_factory.py`)

**测试覆盖**：
- ✅ Pydantic Schema 校验（有效/无效配置）
- ✅ 不同 Sensor 类型的校验
- ✅ DAG Factory 生成逻辑
- ✅ 跳过无效 YAML 的行为
- ✅ 与示例配置的集成测试

**运行命令**：
```bash
pytest tests/test_dag_factory.py -v
```

#### B. 验证脚本 (`tests/validate_dag_factory.sh`)

**验证步骤**：
1. 检查容器是否运行
2. 检查配置文件存在性
3. 验证 Pydantic Schema 导入
4. 测试 DAG Factory 加载
5. 验证示例 YAML 文件
6. 检查 Airflow Import Errors
7. 列出生成的 DAG

**运行命令**：
```bash
./tests/validate_dag_factory.sh
```

---

### 4. 文档

| 文档 | 内容 |
|------|------|
| `DAG_FACTORY_IMPLEMENTATION_GUIDE.md` | 完整的实施指南（架构、配置、使用、故障排查） |
| `DAG_FACTORY_DEPLOYMENT_CHECKLIST.md` | 部署前确认清单和部署步骤 |
| `DAG_FACTORY_SUMMARY.md` | 本文件，实施总结 |

---

### 5. 依赖更新

更新 `deploy/requirements.txt`：
```
pydantic>=2.0.0
pytest>=7.0.0
```

---

## 📐 架构设计

### 核心设计原则

1. **配置驱动**：所有 DAG 配置都来自 YAML，无需编写代码
2. **类型安全**：Pydantic 在加载时就拦截错误配置
3. **自动关联**：基于 `target_entity` 自动查找 Adapter 和 Rules
4. **容错机制**：单个配置错误不影响其他 DAG
5. **可扩展性**：轻松添加新的 Sensor 类型或触发模式

### 数据流

```
YAML Config Files
     ↓
Pydantic Validation ← (错误: Skip & Log)
     ↓
DAG Factory
     ↓
├─ Load Adapter (based on target_entity)
├─ Scan Rules (based on target_entity)
└─ Build Task Pipeline
     ↓
DAG Object
     ↓
Register to globals()
     ↓
Airflow Scheduler
```

### 自动关联机制

```yaml
# source YAML
source_meta:
  target_entity: "Cycle"

# 系统行为：
# 1. 查找 adapters/cycle_adapter.yaml
# 2. 扫描 rules/*.yaml，筛选 target_entity: "Cycle"
# 3. 按依赖关系排序规则（topological sort）
# 4. 生成 TaskGroup: rule_tasks.{rule_id}
```

---

## 🔄 与现有系统的关系

### 替代方案（推荐）✅

**行动**：
```bash
# 1. 备份
cp dags/governance_main_dag.py dags/governance_main_dag.py.bak

# 2. 删除
rm dags/governance_main_dag.py

# 3. 迁移配置
# 将 sources.yaml 调整为新格式，移动到 sources/ 目录
```

**优势**：
- 架构统一，易于维护
- 所有 DAG 都是动态生成
- 配置一致性强

### 共存方案（过渡期）

**行动**：
```bash
# 保留 governance_main_dag.py
# DAG Factory 生成的 DAG 用于新数据源
```

**优势**：
- 风险低，逐步迁移
- 现有业务不受影响

---

## 📊 功能对比

| 功能 | governance_main_dag.py | DAG Factory |
|------|----------------------|------------|
| **配置方式** | 硬编码在 Python 文件中 | YAML 配置驱动 |
| **动态性** | 修改需要重启 Scheduler | 添加 YAML 即可 |
| **Sensor 支持** | 无 | 4 种类型 |
| **触发模式** | 固定 CRON | CRON/DATASET/MANUAL |
| **错误处理** | Python 错误导致 DAG 不可用 | 跳过错误配置 |
| **类型安全** | 无 | Pydantic 严格校验 |
| **扩展性** | 需要修改代码 | 添加 YAML |

---

## 🚀 部署步骤

### 1. 准备工作

```bash
# 确认目录结构
tree plugins/configs/sources/
tree plugins/configs/adapters/
tree plugins/configs/rules/
```

### 2. 重新构建镜像

```bash
cd deploy/
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### 3. 验证部署

```bash
# 运行验证脚本
./tests/validate_dag_factory.sh

# 运行单元测试
docker exec cactus_airflow_container pytest /opt/airflow/tests/test_dag_factory.py -v
```

### 4. 检查 Airflow UI

访问 http://localhost:8080，确认：
- DAG ID 以 `gov_` 开头的 DAG 存在
- 标签包含 `auto-generated`
- Schedule 配置正确

### 5. 触发测试运行

```bash
docker exec cactus_airflow_container airflow dags trigger gov_manual_adhoc_analysis
```

---

## ⚠️ 注意事项

### 1. 配置文件路径

```
plugins/configs/
├── sources/              # ✅ 数据源配置（新）
├── adapters/             # ✅ Adapter 配置（已有）
└── rules/                # ✅ 规则配置（已有）
```

### 2. 命名约定

- **Source YAML**: 任意名称（`xxx.yaml`）
- **Adapter YAML**: `{target_entity}_adapter.yaml`（小写）
- **DAG ID**: `gov_{source_meta.id}`
- **Task ID**: 固定命名（`universal_loader`, `domain_adapter`, etc.）

### 3. 必需的关联关系

```
Source YAML
  ↓ (target_entity: "Cycle")
Adapter YAML
  ↓ (cycle_adapter.yaml)
Rules YAML
  ↓ (target_entity: "Cycle")
```

**如果 Adapter 不存在**：DAG Factory 会跳过该 source，记录警告

**如果 Rules 不存在**：DAG 生成成功，但没有规则任务

### 4. Sensor 配置

- `mode: reschedule` - 推荐，释放 Worker Slot
- `mode: poke` - 占用 Worker，适合短时间等待

### 5. 错误处理

- **Pydantic 校验失败**：跳过，记录 log.error
- **Adapter 缺失**：跳过，记录 log.warning
- **Python 异常**：捕获，不阻塞其他 DAG

---

## 🎯 验证清单

### 代码完成度
- ✅ Pydantic Schema 定义完整
- ✅ DAG Factory 核心逻辑实现
- ✅ DAG 注册入口创建
- ✅ 3 个示例配置文件
- ✅ 单元测试覆盖
- ✅ 验证脚本可用
- ✅ 文档完整

### 功能验证
- ⏳ 部署到环境（待执行）
- ⏳ 验证脚本运行（待执行）
- ⏳ 单元测试通过（待执行）
- ⏳ Airflow UI 中可见 DAG（待执行）
- ⏳ 手动触发 DAG 成功（待执行）

---

## 📞 联系方式

**技术负责人**: box_admin  
**邮件**: xiyan.zhou@westwell-lab.com  

**相关文档**:
- [实施指南](DAG_FACTORY_IMPLEMENTATION_GUIDE.md)
- [部署清单](DAG_FACTORY_DEPLOYMENT_CHECKLIST.md)

---

## 🎉 总结

✅ **DAG Factory 实施已完成**

**核心价值**：
1. **简化配置**：从 Python 代码 → YAML 配置
2. **提升安全**：Pydantic 类型安全 + 错误隔离
3. **增强灵活性**：支持多种触发模式和 Sensor 类型
4. **自动化程度高**：基于 `target_entity` 自动关联
5. **可维护性强**：配置驱动，易于扩展

**下一步**：
1. 执行部署步骤
2. 运行验证脚本
3. 测试示例 DAG
4. 迁移现有配置（可选）

**预期效果**：
- 添加新数据源：只需创建一个 YAML 文件
- 修改调度策略：编辑 YAML，无需重启
- 新增规则：自动被相关 DAG 加载
