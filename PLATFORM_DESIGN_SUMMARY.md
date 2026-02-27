# 港口自动驾驶数据治理平台 - 设计总结

## 📋 目录

1. [核心设计理念](#核心设计理念)
2. [架构设计](#架构设计)
3. [数据流设计](#数据流设计)
4. [核心组件设计](#核心组件设计)
5. [配置系统设计](#配置系统设计)
6. [存储策略设计](#存储策略设计)
7. [错误处理设计](#错误处理设计)
8. [扩展性设计](#扩展性设计)
9. [性能优化设计](#性能优化设计)
10. [运维设计](#运维设计)

---

## 1. 核心设计理念

### 1.1 三大核心原则

#### **配置驱动 (Configuration Driven)**
- **设计思想**: 所有业务逻辑通过 YAML 配置定义，避免硬编码
- **实现细节**:
  - 数据源配置: `configs/sources/*.yaml`
  - Adapter 配置: `configs/adapters/*.yaml`
  - Rule 配置: `configs/rules/*.yaml`
- **优势**: 
  - 业务人员可直接修改配置
  - 无需重新编译部署
  - 配置即文档

#### **存算分离 (Storage-Compute Decoupling)**
- **设计思想**: 计算结果持久化为 Parquet，支持重跑和审计
- **实现细节**:
  - 中间数据存储在 Parquet 文件
  - 元数据通过 XCom 轻量传递
  - 支持本地文件系统和 MinIO
- **优势**:
  - 计算失败可从中间结果恢复
  - 数据可追溯和审计
  - 存储成本低（列式压缩）

#### **上下文总线 (Context Bus)**
- **设计思想**: 标准化的数据传递对象贯穿整个流程
- **实现细节**:
  - `GovernanceContext` 类统一管理状态
  - 包含数据引用、规则结果、告警、资产
  - XCom 序列化传递元数据
- **优势**:
  - 任务间解耦
  - 状态可追踪
  - 支持并行计算

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                        DAG Factory                          │
│         (动态生成 DAG，基于 YAML 配置)                      │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   DAG A: 数据治理主流程                      │
│                                                             │
│  [Sensor] → Loader → Adapter → Rules → Aggregator →       │
│                                           Dispatcher        │
│                                              ↓             │
│                                   Save to Queue (P1 Assets) │
└─────────────────────────────────────────────────────────────┘
                              ↓ (Dataset Event)
┌─────────────────────────────────────────────────────────────┐
│                   DAG B: 资产打包流程                        │
│                                                             │
│  Cleanup Zombies → Get Pending → Pack → Validate → Alert   │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 分层架构

```
┌──────────────────────────────────────────────────────────┐
│  Orchestration Layer (编排层)                            │
│  - DAG Factory: 动态生成 DAG                             │
│  - Rule Scanner: 规则扫描与拓扑排序                      │
└──────────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────────┐
│  Operator Layer (算子层)                                 │
│  - UniversalLoaderOperator: 数据提取                     │
│  - DomainAdapterOperator: 领域转换                       │
│  - GenericRuleOperator: 规则执行                         │
│  - ContextAggregatorOperator: 结果聚合                   │
│  - NotificationDispatcherOperator: 通知分发              │
└──────────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────────┐
│  Service Layer (服务层)                                  │
│  - NotificationService: 邮件渲染与发送                   │
│  - PackingService: 资产打包服务调用                      │
└──────────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────────┐
│  Infrastructure Layer (基础设施层)                        │
│  - IOStrategy: 存储抽象 (Local/MinIO)                    │
│  - ConfigManager: 配置管理                               │
│  - BaseGovernanceOperator: 模板方法模式                 │
└──────────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────────┐
│  Domain Layer (领域层)                                    │
│  - GovernanceContext: 上下文总线                         │
│  - Entities: 领域实体 (Cycle, Vehicle)                   │
└──────────────────────────────────────────────────────────┘
```

---

## 3. 数据流设计

### 3.1 数据演变路径

```
Source DB (MySQL/InfluxDB)
    ↓
RAW Parquet (原始数据)
    ├─ 分区策略: batch_id/stage=RAW/key=xxx/
    ├─ 文件命名: part-{uuid}.parquet
    └─ 压缩策略: Zstd (可配置)
    ↓
ENTITY Parquet (标准化实体)
    ├─ 分区策略: batch_id/stage=ENTITY/key=entity_cycle/
    ├─ Schema: Cycle 实体标准字段
    └─ 数据清洗: NULL 处理、类型转换
    ↓
RESULT Parquet (规则命中结果)
    ├─ 分区策略: batch_id/stage=RESULT/key=rule_xxx/
    ├─ 包含字段: 原始数据 + _severity 列
    └─ 仅存储命中数据
    ↓
GovernanceContext (元数据总线)
    ├─ Alerts: P0/P2 告警列表
    ├─ Assets: P1 资产列表
    └─ RuleOutputRefs: 规则执行结果引用
    ↓
Database (持久化)
    ├─ auto_test_case_catalog: 资产元数据 + 打包队列
    └─ Notification: 邮件发送
```

### 3.2 数据分区设计

#### **Hive 风格分区**
```
/data/governance/
├── batch_id=BATCH_20260204_001/
│   ├── stage=RAW/
│   │   ├── key=raw_cycle_section/
│   │   │   ├── part-00001-uuid-abc.parquet
│   │   │   └── part-00002-uuid-def.parquet
│   │   └── key=raw_cycle_subtarget/
│   │       └── part-00001-uuid-ghi.parquet
│   ├── stage=ENTITY/
│   │   └── key=entity_cycle/
│   │       └── part-00001-uuid-jkl.parquet
│   └── stage=RESULT/
│       ├── key=rule_p0_time_check/
│       │   └── part-00001-uuid-mno.parquet (仅命中数据)
│       ├── key=rule_p1_twin_lift/
│       │   └── part-00001-uuid-pqr.parquet
│       └── key=rule_p2_timeout/
│           └── part-00001-uuid-stu.parquet
```

#### **分区清理策略**
- **重跑保护**: Task 启动时先清空目标分区
- **历史保留**: 不同 batch_id 互不影响
- **原子性**: 先清空再写入，避免部分数据

---

## 4. 核心组件设计

### 4.1 GovernanceContext (上下文总线)

#### **数据结构**
```python
@dataclass
class GovernanceContext:
    batch_id: str                           # 批次ID
    execution_date: str                     # 执行日期
    storage_type: str                       # 存储类型 (local/minio)
    
    # 数据引用
    data_refs: Dict[str, DataRef]           # key -> 数据位置
    rule_output_refs: List[RuleOutputRef]   # 规则执行结果
    
    # 信号
    alerts: List[AlertItem]                 # P0/P2 告警
    assets: List[AssetItem]                 # P1 资产
```

#### **核心方法**
- `put_dataframe()`: 写入 DataFrame 并记录引用
- `get_dataframe()`: 读取 DataFrame
- `register_rule_output()`: 注册规则执行结果
- `add_alert()`: 添加告警
- `add_asset()`: 添加资产
- `to_json()` / `from_json()`: XCom 序列化

#### **设计亮点**
1. **数据与元数据分离**: 大数据存 Parquet，元数据走 XCom
2. **懒加载**: 通过引用读取，避免内存溢出
3. **版本兼容**: JSON 序列化支持跨版本

### 4.2 IOStrategy (存储策略)

#### **抽象接口**
```python
class IOStrategy(ABC):
    @abstractmethod
    def write_parquet(df, path, **kwargs): pass
    
    @abstractmethod
    def read_parquet(path) -> pl.DataFrame: pass
    
    @abstractmethod
    def clean_directory(path): pass
```

#### **实现类**
1. **LocalParquetStrategy**: 本地文件系统
2. **MinIOParquetStrategy**: S3 兼容对象存储

#### **设计亮点**
1. **策略模式**: 存储切换无需改代码
2. **配置化**: 环境变量控制存储类型
3. **压缩可配**: 分阶段配置压缩算法
   - RAW: 无压缩 (原始数据)
   - ENTITY: Zstd Level 3 (平衡)
   - RESULT: Zstd Level 6 (高压缩比)

### 4.3 BaseGovernanceOperator (模板方法)

#### **核心流程**
```python
def execute(self, context):
    # 1. Pre-hook (可选)
    self.pre_execute(context)
    
    # 2. 恢复或创建 Context
    ctx = self._restore_context(context) or self._create_new_context(context)
    
    # 3. 注入 IO 策略
    self._inject_io_strategy(ctx)
    
    # 4. 执行业务逻辑 (子类实现)
    self.execute_logic(ctx, context)
    
    # 5. Post-hook (可选)
    self.post_execute(ctx, context)
    
    # 6. 返回 Context (XCom)
    return ctx.to_json()
```

#### **设计亮点**
1. **模板方法模式**: 流程固定，细节可扩展
2. **Hook 机制**: 支持前后置处理
3. **配置加载**: 统一处理 YAML/Dict 配置
4. **错误处理**: 统一的异常捕获和日志

### 4.4 RuleScanner (规则扫描器)

#### **核心功能**
1. **规则发现**: 扫描 `configs/rules/*.yaml`
2. **依赖解析**: 解析 `depends_on` 字段
3. **拓扑排序**: Kahn 算法处理依赖顺序
4. **循环检测**: 防止死循环

#### **排序算法 (Kahn)**
```python
def _topological_sort(rules):
    # 1. 构建入度表和邻接表
    in_degree = {rule['rule_id']: 0 for rule in rules}
    adjacency = {rule['rule_id']: [] for rule in rules}
    
    # 2. 处理依赖关系
    for rule in rules:
        for dep in rule.get('depends_on', []):
            adjacency[dep].append(rule['rule_id'])
            in_degree[rule['rule_id']] += 1
    
    # 3. 拓扑排序
    queue = [rid for rid, degree in in_degree.items() if degree == 0]
    sorted_rules = []
    
    while queue:
        current = queue.pop(0)
        sorted_rules.append(current)
        
        for neighbor in adjacency[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # 4. 检测循环依赖
    if len(sorted_rules) != len(rules):
        raise ValueError("Circular dependency detected")
    
    return sorted_rules
```

#### **设计亮点**
1. **防御性解析**: Pydantic 校验 + assert 双保险
2. **依赖可视化**: 日志输出执行顺序
3. **错误提示**: 明确指出循环依赖的规则

### 4.5 DAG Factory (动态生成器)

#### **生成流程**
```python
def scan_and_generate_dags():
    # 1. 扫描 sources/*.yaml
    yaml_files = glob("plugins/configs/sources/*.yaml")
    
    # 2. 遍历每个配置
    for yaml_file in yaml_files:
        try:
            # 3. Pydantic 校验
            config = SourceYAMLConfig(**load_yaml(yaml_file))
            
            # 4. 生成 DAG
            dag = create_dag_from_config(config)
            
            # 5. 注册到 globals()
            globals()[dag.dag_id] = dag
        except Exception as e:
            log.error(f"Skip {yaml_file}: {e}")
            continue
```

#### **自动关联机制**
```python
# 基于 target_entity 自动查找
target_entity = config.source_meta.target_entity  # "Cycle"

# 1. 自动加载 Adapter
adapter_path = f"adapters/{target_entity.lower()}_adapter.yaml"  
# → adapters/cycle_adapter.yaml

# 2. 自动扫描 Rules
rules = [r for r in all_rules if r['target_entity'] == target_entity]
# → 匹配所有 target_entity: "Cycle" 的规则
```

#### **设计亮点**
1. **约定优于配置**: 文件名遵循命名规范
2. **自动发现**: 无需手动配置关联
3. **容错机制**: 单个文件错误不影响其他 DAG
4. **类型安全**: Pydantic 在加载时就校验

---

## 5. 配置系统设计

### 5.1 三层配置体系

#### **Source 配置 (sources/*.yaml)**
```yaml
source_meta:
  id: "daily_cycle_etl"              # DAG ID 后缀
  target_entity: "Cycle"             # 关联的实体类型
  
scheduling:
  trigger_mode: "CRON"               # CRON/MANUAL/DATASET
  cron_expression: "0 2 * * *"
  sensor:                            # 可选前置检查
    enabled: true
    type: "SQL"                      # SQL/FILE/TIME/EXTERNAL_TASK
    sql: "SELECT COUNT(*) > 100 FROM ..."
    
extractions:                         # 数据提取配置
  - id: "raw_cycle_section"
    source_type: "mysql"
    conn_id: "datalog_mysql_conn"
    query: "SELECT * FROM ..."
    output_key: "raw_cycle_section"
```

#### **Adapter 配置 (adapters/cycle_adapter.yaml)**
```yaml
adapter_meta:
  id: "cycle_adapter"
  target_entity: "Cycle"
  
input_schema:
  primary_source: "raw_cycle_section"
  joins:                             # 链式 JOIN
    - join_source: "raw_cycle_subtarget"
      type: "left"
      left_on: "cycle_id"
      right_on: "cycle_id"
      
field_mapping:                       # 字段映射
  - target: "cycle_id"
    source_expr: "col('cycle_id')"
    
  - target: "duration"
    source_expr: "(col('end_time') - col('start_time')).dt.total_seconds()"
    
  - target: "is_twin_lift"
    source_expr: "col('twin_lift') == 1"
    
  - target: "extra_attributes"
    source_expr: "pl.struct([col('task_id'), col('section_type')])"
```

#### **Rule 配置 (rules/p0_time_check.yaml)**
```yaml
rule_meta:
  rule_id: "rule_p0_time_check"
  severity: "P0"
  description: "时间倒挂检测"
  
target_entity: "Cycle"               # 关联实体

logic:
  filter_expr: "col('end_time') < col('start_time')"
  
alert:                               # P0 生成告警
  title: "[P0] 物理时间倒挂"
  content: |
    批次: {{ batch_id }}
    车辆: {{ vehicle_id }}
    周期: {{ cycle_id }}
  severity: "CRITICAL"
  
depends_on: []                       # 依赖的其他规则
```

### 5.2 配置校验设计

#### **Pydantic Schema**
```python
class SourceYAMLConfig(BaseModel):
    source_meta: SourceMetaConfig
    scheduling: SchedulingConfig
    extractions: List[ExtractionConfig]
    
    class Config:
        extra = 'forbid'  # 禁止未定义字段

class SensorConfig(BaseModel):
    enabled: bool = True
    type: Literal['SQL', 'FILE', 'TIME', 'EXTERNAL_TASK']
    
    # 条件校验
    @validator('sql', always=True)
    def validate_sql_sensor(cls, v, values):
        if values.get('type') == 'SQL' and not v:
            raise ValueError("type=SQL requires 'sql' field")
        return v
```

#### **设计亮点**
1. **编译时校验**: Pydantic 在 DAG 解析时就发现错误
2. **条件约束**: 不同 type 有不同的必填字段
3. **禁止未知字段**: `extra='forbid'` 防止拼写错误
4. **自动文档**: Pydantic 模型即文档

---

## 6. 存储策略设计

### 6.1 Parquet 文件组织

#### **文件命名规范**
```
part-{uuid}.parquet
    ↓
part-00001-abc123def.parquet
part-00002-ghi456jkl.parquet
```

- **UUID**: 保证文件名唯一，支持并发写入
- **序号**: 方便排序和追踪
- **无时间戳**: 避免时钟不同步问题

#### **压缩策略**
```python
# 分阶段配置压缩
COMPRESSION_CONFIG = {
    'RAW': None,                # 原始数据，优先写入速度
    'ENTITY': 'zstd:3',         # 实体数据，平衡压缩比和速度
    'RESULT': 'zstd:6',         # 结果数据，高压缩比（数据量小）
}
```

**压缩比对比** (测试数据):
- 无压缩: 100 MB
- Zstd Level 3: 15 MB (压缩比 6.7:1，速度 300 MB/s)
- Zstd Level 6: 12 MB (压缩比 8.3:1，速度 150 MB/s)

### 6.2 XCom 设计

#### **仅传递元数据**
```json
{
  "batch_id": "BATCH_20260204_001",
  "storage_type": "local",
  "data_refs": {
    "raw_cycle_section": {
      "uri": "/data/governance/batch_id=XXX/stage=RAW/key=raw_cycle_section",
      "row_count": 1523,
      "column_count": 8
    }
  },
  "alerts": [
    {
      "rule_id": "rule_p0_time_check",
      "severity": "P0",
      "trigger_cycle_ids": ["cycle_001", "cycle_002"]
    }
  ]
}
```

#### **设计亮点**
1. **轻量**: XCom 只存元数据，避免序列化大对象
2. **可读**: JSON 格式方便调试
3. **引用**: 通过 URI 引用实际数据
4. **审计**: 记录数据行列数便于追踪

---

## 7. 错误处理设计

### 7.1 错误分类策略

#### **业务逻辑错误**
- **示例**: 除零、数据格式错误、规则表达式语法错误
- **处理**: 捕获异常，生成 ErrorResult，Task 标记为 Success
- **原因**: 重试无法解决，避免阻塞流程

```python
try:
    df_hits = df.filter(eval(filter_expr))
except ZeroDivisionError as e:
    # 业务逻辑错误，不阻断
    ctx.register_rule_output(
        rule_id=rule_id,
        status="ERROR",
        error_message=str(e)
    )
    return  # Task 标记为 Success
```

#### **系统级错误**
- **示例**: MinIO 连接失败、磁盘满、网络超时
- **处理**: 抛出异常，触发 Airflow Retry
- **原因**: 重试可能解决，需要系统介入

```python
try:
    io_strategy.write_parquet(df, path)
except ConnectionError as e:
    # 系统级错误，触发重试
    raise AirflowException(f"MinIO connection failed: {e}")
```

### 7.2 错误判断逻辑

```python
def _is_system_error(exception):
    """判断是否为系统级错误"""
    system_errors = (
        ConnectionError,
        TimeoutError,
        OSError,
        IOError,
        MemoryError,
    )
    return isinstance(exception, system_errors)

# 使用
try:
    result = execute_rule(df)
except Exception as e:
    if _is_system_error(e):
        raise  # 触发 Airflow Retry
    else:
        log_error_and_continue(e)  # 记录并继续
```

### 7.3 Graceful Degradation (优雅降级)

#### **模板渲染降级**
```python
def _render_body(alert, template):
    try:
        return jinja2_env.render(template, **alert)
    except TemplateError as e:
        log.warning(f"Template render failed: {e}")
        # 降级为 JSON
        return _render_fallback_body(alert)

def _render_fallback_body(alert):
    """降级方案：返回 JSON 字符串"""
    return json.dumps(alert, indent=2, ensure_ascii=False)
```

**设计理念**: 告警必须发送，即使格式不美观

---

## 8. 扩展性设计

### 8.1 新增数据源

**步骤**:
1. 在 `sources/` 下创建新 YAML 文件
2. 配置 `target_entity`、`extractions`
3. 重启 Scheduler，自动生成 DAG

**无需修改代码**

### 8.2 新增实体类型

**步骤**:
1. 在 `entities/` 下定义新实体类
   ```python
   @dataclass
   class Vehicle:
       vehicle_id: str
       speed: float
       location: Dict[str, float]
   ```

2. 创建对应的 Adapter
   ```yaml
   # adapters/vehicle_adapter.yaml
   target_entity: "Vehicle"
   field_mapping: ...
   ```

3. 创建规则
   ```yaml
   # rules/vehicle_speed_check.yaml
   target_entity: "Vehicle"
   ```

**自动关联**: DAG Factory 会自动发现并关联

### 8.3 新增 Sensor 类型

**步骤**:
1. 在 `SourceConfigSchema` 中添加类型
   ```python
   class SensorConfig(BaseModel):
       type: Literal['SQL', 'FILE', 'HTTP', 'S3KEY']  # 添加新类型
   ```

2. 在 `DAGFactory._create_sensor_task()` 中添加分支
   ```python
   elif sensor_config.type == 'HTTP':
       return HttpSensor(
           endpoint=sensor_config.endpoint,
           **common_args
       )
   ```

### 8.4 新增存储后端

**步骤**:
1. 实现 `IOStrategy` 接口
   ```python
   class HDFSParquetStrategy(IOStrategy):
       def write_parquet(self, df, path): ...
       def read_parquet(self, path): ...
   ```

2. 在 `ConfigManager` 中添加配置
   ```python
   if storage_type == 'hdfs':
       return HDFSParquetStrategy(...)
   ```

---

## 9. 性能优化设计

### 9.1 计算优化

#### **Polars 列式计算**
- **选择原因**: 
  - Rust 实现，性能优于 Pandas 5-10倍
  - 原生支持多核并行
  - 懒加载 + 查询优化

- **使用示例**:
  ```python
  # 链式操作，自动优化
  df = (
      pl.scan_parquet(path)         # 懒加载
      .filter(col('status') == 'COMPLETED')
      .select(['cycle_id', 'duration'])
      .collect()                     # 触发计算
  )
  ```

#### **表达式编译**
```python
# 规则表达式预编译
SAFE_SCOPE = {
    'col': col, 'lit': lit, 'when': when,
    'pl': pl, 'datetime': datetime
}

# 编译一次，多次使用
compiled_expr = eval(filter_expr, SAFE_SCOPE)
df_hits = df.filter(compiled_expr)
```

### 9.2 I/O 优化

#### **分区裁剪**
```python
# 只读取需要的分区
df = pl.read_parquet(
    f"/data/governance/batch_id={batch_id}/stage=RAW/key=*/",
    hive_partitioning=False  # 避免自动添加分区列
)
```

#### **列裁剪**
```python
# 只读取需要的列
df = pl.read_parquet(path, columns=['cycle_id', 'duration'])
```

#### **压缩权衡**
- RAW: 无压缩，优先写入速度
- ENTITY: Zstd Level 3，平衡
- RESULT: Zstd Level 6，优先压缩比

### 9.3 并发设计

#### **规则并行执行**
```python
# Airflow TaskGroup 自动并行
with TaskGroup('rule_tasks') as group:
    for rule in rules:
        GenericRuleOperator(task_id=rule['rule_id'])
        
# Airflow 根据资源自动调度并行
```

#### **避免 Hive 分区列问题**
```python
# 问题：Polars 0.19 自动添加分区列导致重复
df = pl.read_parquet(".../batch_id=*/")  # 自动添加 batch_id 列

# 解决：显式列出文件逐个读取
files = list_files_in_directory(path)
dfs = [pl.read_parquet(f) for f in files]
df = pl.concat(dfs)
```

---

## 10. 运维设计

### 10.1 监控设计

#### **DAG 级别监控**
- **指标**: 
  - DAG 生成数量
  - DAG 成功/失败率
  - 平均执行时长

- **实现**:
  ```sql
  SELECT 
      dag_id,
      COUNT(*) as run_count,
      SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as success_count,
      AVG(TIMESTAMPDIFF(SECOND, start_date, end_date)) as avg_duration
  FROM dag_run
  WHERE dag_id LIKE 'gov_%'
  GROUP BY dag_id;
  ```

#### **任务级别监控**
- **指标**:
  - 规则命中率
  - P0/P1/P2 数量
  - 数据量 (行数/文件大小)

- **实现**:
  ```python
  # 在 Context 中记录
  ctx.register_rule_output(
      rule_id=rule_id,
      hit_count=len(df_hits),
      total_count=len(df),
      file_size_mb=file_size / 1024 / 1024
  )
  ```

### 10.2 日志设计

#### **分级日志**
```python
# INFO: 正常流程
self.log.info(f"✅ Loaded {df.height} rows from {source}")

# WARNING: 可恢复的问题
self.log.warning(f"⚠️ No data found for {batch_id}, skip")

# ERROR: 需要关注的错误
self.log.error(f"❌ Rule execution failed: {e}", exc_info=True)
```

#### **结构化日志**
```python
{
    "timestamp": "2026-02-04T12:00:00Z",
    "batch_id": "BATCH_20260204_001",
    "task_id": "rule_p0_time_check",
    "rule_id": "rule_p0_time_check",
    "hit_count": 5,
    "total_count": 1523,
    "duration_ms": 234
}
```

### 10.3 告警设计

#### **告警分级**
- **P0 (CRITICAL)**: 立即发送，单独邮件
- **P2 (WARNING)**: 批量发送，汇总邮件
- **ERROR**: 系统错误，发送给运维团队

#### **告警模板**
```jinja2
【{{ severity }}】{{ title }}

批次: {{ batch_id }}
执行时间: {{ execution_date }}
命中数量: {{ hit_count }}

详情:
{% for cycle_id in trigger_cycle_ids[:5] %}
  - Cycle {{ cycle_id }}
{% endfor %}
{% if trigger_cycle_ids|length > 5 %}
  ... 还有 {{ trigger_cycle_ids|length - 5 }} 条
{% endif %}

查看详情: {{ airflow_ui_url }}
```

### 10.4 数据保留策略

#### **Parquet 文件**
- **保留周期**: 7 天（可配置）
- **清理脚本**: 
  ```bash
  find /data/governance -type d -name "batch_id=*" -mtime +7 -exec rm -rf {} \;
  ```

#### **数据库记录**
- **auto_test_case_catalog**: 永久保留（业务数据）
- **dag_run / task_instance**: 30 天（Airflow 元数据）

### 10.5 灾难恢复

#### **重跑机制**
```bash
# 重跑整个 DAG
airflow dags backfill gov_daily_cycle_etl \
  --start-date 2026-02-01 \
  --end-date 2026-02-03 \
  --rerun-failed-tasks
```

#### **数据恢复**
- **Parquet 备份**: 定期同步到对象存储
- **数据库备份**: 每日全量 + 每小时增量

---

## 11. 关键设计决策记录

### 11.1 为什么选择 Polars 而非 Pandas？

**决策**: 使用 Polars 作为数据处理引擎

**原因**:
1. **性能**: Rust 实现，多核并行，比 Pandas 快 5-10 倍
2. **内存效率**: 列式存储，内存占用更小
3. **懒加载**: 支持查询优化，类似 Spark
4. **API 简洁**: 链式操作，代码可读性强

**权衡**: Polars 生态不如 Pandas 成熟，但核心功能满足需求

### 11.2 为什么使用 Airflow 而非 Prefect/Dagster？

**决策**: 使用 Airflow 作为编排引擎

**原因**:
1. **成熟度**: 社区活跃，生产验证充分
2. **生态**: Provider 丰富，易于集成
3. **UI**: 功能强大，便于监控和调试
4. **熟悉度**: 团队已有经验

**权衡**: Airflow 配置复杂，但可通过 DAG Factory 简化

### 11.3 为什么使用单表而非双表（DAG B）？

**决策**: `auto_test_case_catalog` 单表管理队列 + 元数据

**原因**:
1. **事务一致性**: 单表更新原子性强
2. **查询性能**: 避免 JOIN，减少锁竞争
3. **维护简单**: 一个表 vs 两个表同步

**权衡**: 表字段较多，但通过索引优化查询

### 11.4 为什么 Context 只传元数据？

**决策**: XCom 只传 GovernanceContext JSON，数据存 Parquet

**原因**:
1. **XCom 限制**: 默认 SQLite 存储，大对象序列化慢
2. **可追溯性**: Parquet 文件可独立查看和审计
3. **重跑支持**: 从 Parquet 恢复，无需重新计算上游

**权衡**: 需要管理文件生命周期，但存算分离收益更大

---

## 12. 未来演进方向

### 12.1 短期优化 (1-3 个月)

1. **Web 配置界面**
   - 可视化编辑 YAML 配置
   - 实时预览 DAG 结构
   - 配置版本控制

2. **更多 Sensor 类型**
   - HttpSensor: API 就绪检查
   - S3KeySensor: 对象存储文件检查
   - KafkaSensor: 消息队列监听

3. **性能监控仪表盘**
   - Grafana + Prometheus
   - 实时监控规则命中率
   - 数据量趋势分析

### 12.2 中期规划 (3-6 个月)

1. **多租户支持**
   - 按业务线隔离配置
   - RBAC 权限管理
   - 资源配额限制

2. **智能推荐**
   - 根据历史数据推荐调度时间
   - 自动优化规则执行顺序
   - 异常数据自动打标

3. **流式处理**
   - Kafka + Flink 实时流处理
   - 分钟级数据治理
   - 实时告警推送

### 12.3 长期愿景 (6-12 个月)

1. **AI 驱动的数据治理**
   - 异常检测模型
   - 自动生成规则建议
   - 根因分析

2. **数据血缘追踪**
   - 字段级血缘关系
   - 影响分析
   - 合规审计

3. **跨数据中心部署**
   - 多活架构
   - 数据同步
   - 灾备切换

---

## 13. 总结

### 核心亮点

1. **配置驱动**: YAML 定义一切，业务人员可直接操作
2. **存算分离**: Parquet 持久化，支持审计和重跑
3. **自动关联**: 基于 target_entity 自动发现配置
4. **类型安全**: Pydantic 编译时校验
5. **错误隔离**: 业务错误不阻塞流程
6. **性能优越**: Polars + 列式存储 + 并行计算
7. **可扩展性**: 新增数据源/实体/规则无需改代码

### 设计哲学

> **"约定优于配置，配置优于编码"**

- 遵循命名约定自动关联
- 通过 YAML 配置控制行为
- 最小化代码侵入

> **"存算分离，数据优先"**

- 计算结果持久化
- 元数据轻量传递
- 支持独立审计

> **"失败友好，优雅降级"**

- 错误分类处理
- 部分失败不影响整体
- 降级方案保证可用性

---

**文档版本**: v1.0  
**最后更新**: 2026-02-04  
**维护者**: box_admin  
**联系方式**: xiyan.zhou@westwell-lab.com
