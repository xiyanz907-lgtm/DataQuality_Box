# 数据质量测试平台目标与实现
_Exported on 3/10/2026 at 13:45:38 GMT+8 from Cursor (2.6.14)_

---

**User**

关于整个项目，我当初的目标是做一个数据质量测试平台，它的测试步骤是这样的：首先接入多种数据源，然后将数据源通过一系列整合成一个实体entity，接着对这个实体做一系列质量校验，对于错误的数据我要发出告警，甚至可能要对原数据进行修改；对于有价值的数据，我要去存储它的raw data。最后，我要将最终的检测结果汇总。我的代码是否实现了我的目标，对于不同的业务来说是否有局限性，请分析

---

**Cursor**

Let me thoroughly explore the project structure and codebase to understand what has been implemented.

Now let me read the key files to understand the implementation:

Now let me read the DAGs and remaining important files:

Let me also check the remaining service files and rule configs:

我已经完整阅读了项目的所有核心代码。下面从你描述的目标出发，逐一分析实现程度和局限性。

---

## 一、目标达成分析

### ✅ 1. 接入多种数据源

**实现情况：** 已实现，但接入面较窄。

`UniversalLoaderOperator` 通过 YAML 配置 (`daily_cycle_etl.yaml`) 支持声明式多表抽取，每个 extraction 可以配置独立的 `source_type` 和 `conn_id`。当前支持 MySQL，预留了 PostgreSQL 和 InfluxDB 接口：

```156:165:plugins/operators/loader.py
        if source_type == 'mysql':
            return self._extract_from_mysql(sql, task)
        elif source_type == 'postgresql':
            # 预留：PostgreSQL 抽取逻辑
            raise NotImplementedError("PostgreSQL support not implemented yet")
        elif source_type == 'influxdb':
            # 预留：InfluxDB 抽取逻辑
            raise NotImplementedError("InfluxDB support not implemented yet")
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
```

Schema 校验 (`source_config_schema.py`) 也声明了多种 source type：

```94:94:plugins/schemas/source_config_schema.py
    source_type: Literal['mysql', 'postgresql', 'influxdb', 's3', 'minio']
```

**局限性：**
- **实际仅实现了 MySQL**，PostgreSQL / InfluxDB / S3 / MinIO 作为数据源读取都还是 `NotImplementedError`
- 不支持**文件类数据源**（CSV、Excel、JSON）的直接加载
- 不支持 **API / Kafka / 消息队列** 等流式数据源
- Loader 依赖 `MySqlHook` → Pandas → Polars 的转换链路，对于**大数据量**效率不高（内存翻倍）

---

### ✅ 2. 数据整合为实体 (Entity)

**实现情况：** 已实现，设计精良。

这是整个架构中设计最好的部分。`DomainAdapterOperator` + `cycle_adapter.yaml` 实现了：
- **多表 Join**（支持 left / inner / outer，链式配置）
- **字段映射**（低代码 Polars 表达式，包括 `col()`, `lit()`, `when()` 和窗口函数 `.over()`）
- **派生字段**（如 `non_null_subtask_count`、`daily_total_box` 等组级聚合）
- **差异字段**（如 `diff_box_count1`, `diff_mileage` 用于一致性校验）

`Cycle` 实体类定义了清晰的领域契约文档：

```5:82:plugins/domain/entities/cycle.py
@dataclass
class Cycle:
    """
    标准作业圈实体 (Standard Domain Entity)
    所有源数据 (MySQL/Influx) 最终都必须清洗成这个样子
    ...
    """
```

**局限性：**
- **Cycle 实体是硬编码的**：目前只有一个实体 `Cycle`，一个 adapter 配置 `cycle_adapter.yaml`。如果要接入新的业务（比如"Vehicle"实体或"Shift"实体），需要手动编写新的 entity dataclass 和 adapter YAML
- **`Cycle` dataclass 仅作文档用途**，代码中没有做实际的 Schema 校验（adapter 配置的 `data_cleaning` 部分被注释掉了）：

```207:219:plugins/configs/adapters/cycle_adapter.yaml
# 数据清洗规则（在映射之前应用，可选）
# 注：当前版本代码中未实现此功能
# data_cleaning:
#   drop_duplicates:
#     - cycle_id
#   drop_nulls:
#     - cycle_id
```

- 表达式解析使用 `eval()`，虽然限制了作用域，但仍存在安全隐患

---

### ✅ 3. 质量校验（规则引擎）

**实现情况：** 已实现，覆盖面中等。

`GenericRuleOperator` 通过 YAML 配置实现规则声明式执行。当前有 **11 条规则**覆盖 P0/P1/P2 三个等级：

| 等级 | 规则数 | 示例 |
|------|--------|------|
| P0 | 9 | 时间倒挂、箱数差异、电耗异常、里程/功率/运行时间校验、CKP检查、子任务数 |
| P1 | 1 | 双箱作业识别（资产挖掘） |
| P2 | 1 | 作业超时（结合P1白名单豁免） |

规则扫描器 (`RuleScanner`) 支持：
- 自动扫描 `configs/rules/*.yaml`
- 拓扑排序处理依赖关系
- 配置验证（防御性解析）

**局限性：**
- **规则类型单一**：所有规则都是 `filter_expr` 过滤型，只做"找出异常行"。缺少：
  - **聚合校验**（如总行数/总箱数与预期值对比）
  - **分布校验**（如字段值是否符合正态分布）
  - **趋势校验**（如与历史数据对比是否异常波动）
  - **跨实体校验**（一条规则需要读取多个实体）
  - **自定义函数规则**（对于复杂逻辑，纯 Polars 表达式表达力有限）
- `dq_lib/` 中已有 `completeness.py`、`consistency.py`、`distribution.py` 等模块，但这些库**没有被规则引擎集成**，只被 `reconciliation_runner.py` 独立使用
- 规则引擎只读取 `entity_cycle`（通过 `target_entity` 匹配），无法写一条规则同时比较两个不同的实体

---

### ✅ 4. 告警通知

**实现情况：** 已实现。

告警流程完整：
1. `ContextAggregatorOperator` 按 P0/P1/P2 分别生成 `AlertItem`
2. P2 支持**白名单豁免**（P1 识别出的 cycle_id 不纳入 P2 告警）
3. `NotificationDispatcherOperator` 发送 **HTML 格式邮件**，P0 批量汇总、P2 批量汇总
4. `global_alert_reporter` DAG 每小时扫描失败任务并发送汇总告警

**局限性：**
- **仅支持邮件**，未集成 Webhook / Slack / 企业微信 / PagerDuty 等
- 告警内容模板在代码中硬编码（HTML 模板），不可配置
- 没有告警去重/收敛机制（如果 DAG 重跑，会发重复邮件）
- 没有告警确认/消息状态追踪

---

### ⚠️ 5. 对原数据进行修改

**实现情况：** 部分实现（独立子系统）。

这个能力存在于 `reconciliation_runner.py` 和 `scanner_cactus_monitor.py` 组成的**回填子系统**中，而非主治理流水线。它的逻辑是：
- Scanner 检测 Cactus (`cnt_cycles_check`) 的变更
- Worker 从 nGen 拉取数据，与 Cactus 做两级匹配（严格5分钟 + 宽松3小时+箱号）
- **回写数据库**：将匹配到的 `cycleId` 和 `sync_status` 更新回 Cactus 表

但在**主治理流水线** (DAG A: `gov_daily_cycle_etl`) 中，规则命中的异常数据**不会修改原数据**。规则引擎只做"发现 + 记录"，没有 "修复" 动作。

**局限性：**
- 主流水线的规则命中后，没有 **自动修复（Auto-Fix）** 能力
- 回填子系统与主治理管线是**完全独立的两套代码**，没有共享实体模型和规则引擎
- 回填子系统的箱号覆盖逻辑已被**注释禁用**：

```397:406:plugins/services/reconciliation_runner.py
            join_update_sql = """
                UPDATE cnt_cycles_check t
                INNER JOIN tmp_cnt_cycles_check_updates s ON t.id = s.id
                SET 
                    t.cycleId = s.cycleId,
                    -- [注释] 箱号覆盖逻辑已禁用，但报告中仍保留箱号 mismatch 对比记录
                    -- t.cnt01 = s.cnt01,
                    -- t.cnt02 = s.cnt02,
                    -- t.cnt03 = s.cnt03,
                    t.sync_status = s.sync_status
            """
```

---

### ✅ 6. 有价值数据存储 Raw Data

**实现情况：** 已实现。

分两个层面：
1. **治理流水线**：所有数据（RAW / ENTITY / RESULT）通过 `IOStrategy` 以 **Parquet 格式**写入 Hive 风格分区目录 (`batch_id=.../stage=.../key=.../`)，支持本地和 MinIO。从 `data/governance/` 目录可以看到大量批次数据已被持久化
2. **P1 资产打包**：识别到有价值数据（如双箱作业）后，通过 Dataset 事件触发 `asset_packing_dag`，调用外部打包服务将 raw data（InfluxDB 时序数据）打包存储

**局限性：**
- raw data 存储没有**生命周期管理**（没有 TTL / 过期清理）
- `maintenance_dag` 只清理 XCom 和 Airflow 日志，不清理 `data/governance/` 下的 Parquet 文件
- 打包服务是调用外部 API（mock 接口），打包是否成功完全依赖第三方

---

### ✅ 7. 检测结果汇总

**实现情况：** 已实现。

- `ContextAggregatorOperator` 合并所有规则结果，统计 success / failed / hits
- `GovernanceContext.summary()` 生成上下文摘要
- 每个批次的完整上下文通过 XCom 序列化传递
- Reconciliation Worker 有 `summarize_and_notify` 任务做结果汇总 + 邮件报告

**局限性：**
- 没有**持久化的结果报告**（不存数据库，不生成报告文件），只在 Airflow 日志和邮件中
- 没有**历史趋势看板 / Dashboard**
- 没有 `report_generator.py`（文件存在但未被使用或可能为空）
- 各批次之间的结果无法对比

---

## 二、整体架构评价

### 架构亮点 ⭐
1. **配置驱动**：源、适配器、规则全部 YAML 化，DAG Factory 自动生成，做到了"添加规则不改代码"
2. **Context Bus 设计**：`GovernanceContext` 作为总线，管理数据指针、规则结果、告警信号、资产信号，序列化通过 XCom 传递
3. **P0/P1/P2 三级治理**：清晰的语义——P0 阻断告警、P1 资产挖掘（豁免源）、P2 违规（支持白名单豁免）
4. **事件驱动**：DAG A → Dataset → DAG B 的打包流程设计
5. **存储抽象**：`IOStrategy` 屏蔽 Local/MinIO 差异，未来切换存储只需改环境变量

### 业务局限性 ⚠️

| 维度 | 当前状态 | 泛化挑战 |
|------|---------|---------|
| **实体** | 只有 Cycle | 新业务需手写 entity + adapter YAML |
| **数据源** | 只有 MySQL | 无法接入 Kafka、API、文件等 |
| **规则** | 只有 filter 类 | 无法做统计检验、分布校验、跨实体规则 |
| **修复** | 不支持 | 无 Auto-Fix / 数据回写能力 |
| **告警** | 仅邮件 | 无 Webhook/IM 集成 |
| **报告** | 仅日志/邮件 | 无持久化报告、无 Dashboard |
| **多租户** | 不支持 | 无权限/隔离机制 |
| **增量** | 全量抽取 | SQL 写死 `DATE_SUB(CURDATE(), INTERVAL 1 DAY)`，不支持 CDC |

### 不同业务场景适配评估

- **相同港口、相同实体（Cycle）**：✅ 可直接使用，只需改 SQL 和 conn_id
- **不同实体（如 Vehicle、Container）**：⚠️ 需新增 entity dataclass + adapter YAML + 规则 YAML，工作量中等
- **非关系型数据（时序、日志）**：❌ Loader 不支持，需要开发新的 source_type
- **实时/近实时质量监控**：❌ 基于 Airflow 定时调度，无法做到秒级响应
- **通用数据质量平台（多团队、多业务线）**：❌ 缺少 UI、多租户、自助配置能力

---

## 三、总结

你的代码**实现了数据质量测试平台的核心骨架**，特别是 `数据接入 → 实体整合 → 规则校验 → 告警 → 资产存储` 这条主链路的完成度很高。它对于当前的 **Cycle 业务场景**已经能正常运转（从 `data/governance/` 下多个批次的数据可以印证）。

但如果目标是一个**通用的数据质量测试平台**，最大的差距在于：

1. **规则表达力不足**——只有 filter 型，需要扩展为聚合规则、分布规则、自定义函数规则
2. **数据源接入面窄**——只实现了 MySQL
3. **缺少结果持久化和可视化**——没有 Dashboard，无法看历史趋势
4. **缺少数据修复闭环**——主流水线发现异常后没有回写能力
5. **Reconciliation 子系统与主管线割裂**——两套独立逻辑，没有复用实体模型和规则框架

```python
        if source_type == 'mysql':
            return self._extract_from_mysql(sql, task)
        elif source_type == 'postgresql':
            # 预留：PostgreSQL 抽取逻辑
            raise NotImplementedError("PostgreSQL support not implemented yet")
        elif source_type == 'influxdb':
            # 预留：InfluxDB 抽取逻辑
            raise NotImplementedError("InfluxDB support not implemented yet")
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
```

```python
    source_type: Literal['mysql', 'postgresql', 'influxdb', 's3', 'minio']
```

```python
@dataclass
class Cycle:
    """
    标准作业圈实体 (Standard Domain Entity)
    所有源数据 (MySQL/Influx) 最终都必须清洗成这个样子
    ...
    """
```

```yaml
# 数据清洗规则（在映射之前应用，可选）
# 注：当前版本代码中未实现此功能
# data_cleaning:
#   drop_duplicates:
#     - cycle_id
#   drop_nulls:
#     - cycle_id
```

```python
            join_update_sql = """
                UPDATE cnt_cycles_check t
                INNER JOIN tmp_cnt_cycles_check_updates s ON t.id = s.id
                SET 
                    t.cycleId = s.cycleId,
                    -- [注释] 箱号覆盖逻辑已禁用，但报告中仍保留箱号 mismatch 对比记录
                    -- t.cnt01 = s.cnt01,
                    -- t.cnt02 = s.cnt02,
                    -- t.cnt03 = s.cnt03,
                    t.sync_status = s.sync_status
            """
```

---

**User**

在数据接入到整合的部分，pipeline是否有局限性，它只能做到一次性接入多个数据源并整合，但不能做到漏斗式提取数据，比如有这种场景：根据A数据源的数据去筛选B数据源的数据，再根据这一部分数据筛选C数据源，最终得到想要的结果。在这个过程中三种数据源分别来自mysql、influxdb、map api，也就是说后两者是时序数据和json数据

---

**Cursor**

你发现了一个非常关键的架构局限性。让我深入分析当前 pipeline 在"漏斗式提取"场景下的根本问题。

---

## 当前架构的数据提取模式：扁平并行

看一下 Loader 的核心执行逻辑：

```53:78:plugins/operators/loader.py
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        # ...
        # 执行每个抽取任务
        for task in extractions:
            self._execute_extraction_task(task, source_meta, ctx, context)
        
        self.log.info(f"✅ Completed {len(extractions)} extractions")
```

每个 extraction 是**独立**执行的，它们之间**没有任何依赖关系**。再看 SQL 模板渲染，能用的变量只有 Airflow 宏：

```232:250:plugins/operators/loader.py
        template = Template(sql_template)
        
        # 提取常用宏
        template_vars = {
            'ds': context.get('ds'),                    # 2026-01-26
            'ds_nodash': context.get('ds_nodash'),      # 20260126
            'ts': context.get('ts'),                    # 2026-01-26T12:00:00+00:00
            'ts_nodash': context.get('ts_nodash'),      # 20260126T120000
            'execution_date': context.get('execution_date'),
            'prev_ds': context.get('prev_ds'),
            'next_ds': context.get('next_ds'),
            'yesterday_ds': context.get('yesterday_ds'),
            'tomorrow_ds': context.get('tomorrow_ds'),
        }
        
        # 渲染
        rendered_sql = template.render(**template_vars)
```

**SQL 模板只能注入时间类宏，无法注入前一步提取的数据结果。**

而 DAG Factory 也硬编码了单阶段的 pipeline 结构：

```174:214:plugins/orchestration/dag_factory.py
    def _build_task_pipeline(self, config: SourceYAMLConfig, dag: DAG, sensor_config: Any):
        """
        构建完整的任务流水线
        
        Pipeline: [Sensor] -> Loader -> Adapter -> Rules -> Aggregator -> Dispatcher
        """
        # ... 只有一个 Loader、一个 Adapter ...
```

---

## 三个根本性缺陷

### 缺陷 1：提取步骤之间没有数据传递通道

当前模式是：

```
Extract A (独立) ──┐
Extract B (独立) ──┼── 全部写入 Context ──→ Adapter (Join) ──→ Entity
Extract C (独立) ──┘
```

你需要的漏斗模式是：

```
Extract A ──→ A的结果作为参数 ──→ Extract B ──→ B的结果作为参数 ──→ Extract C ──→ Entity
```

关键差异：**后续提取的查询条件由前一步的结果决定**。当前的 `_execute_extraction_task()` 写入 Context 后，下一个 extraction 根本不会去读前一个的结果来构造查询。

### 缺陷 2：缺少异构数据源的查询范式

你描述的场景涉及三种完全不同的查询范式：

| 数据源 | 查询方式 | 当前支持 |
|--------|---------|---------|
| MySQL | SQL | ✅ 已实现 |
| InfluxDB | Flux/InfluxQL（时序查询，需要 `time range` + `tag filter`）| ❌ 只有占位符 |
| Map API | HTTP REST（JSON 响应，需要 `lat/lon`、`vehicle_id` 等参数）| ❌ 完全未考虑 |

对于 InfluxDB，查询模式本质上不同——你需要用 A 步的 `vehicle_id` 和 `start_time/end_time` 来构造 Flux 查询的 `range()` 和 `filter()`。对于 Map API，你可能需要对 A 步的每一行数据发一个 HTTP 请求，或者批量请求。

当前架构的 `_extract_from_source()` 只考虑了 SQL 类查询的模板渲染，完全没有"用前一步 DataFrame 的行数据来参数化后一步查询"的设计。

### 缺陷 3：Adapter 只做后置 Join，不做中间筛选

`DomainAdapterOperator` 的 `_apply_joins()` 做的是**等值 Join**：

```128:169:plugins/operators/adapter.py
    def _apply_joins(self, df: pl.DataFrame, ctx: GovernanceContext) -> pl.DataFrame:
        # ...
        for idx, join_config in enumerate(joins):
            # ...
            df = df.join(
                df_right,
                left_on=left_on,
                right_on=right_on,
                how=join_type,
                suffix=suffix
            )
        return df
```

漏斗式场景的本质是 **"先筛选，再提取"**，而不是 **"先提取全量，再 Join"**。如果 InfluxDB 的数据量很大（时序数据通常如此），先全量拉出来再 Join 在性能上是不可接受的——你必须在查询时就带上时间窗口等筛选条件。

---

## 一个具体场景说明

以你描述的场景举例：

> **Step 1（MySQL）**：从 `cycle_section_summary` 查出当天所有异常 cycle，获得 `vehicle_id`、`start_time`、`end_time`
> 
> **Step 2（InfluxDB）**：用 Step 1 的每个 cycle 的 `vehicle_id` + 时间窗口，从 InfluxDB 查出这些 cycle 对应的 CAN 总线时序数据（比如速度曲线、电池电流等）
> 
> **Step 3（Map API）**：用 Step 2 中提取的 GPS 坐标轨迹，调用地图 API 获取道路信息（坡度、路况等 JSON 数据）

当前架构做不到这一点，因为：
1. Step 2 的 InfluxDB 查询需要 Step 1 的**每一行数据的字段值**作为参数
2. Step 3 的 API 调用需要 Step 2 的结果
3. InfluxDB 返回的时序数据和 Map API 返回的 JSON 与 SQL 表结果的格式完全不同，当前的 `_extract_from_source` 方法没有统一这些异构数据的入口

---

## 解决方向

要支持漏斗式提取，你需要在架构层面做几件事：

### 1. 引入 `depends_on` 和 `input_ref` 语义

在 extraction 配置中允许声明依赖关系：

```yaml
extractions:
  - id: "step1_cycles"
    source_type: "mysql"
    query: "SELECT vehicle_id, start_time, end_time FROM ... WHERE ..."
    output_key: "raw_cycles"
    
  - id: "step2_telemetry"
    source_type: "influxdb"
    depends_on: "step1_cycles"           # 声明依赖
    input_ref: "raw_cycles"              # 引用前一步结果
    iteration_mode: "per_row"            # 每行迭代 or "batch"
    query: |
      from(bucket: "vehicle_can")
        |> range(start: {{ row.start_time }}, stop: {{ row.end_time }})
        |> filter(fn: (r) => r.vehicle_id == "{{ row.vehicle_id }}")
    output_key: "raw_telemetry"

  - id: "step3_map"
    source_type: "http_json"
    depends_on: "step2_telemetry"
    input_ref: "raw_telemetry"
    iteration_mode: "batch"
    endpoint: "https://map-api.example.com/v1/road_info"
    request_body_template: |
      { "coordinates": {{ batch.gps_points | tojson }} }
    output_key: "raw_road_info"
```

### 2. Loader 改造为拓扑排序执行

把 `for task in extractions` 的扁平循环改为**拓扑排序**，有依赖的 step 延后执行，并在渲染查询模板时注入前一步的 DataFrame：

```python
# 伪代码
for task in topological_sort(extractions):
    if task.depends_on:
        upstream_df = ctx.get_dataframe(task.input_ref)
        # 将 upstream_df 的数据注入查询模板
        df = self._extract_with_ref(task, upstream_df, context)
    else:
        df = self._extract_from_source(task, source_meta, context)
    ctx.put_dataframe(task.output_key, df, ...)
```

### 3. 抽象 DataSourceExtractor 接口

用策略模式统一不同数据源的提取方式：

```python
class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, query_params: dict, ref_df: Optional[pl.DataFrame]) -> pl.DataFrame:
        """统一接口：支持有/无上游引用两种模式"""

class MySQLExtractor(BaseExtractor): ...
class InfluxDBExtractor(BaseExtractor): ...    # Flux 查询 + 时间窗口过滤
class HttpJsonExtractor(BaseExtractor): ...    # REST API + JSON 解析
```

### 4. 引入 iteration_mode（迭代模式）

对于 InfluxDB 和 Map API，你需要支持两种迭代方式：
- `per_row`：前一步结果的每一行生成一个查询（适用于时序数据按 vehicle 查询）
- `batch`：整个 DataFrame 作为参数构造一个查询（适用于批量 API 调用）

---

## 总结

| 能力 | 当前状态 | 漏斗场景需要 |
|------|---------|-------------|
| 并行提取多个同构数据源 | ✅ | ✅ |
| 提取步骤间的数据依赖 | ❌ | ✅ 核心需求 |
| InfluxDB 时序查询 | ❌ 占位符 | ✅ 按时间窗口 + tag 查询 |
| HTTP API / JSON 数据源 | ❌ 完全未涉及 | ✅ REST 调用 + JSON 解析 |
| 查询参数来自上游结果 | ❌ 只有 Airflow 宏 | ✅ 行级/批量参数化 |
| 多种 iteration mode | ❌ | ✅ per_row / batch |

当前的 pipeline 本质上是一个 **"并行扇出 → 后置合并"** 的模型，而你需要的是一个 **"串行漏斗 → 逐级收窄"** 的模型。这两者在数据流向上是根本不同的。要支持漏斗模式，需要对 `UniversalLoaderOperator` 做较大的架构升级，核心是引入 extraction 之间的依赖图和参数化查询能力。

```python
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        # ...
        # 执行每个抽取任务
        for task in extractions:
            self._execute_extraction_task(task, source_meta, ctx, context)
        
        self.log.info(f"✅ Completed {len(extractions)} extractions")
```

```python
        template = Template(sql_template)
        
        # 提取常用宏
        template_vars = {
            'ds': context.get('ds'),                    # 2026-01-26
            'ds_nodash': context.get('ds_nodash'),      # 20260126
            'ts': context.get('ts'),                    # 2026-01-26T12:00:00+00:00
            'ts_nodash': context.get('ts_nodash'),      # 20260126T120000
            'execution_date': context.get('execution_date'),
            'prev_ds': context.get('prev_ds'),
            'next_ds': context.get('next_ds'),
            'yesterday_ds': context.get('yesterday_ds'),
            'tomorrow_ds': context.get('tomorrow_ds'),
        }
        
        # 渲染
        rendered_sql = template.render(**template_vars)
```

```python
    def _build_task_pipeline(self, config: SourceYAMLConfig, dag: DAG, sensor_config: Any):
        """
        构建完整的任务流水线
        
        Pipeline: [Sensor] -> Loader -> Adapter -> Rules -> Aggregator -> Dispatcher
        """
        # ... 只有一个 Loader、一个 Adapter ...
```

```plaintext
Extract A (独立) ──┐
Extract B (独立) ──┼── 全部写入 Context ──→ Adapter (Join) ──→ Entity
Extract C (独立) ──┘
```

```plaintext
Extract A ──→ A的结果作为参数 ──→ Extract B ──→ B的结果作为参数 ──→ Extract C ──→ Entity
```

```python
    def _apply_joins(self, df: pl.DataFrame, ctx: GovernanceContext) -> pl.DataFrame:
        # ...
        for idx, join_config in enumerate(joins):
            # ...
            df = df.join(
                df_right,
                left_on=left_on,
                right_on=right_on,
                how=join_type,
                suffix=suffix
            )
        return df
```

```yaml
extractions:
  - id: "step1_cycles"
    source_type: "mysql"
    query: "SELECT vehicle_id, start_time, end_time FROM ... WHERE ..."
    output_key: "raw_cycles"
    
  - id: "step2_telemetry"
    source_type: "influxdb"
    depends_on: "step1_cycles"           # 声明依赖
    input_ref: "raw_cycles"              # 引用前一步结果
    iteration_mode: "per_row"            # 每行迭代 or "batch"
    query: |
      from(bucket: "vehicle_can")
        |> range(start: {{ row.start_time }}, stop: {{ row.end_time }})
        |> filter(fn: (r) => r.vehicle_id == "{{ row.vehicle_id }}")
    output_key: "raw_telemetry"

  - id: "step3_map"
    source_type: "http_json"
    depends_on: "step2_telemetry"
    input_ref: "raw_telemetry"
    iteration_mode: "batch"
    endpoint: "https://map-api.example.com/v1/road_info"
    request_body_template: |
      { "coordinates": {{ batch.gps_points | tojson }} }
    output_key: "raw_road_info"
```

```python
# 伪代码
for task in topological_sort(extractions):
    if task.depends_on:
        upstream_df = ctx.get_dataframe(task.input_ref)
        # 将 upstream_df 的数据注入查询模板
        df = self._extract_with_ref(task, upstream_df, context)
    else:
        df = self._extract_from_source(task, source_meta, context)
    ctx.put_dataframe(task.output_key, df, ...)
```

```python
class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, query_params: dict, ref_df: Optional[pl.DataFrame]) -> pl.DataFrame:
        """统一接口：支持有/无上游引用两种模式"""

class MySQLExtractor(BaseExtractor): ...
class InfluxDBExtractor(BaseExtractor): ...    # Flux 查询 + 时间窗口过滤
class HttpJsonExtractor(BaseExtractor): ...    # REST API + JSON 解析
```

---

**User**

接下来我所说的优化都围绕着这个项目的主治理流水线。首先我要补充一点：主治理流水线必须可以根据dag_factory动态地生成dag，也就是根据sources中的yaml文件动态生成不同的pipeline。你曾经提到过这个项目的不足之处有很多，比如规则表达力有局限性，那你会如何进行优化呢？我们最好先讨论方案再写代码

---

**Cursor**

让我先全面审视当前代码的所有关键模块，确保我对现状有完整的理解，然后我们来讨论优化方案。

我已经全面审视了整个代码库。现在来系统讨论优化方案。

---

## 当前架构的完整画面

先回顾一下 DAG Factory 动态生成的主治理流水线：

```
[Sensor] → Loader → Adapter → [Rule TaskGroup] → Aggregator → Dispatcher → [AssetSave]
```

这个结构是由 `_build_task_pipeline()` 硬编码的，每个 `sources/*.yaml` 都会生成一套完整的 pipeline。这个核心流程是正确的，我们的优化不应该改变这个拓扑结构，而是**增强每个节点的能力**。

---

## 🔍 局限性全景分析

我按优先级和影响面排列：

### ⚡ 局限性 1：规则引擎只能做纯过滤，表达力严重受限

当前 `GenericRuleOperator` 的核心逻辑只有一行：

```88:93:plugins/operators/rule_engine.py
            filter_expr_str = self._config['logic']['filter_expr']
            predicate = self._parse_expression(filter_expr_str)
            
            hit_df = df.filter(predicate)
```

它**只能做 `df.filter(expr)`**——即对每一行做布尔判断。无法表达以下常见的数据质量检查场景：

| 场景 | 示例 | 当前能否实现 |
|------|------|:----------:|
| 简单行过滤 | `end_time < start_time` | ✅ |
| 先计算再过滤 | 先算 `efficiency = box / duration`，再判断 `efficiency < 0.01` | ❌ |
| 聚合校验 | 按 `vehicle_id` 分组求和，检查某车当天总箱数是否为 0 | ❌ |
| 统计异常检测 | `power > mean + 3σ` （Z-Score 异常值） | ❌ |
| 窗口规则 | 连续 3 个 cycle 都超时的车辆 | ❌ |
| 跨实体关联 | 将 cycle 数据与外部排程表对比 | ❌ |

### ⚡ 局限性 2：Aggregator 硬编码了字段名和处理逻辑

看 `_process_p1_assets`：

```204:219:plugins/operators/aggregator.py
                        for row in p1_df.iter_rows(named=True):
                            # 构建目标路径
                            target_path = asset_path_template.format(
                                batch_id=ctx.batch_id,
                                cycle_id=row['cycle_id']
                            )
                            
                            ctx.add_asset(
                                asset_id=row['cycle_id'],
                                asset_type="HIGH_VALUE_SCENARIO",  # 可从配置读取
                                rule_id=rule_id,  # 添加规则ID
                                vehicle_id=row['vehicle_id'],
                                start_ts=str(row['start_time']),
                                end_ts=str(row['end_time']),
                                tags=["VALID_TWIN_LIFT"],  # 标记为白名单
                                target_path=target_path
                            )
```

问题：
- `row['cycle_id']`, `row['vehicle_id']`, `row['start_time']` 全部硬编码
- `asset_type="HIGH_VALUE_SCENARIO"` 硬编码
- `tags=["VALID_TWIN_LIFT"]` 硬编码
- 如果换一个 entity（比如 `Vehicle`、`Task`），字段名完全不对

**这意味着 Aggregator 无法适配不同的实体和业务场景**，每换一种 entity 就要改代码。

### ⚡ 局限性 3：Rule YAML 的 `input`、`output`、`alert.content_template` 都是摆设

对比规则 YAML 声明的字段 vs 代码实际使用的字段：

| YAML 字段 | 代码是否使用 | 说明 |
|-----------|:-----------:|------|
| `meta.rule_id` | ✅ | 用于标识 |
| `meta.severity` | ✅ | 写入 `_severity` 列 |
| `target_entity` | ✅ | 作为 entity key（但混用了用途） |
| `logic.filter_expr` | ✅ | 核心过滤逻辑 |
| `alert.title` | ✅ | 告警标题 |
| `input.key` | ❌ | **完全未读取**，代码用 `target_entity` 替代 |
| `output.key` | ❌ | **完全未读取**，代码硬编码 `f"{rule_id}_hits"` |
| `output.stage` | ❌ | **完全未读取**，代码硬编码 `"RESULT"` |
| `alert.template` | ❌ | **完全未读取**，Aggregator 硬编码告警内容 |
| `asset.tag` | ❌ | **完全未读取**，Aggregator 硬编码 tag |
| `asset.category` | ❌ | **完全未读取** |

这些字段写了但没用，而代码里又用硬编码值替代，说明 **规则的声明式配置和代码的执行逻辑是脱节的**。

### 局限性 4：`target_entity` 一词多义

在 rule YAML 中：
```yaml
target_entity: Cycle  # 这里 "Cycle" 被当作 alt_key 来查找 entity DataFrame
```

但在 source YAML 中：
```yaml
target_entity: "Cycle"  # 这里 "Cycle" 用于匹配规则和 adapter 文件名
```

同一个字段在不同上下文有不同含义，且规则中真正的 entity key `entity_cycle` 实际上是通过 adapter 的 `alt_key` 间接映射的。

---

## 📋 优化方案

我建议分 **两个阶段** 实施，先处理影响最大且不破坏现有功能的部分：

### 阶段一：Rule Engine + Aggregator 增强（核心）

#### 1.1 规则 YAML V2 格式

向后兼容现有格式，新增可选字段：

```yaml
meta:
  rule_id: rule_p0_efficiency_check
  severity: P0
  description: "效率异常检测"

target_entity: Cycle

# 输入（修复：code 真正读这个字段）
input:
  entity_key: entity_cycle   # 明确的 Context key

# ✨ NEW: 支持多种逻辑类型
logic:
  type: filter              # filter（默认） | aggregate | composite

  # ✨ NEW: 预计算步骤（在 filter 前添加计算列）
  pre_compute:
    - name: "efficiency"
      expr: "col('box') / col('duration')"
    - name: "power_z_score"
      expr: "(col('power_consumption') - col('power_consumption').mean()) / col('power_consumption').std()"

  # 过滤表达式（和现在一样）
  filter_expr: "col('efficiency') < 0.01"

# ✨ NEW: 告警配置增强
alert:
  enabled: true
  title: "[P0] 效率异常"
  # ✨ NEW: Jinja 模板渲染告警内容
  content_template: |
    发现 {{ hit_count }} 条效率异常数据
    日期: {{ run_date }}
    涉及车辆: {{ vehicle_ids[:5] | join(', ') }}

# ✨ NEW: 资产配置增强（从规则自描述）
asset:
  enabled: false   # P0 通常不需要保存资产

depends_on: []
```

对于 P1 资产型规则：
```yaml
meta:
  rule_id: rule_p1_twin_lift
  severity: P1

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('is_twin_lift') == 1"

# ✨ NEW: 资产元数据映射（完全数据驱动，不再硬编码）
asset:
  enabled: true
  asset_type: "TWIN_LIFT"
  field_mapping:
    asset_id: "cycle_id"         # 哪个字段作为 asset_id
    vehicle_id: "vehicle_id"     # 哪个字段作为 vehicle_id
    start_ts: "start_time"       # 时间窗口起点
    end_ts: "end_time"           # 时间窗口终点
  tags: ["twin_lift", "high_value"]
  target_path_template: "corner_case/{batch_id}/{cycle_id}/"

alert:
  enabled: false   # P1 不发告警

depends_on: []
```

对于聚合型规则（新增）：
```yaml
meta:
  rule_id: rule_p0_daily_box_zero
  severity: P0
  description: "检测某车某天作业箱数为 0"

target_entity: Cycle

input:
  entity_key: entity_cycle

# ✨ NEW: 聚合型规则
logic:
  type: aggregate
  group_by: ["vehicle_id", "shift_date"]
  agg_exprs:
    - name: "total_box"
      expr: "col('box').sum()"
    - name: "cycle_count"
      expr: "col('cycle_id').count()"
  having_expr: "col('total_box') == 0"

alert:
  enabled: true
  title: "[P0] 车辆整日零箱"
  content_template: "{{ hit_count }} 台车当天作业箱数为0"

depends_on: []
```

#### 1.2 GenericRuleOperator V2 执行逻辑

核心变更是 `execute_logic` 从单一 filter 升级为多类型分发：

```python
def execute_logic(self, ctx, context):
    logic_type = self._config['logic'].get('type', 'filter')
    
    # 读取 entity（使用 input.entity_key，有兜底）
    entity_key = self._config.get('input', {}).get('entity_key') \
                 or self._config['target_entity']
    df = ctx.get_dataframe(key=entity_key, use_alt_key=True)
    
    # 预计算步骤（可选）
    df = self._apply_pre_compute(df)
    
    # 根据逻辑类型分发
    if logic_type == 'filter':
        hit_df = self._execute_filter(df)
    elif logic_type == 'aggregate':
        hit_df = self._execute_aggregate(df)
    elif logic_type == 'composite':
        hit_df = self._execute_composite(df)
    else:
        raise ValueError(f"Unknown logic type: {logic_type}")
    
    # 写入结果 + 注册（和现在一样）
    ...
```

`_apply_pre_compute` 的实现：
```python
def _apply_pre_compute(self, df):
    pre_computes = self._config['logic'].get('pre_compute', [])
    for step in pre_computes:
        expr = self._parse_expression(step['expr']).alias(step['name'])
        df = df.with_columns(expr)
    return df
```

`_execute_aggregate` 的实现：
```python
def _execute_aggregate(self, df):
    config = self._config['logic']
    group_cols = config['group_by']
    
    # 构建聚合表达式
    agg_exprs = []
    for agg in config['agg_exprs']:
        expr = self._parse_expression(agg['expr']).alias(agg['name'])
        agg_exprs.append(expr)
    
    # 分组聚合
    agg_df = df.group_by(group_cols).agg(agg_exprs)
    
    # HAVING 过滤
    having_expr = self._parse_expression(config['having_expr'])
    hit_df = agg_df.filter(having_expr)
    
    return hit_df
```

#### 1.3 Aggregator 数据驱动化

核心思路：**Aggregator 不再硬编码字段名，而是从每个规则的完整 config 中读取元数据**。

当前 Aggregator 只从 `RuleOutputRef` 里拿到 `rule_id`、`hit_count`、`alert_title`，无法知道这个规则的完整 YAML 配置。

**解决方案**：在 `RuleOutputRef` 上扩展一个 `rule_config` 字段，或者让 Aggregator 自己加载规则配置文件。我倾向后者（更解耦）：

```python
class ContextAggregatorOperator(BaseGovernanceOperator):
    def __init__(self, rule_task_ids, rules_dir=None, **kwargs):
        self.rule_task_ids = rule_task_ids
        # NEW: 聚合器可以自己扫描规则配置
        self.rule_scanner = RuleScanner(rules_dir=rules_dir)
        super().__init__(**kwargs)
```

然后在处理 P1 资产时：
```python
def _process_p1_assets(self, ctx, rule_configs):
    for rule_id, rule_output in ctx.rule_outputs.items():
        if rule_output.hit_count == 0:
            continue
        
        # 从规则配置中读取 asset 定义
        rule_cfg = rule_configs.get(rule_id, {})
        asset_cfg = rule_cfg.get('asset', {})
        if not asset_cfg.get('enabled', False):
            continue
        
        hit_df = ctx.get_dataframe(key=f"{rule_id}_hits")
        p1_df = hit_df.filter(pl.col('_severity') == 'P1')
        
        if p1_df.height == 0:
            continue
        
        # ✨ 字段映射完全来自 YAML 配置
        field_map = asset_cfg['field_mapping']
        for row in p1_df.iter_rows(named=True):
            ctx.add_asset(
                asset_id=str(row[field_map['asset_id']]),
                asset_type=asset_cfg.get('asset_type', 'UNKNOWN'),
                rule_id=rule_id,
                vehicle_id=str(row[field_map['vehicle_id']]),
                start_ts=str(row[field_map['start_ts']]),
                end_ts=str(row[field_map['end_ts']]),
                tags=asset_cfg.get('tags', []),
                target_path=asset_cfg.get('target_path_template', '').format(
                    batch_id=ctx.batch_id,
                    **{k: row.get(v, '') for k, v in field_map.items()}
                )
            )
```

#### 1.4 告警内容模板化

在 Dispatcher 中支持 Jinja 渲染 `content_template`：

```python
def _render_alert_content(self, alert, ctx, rule_config):
    template_str = rule_config.get('alert', {}).get('content_template')
    if template_str:
        template = Template(template_str)
        return template.render(
            hit_count=len(alert.trigger_cycle_ids),
            run_date=ctx.run_date,
            batch_id=ctx.batch_id,
            vehicle_ids=alert.trigger_cycle_ids,  # 可扩展
        )
    return alert.content  # 兜底
```

---

### 阶段二：Loader 漏斗式提取（上次讨论的）

这个我们上次已详细讨论过，核心改动：
- extraction 支持 `depends_on` + `input_ref`
- Loader 按拓扑排序执行 extraction
- 新增 InfluxDB / HTTP 数据源 extractor

---

### 实施影响分析

| 改动项 | 影响范围 | 向后兼容 |
|--------|---------|:--------:|
| Rule YAML 新增 `logic.type`, `pre_compute` | rule YAML + rule_engine.py | ✅ `type` 默认 `filter` |
| Rule YAML 新增 `input.entity_key` | rule YAML + rule_engine.py | ✅ 兜底用 `target_entity` |
| Rule YAML 新增 `asset.field_mapping` | rule YAML + aggregator.py | ✅ 无则走旧逻辑 |
| Rule YAML 新增 `alert.content_template` | rule YAML + dispatcher.py | ✅ 无则走旧逻辑 |
| Aggregator 加载规则配置 | aggregator.py + dag_factory.py | ✅ 新增参数 |
| RuleScanner 校验放宽 | rule_scanner.py | ✅ 新字段可选 |
| source_config_schema 无变动 | - | ✅ |

---

## 讨论问题

在我们开始写代码之前，有几个决策点想确认：

1. **阶段优先级**：你是否同意先做阶段一（规则引擎增强），再做阶段二（Loader 漏斗）？

2. **聚合型规则的需求**：你当前的业务中是否确实有"按 `vehicle_id` 分组后检查聚合值"这类需求？这决定了 `logic.type: aggregate` 是否需要立即实现。

3. **Aggregator 规则配置来源**：Aggregator 获取规则完整配置有两种方式：
   - **方式 A**：DAG Factory 在创建 Aggregator 时把所有规则配置传给它（通过 `config_dict`）
   - **方式 B**：Aggregator 自己扫描 `rules/` 目录加载配置
   - 方式 A 更简单直接，方式 B 更解耦。你倾向哪个？

4. **现有规则 YAML 迁移**：现有的 5 个规则 YAML（`p0_time_check`、`p0_box_count`、`p0_ckp_check`、`p0_power_consumption`、`p1_twin_lift`、`p2_timeout`）是否需要在改完代码后统一升级到新格式？

请告诉我你的想法，然后我们就可以开始实施了。

```plaintext
[Sensor] → Loader → Adapter → [Rule TaskGroup] → Aggregator → Dispatcher → [AssetSave]
```

```python
            filter_expr_str = self._config['logic']['filter_expr']
            predicate = self._parse_expression(filter_expr_str)
            
            hit_df = df.filter(predicate)
```

```python
                        for row in p1_df.iter_rows(named=True):
                            # 构建目标路径
                            target_path = asset_path_template.format(
                                batch_id=ctx.batch_id,
                                cycle_id=row['cycle_id']
                            )
                            
                            ctx.add_asset(
                                asset_id=row['cycle_id'],
                                asset_type="HIGH_VALUE_SCENARIO",  # 可从配置读取
                                rule_id=rule_id,  # 添加规则ID
                                vehicle_id=row['vehicle_id'],
                                start_ts=str(row['start_time']),
                                end_ts=str(row['end_time']),
                                tags=["VALID_TWIN_LIFT"],  # 标记为白名单
                                target_path=target_path
                            )
```

```yaml
target_entity: Cycle  # 这里 "Cycle" 被当作 alt_key 来查找 entity DataFrame
```

```yaml
target_entity: "Cycle"  # 这里 "Cycle" 用于匹配规则和 adapter 文件名
```

```yaml
meta:
  rule_id: rule_p0_efficiency_check
  severity: P0
  description: "效率异常检测"

target_entity: Cycle

# 输入（修复：code 真正读这个字段）
input:
  entity_key: entity_cycle   # 明确的 Context key

# ✨ NEW: 支持多种逻辑类型
logic:
  type: filter              # filter（默认） | aggregate | composite

  # ✨ NEW: 预计算步骤（在 filter 前添加计算列）
  pre_compute:
    - name: "efficiency"
      expr: "col('box') / col('duration')"
    - name: "power_z_score"
      expr: "(col('power_consumption') - col('power_consumption').mean()) / col('power_consumption').std()"

  # 过滤表达式（和现在一样）
  filter_expr: "col('efficiency') < 0.01"

# ✨ NEW: 告警配置增强
alert:
  enabled: true
  title: "[P0] 效率异常"
  # ✨ NEW: Jinja 模板渲染告警内容
  content_template: |
    发现 {{ hit_count }} 条效率异常数据
    日期: {{ run_date }}
    涉及车辆: {{ vehicle_ids[:5] | join(', ') }}

# ✨ NEW: 资产配置增强（从规则自描述）
asset:
  enabled: false   # P0 通常不需要保存资产

depends_on: []
```

```yaml
meta:
  rule_id: rule_p1_twin_lift
  severity: P1

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('is_twin_lift') == 1"

# ✨ NEW: 资产元数据映射（完全数据驱动，不再硬编码）
asset:
  enabled: true
  asset_type: "TWIN_LIFT"
  field_mapping:
    asset_id: "cycle_id"         # 哪个字段作为 asset_id
    vehicle_id: "vehicle_id"     # 哪个字段作为 vehicle_id
    start_ts: "start_time"       # 时间窗口起点
    end_ts: "end_time"           # 时间窗口终点
  tags: ["twin_lift", "high_value"]
  target_path_template: "corner_case/{batch_id}/{cycle_id}/"

alert:
  enabled: false   # P1 不发告警

depends_on: []
```

```yaml
meta:
  rule_id: rule_p0_daily_box_zero
  severity: P0
  description: "检测某车某天作业箱数为 0"

target_entity: Cycle

input:
  entity_key: entity_cycle

# ✨ NEW: 聚合型规则
logic:
  type: aggregate
  group_by: ["vehicle_id", "shift_date"]
  agg_exprs:
    - name: "total_box"
      expr: "col('box').sum()"
    - name: "cycle_count"
      expr: "col('cycle_id').count()"
  having_expr: "col('total_box') == 0"

alert:
  enabled: true
  title: "[P0] 车辆整日零箱"
  content_template: "{{ hit_count }} 台车当天作业箱数为0"

depends_on: []
```

```python
def execute_logic(self, ctx, context):
    logic_type = self._config['logic'].get('type', 'filter')
    
    # 读取 entity（使用 input.entity_key，有兜底）
    entity_key = self._config.get('input', {}).get('entity_key') \
                 or self._config['target_entity']
    df = ctx.get_dataframe(key=entity_key, use_alt_key=True)
    
    # 预计算步骤（可选）
    df = self._apply_pre_compute(df)
    
    # 根据逻辑类型分发
    if logic_type == 'filter':
        hit_df = self._execute_filter(df)
    elif logic_type == 'aggregate':
        hit_df = self._execute_aggregate(df)
    elif logic_type == 'composite':
        hit_df = self._execute_composite(df)
    else:
        raise ValueError(f"Unknown logic type: {logic_type}")
    
    # 写入结果 + 注册（和现在一样）
    ...
```

```python
def _apply_pre_compute(self, df):
    pre_computes = self._config['logic'].get('pre_compute', [])
    for step in pre_computes:
        expr = self._parse_expression(step['expr']).alias(step['name'])
        df = df.with_columns(expr)
    return df
```

```python
def _execute_aggregate(self, df):
    config = self._config['logic']
    group_cols = config['group_by']
    
    # 构建聚合表达式
    agg_exprs = []
    for agg in config['agg_exprs']:
        expr = self._parse_expression(agg['expr']).alias(agg['name'])
        agg_exprs.append(expr)
    
    # 分组聚合
    agg_df = df.group_by(group_cols).agg(agg_exprs)
    
    # HAVING 过滤
    having_expr = self._parse_expression(config['having_expr'])
    hit_df = agg_df.filter(having_expr)
    
    return hit_df
```

```python
class ContextAggregatorOperator(BaseGovernanceOperator):
    def __init__(self, rule_task_ids, rules_dir=None, **kwargs):
        self.rule_task_ids = rule_task_ids
        # NEW: 聚合器可以自己扫描规则配置
        self.rule_scanner = RuleScanner(rules_dir=rules_dir)
        super().__init__(**kwargs)
```

```python
def _process_p1_assets(self, ctx, rule_configs):
    for rule_id, rule_output in ctx.rule_outputs.items():
        if rule_output.hit_count == 0:
            continue
        
        # 从规则配置中读取 asset 定义
        rule_cfg = rule_configs.get(rule_id, {})
        asset_cfg = rule_cfg.get('asset', {})
        if not asset_cfg.get('enabled', False):
            continue
        
        hit_df = ctx.get_dataframe(key=f"{rule_id}_hits")
        p1_df = hit_df.filter(pl.col('_severity') == 'P1')
        
        if p1_df.height == 0:
            continue
        
        # ✨ 字段映射完全来自 YAML 配置
        field_map = asset_cfg['field_mapping']
        for row in p1_df.iter_rows(named=True):
            ctx.add_asset(
                asset_id=str(row[field_map['asset_id']]),
                asset_type=asset_cfg.get('asset_type', 'UNKNOWN'),
                rule_id=rule_id,
                vehicle_id=str(row[field_map['vehicle_id']]),
                start_ts=str(row[field_map['start_ts']]),
                end_ts=str(row[field_map['end_ts']]),
                tags=asset_cfg.get('tags', []),
                target_path=asset_cfg.get('target_path_template', '').format(
                    batch_id=ctx.batch_id,
                    **{k: row.get(v, '') for k, v in field_map.items()}
                )
            )
```

```python
def _render_alert_content(self, alert, ctx, rule_config):
    template_str = rule_config.get('alert', {}).get('content_template')
    if template_str:
        template = Template(template_str)
        return template.render(
            hit_count=len(alert.trigger_cycle_ids),
            run_date=ctx.run_date,
            batch_id=ctx.batch_id,
            vehicle_ids=alert.trigger_cycle_ids,  # 可扩展
        )
    return alert.content  # 兜底
```

---

**User**

1.同意你的阶段优先级 2.有聚合型需求，需要立即实现 3.我希望使用方案B 4.需要把现有的yaml规则都统一升级到新格式

---

**Cursor**

好的，所有决策已确认。让我开始系统性地实施阶段一的优化。

现在我有了完整的全景——共 11 个规则文件需要升级。让我开始实施。

### Step 1: 更新 rule_scanner.py — 支持 V2 YAML 校验

```python
"""
orchestration/rule_scanner.py
规则扫描器

职责：
- 扫描规则配置文件（configs/rules/*.yaml）
- 解析规则依赖关系
- 拓扑排序（处理依赖顺序）
- 验证配置完整性（防御性解析）
- 支持 V2 规则格式（filter / aggregate 类型）

运行时机：
- DAG 文件顶层代码（解析时）
- Aggregator 执行时（加载规则元数据）
- 不能有耗时 I/O 操作（只读本地文件）
"""
import os
import glob
import yaml
from typing import List, Dict, Any, Optional


class RuleScanner:
    """
    规则扫描器
    
    使用示例:
    ```python
    # 在 DAG 文件顶层代码中
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    
    # 动态生成 Rule Tasks
    for rule in rules:
        GenericRuleOperator(
            task_id=rule['rule_id'],
            config_path=rule['config_path'],
            ...
        )
    ```
    """
    
    def __init__(self, rules_dir: str = None):
        """
        初始化
        
        Args:
            rules_dir: 规则目录路径（相对于 $AIRFLOW_HOME/plugins/）
        """
        self.rules_dir = rules_dir or 'configs/rules'
    
    def scan_rules(self) -> List[Dict[str, Any]]:
        """
        扫描规则并返回排序后的列表
        
        Returns:
            规则配置列表（已按依赖关系拓扑排序）
            
        示例:
            [
                {
                    'rule_id': 'rule_p0_time_check',
                    'config_path': 'configs/rules/p0_time_check.yaml',
                    'depends_on': [],
                    'severity': 'P0',
                    'config': {...}
                },
                ...
            ]
        
        Raises:
            ValueError: 配置验证失败、循环依赖等
        """
        # 1. 扫描目录
        rule_files = self._scan_rule_files()
        
        if not rule_files:
            raise ValueError(f"No rule files found in {self.rules_dir}")
        
        print(f"📋 Found {len(rule_files)} rule files")
        
        # 2. 加载并验证规则
        rules = []
        for file_path in rule_files:
            try:
                rule_config = self._load_and_validate_rule(file_path)
                rules.append(rule_config)
            except Exception as e:
                # 配置错误：拒绝加载 DAG
                raise ValueError(f"Failed to load rule from {file_path}: {e}")
        
        # 3. 拓扑排序（处理依赖关系）
        sorted_rules = self._topological_sort(rules)
        
        print(f"✅ Loaded {len(sorted_rules)} rules (topologically sorted)")
        
        return sorted_rules
    
    def get_rule_configs_by_id(self, target_entity: Optional[str] = None) -> Dict[str, Dict]:
        """
        获取规则配置字典（按 rule_id 索引）
        
        用于 Aggregator 在执行时获取规则的完整配置，
        以驱动数据驱动的 alert/asset 处理逻辑。
        
        Args:
            target_entity: 可选，按 target_entity 过滤
        
        Returns:
            {rule_id: full_config_dict}
        """
        rules = self.scan_rules()
        
        result = {}
        for rule in rules:
            if target_entity and rule.get('target_entity') != target_entity:
                continue
            result[rule['rule_id']] = rule['config']
        
        return result
    
    def _scan_rule_files(self) -> List[str]:
        """
        扫描规则目录，返回所有 YAML 文件路径
        
        Returns:
            文件路径列表（已排序）
        """
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        rules_path = os.path.join(airflow_home, 'plugins', self.rules_dir)
        
        # 扫描 *.yaml 文件
        pattern = os.path.join(rules_path, '*.yaml')
        files = glob.glob(pattern)
        
        # 排序保证稳定性
        return sorted(files)
    
    def _load_and_validate_rule(self, file_path: str) -> Dict[str, Any]:
        """
        加载并验证单个规则配置
        
        Args:
            file_path: 规则文件路径
        
        Returns:
            规则配置字典
        
        Raises:
            ValueError: 配置验证失败
        """
        # 1. 加载 YAML
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 2. 防御性验证（Schema Validation）
        self._validate_rule_config(config, file_path)
        
        # 3. 提取关键信息
        # 计算相对路径（相对于 plugins/）
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        plugins_dir = os.path.join(airflow_home, 'plugins')
        relative_path = os.path.relpath(file_path, plugins_dir)
        
        return {
            'rule_id': config['meta']['rule_id'],
            'config_path': relative_path,  # 相对路径
            'depends_on': config.get('depends_on', []),
            'severity': config['meta']['severity'],
            'description': config['meta'].get('description', ''),
            'target_entity': config.get('target_entity'),
            'config': config  # 完整配置（供调试和 Aggregator 使用）
        }
    
    def _validate_rule_config(self, config: Dict, file_path: str) -> None:
        """
        验证规则配置完整性（防御性解析，支持 V2 格式）
        
        V2 格式新增支持：
        - logic.type: filter（默认） | aggregate
        - logic.pre_compute: 预计算步骤（可选）
        - logic.group_by / agg_exprs / having_expr: 聚合型规则必需
        - input.entity_key: 明确的实体读取键（可选）
        - alert.content_template: Jinja 模板（可选）
        - alert.trigger_id_field: 触发 ID 字段名（可选）
        - asset.field_mapping: 资产字段映射（可选）
        
        必需字段：
        - meta
        - meta.rule_id
        - meta.severity
        - target_entity
        - logic
        - logic.filter_expr（当 type=filter 时）
        - logic.group_by / agg_exprs / having_expr（当 type=aggregate 时）
        
        Args:
            config: 规则配置字典
            file_path: 文件路径（用于错误提示）
        
        Raises:
            ValueError: 配置验证失败
        """
        errors = []
        
        # ========== meta 校验 ==========
        if 'meta' not in config:
            errors.append("Missing 'meta' field")
        else:
            if 'rule_id' not in config['meta']:
                errors.append("Missing 'meta.rule_id'")
            if 'severity' not in config['meta']:
                errors.append("Missing 'meta.severity'")
            else:
                # 验证 severity 取值
                valid_severities = ['P0', 'P1', 'P2', 'INFO']
                if config['meta']['severity'] not in valid_severities:
                    errors.append(f"Invalid severity: {config['meta']['severity']} (must be one of {valid_severities})")
        
        # ========== target_entity 校验 ==========
        if 'target_entity' not in config:
            errors.append("Missing 'target_entity'")
        
        # ========== logic 校验（支持 V2 多类型） ==========
        if 'logic' not in config:
            errors.append("Missing 'logic' field")
        else:
            logic = config['logic']
            logic_type = logic.get('type', 'filter')  # 默认 filter（向后兼容）
            
            if logic_type == 'filter':
                # filter 类型必须有 filter_expr
                if 'filter_expr' not in logic:
                    errors.append("Missing 'logic.filter_expr' (required for type=filter)")
            
            elif logic_type == 'aggregate':
                # aggregate 类型必须有 group_by, agg_exprs, having_expr
                if 'group_by' not in logic:
                    errors.append("Missing 'logic.group_by' (required for type=aggregate)")
                elif not isinstance(logic['group_by'], list):
                    errors.append("'logic.group_by' must be a list of column names")
                
                if 'agg_exprs' not in logic:
                    errors.append("Missing 'logic.agg_exprs' (required for type=aggregate)")
                elif not isinstance(logic['agg_exprs'], list):
                    errors.append("'logic.agg_exprs' must be a list")
                else:
                    for idx, agg in enumerate(logic['agg_exprs']):
                        if 'name' not in agg:
                            errors.append(f"Missing 'name' in logic.agg_exprs[{idx}]")
                        if 'expr' not in agg:
                            errors.append(f"Missing 'expr' in logic.agg_exprs[{idx}]")
                
                if 'having_expr' not in logic:
                    errors.append("Missing 'logic.having_expr' (required for type=aggregate)")
            
            else:
                errors.append(f"Unknown logic type: '{logic_type}' (must be 'filter' or 'aggregate')")
            
            # pre_compute 校验（可选，两种类型都支持）
            if 'pre_compute' in logic:
                if not isinstance(logic['pre_compute'], list):
                    errors.append("'logic.pre_compute' must be a list")
                else:
                    for idx, step in enumerate(logic['pre_compute']):
                        if 'name' not in step:
                            errors.append(f"Missing 'name' in logic.pre_compute[{idx}]")
                        if 'expr' not in step:
                            errors.append(f"Missing 'expr' in logic.pre_compute[{idx}]")
        
        # ========== asset 校验（可选） ==========
        if 'asset' in config:
            asset = config['asset']
            if asset.get('enabled', False) and 'field_mapping' in asset:
                field_map = asset['field_mapping']
                required_mapping_fields = ['asset_id', 'vehicle_id', 'start_ts', 'end_ts']
                for f in required_mapping_fields:
                    if f not in field_map:
                        errors.append(f"Missing 'asset.field_mapping.{f}' (required when asset.enabled=true)")
        
        # ========== 汇总错误 ==========
        if errors:
            error_msg = f"Invalid rule config in {file_path}:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
    
    def _topological_sort(self, rules: List[Dict]) -> List[Dict]:
        """
        拓扑排序（Kahn 算法）
        
        处理规则依赖关系，确保：
        - 被依赖的规则先执行
        - 检测循环依赖
        
        Args:
            rules: 规则列表
        
        Returns:
            排序后的规则列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 构建依赖图和规则字典
        graph = {}
        rule_dict = {}
        
        for rule in rules:
            rule_id = rule['rule_id']
            graph[rule_id] = rule['depends_on']
            rule_dict[rule_id] = rule
        
        # 验证依赖是否存在
        for rule_id, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise ValueError(
                        f"Rule '{rule_id}' depends on '{dep}', but '{dep}' not found. "
                        f"Available rules: {list(graph.keys())}"
                    )
        
        # 计算入度（我依赖多少个规则）
        # depends_on 的含义：我依赖谁（即存在边 dep -> me）
        # 入度 = len(depends_on)
        in_degree = {rule_id: len(deps) for rule_id, deps in graph.items()}
        
        # Kahn 算法：从入度为 0 的节点开始
        queue = [rule_id for rule_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # 取出一个入度为 0 的节点（没有依赖的规则，可以先执行）
            rule_id = queue.pop(0)
            result.append(rule_dict[rule_id])
            
            # 遍历所有规则，找出"依赖于当前规则"的规则
            for other_rule_id, deps in graph.items():
                if rule_id in deps:
                    # 当前规则已完成，依赖它的规则入度减 1
                    in_degree[other_rule_id] -= 1
                    # 如果入度变为 0，说明所有依赖都已满足，可以执行了
                    if in_degree[other_rule_id] == 0:
                        queue.append(other_rule_id)
        
        # 检测循环依赖
        if len(result) != len(rules):
            # 找出参与循环依赖的规则
            unprocessed = [r['rule_id'] for r in rules if r not in result]
            raise ValueError(
                f"Circular dependency detected in rules! "
                f"Unprocessed rules: {unprocessed}. "
                f"Check the 'depends_on' fields."
            )
        
        return result
    
    def get_rule_summary(self, rules: List[Dict]) -> str:
        """
        生成规则摘要（用于日志）
        
        Args:
            rules: 规则列表
        
        Returns:
            摘要字符串
        """
        lines = ["Rule Execution Order:"]
        for idx, rule in enumerate(rules, 1):
            deps_str = f" (depends on: {', '.join(rule['depends_on'])})" if rule['depends_on'] else ""
            logic_type = rule['config'].get('logic', {}).get('type', 'filter')
            lines.append(f"  {idx}. [{rule['severity']}] {rule['rule_id']} (type={logic_type}){deps_str}")
        
        return "\n".join(lines)

```

### Step 2: 升级 rule_engine.py — 支持 pre_compute + aggregate

```python
"""
operators/rule_engine.py
通用规则执行器 V2

职责：
- 读取标准化实体数据
- 支持多种规则逻辑类型（filter / aggregate）
- 支持预计算步骤（pre_compute）
- 写入命中数据到 Result Parquet
- 添加 severity 常量列
- 区分业务错误和系统错误

V2 新增：
- logic.type: filter（默认） | aggregate
- logic.pre_compute: 预计算列（可选，在主逻辑前添加计算列）
- logic.group_by / agg_exprs / having_expr: 聚合型规则
- input.entity_key: 明确的实体读取键（可选，兜底用 target_entity）
"""
import time
import polars as pl
from polars import col, lit, when
from typing import Dict, Any, List, Optional

from plugins.infra.operators import BaseGovernanceOperator
from plugins.domain.context import GovernanceContext


class GenericRuleOperator(BaseGovernanceOperator):
    """
    通用规则执行器 V2
    
    配置示例 - filter 类型 (configs/rules/p0_time_check.yaml):
    ```yaml
    meta:
      rule_id: "rule_p0_time_inversion"
      severity: "P0"
    
    target_entity: "Cycle"
    
    input:
      entity_key: "entity_cycle"
    
    logic:
      type: filter
      pre_compute:
        - name: "efficiency"
          expr: "col('box') / col('duration')"
      filter_expr: "col('end_time') < col('start_time')"
    
    alert:
      enabled: true
      title: "[P0] 物理时间倒挂"
      trigger_id_field: "cycle_id"
      content_template: "发现 {{ hit_count }} 条时间倒挂数据"
    ```
    
    配置示例 - aggregate 类型:
    ```yaml
    meta:
      rule_id: "rule_p0_daily_box_zero"
      severity: "P0"
    
    target_entity: "Cycle"
    
    input:
      entity_key: "entity_cycle"
    
    logic:
      type: aggregate
      group_by: ["vehicle_id", "shift_date"]
      agg_exprs:
        - name: "total_box"
          expr: "col('box').sum()"
      having_expr: "col('total_box') == 0"
    
    alert:
      enabled: true
      title: "[P0] 车辆整日零箱"
      trigger_id_field: "vehicle_id"
    ```
    """
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        规则执行逻辑 V2
        
        流程：
        1. 读取实体数据（支持 input.entity_key）
        2. 应用预计算步骤（pre_compute）
        3. 根据 logic.type 分发执行
        4. 添加 severity 列
        5. 写入结果（如有命中）
        6. 注册规则输出
        
        异常处理：
        - 业务逻辑错误（如除零）：捕获，生成 ErrorResult，不阻断流程
        - 系统级错误（如 MinIO 连不上）：抛出，触发 Airflow Retry
        """
        # 验证配置
        self._validate_config(['meta', 'target_entity', 'logic'])
        
        rule_id = self._config['meta']['rule_id']
        severity = self._config['meta']['severity']
        alert_title = self._config.get('alert', {}).get('title', '')
        logic_type = self._config['logic'].get('type', 'filter')
        
        self.log.info(f"🔍 Executing rule: {rule_id} ({severity}, type={logic_type})")
        
        # 开始计时
        start_time = time.time()
        
        try:
            # 1. 读取实体数据
            df = self._load_entity(ctx)
            self.log.info(f"Loaded entity: {df.height} rows, {df.width} columns")
            
            # 2. 应用预计算步骤（可选）
            df = self._apply_pre_compute(df)
            
            # 3. 根据逻辑类型分发执行
            if logic_type == 'filter':
                hit_df = self._execute_filter(df)
            elif logic_type == 'aggregate':
                hit_df = self._execute_aggregate(df)
            else:
                raise ValueError(f"Unknown logic type: {logic_type}")
            
            self.log.info(f"Rule result: {hit_df.height} hits")
            
            # 4. 如果有命中，写入结果
            if hit_df.height > 0:
                # 4.1 添加 severity 常量列
                hit_df = hit_df.with_columns(
                    pl.lit(severity).alias('_severity')
                )
                
                # 4.2 清理旧结果
                result_key = f"{rule_id}_hits"
                self._clean_partition(ctx, stage="RESULT", key=result_key)
                
                # 4.3 写入结果 Parquet
                ctx.put_dataframe(
                    key=result_key,
                    df=hit_df,
                    stage="RESULT"
                )
                
                # 4.4 注册规则输出（成功）
                execution_time = time.time() - start_time
                ctx.register_rule_output(
                    rule_id=rule_id,
                    status="SUCCESS",
                    output_uri=ctx.get_data_uri(result_key),
                    hit_count=hit_df.height,
                    execution_time=execution_time,
                    alert_title=alert_title
                )
                
                self.log.info(f"✅ Rule [{rule_id}] completed: {hit_df.height} hits in {execution_time:.2f}s")
            else:
                # 无命中
                execution_time = time.time() - start_time
                ctx.register_rule_output(
                    rule_id=rule_id,
                    status="SUCCESS",
                    hit_count=0,
                    execution_time=execution_time,
                    alert_title=alert_title
                )
                
                self.log.info(f"✅ Rule [{rule_id}] passed: 0 hits in {execution_time:.2f}s")
        
        except Exception as e:
            # 异常处理：区分业务错误和系统错误
            execution_time = time.time() - start_time
            
            if self._is_system_error(e):
                # 系统级错误：抛出，触发 Airflow Retry
                self.log.error(f"🔴 System error in rule [{rule_id}]: {e}")
                raise
            else:
                # 业务逻辑错误：捕获，生成 ErrorResult
                self.log.error(f"⚠️ Logic error in rule [{rule_id}]: {e}")
                
                # 注册规则输出（失败）
                ctx.register_rule_output(
                    rule_id=rule_id,
                    status="FAILED",
                    error_message=str(e),
                    hit_count=0,
                    execution_time=execution_time,
                    alert_title=alert_title
                )
                
                # Task 仍标记为 Success（不阻断流程）
                self.log.info(f"⚠️ Rule [{rule_id}] marked as FAILED but task continues")
    
    # ========================================================================
    # 实体加载
    # ========================================================================
    
    def _load_entity(self, ctx: GovernanceContext) -> pl.DataFrame:
        """
        加载实体数据
        
        优先级：
        1. input.entity_key（V2 明确指定）
        2. target_entity（V1 兼容，通过 alt_key 查找）
        
        Args:
            ctx: 治理上下文
        
        Returns:
            Polars DataFrame
        """
        # V2：优先读 input.entity_key
        entity_key = self._config.get('input', {}).get('entity_key')
        
        if entity_key:
            self.log.info(f"Loading entity via input.entity_key: {entity_key}")
            return ctx.get_dataframe(key=entity_key, use_alt_key=True)
        
        # V1 兼容：使用 target_entity 作为 alt_key
        target_entity = self._config['target_entity']
        self.log.info(f"Loading entity via target_entity (alt_key): {target_entity}")
        return ctx.get_dataframe(key=target_entity, use_alt_key=True)
    
    # ========================================================================
    # 预计算步骤
    # ========================================================================
    
    def _apply_pre_compute(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        应用预计算步骤（在主逻辑前添加计算列）
        
        配置示例:
        ```yaml
        logic:
          pre_compute:
            - name: "efficiency"
              expr: "col('box') / col('duration')"
            - name: "power_z_score"
              expr: "(col('power_consumption') - col('power_consumption').mean()) / col('power_consumption').std()"
        ```
        
        Args:
            df: 输入 DataFrame
        
        Returns:
            添加计算列后的 DataFrame
        """
        pre_computes = self._config['logic'].get('pre_compute', [])
        
        if not pre_computes:
            return df
        
        self.log.info(f"Applying {len(pre_computes)} pre_compute steps...")
        
        for step in pre_computes:
            name = step['name']
            expr_str = step['expr']
            
            try:
                expr = self._parse_expression(expr_str).alias(name)
                df = df.with_columns(expr)
                self.log.info(f"  ✅ Pre-computed: {name}")
            except Exception as e:
                self.log.error(f"  ❌ Failed to pre-compute '{name}': {e}")
                raise ValueError(f"Pre-compute '{name}' failed: {e}")
        
        return df
    
    # ========================================================================
    # 逻辑类型：Filter
    # ========================================================================
    
    def _execute_filter(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        执行过滤型规则
        
        逻辑：df.filter(filter_expr) → 命中的行
        
        Args:
            df: 输入 DataFrame（可能已经过 pre_compute）
        
        Returns:
            命中的 DataFrame
        """
        filter_expr_str = self._config['logic']['filter_expr']
        
        self.log.info(f"Filter expression: {filter_expr_str[:200]}")
        
        predicate = self._parse_expression(filter_expr_str)
        hit_df = df.filter(predicate)
        
        return hit_df
    
    # ========================================================================
    # 逻辑类型：Aggregate
    # ========================================================================
    
    def _execute_aggregate(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        执行聚合型规则
        
        逻辑：df.group_by(cols).agg(exprs).filter(having) → 命中的分组
        
        配置示例:
        ```yaml
        logic:
          type: aggregate
          group_by: ["vehicle_id", "shift_date"]
          agg_exprs:
            - name: "total_box"
              expr: "col('box').sum()"
            - name: "cycle_count"
              expr: "col('cycle_id').count()"
          having_expr: "col('total_box') == 0"
        ```
        
        Args:
            df: 输入 DataFrame
        
        Returns:
            命中的聚合结果 DataFrame（列 = group_by cols + agg cols）
        """
        logic = self._config['logic']
        group_cols = logic['group_by']
        
        self.log.info(f"Aggregate: group_by={group_cols}")
        
        # 1. 构建聚合表达式
        agg_exprs = []
        for agg in logic['agg_exprs']:
            expr = self._parse_expression(agg['expr']).alias(agg['name'])
            agg_exprs.append(expr)
            self.log.info(f"  Agg: {agg['name']} = {agg['expr'][:100]}")
        
        # 2. 分组聚合
        agg_df = df.group_by(group_cols).agg(agg_exprs)
        self.log.info(f"  After group_by: {agg_df.height} groups")
        
        # 3. HAVING 过滤
        having_expr_str = logic['having_expr']
        having_predicate = self._parse_expression(having_expr_str)
        hit_df = agg_df.filter(having_predicate)
        
        self.log.info(f"  After having: {hit_df.height} groups match")
        
        return hit_df
    
    # ========================================================================
    # 表达式解析
    # ========================================================================
    
    def _parse_expression(self, expr_str: str):
        """
        安全解析 Polars 表达式
        
        Args:
            expr_str: 表达式字符串
        
        Returns:
            Polars Expression 对象
        """
        # 定义安全上下文
        safe_context = {
            'col': col,
            'lit': lit,
            'when': when,
            'pl': pl,
            '__builtins__': {}
        }
        
        try:
            expr = eval(expr_str, safe_context)
            return expr
        except Exception as e:
            raise ValueError(f"Invalid expression: {expr_str}. Error: {e}")
    
    # ========================================================================
    # 错误分类
    # ========================================================================
    
    def _is_system_error(self, error: Exception) -> bool:
        """
        判断是否为系统级错误
        
        系统级错误：
        - 网络错误（ConnectionError, TimeoutError）
        - IO 错误（IOError, OSError）
        - 数据库连接错误
        - MinIO 连接错误
        
        业务逻辑错误：
        - 除零错误（ZeroDivisionError）
        - 类型错误（TypeError）
        - 值错误（ValueError）
        - 键错误（KeyError）
        
        Args:
            error: 异常对象
        
        Returns:
            是否为系统级错误
        """
        # 定义系统级错误类型
        system_error_types = (
            ConnectionError,
            IOError,
            OSError,
            TimeoutError,
        )
        
        return isinstance(error, system_error_types)

```

### Step 3: 升级 aggregator.py — 数据驱动化（方案B）

```python
"""
operators/aggregator.py
上下文聚合器 V2（Reduce 阶段）

职责：
- 合并多个 Rule Task 的 Context
- 读取所有 Rule Result Parquet
- 数据驱动的 P0/P1/P2 处理（从规则 YAML 读取配置）
- 生成最终的 Alerts 和 Assets

V2 改进（方案B）：
- Aggregator 自己扫描 rules/ 目录加载规则配置
- Alert 内容、trigger_id_field 从 rule YAML 读取
- Asset 字段映射从 rule YAML 的 asset.field_mapping 读取
- P2 白名单豁免字段可配置
- 不再硬编码字段名
"""
import polars as pl
from typing import Dict, Any, List, Optional
from jinja2 import Template

from plugins.infra.operators import BaseGovernanceOperator, get_multiple_upstream_contexts
from plugins.domain.context import GovernanceContext
from plugins.orchestration.rule_scanner import RuleScanner


class ContextAggregatorOperator(BaseGovernanceOperator):
    """
    上下文聚合器 V2
    
    核心改进：
    1. 通过 RuleScanner（方案B）自动加载规则配置
    2. 所有 alert/asset 处理逻辑由规则 YAML 配置驱动
    3. 不再硬编码字段名、asset_type、tags
    
    使用示例:
    ```python
    aggregator = ContextAggregatorOperator(
        task_id='context_aggregator',
        rule_task_ids=['rule_tasks.rule_p0', 'rule_tasks.rule_p1'],
        rules_dir='configs/rules',  # 方案B：自己扫描规则目录
        dag=dag
    )
    ```
    """
    
    def __init__(
        self,
        upstream_task_ids: List[str] = None,
        rule_task_ids: List[str] = None,
        rules_dir: str = None,
        **kwargs
    ):
        """
        初始化
        
        Args:
            upstream_task_ids: 上游 Task 的 ID 列表（推荐使用）
            rule_task_ids: 上游 Rule Task 的 ID 列表（兼容旧参数名）
            rules_dir: 规则目录路径（方案B，用于加载规则配置）
        """
        # 兼容两种参数名
        if upstream_task_ids is not None:
            self.rule_task_ids = upstream_task_ids
        elif rule_task_ids is not None:
            self.rule_task_ids = rule_task_ids
        else:
            raise ValueError("Must provide either 'upstream_task_ids' or 'rule_task_ids'")
        
        # 方案B：Aggregator 自己加载规则配置
        self.rules_dir = rules_dir or 'configs/rules'
        
        super().__init__(**kwargs)
    
    def _restore_context(self, context: Dict[str, Any]) -> GovernanceContext:
        """
        重写：支持多上游合并
        
        Args:
            context: Airflow context 字典
        
        Returns:
            合并后的 GovernanceContext
        """
        ti = context['task_instance']
        
        # 拉取所有上游 Context
        contexts = get_multiple_upstream_contexts(ti, self.rule_task_ids)
        
        if not contexts:
            raise ValueError("No upstream contexts found! Check rule_task_ids.")
        
        self.log.info(f"Merging {len(contexts)} upstream contexts")
        
        # 合并 Context（取第一个作为基础）
        merged_ctx = contexts[0]
        
        # 合并其他 Context 的 rule_outputs、data_registry 和 audit_logs
        for ctx in contexts[1:]:
            merged_ctx.rule_outputs.update(ctx.rule_outputs)
            merged_ctx.data_registry.update(ctx.data_registry)
            merged_ctx.audit_logs.extend(ctx.audit_logs)
        
        self.log.info(
            f"✅ Merged context: {len(merged_ctx.rule_outputs)} rule outputs, "
            f"{len(merged_ctx.data_registry)} data refs"
        )
        
        return merged_ctx
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        聚合逻辑 V2
        
        流程：
        1. 加载所有规则配置（方案B：自己扫描目录）
        2. 处理 P0 告警
        3. 处理 P1 资产（数据驱动的字段映射）
        4. 处理 P2 违规（可配置的白名单豁免）
        5. 打印统计信息
        """
        self.log.info("🔄 Starting context aggregation V2...")
        
        # 0. 加载规则配置（方案B）
        rule_configs = self._load_rule_configs()
        self.log.info(f"📚 Loaded {len(rule_configs)} rule configs from {self.rules_dir}")
        
        # 统计规则执行结果
        total_rules = len(ctx.rule_outputs)
        success_rules = sum(1 for r in ctx.rule_outputs.values() if r.status == "SUCCESS")
        failed_rules = sum(1 for r in ctx.rule_outputs.values() if r.status == "FAILED")
        total_hits = sum(r.hit_count for r in ctx.rule_outputs.values())
        
        self.log.info(
            f"Rule execution summary: "
            f"{total_rules} total, {success_rules} success, {failed_rules} failed, {total_hits} hits"
        )
        
        # 1. 处理 P0 告警
        self._process_p0_alerts(ctx, rule_configs)
        
        # 2. 处理 P1 资产
        self._process_p1_assets(ctx, rule_configs)
        
        # 3. 处理 P2 违规
        self._process_p2_violations(ctx, rule_configs)
        
        # 4. 打印最终统计
        self.log.info(f"✅ Aggregation completed: {len(ctx.alerts)} alerts, {len(ctx.assets)} assets")
        
        # 5. 推送 GovernanceContext 到 XCom（供 save_assets_to_queue 使用）
        ctx_json = ctx.to_json()
        context['ti'].xcom_push(key='governance_context', value=ctx_json)
        self.log.info(f"📤 Pushed GovernanceContext to XCom (key='governance_context')")
    
    # ========================================================================
    # 方案B：自己加载规则配置
    # ========================================================================
    
    def _load_rule_configs(self) -> Dict[str, Dict]:
        """
        加载所有规则的完整配置（方案B）
        
        Returns:
            {rule_id: full_yaml_config}
        """
        try:
            scanner = RuleScanner(rules_dir=self.rules_dir)
            return scanner.get_rule_configs_by_id()
        except Exception as e:
            self.log.warning(f"⚠️ Failed to load rule configs from {self.rules_dir}: {e}")
            return {}
    
    # ========================================================================
    # P0 处理：数据驱动的告警生成
    # ========================================================================
    
    def _process_p0_alerts(self, ctx: GovernanceContext, rule_configs: Dict[str, Dict]) -> None:
        """
        处理 P0 告警（数据驱动）
        
        改进：
        - trigger_id_field 从 rule YAML 的 alert.trigger_id_field 读取
        - 告警内容支持 Jinja 模板渲染
        - 不再硬编码 'cycle_id'
        
        Args:
            ctx: 治理上下文
            rule_configs: 规则配置字典
        """
        self.log.info("📋 Processing P0 alerts...")
        
        p0_count = 0
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # 使用 _severity 列过滤
                    p0_df = hit_df.filter(pl.col('_severity') == 'P0')
                    
                    if p0_df.height > 0:
                        # 获取规则配置
                        rule_cfg = rule_configs.get(rule_id, {})
                        alert_cfg = rule_cfg.get('alert', {})
                        
                        # 从配置读取 trigger_id_field（默认 cycle_id）
                        trigger_id_field = alert_cfg.get('trigger_id_field', 'cycle_id')
                        
                        # 安全提取 trigger IDs
                        trigger_ids = self._safe_extract_ids(p0_df, trigger_id_field)
                        
                        # 渲染告警内容
                        title = rule_output.alert_title or alert_cfg.get('title', f"[P0] 数据质量异常 - {rule_id}")
                        content = self._render_alert_content(
                            rule_cfg=rule_cfg,
                            trigger_ids=trigger_ids,
                            ctx=ctx,
                            hit_count=p0_df.height,
                            default_content=f"发现 {len(trigger_ids)} 条异常数据"
                        )
                        
                        # 生成告警
                        ctx.add_alert(
                            rule_id=rule_id,
                            severity="P0",
                            title=title,
                            content=content,
                            trigger_cycle_ids=[str(t) for t in trigger_ids]
                        )
                        
                        p0_count += 1
                        self.log.info(f"  ✅ Generated P0 alert: {rule_id} ({p0_df.height} hits)")
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"📋 P0 alerts processed: {p0_count} alerts generated")
    
    # ========================================================================
    # P1 处理：数据驱动的资产生成
    # ========================================================================
    
    def _process_p1_assets(self, ctx: GovernanceContext, rule_configs: Dict[str, Dict]) -> None:
        """
        处理 P1 资产（数据驱动的字段映射）
        
        改进：
        - 字段映射从 rule YAML 的 asset.field_mapping 读取
        - asset_type、tags 从 rule YAML 的 asset 配置读取
        - 不再硬编码 'cycle_id', 'vehicle_id' 等字段名
        
        Args:
            ctx: 治理上下文
            rule_configs: 规则配置字典
        """
        self.log.info("📦 Processing P1 assets...")
        
        asset_count = 0
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # 使用 _severity 列过滤
                    p1_df = hit_df.filter(pl.col('_severity') == 'P1')
                    
                    if p1_df.height > 0:
                        # 获取规则配置
                        rule_cfg = rule_configs.get(rule_id, {})
                        asset_cfg = rule_cfg.get('asset', {})
                        
                        # 检查 asset 是否启用（P1 默认启用）
                        if not asset_cfg.get('enabled', True):
                            self.log.info(f"  ⏭️ Asset disabled for {rule_id}, skipping")
                            continue
                        
                        # 获取字段映射（有默认值兜底）
                        field_map = asset_cfg.get('field_mapping', {
                            'asset_id': 'cycle_id',
                            'vehicle_id': 'vehicle_id',
                            'start_ts': 'start_time',
                            'end_ts': 'end_time'
                        })
                        
                        asset_type = asset_cfg.get('asset_type', 'HIGH_VALUE_SCENARIO')
                        tags = asset_cfg.get('tags', [])
                        path_template = asset_cfg.get(
                            'target_path_template',
                            'corner_case/{batch_id}/{asset_id}/'
                        )
                        
                        # 生成 Assets
                        for row in p1_df.iter_rows(named=True):
                            # 安全读取映射字段
                            asset_id_val = str(row.get(field_map.get('asset_id', 'cycle_id'), 'UNKNOWN'))
                            vehicle_id_val = str(row.get(field_map.get('vehicle_id', 'vehicle_id'), 'UNKNOWN'))
                            start_ts_val = str(row.get(field_map.get('start_ts', 'start_time'), ''))
                            end_ts_val = str(row.get(field_map.get('end_ts', 'end_time'), ''))
                            
                            # 构建目标路径
                            try:
                                target_path = path_template.format(
                                    batch_id=ctx.batch_id,
                                    asset_id=asset_id_val,
                                    vehicle_id=vehicle_id_val,
                                    rule_id=rule_id
                                )
                            except KeyError:
                                target_path = f"corner_case/{ctx.batch_id}/{asset_id_val}/"
                            
                            ctx.add_asset(
                                asset_id=asset_id_val,
                                asset_type=asset_type,
                                rule_id=rule_id,
                                vehicle_id=vehicle_id_val,
                                start_ts=start_ts_val,
                                end_ts=end_ts_val,
                                tags=tags,
                                target_path=target_path
                            )
                        
                        asset_count += p1_df.height
                        self.log.info(f"  ✅ Generated P1 assets: {rule_id} ({p1_df.height} assets)")
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"📦 P1 assets processed: {asset_count} assets generated")
    
    # ========================================================================
    # P2 处理：可配置的白名单豁免
    # ========================================================================
    
    def _process_p2_violations(self, ctx: GovernanceContext, rule_configs: Dict[str, Dict]) -> None:
        """
        处理 P2 违规（可配置的白名单豁免）
        
        改进：
        - 白名单豁免字段可通过 exemption.exempt_field 配置
        - 告警内容支持模板渲染
        - trigger_id_field 可配置
        
        逻辑：
        1. 提取 P1 资产的 asset_id 作为白名单
        2. 读取 P2 规则的命中数据
        3. 按 exempt_field 过滤掉白名单
        4. 生成告警
        
        Args:
            ctx: 治理上下文
            rule_configs: 规则配置字典
        """
        self.log.info("⚠️ Processing P2 violations...")
        
        # 1. 提取白名单（P1 资产的 asset_id）
        exempted_ids = set(asset.asset_id for asset in ctx.assets)
        
        if exempted_ids:
            self.log.info(f"  白名单: {len(exempted_ids)} asset_ids")
        
        p2_count = 0
        
        for rule_id, rule_output in ctx.rule_outputs.items():
            if rule_output.hit_count > 0:
                try:
                    # 读取命中数据
                    hit_df = ctx.get_dataframe(key=f"{rule_id}_hits", use_alt_key=False)
                    
                    # 使用 _severity 列过滤
                    p2_df = hit_df.filter(pl.col('_severity') == 'P2')
                    
                    if p2_df.height > 0:
                        # 获取规则配置
                        rule_cfg = rule_configs.get(rule_id, {})
                        alert_cfg = rule_cfg.get('alert', {})
                        exemption_cfg = rule_cfg.get('exemption', {})
                        
                        # 可配置的豁免字段（默认 cycle_id）
                        exempt_field = exemption_cfg.get('exempt_field', 'cycle_id')
                        
                        # 2. 过滤白名单
                        if exempted_ids and exempt_field in p2_df.columns:
                            final_df = p2_df.filter(
                                ~pl.col(exempt_field).cast(pl.Utf8).is_in(
                                    [str(eid) for eid in exempted_ids]
                                )
                            )
                        else:
                            final_df = p2_df
                        
                        # 3. 生成告警
                        if final_df.height > 0:
                            trigger_id_field = alert_cfg.get('trigger_id_field', 'cycle_id')
                            trigger_ids = self._safe_extract_ids(final_df, trigger_id_field)
                            
                            title = rule_output.alert_title or alert_cfg.get(
                                'title', f"[P2] SLA 违规 - {rule_id}"
                            )
                            content = self._render_alert_content(
                                rule_cfg=rule_cfg,
                                trigger_ids=trigger_ids,
                                ctx=ctx,
                                hit_count=final_df.height,
                                exempted_count=len(exempted_ids),
                                default_content=(
                                    f"发现 {len(trigger_ids)} 条违规数据"
                                    f"（已排除白名单 {len(exempted_ids)} 条）"
                                )
                            )
                            
                            ctx.add_alert(
                                rule_id=rule_id,
                                severity="P2",
                                title=title,
                                content=content,
                                trigger_cycle_ids=[str(t) for t in trigger_ids]
                            )
                            
                            p2_count += 1
                            self.log.info(
                                f"  ✅ Generated P2 alert: {rule_id} "
                                f"({len(trigger_ids)} violations after exemption)"
                            )
                
                except Exception as e:
                    self.log.warning(f"  ⚠️ Failed to process rule {rule_id}: {e}")
        
        self.log.info(f"⚠️ P2 violations processed: {p2_count} alerts generated")
    
    # ========================================================================
    # 工具方法
    # ========================================================================
    
    def _safe_extract_ids(self, df: pl.DataFrame, field: str) -> list:
        """
        安全提取 ID 列表（字段不存在时返回空列表）
        
        Args:
            df: DataFrame
            field: 字段名
        
        Returns:
            ID 列表
        """
        if field in df.columns:
            return df[field].to_list()
        else:
            self.log.warning(f"⚠️ Field '{field}' not found in columns: {df.columns}")
            return []
    
    def _render_alert_content(
        self,
        rule_cfg: Dict,
        trigger_ids: list,
        ctx: GovernanceContext,
        hit_count: int = 0,
        exempted_count: int = 0,
        default_content: str = ""
    ) -> str:
        """
        渲染告警内容（支持 Jinja 模板）
        
        如果规则配置中有 alert.content_template，使用 Jinja 渲染；
        否则使用 default_content。
        
        可用模板变量：
        - hit_count: 命中行数
        - trigger_ids: 触发 ID 列表
        - run_date: 运行日期
        - batch_id: 批次 ID
        - exempted_count: 白名单豁免数量
        
        Args:
            rule_cfg: 规则完整配置
            trigger_ids: 触发 ID 列表
            ctx: 治理上下文
            hit_count: 命中数量
            exempted_count: 豁免数量
            default_content: 默认内容（无模板时使用）
        
        Returns:
            渲染后的告警内容字符串
        """
        template_str = rule_cfg.get('alert', {}).get('content_template')
        
        if template_str:
            try:
                template = Template(template_str)
                return template.render(
                    hit_count=hit_count,
                    trigger_ids=trigger_ids,
                    trigger_ids_preview=trigger_ids[:10],
                    run_date=ctx.run_date,
                    batch_id=ctx.batch_id,
                    exempted_count=exempted_count,
                )
            except Exception as e:
                self.log.warning(f"⚠️ Failed to render content_template: {e}")
                return default_content
        
        return default_content

```

文件已成功写入。让我确认 `get_multiple_upstream_contexts` 在 `infra/operators.py` 中是否存在：

Good, I have a complete picture. Let me now update `rule_scanner.py` to handle absolute paths, then update `dag_factory.py`, and finally upgrade all YAML files.

```python
"""
orchestration/rule_scanner.py
规则扫描器

职责：
- 扫描规则配置文件（configs/rules/*.yaml）
- 解析规则依赖关系
- 拓扑排序（处理依赖顺序）
- 验证配置完整性（防御性解析）
- 支持 V2 规则格式（filter / aggregate 类型）

运行时机：
- DAG 文件顶层代码（解析时）
- Aggregator 执行时（加载规则元数据）
- 不能有耗时 I/O 操作（只读本地文件）
"""
import os
import glob
import yaml
from typing import List, Dict, Any, Optional


class RuleScanner:
    """
    规则扫描器
    
    使用示例:
    ```python
    # 在 DAG 文件顶层代码中
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    
    # 动态生成 Rule Tasks
    for rule in rules:
        GenericRuleOperator(
            task_id=rule['rule_id'],
            config_path=rule['config_path'],
            ...
        )
    ```
    """
    
    def __init__(self, rules_dir: str = None):
        """
        初始化
        
        Args:
            rules_dir: 规则目录路径（相对于 $AIRFLOW_HOME/plugins/）
        """
        self.rules_dir = rules_dir or 'configs/rules'
    
    def scan_rules(self) -> List[Dict[str, Any]]:
        """
        扫描规则并返回排序后的列表
        
        Returns:
            规则配置列表（已按依赖关系拓扑排序）
            
        示例:
            [
                {
                    'rule_id': 'rule_p0_time_check',
                    'config_path': 'configs/rules/p0_time_check.yaml',
                    'depends_on': [],
                    'severity': 'P0',
                    'config': {...}
                },
                ...
            ]
        
        Raises:
            ValueError: 配置验证失败、循环依赖等
        """
        # 1. 扫描目录
        rule_files = self._scan_rule_files()
        
        if not rule_files:
            raise ValueError(f"No rule files found in {self.rules_dir}")
        
        print(f"📋 Found {len(rule_files)} rule files")
        
        # 2. 加载并验证规则
        rules = []
        for file_path in rule_files:
            try:
                rule_config = self._load_and_validate_rule(file_path)
                rules.append(rule_config)
            except Exception as e:
                # 配置错误：拒绝加载 DAG
                raise ValueError(f"Failed to load rule from {file_path}: {e}")
        
        # 3. 拓扑排序（处理依赖关系）
        sorted_rules = self._topological_sort(rules)
        
        print(f"✅ Loaded {len(sorted_rules)} rules (topologically sorted)")
        
        return sorted_rules
    
    def get_rule_configs_by_id(self, target_entity: Optional[str] = None) -> Dict[str, Dict]:
        """
        获取规则配置字典（按 rule_id 索引）
        
        用于 Aggregator 在执行时获取规则的完整配置，
        以驱动数据驱动的 alert/asset 处理逻辑。
        
        Args:
            target_entity: 可选，按 target_entity 过滤
        
        Returns:
            {rule_id: full_config_dict}
        """
        rules = self.scan_rules()
        
        result = {}
        for rule in rules:
            if target_entity and rule.get('target_entity') != target_entity:
                continue
            result[rule['rule_id']] = rule['config']
        
        return result
    
    def _scan_rule_files(self) -> List[str]:
        """
        扫描规则目录，返回所有 YAML 文件路径
        
        支持绝对路径和相对路径（相对于 $AIRFLOW_HOME/plugins/）
        
        Returns:
            文件路径列表（已排序）
        """
        if os.path.isabs(self.rules_dir):
            rules_path = self.rules_dir
        else:
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            rules_path = os.path.join(airflow_home, 'plugins', self.rules_dir)
        
        # 扫描 *.yaml 文件
        pattern = os.path.join(rules_path, '*.yaml')
        files = glob.glob(pattern)
        
        # 排序保证稳定性
        return sorted(files)
    
    def _load_and_validate_rule(self, file_path: str) -> Dict[str, Any]:
        """
        加载并验证单个规则配置
        
        Args:
            file_path: 规则文件路径
        
        Returns:
            规则配置字典
        
        Raises:
            ValueError: 配置验证失败
        """
        # 1. 加载 YAML
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 2. 防御性验证（Schema Validation）
        self._validate_rule_config(config, file_path)
        
        # 3. 提取关键信息
        # 计算相对路径（相对于 plugins/）
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        plugins_dir = os.path.join(airflow_home, 'plugins')
        relative_path = os.path.relpath(file_path, plugins_dir)
        
        return {
            'rule_id': config['meta']['rule_id'],
            'config_path': relative_path,  # 相对路径
            'depends_on': config.get('depends_on', []),
            'severity': config['meta']['severity'],
            'description': config['meta'].get('description', ''),
            'target_entity': config.get('target_entity'),
            'config': config  # 完整配置（供调试和 Aggregator 使用）
        }
    
    def _validate_rule_config(self, config: Dict, file_path: str) -> None:
        """
        验证规则配置完整性（防御性解析，支持 V2 格式）
        
        V2 格式新增支持：
        - logic.type: filter（默认） | aggregate
        - logic.pre_compute: 预计算步骤（可选）
        - logic.group_by / agg_exprs / having_expr: 聚合型规则必需
        - input.entity_key: 明确的实体读取键（可选）
        - alert.content_template: Jinja 模板（可选）
        - alert.trigger_id_field: 触发 ID 字段名（可选）
        - asset.field_mapping: 资产字段映射（可选）
        
        必需字段：
        - meta
        - meta.rule_id
        - meta.severity
        - target_entity
        - logic
        - logic.filter_expr（当 type=filter 时）
        - logic.group_by / agg_exprs / having_expr（当 type=aggregate 时）
        
        Args:
            config: 规则配置字典
            file_path: 文件路径（用于错误提示）
        
        Raises:
            ValueError: 配置验证失败
        """
        errors = []
        
        # ========== meta 校验 ==========
        if 'meta' not in config:
            errors.append("Missing 'meta' field")
        else:
            if 'rule_id' not in config['meta']:
                errors.append("Missing 'meta.rule_id'")
            if 'severity' not in config['meta']:
                errors.append("Missing 'meta.severity'")
            else:
                # 验证 severity 取值
                valid_severities = ['P0', 'P1', 'P2', 'INFO']
                if config['meta']['severity'] not in valid_severities:
                    errors.append(f"Invalid severity: {config['meta']['severity']} (must be one of {valid_severities})")
        
        # ========== target_entity 校验 ==========
        if 'target_entity' not in config:
            errors.append("Missing 'target_entity'")
        
        # ========== logic 校验（支持 V2 多类型） ==========
        if 'logic' not in config:
            errors.append("Missing 'logic' field")
        else:
            logic = config['logic']
            logic_type = logic.get('type', 'filter')  # 默认 filter（向后兼容）
            
            if logic_type == 'filter':
                # filter 类型必须有 filter_expr
                if 'filter_expr' not in logic:
                    errors.append("Missing 'logic.filter_expr' (required for type=filter)")
            
            elif logic_type == 'aggregate':
                # aggregate 类型必须有 group_by, agg_exprs, having_expr
                if 'group_by' not in logic:
                    errors.append("Missing 'logic.group_by' (required for type=aggregate)")
                elif not isinstance(logic['group_by'], list):
                    errors.append("'logic.group_by' must be a list of column names")
                
                if 'agg_exprs' not in logic:
                    errors.append("Missing 'logic.agg_exprs' (required for type=aggregate)")
                elif not isinstance(logic['agg_exprs'], list):
                    errors.append("'logic.agg_exprs' must be a list")
                else:
                    for idx, agg in enumerate(logic['agg_exprs']):
                        if 'name' not in agg:
                            errors.append(f"Missing 'name' in logic.agg_exprs[{idx}]")
                        if 'expr' not in agg:
                            errors.append(f"Missing 'expr' in logic.agg_exprs[{idx}]")
                
                if 'having_expr' not in logic:
                    errors.append("Missing 'logic.having_expr' (required for type=aggregate)")
            
            else:
                errors.append(f"Unknown logic type: '{logic_type}' (must be 'filter' or 'aggregate')")
            
            # pre_compute 校验（可选，两种类型都支持）
            if 'pre_compute' in logic:
                if not isinstance(logic['pre_compute'], list):
                    errors.append("'logic.pre_compute' must be a list")
                else:
                    for idx, step in enumerate(logic['pre_compute']):
                        if 'name' not in step:
                            errors.append(f"Missing 'name' in logic.pre_compute[{idx}]")
                        if 'expr' not in step:
                            errors.append(f"Missing 'expr' in logic.pre_compute[{idx}]")
        
        # ========== asset 校验（可选） ==========
        if 'asset' in config:
            asset = config['asset']
            if asset.get('enabled', False) and 'field_mapping' in asset:
                field_map = asset['field_mapping']
                required_mapping_fields = ['asset_id', 'vehicle_id', 'start_ts', 'end_ts']
                for f in required_mapping_fields:
                    if f not in field_map:
                        errors.append(f"Missing 'asset.field_mapping.{f}' (required when asset.enabled=true)")
        
        # ========== 汇总错误 ==========
        if errors:
            error_msg = f"Invalid rule config in {file_path}:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
    
    def _topological_sort(self, rules: List[Dict]) -> List[Dict]:
        """
        拓扑排序（Kahn 算法）
        
        处理规则依赖关系，确保：
        - 被依赖的规则先执行
        - 检测循环依赖
        
        Args:
            rules: 规则列表
        
        Returns:
            排序后的规则列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 构建依赖图和规则字典
        graph = {}
        rule_dict = {}
        
        for rule in rules:
            rule_id = rule['rule_id']
            graph[rule_id] = rule['depends_on']
            rule_dict[rule_id] = rule
        
        # 验证依赖是否存在
        for rule_id, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise ValueError(
                        f"Rule '{rule_id}' depends on '{dep}', but '{dep}' not found. "
                        f"Available rules: {list(graph.keys())}"
                    )
        
        # 计算入度（我依赖多少个规则）
        # depends_on 的含义：我依赖谁（即存在边 dep -> me）
        # 入度 = len(depends_on)
        in_degree = {rule_id: len(deps) for rule_id, deps in graph.items()}
        
        # Kahn 算法：从入度为 0 的节点开始
        queue = [rule_id for rule_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # 取出一个入度为 0 的节点（没有依赖的规则，可以先执行）
            rule_id = queue.pop(0)
            result.append(rule_dict[rule_id])
            
            # 遍历所有规则，找出"依赖于当前规则"的规则
            for other_rule_id, deps in graph.items():
                if rule_id in deps:
                    # 当前规则已完成，依赖它的规则入度减 1
                    in_degree[other_rule_id] -= 1
                    # 如果入度变为 0，说明所有依赖都已满足，可以执行了
                    if in_degree[other_rule_id] == 0:
                        queue.append(other_rule_id)
        
        # 检测循环依赖
        if len(result) != len(rules):
            # 找出参与循环依赖的规则
            unprocessed = [r['rule_id'] for r in rules if r not in result]
            raise ValueError(
                f"Circular dependency detected in rules! "
                f"Unprocessed rules: {unprocessed}. "
                f"Check the 'depends_on' fields."
            )
        
        return result
    
    def get_rule_summary(self, rules: List[Dict]) -> str:
        """
        生成规则摘要（用于日志）
        
        Args:
            rules: 规则列表
        
        Returns:
            摘要字符串
        """
        lines = ["Rule Execution Order:"]
        for idx, rule in enumerate(rules, 1):
            deps_str = f" (depends on: {', '.join(rule['depends_on'])})" if rule['depends_on'] else ""
            logic_type = rule['config'].get('logic', {}).get('type', 'filter')
            lines.append(f"  {idx}. [{rule['severity']}] {rule['rule_id']} (type={logic_type}){deps_str}")
        
        return "\n".join(lines)

```

### Step 4: 更新 dag_factory.py — 传递 rules_dir 给 Aggregator

```python
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
            upstream_task_id="universal_loader",
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
            rule_tasks_dict = {}  # 使用字典，方便建立依赖关系
            
            # Step 4.1: 创建所有规则任务
            for rule in target_rules:
                rule_task = GenericRuleOperator(
                    task_id=rule['rule_id'],
                    config_dict=rule['config'],  # 传递完整的 YAML 配置
                    upstream_task_id="domain_adapter",
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
        
        adapter >> rule_group
        
        # ========== Step 5: Context Aggregator ==========
        aggregator = ContextAggregatorOperator(
            task_id="context_aggregator",
            rule_task_ids=[f"rule_tasks.{rule['rule_id']}" for rule in target_rules],
            rules_dir='configs/rules',  # 方案B：Aggregator 自己扫描规则目录
            dag=dag
        )
        rule_group >> aggregator
        
        # ========== Step 6: Notification Dispatcher ==========
        dispatcher_config = {}
        if config.notification:
            dispatcher_config['email_to'] = ','.join(config.notification.email_to)
            if config.notification.email_cc:
                dispatcher_config['email_cc'] = ','.join(config.notification.email_cc)
        
        dispatcher = NotificationDispatcherOperator(
            task_id="notification_dispatcher",
            config_dict=dispatcher_config if dispatcher_config else None,
            upstream_task_id="context_aggregator",
            dag=dag
        )
        aggregator >> dispatcher
        
        # ========== Step 7: Save Assets to Queue (触发 DAG B) ==========
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

```

### Step 5: 升级所有 11 个规则 YAML 文件到 V2 格式

先读取剩余未读的规则文件：

现在批量升级所有 11 个规则 YAML 文件到 V2 格式：

```yaml
# p0_time_check.yaml
# 规则配置文件（V2）：P0 - 时间倒挂校验
#
# 功能：检测 end_time < start_time 的数据错误
# 治理等级：P0 (Blocker)

meta:
  rule_id: rule_p0_time_check
  severity: P0
  description: "时间倒挂校验：检测 end_time < start_time 的物理错误"
  owner: data-governance@example.com
  created_at: "2026-01-23"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('end_time') < pl.col('start_time')"

output:
  key: rule_p0_time_check_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Checkpoint log]任务起止时间倒挂"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条时间倒挂数据，Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p0_box_count.yaml
# 规则配置文件（V2）：P0 - 箱数差异校验
#
# 功能：识别 Cycle log / Daily report / Checkpoint log / ngen 箱数差异的数据
# 治理等级：P0 (Immediate)

meta:
  rule_id: rule_p0_box_count
  severity: P0
  description: "箱数差异校验：识别Cycle log/Daily report/Checkpoint log/ngen箱数差异的数据"
  owner: data-governance@example.com
  created_at: "2026-02-06"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "(pl.col('diff_box_count1') != 0) | (pl.col('diff_box_count2') != 0) | (pl.col('diff_box_count_ngen') != 0)"

output:
  key: rule_p0_box_count_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Cycle log/Daily report/Checkpoint log/ngen]箱数有差异"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条箱数差异数据，Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p0_ckp_check.yaml
# 规则配置文件（V2）：P0 - Checkpoint log 阶段任务非空校验
#
# 功能：Checkpoint log 部分阶段任务不可为空（3-4 / 7-8）
# 治理等级：P0 (Immediate)

meta:
  rule_id: rule_p0_ckp_check
  severity: P0
  description: "Checkpoint log部分阶段任务不可为空：3-4/7-8"
  owner: data-governance@example.com
  created_at: "2026-02-28"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('ckp_3to4_total_time').is_null() | pl.col('ckp_7to8_total_time').is_null()"

output:
  key: rule_p0_ckp_check_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Checkpoint log]有阶段任务为空：3-4/7-8"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条阶段任务为空数据，Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p0_power_consumption.yaml
# 规则配置文件（V2）：P0 - 功率消耗校验
#
# 功能：识别电耗异常的数据（power_consumption > 50）
# 治理等级：P0 (Immediate)

meta:
  rule_id: rule_p0_power_consumption
  severity: P0
  description: "功率消耗校验：识别电耗异常的数据"
  owner: data-governance@example.com
  created_at: "2026-02-06"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('power_consumption') > 50"

output:
  key: rule_p0_power_consumption_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Checkpoint log]功率消耗校验"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条功率消耗异常数据，Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p0_kwh_per_box.yaml
# 规则配置文件（V2）：P0 - 每个 box 电耗校验
#
# 功能：每个 box 电耗不能超过 30kWh
# 治理等级：P0 (Immediate)

meta:
  rule_id: rule_p0_kwh_per_box
  severity: P0
  description: "每个box电耗校验：识别每个box电耗异常的数据"
  owner: data-governance@example.com
  created_at: "2026-02-06"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('daily_avg_power_per_box') > 30"

output:
  key: rule_p0_kwh_per_box_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Daily report]每个box电耗校验"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条box电耗异常数据（>30kWh），Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p0_kwh_per_km.yaml
# 规则配置文件（V2）：P0 - 每公里电耗校验
#
# 功能：每公里电耗不能超过 10kWh
# 治理等级：P0 (Immediate)

meta:
  rule_id: rule_p0_kwh_per_km
  severity: P0
  description: "每公里电耗校验：识别每公里电耗异常的数据"
  owner: data-governance@example.com
  created_at: "2026-02-06"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('daily_avg_power_per_km') > 10"

output:
  key: rule_p0_kwh_per_km_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Daily report]每公里电耗校验"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条公里电耗异常数据（>10kWh），Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p0_mileage_check.yaml
# 规则配置文件（V2）：P0 - 里程字段校验
#
# 功能：若里程为 0，箱数不可能大于 0
# 治理等级：P0 (Immediate)

meta:
  rule_id: rule_p0_mileage_check
  severity: P0
  description: "mileage字段校验：识别mileage异常数据"
  owner: data-governance@example.com
  created_at: "2026-02-06"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "(pl.col('mileage') == 0) & (pl.col('box') > 0)"

output:
  key: rule_p0_mileage_check_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Checkpoint log]mileage为0，箱数大于0"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条里程为0但箱数>0的异常数据，Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p0_running_time.yaml
# 规则配置文件（V2）：P0 - 运行时间校验
#
# 功能：单圈作业时长超过 2 小时（120分钟）需要告警
# 治理等级：P0 (Immediate)

meta:
  rule_id: rule_p0_running_time
  severity: P0
  description: "运行时间校验：识别单圈作业时间异常的数据"
  owner: data-governance@example.com
  created_at: "2026-02-06"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('running_time') > 120"

output:
  key: rule_p0_running_time_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Checkpoint log]单圈作业时长超过2小时"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条运行时间超2小时的数据，Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p0_subtask_count.yaml
# 规则配置文件（V2）：P0 - 子任务数量校验
#
# 功能：如果箱数 > 0，任务一定有两个阶段，subtask_type_2 和 page_time_2_subtask 不为空
# 治理等级：P0 (Immediate)

meta:
  rule_id: rule_p0_subtask_count
  severity: P0
  description: "子任务数量校验：识别子任务数量异常的数据"
  owner: data-governance@example.com
  created_at: "2026-02-06"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "(pl.col('box') > 0) & (pl.col('subtask_type_2').is_null() | pl.col('page_time_2_subtask').is_null())"

output:
  key: rule_p0_subtask_count_hits
  stage: RESULT

alert:
  enabled: true
  title: "[Cycle log]箱数大于0，子任务相关字段为空"
  trigger_id_field: cycle_id
  content_template: "发现 {{ hit_count }} 条子任务异常数据（箱数>0但子任务字段为空），Cycle IDs: {{ trigger_ids_preview }}"

depends_on: []

```

```yaml
# p1_twin_lift.yaml
# 规则配置文件（V2）：P1 - 双箱作业挖掘
#
# 功能：识别 is_twin_lift == 1 的高价值特征数据
# 治理等级：P1 (Asset)
# 处理方式：打标、豁免、打包

meta:
  rule_id: rule_p1_twin_lift
  severity: P1
  description: "双箱作业挖掘：识别 is_twin_lift == 1 的高价值作业"
  owner: data-governance@example.com
  created_at: "2026-01-23"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('is_twin_lift') == 1"

output:
  key: rule_p1_twin_lift_hits
  stage: RESULT

# P1 资产配置（数据驱动字段映射）
asset:
  enabled: true
  asset_type: TWIN_LIFT_SCENARIO
  field_mapping:
    asset_id: cycle_id
    vehicle_id: vehicle_id
    start_ts: start_time
    end_ts: end_time
  tags:
    - twin_lift
    - feature
    - high_value
  target_path_template: "corner_case/{batch_id}/{asset_id}/"

depends_on: []

```

```yaml
# p2_timeout.yaml
# 规则配置文件（V2）：P2 - 作业超时监控
#
# 功能：检测 duration > 7200 秒（2小时）的作业超时
# 治理等级：P2 (Violation)
# 处理方式：结合 P1 结果进行判决、发工单
#
# 特殊逻辑：
# - 如果作业是双箱作业（P1 命中），则通过白名单豁免超时告警
# - 豁免逻辑在 ContextAggregatorOperator 中实现（数据驱动）

meta:
  rule_id: rule_p2_timeout
  severity: P2
  description: "作业超时监控：检测 duration > 2h 的作业，结合 P1 豁免"
  owner: data-governance@example.com
  created_at: "2026-01-23"

target_entity: Cycle

input:
  entity_key: entity_cycle

logic:
  type: filter
  filter_expr: "pl.col('duration') > 7200"

output:
  key: rule_p2_timeout_hits
  stage: RESULT

alert:
  enabled: true
  title: "[P2]作业超时监控"
  trigger_id_field: cycle_id
  content_template: >-
    发现 {{ hit_count }} 条作业超时数据（已排除白名单 {{ exempted_count }} 条），
    Cycle IDs: {{ trigger_ids_preview }}

# P2 白名单豁免配置
exemption:
  enabled: true
  exempt_field: cycle_id
  source: p1_assets

depends_on:
  - rule_p1_twin_lift

```

所有 11 个 YAML 文件已升级。现在做 lint 检查和语法验证：

全部 11 个规则通过 V2 验证，拓扑排序正确（`p2_timeout` 排在 `p1_twin_lift` 之后），规则配置分布正确。

再快速验证一下聚合型规则的 Scanner 校验逻辑：

全部通过。最后再验证 aggregator 的 content_template 渲染逻辑：

所有改动已完成并通过验证。下面是改动总结：

---

## 实施总结：规则表达力优化（阶段一）

### 修改的代码文件（4 个）

| 文件 | 改动要点 |
|---|---|
| `plugins/orchestration/rule_scanner.py` | V2 校验：支持 `logic.type`（filter/aggregate）、`pre_compute`、`agg_exprs`/`having_expr`、`asset.field_mapping`；支持绝对路径；新增 `get_rule_configs_by_id()` 方法供 Aggregator 调用 |
| `plugins/operators/rule_engine.py` | 新增 `_apply_pre_compute()`（预计算列）、`_execute_aggregate()`（group_by → agg → having）；`_load_entity()` 优先读 `input.entity_key` |
| `plugins/operators/aggregator.py` | **方案B**：自己创建 `RuleScanner` 加载规则配置；P0/P1/P2 处理全部数据驱动——`trigger_id_field`、`content_template`、`field_mapping`、`exemption.exempt_field` 全部从规则 YAML 读取，不再硬编码 |
| `plugins/orchestration/dag_factory.py` | 向 `ContextAggregatorOperator` 传递 `rules_dir='configs/rules'` |

### 升级的规则 YAML 文件（11 个）

所有规则统一到 V2 格式，新增字段：

| 新增字段 | 适用规则 | 说明 |
|---|---|---|
| `logic.type: filter` | 全部 11 个 | 显式声明逻辑类型（兼容原格式） |
| `input.entity_key` | 全部 11 个 | 替代原 `input.key`，语义更明确 |
| `alert.trigger_id_field` | P0 × 9 + P2 × 1 | 告警触发 ID 字段名（不再硬编码 `cycle_id`） |
| `alert.content_template` | P0 × 9 + P2 × 1 | Jinja2 告警内容模板 |
| `asset.field_mapping` | P1 × 1 | 数据驱动的资产字段映射 |
| `exemption` | P2 × 1 | 白名单豁免配置 |

### 新增能力

1. **聚合型规则**（`type: aggregate`）— 现在可以写这样的规则：

```yaml
logic:
  type: aggregate
  group_by: ["vehicle_id", "shift_date"]
  agg_exprs:
    - name: "total_box"
      expr: "col('box').sum()"
  having_expr: "col('total_box') == 0"
```

2. **预计算列**（`pre_compute`）— 在过滤/聚合前先计算衍生列：

```yaml
logic:
  type: filter
  pre_compute:
    - name: "efficiency"
      expr: "col('box') / col('duration')"
  filter_expr: "col('efficiency') > 10"
```

3. **Aggregator 数据驱动化** — 不再需要在代码中修改字段名或 asset 逻辑，只要改 YAML。

```yaml
logic:
  type: aggregate
  group_by: ["vehicle_id", "shift_date"]
  agg_exprs:
    - name: "total_box"
      expr: "col('box').sum()"
  having_expr: "col('total_box') == 0"
```

```yaml
logic:
  type: filter
  pre_compute:
    - name: "efficiency"
      expr: "col('box') / col('duration')"
  filter_expr: "col('efficiency') > 10"
```

---

**User**

在数据质量检测方面，本平台目前的方法是用loader加载一批数据源，通过一系列操作（adapter）合并成一张表（parquet文件），再对这张表的数据使用rule进行过滤。这种操作方式在业内是否普遍？这种做法是否专业？有什么局限？比如这个平台能否升级成，一条pipeline中adapt出两张表，这两张表在rule阶段进行过滤、比对等操作

---

**Cursor**

这是一个非常好的架构层面的问题。让我先深入理解当前的流水线结构，然后系统分析。

现在我完整读完了 `adapter.py`、`rule_engine.py`、`cycle_adapter.yaml`、`daily_cycle_etl.yaml`，对你的架构有了全景理解。下面来系统回答。

---

## 一、"加载 → 合并成一张宽表 → 对宽表做规则过滤"这个做法在业内是否普遍？

**非常普遍，而且是主流模式之一。** 业内的叫法有几种：

| 叫法 | 描述 | 代表 |
|---|---|---|
| **OBT (One Big Table)** | 所有维度和事实打成一张宽表，在上面做分析/校验 | Netflix, Uber 内部数据治理 |
| **ELT → Test** | 先 Extract-Load-Transform 到宽表，再写 assertions | dbt + dbt tests |
| **Single-Dataset Expectations** | 每条规则针对单一 DataFrame 做校验 | Great Expectations, Soda Core |

你的架构本质上就是这个模式：

```
sources(MySQL × 3) → Loader(3 个 raw parquet) → Adapter(join + map → 1 个 entity parquet) → Rules(filter)
```

**这种做法是专业的**，尤其适合以下场景：
- 校验对象是"同一批数据的不同维度"（如你的 cycle 的各个属性）
- 规则逻辑是"行级过滤"（某一行的字段是否异常）
- 数据量可以放进单个 DataFrame（几十万行级别）

---

## 二、局限性分析

但它有**明确的天花板**，你的直觉是对的：

### 局限 1：只能产出一张实体表

当前的 `dag_factory` 固定链路是：

```
1 Loader → 1 Adapter → N Rules → 1 Aggregator → 1 Dispatcher
```

`cycle_adapter.yaml` 把 3 个 raw 表 join 成 1 张 `entity_cycle`。所有 11 条规则都只能读这一张表。这意味着：

- **不能同时产出** `entity_cycle`（单圈级别）和 `entity_daily_report`（天级别汇总），它们的粒度不同，无法自然地 join 成一张表。
- **不能做跨表比对**，比如"cycle 表的 `SUM(box)` 是否等于 daily_report 表的 `total_box`"。

### 局限 2：JOIN 带来的信息损失

当前 adapter 用 `left join` 把 3 张表拼成宽表。这个过程中：
- **right-only 的行丢失**（如 `subtarget_vehicle_cycle` 中有但 `cycle_section_summary` 中没有的 cycle_id）
- **粒度被锁死**（join 后只能是 cycle 级别，无法再有日级别或车辆级别的独立视图）

如果业务需要检查"subtarget 表中有多少条数据在 cycle_section 中找不到匹配"——这是典型的**参照完整性校验**——当前架构做不到，因为 left join 之后那些不匹配的行已经被吞掉了。

### 局限 3：规则只能读一张表

看当前 `rule_engine.py` 的 `_load_entity()`：

```python
entity_key = self._config.get('input', {}).get('entity_key')
return ctx.get_dataframe(key=entity_key, use_alt_key=True)
```

只读一个 key，只拿一张表。无法实现"拿 A 表和 B 表做 anti-join"之类的跨表规则。

### 局限 4：聚合和明细不能并存

你在 adapter 中用 `.over('shift_date')` 做了窗口聚合（比如 `daily_total_box`），这相当于把聚合结果"广播"回行级别。但这有限制：
- 不能独立产出一张"日级别汇总表"来单独校验
- 如果想比较"日汇总表的值 vs 行明细的 SUM"，当前做不到

---

## 三、行业对比：哪些平台支持多表校验？

| 平台 | 多表支持方式 | 你的平台现状 |
|---|---|---|
| **dbt tests** | `relationships` 测试：检查外键在另一张表中是否存在 | ❌ 不支持 |
| **Great Expectations** | `cross-table expectations`：比较两个 batch 的统计量 | ❌ 不支持 |
| **Apache Griffin** | `accuracy` 模式：source 与 target 两张表做精确比对 | ❌ 不支持 |
| **Soda Core** | `cross checks`：两个数据源的 SUM、COUNT 做比较 | ❌ 不支持 |

**结论**：你当前的水平大致对应 dbt tests 的 `generic test`（单表断言），但还缺少 `relationship test`（跨表断言）这一层。

---

## 四、"一条 pipeline 中 adapt 出两张表，在 rule 阶段进行比对"——可行性分析

**完全可行，而且是自然的升级方向。** 我列出两种实现思路的对比：

### 方案 A：多 Adapter 并行（推荐）

```
                    ┌→ Adapter_A (entity_cycle)      ─┐
Loader (N 个 raw) ─┤                                   ├→ Rules → Aggregator → Dispatcher
                    └→ Adapter_B (entity_daily_report) ─┘
```

改动点：
1. **source YAML** 配置多个 adapter 路径
2. **dag_factory** 生成多个并行的 Adapter Task
3. **rule YAML** 的 `input` 支持多个 entity_key
4. **rule_engine** 加载多张表，新增 `cross_entity` 逻辑类型

规则 YAML 示例：
```yaml
meta:
  rule_id: rule_p0_cross_box_count
  severity: P0

logic:
  type: cross_entity
  inputs:
    left:
      entity_key: entity_cycle
      expr: "col('box').sum()"
    right:
      entity_key: entity_daily_report
      expr: "col('total_box').sum()"
  assertion: "left == right"   # 断言两边相等
```

或更实际的参照完整性校验：
```yaml
logic:
  type: cross_entity
  inputs:
    left:
      entity_key: entity_cycle
    right:
      entity_key: entity_subtarget
  operation: anti_join           # 左表中右表不存在的行
  join_on: ["cycle_id"]
  # hit_df = left 中找不到匹配的行
```

### 方案 B：单 Adapter 多输出

让一个 Adapter 同时输出多张实体表：

```yaml
# cycle_adapter.yaml
outputs:
  - output_key: entity_cycle
    target_entity: Cycle
    fields: [...]
  - output_key: entity_daily_report
    target_entity: DailyReport
    group_by: [vehicle_id, shift_date]
    agg_fields: [...]
```

这种方式改 adapter 较多，且把 adapter 变得太重。

---

### 我的建议

**方案 A（多 Adapter 并行）更优**，原因：

1. **单一职责**：每个 adapter 只负责一张实体表的转换逻辑
2. **复用性**：同一个 adapter 可以被不同 pipeline 复用
3. **与现有架构兼容**：只需要在 dag_factory 中加一个循环，以及在 rule_engine 中加一个 `cross_entity` 类型
4. **粒度解耦**：cycle 级和 daily 级是不同粒度，理应由不同的 adapter 处理

实施的改动量级：
- `source YAML schema`：新增 `adapters` 字段（列表，替代隐式的单 adapter）
- `dag_factory`：生成多个并行 Adapter Task，Rules 等待所有 Adapter 完成
- `rule_engine`：新增 `cross_entity` 逻辑类型（加载多个 DataFrame）
- `rule_scanner`：校验 `cross_entity` 类型的必需字段

**你要继续讨论方案细节，还是直接开始实现方案 A？**

```plaintext
sources(MySQL × 3) → Loader(3 个 raw parquet) → Adapter(join + map → 1 个 entity parquet) → Rules(filter)
```

```plaintext
1 Loader → 1 Adapter → N Rules → 1 Aggregator → 1 Dispatcher
```

```python
entity_key = self._config.get('input', {}).get('entity_key')
return ctx.get_dataframe(key=entity_key, use_alt_key=True)
```

```plaintext
                    ┌→ Adapter_A (entity_cycle)      ─┐
Loader (N 个 raw) ─┤                                   ├→ Rules → Aggregator → Dispatcher
                    └→ Adapter_B (entity_daily_report) ─┘
```

```yaml
meta:
  rule_id: rule_p0_cross_box_count
  severity: P0

logic:
  type: cross_entity
  inputs:
    left:
      entity_key: entity_cycle
      expr: "col('box').sum()"
    right:
      entity_key: entity_daily_report
      expr: "col('total_box').sum()"
  assertion: "left == right"   # 断言两边相等
```

```yaml
logic:
  type: cross_entity
  inputs:
    left:
      entity_key: entity_cycle
    right:
      entity_key: entity_subtarget
  operation: anti_join           # 左表中右表不存在的行
  join_on: ["cycle_id"]
  # hit_df = left 中找不到匹配的行
```

```yaml
# cycle_adapter.yaml
outputs:
  - output_key: entity_cycle
    target_entity: Cycle
    fields: [...]
  - output_key: entity_daily_report
    target_entity: DailyReport
    group_by: [vehicle_id, shift_date]
    agg_fields: [...]
```

---

**User**

继续讨论方案A的细节

---

**Cursor**

好，让我系统地把方案 A 的所有设计决策点拆开来讨论。

好。现在全景已经非常清楚了，我把方案 A 拆成 **6 个设计决策点**逐个展开讨论。

---

## 决策点 1：Source YAML 如何声明多个 Adapter？

**现状：** 隐式单 Adapter

```yaml
source_meta:
  target_entity: "Cycle"  # dag_factory 用这个找 adapters/cycle_adapter.yaml
```

```python
# dag_factory.py 当前逻辑
adapter_config_path = self.adapters_dir / f"{config.source_meta.target_entity.lower()}_adapter.yaml"
```

**提议：新增可选 `adapters` 列表**

```yaml
source_meta:
  id: "daily_cycle_etl"
  target_entity: "Cycle"  # 保留，向后兼容 + 作为"主实体"

# ✅ 新增：显式声明多个 adapter（可选）
adapters:
  - id: "cycle"
    config_path: "configs/adapters/cycle_adapter.yaml"
  - id: "daily_report"
    config_path: "configs/adapters/daily_report_adapter.yaml"
```

**向后兼容逻辑：**
- 如果 `adapters` 字段**存在** → 使用声明的列表
- 如果 `adapters` 字段**不存在** → 退化为当前行为（从 `target_entity` 自动推断）

```python
# dag_factory 兼容逻辑伪代码
if config.adapters:
    adapter_configs = config.adapters
else:
    # 退化：自动推断
    adapter_configs = [{
        'id': config.source_meta.target_entity.lower(),
        'config_path': f"configs/adapters/{target_entity.lower()}_adapter.yaml"
    }]
```

**这意味着你现有的 `daily_cycle_etl.yaml` 不需要改动就能继续运行。**

---

## 决策点 2：DAG 拓扑图怎么变？

**现有拓扑：**
```
Sensor → Loader → Adapter → [Rule_1, Rule_2, ...] → Aggregator → Dispatcher
```

**方案 A 拓扑：**
```
                      ┌→ Adapter_cycle          ──┐
Sensor → Loader ──────┤                            ├→ [Rule_1, Rule_2, ...] → Aggregator → Dispatcher
                      └→ Adapter_daily_report   ──┘
```

关键点：
- **多个 Adapter 并行执行**（它们都依赖 Loader，互相不依赖）
- **所有 Rule 等待所有 Adapter 完成**（因为跨表规则可能需要两张表都就绪）
- Aggregator、Dispatcher 不变

在 DAG Factory 代码中，这对应：
```python
# 创建多个 Adapter Task
adapter_tasks = []
for adapter_cfg in adapter_configs:
    adapter = DomainAdapterOperator(
        task_id=f"adapter_{adapter_cfg.id}",
        config_path=adapter_cfg.config_path,
        upstream_task_id="universal_loader",
    )
    loader >> adapter
    adapter_tasks.append(adapter)

# 所有 adapter → rule_group（所有规则等待所有 adapter 完成）
for adapter in adapter_tasks:
    adapter >> rule_group
```

---

## 决策点 3：Rule YAML 跨表语法怎么设计？

这是核心设计。**单表规则不变**，只新增 `cross_entity` 类型。

### 单表规则（不变）
```yaml
input:
  entity_key: entity_cycle       # 单个 key

logic:
  type: filter
  filter_expr: "col('end_time') < col('start_time')"
```

### 跨表规则（新增）

关键变化：`input` 从 `entity_key`（单数）变为 `entity_keys`（复数，字典形式）：

```yaml
target_entity: Cycle           # 仍然用于匹配到"属于哪个 pipeline"

input:
  entity_keys:                 # ← 复数，字典形式
    left: entity_cycle         # 左表
    right: entity_daily_report # 右表

logic:
  type: cross_entity
  operation: anti_join         # 操作类型
  join_on:
    left: cycle_id             # 左表连接键
    right: cycle_id            # 右表连接键
```

**Rule Engine 判断逻辑：**
```python
if 'entity_key' in self._config.get('input', {}):
    # 单表模式（现有逻辑）
    df = self._load_entity(ctx)
elif 'entity_keys' in self._config.get('input', {}):
    # 跨表模式（新增逻辑）
    left_df, right_df = self._load_cross_entities(ctx)
```

---

## 决策点 4：支持哪些跨表操作类型？

我建议首批支持 **3 种操作**，覆盖 90% 的跨表校验场景：

### 操作 1：`anti_join` — 参照完整性

**场景：** "Cycle 表中哪些 cycle_id 在 subtarget 表中找不到？"

```yaml
logic:
  type: cross_entity
  operation: anti_join
  join_on:
    left: cycle_id
    right: cycle_id
  # hit_df = left 中在 right 找不到匹配的行
```

**Polars 实现：**
```python
hit_df = left_df.join(right_df, left_on="cycle_id", right_on="cycle_id", how="anti")
```

### 操作 2：`compare` — 关联值比对

**场景：** "join 之后，cycle 表的 box 和 daily_report 表的 total_box 是否一致？"

```yaml
logic:
  type: cross_entity
  operation: compare
  join_on:
    left: [vehicle_id, shift_date]
    right: [vehicle_id, shift_date]
  suffix: "_right"
  assertion: "col('box_sum') != col('total_box_right')"
  # 先 inner_join，再 filter(assertion)，hit_df = 不一致的行
```

**Polars 实现：**
```python
joined = left_df.join(right_df, left_on=[...], right_on=[...], how="inner", suffix="_right")
hit_df = joined.filter(self._parse_expression(assertion))
```

### 操作 3：`agg_compare` — 聚合指标比对

**场景：** "cycle 表的 `SUM(box)` 按 (vehicle_id, shift_date) 分组后，是否等于 daily_report 表的 `total_box`？"

```yaml
logic:
  type: cross_entity
  operation: agg_compare
  left:
    entity_key: entity_cycle
    group_by: [vehicle_id, shift_date]
    agg_exprs:
      - name: box_sum
        expr: "col('box').sum()"
  right:
    entity_key: entity_daily_report
    # 不需要聚合（本身已是日级别表）
  join_on:
    left: [vehicle_id, shift_date]
    right: [vehicle_id, shift_date]
  assertion: "col('box_sum') != col('total_box_right')"
```

**Polars 实现：**
```python
# 左表聚合
left_agg = left_df.group_by(group_cols).agg(agg_exprs)
# 右表保持原样（或也聚合）
# Inner join + filter
joined = left_agg.join(right_df, ..., how="inner", suffix="_right")
hit_df = joined.filter(assertion)
```

### 对比总结

| 操作 | 用途 | 输入 | hit_df 含义 |
|---|---|---|---|
| `anti_join` | A 中不在 B 的行 | 两张表 + join_on | 左表中无匹配的行 |
| `compare` | 匹配后字段不一致 | 两张表 + join_on + assertion | 不一致的行 |
| `agg_compare` | 聚合后指标不一致 | 两张表 + group_by + join_on + assertion | 聚合后不一致的组 |

---

## 决策点 5：RuleScanner 怎么匹配跨表规则到 pipeline？

**现状：** 匹配逻辑是 `rule.target_entity == pipeline.target_entity`

**问题：** 如果 pipeline 产出 `Cycle` 和 `DailyReport` 两张表，target_entity 为 `DailyReport` 的规则也应该被加载。

**提议：构建 pipeline 的 "实体清单"，规则只要 target_entity 在清单中就匹配**

```python
# dag_factory._build_task_pipeline() 中
pipeline_entities = set()
pipeline_entities.add(config.source_meta.target_entity)  # 主实体

# 从每个 adapter YAML 读取 target_entity
for adapter_cfg in adapter_configs:
    with open(adapter_cfg.config_path) as f:
        adapter_yaml = yaml.safe_load(f)
    pipeline_entities.add(adapter_yaml['target_entity'])

# 匹配规则：target_entity 在清单中即可
target_rules = [
    rule for rule in all_rules
    if rule.get('target_entity') in pipeline_entities
]
```

这样，一个声明了 `target_entity: DailyReport` 的纯单表规则，也能被正确加载到这条 pipeline 中。

---

## 决策点 6：改动文件清单

| 层 | 文件 | 改动类型 | 改动说明 |
|---|---|---|---|
| **Schema** | `source_config_schema.py` | 新增字段 | 新增 `AdapterConfig` 模型和 `adapters` 可选字段 |
| **Orchestration** | `dag_factory.py` | 核心改动 | 多 Adapter 并行创建、实体清单匹配 |
| **Orchestration** | `rule_scanner.py` | 小改 | 校验 `cross_entity` 类型的必需字段 |
| **Operator** | `rule_engine.py` | 核心改动 | 新增 `_load_cross_entities()` + 3 种 `cross_entity` 操作 |
| **Config** | `adapters/daily_report_adapter.yaml` | 新增文件 | 新的 adapter（如果要实际使用） |
| **Config** | `sources/daily_cycle_etl.yaml` | 小改 | 添加 `adapters` 列表（可选） |
| **Config** | `rules/p0_cross_xxx.yaml` | 新增文件 | 跨表规则（示例） |

**不需要改动的文件：** `aggregator.py`、`dispatcher.py`、`context.py`、`adapter.py`、`loader.py`
— 因为跨表逻辑完全在 Rule Engine 层闭合，下游不感知。

---

## 需要你确认的问题

1. **`adapters` 列表的位置**：我建议放在 source YAML 顶层（和 `extractions` 同级），而不是嵌在 `source_meta` 里。你觉得呢？

2. **跨表规则的 `target_entity` 归属**：跨表规则写 `target_entity: Cycle`（主实体）即可——因为它"属于" Cycle 这条 pipeline。还是你希望用列表 `target_entities: [Cycle, DailyReport]`？

3. **首批实现哪些操作？** 我建议先实现 `anti_join` + `compare` 两个（最常用），`agg_compare` 看你是否有即时需求。

4. **你现在有实际的第二张实体表的需求吗？** 比如需要一张 `entity_daily_report`？如果有的话我可以在实现时直接用真实例子。

```yaml
source_meta:
  target_entity: "Cycle"  # dag_factory 用这个找 adapters/cycle_adapter.yaml
```

```python
# dag_factory.py 当前逻辑
adapter_config_path = self.adapters_dir / f"{config.source_meta.target_entity.lower()}_adapter.yaml"
```

```yaml
source_meta:
  id: "daily_cycle_etl"
  target_entity: "Cycle"  # 保留，向后兼容 + 作为"主实体"

# ✅ 新增：显式声明多个 adapter（可选）
adapters:
  - id: "cycle"
    config_path: "configs/adapters/cycle_adapter.yaml"
  - id: "daily_report"
    config_path: "configs/adapters/daily_report_adapter.yaml"
```

```python
# dag_factory 兼容逻辑伪代码
if config.adapters:
    adapter_configs = config.adapters
else:
    # 退化：自动推断
    adapter_configs = [{
        'id': config.source_meta.target_entity.lower(),
        'config_path': f"configs/adapters/{target_entity.lower()}_adapter.yaml"
    }]
```

```plaintext
Sensor → Loader → Adapter → [Rule_1, Rule_2, ...] → Aggregator → Dispatcher
```

```plaintext
                      ┌→ Adapter_cycle          ──┐
Sensor → Loader ──────┤                            ├→ [Rule_1, Rule_2, ...] → Aggregator → Dispatcher
                      └→ Adapter_daily_report   ──┘
```

```python
# 创建多个 Adapter Task
adapter_tasks = []
for adapter_cfg in adapter_configs:
    adapter = DomainAdapterOperator(
        task_id=f"adapter_{adapter_cfg.id}",
        config_path=adapter_cfg.config_path,
        upstream_task_id="universal_loader",
    )
    loader >> adapter
    adapter_tasks.append(adapter)

# 所有 adapter → rule_group（所有规则等待所有 adapter 完成）
for adapter in adapter_tasks:
    adapter >> rule_group
```

```yaml
input:
  entity_key: entity_cycle       # 单个 key

logic:
  type: filter
  filter_expr: "col('end_time') < col('start_time')"
```

```yaml
target_entity: Cycle           # 仍然用于匹配到"属于哪个 pipeline"

input:
  entity_keys:                 # ← 复数，字典形式
    left: entity_cycle         # 左表
    right: entity_daily_report # 右表

logic:
  type: cross_entity
  operation: anti_join         # 操作类型
  join_on:
    left: cycle_id             # 左表连接键
    right: cycle_id            # 右表连接键
```

```python
if 'entity_key' in self._config.get('input', {}):
    # 单表模式（现有逻辑）
    df = self._load_entity(ctx)
elif 'entity_keys' in self._config.get('input', {}):
    # 跨表模式（新增逻辑）
    left_df, right_df = self._load_cross_entities(ctx)
```

```yaml
logic:
  type: cross_entity
  operation: anti_join
  join_on:
    left: cycle_id
    right: cycle_id
  # hit_df = left 中在 right 找不到匹配的行
```

```python
hit_df = left_df.join(right_df, left_on="cycle_id", right_on="cycle_id", how="anti")
```

```yaml
logic:
  type: cross_entity
  operation: compare
  join_on:
    left: [vehicle_id, shift_date]
    right: [vehicle_id, shift_date]
  suffix: "_right"
  assertion: "col('box_sum') != col('total_box_right')"
  # 先 inner_join，再 filter(assertion)，hit_df = 不一致的行
```

```python
joined = left_df.join(right_df, left_on=[...], right_on=[...], how="inner", suffix="_right")
hit_df = joined.filter(self._parse_expression(assertion))
```

```yaml
logic:
  type: cross_entity
  operation: agg_compare
  left:
    entity_key: entity_cycle
    group_by: [vehicle_id, shift_date]
    agg_exprs:
      - name: box_sum
        expr: "col('box').sum()"
  right:
    entity_key: entity_daily_report
    # 不需要聚合（本身已是日级别表）
  join_on:
    left: [vehicle_id, shift_date]
    right: [vehicle_id, shift_date]
  assertion: "col('box_sum') != col('total_box_right')"
```

```python
# 左表聚合
left_agg = left_df.group_by(group_cols).agg(agg_exprs)
# 右表保持原样（或也聚合）
# Inner join + filter
joined = left_agg.join(right_df, ..., how="inner", suffix="_right")
hit_df = joined.filter(assertion)
```

```python
# dag_factory._build_task_pipeline() 中
pipeline_entities = set()
pipeline_entities.add(config.source_meta.target_entity)  # 主实体

# 从每个 adapter YAML 读取 target_entity
for adapter_cfg in adapter_configs:
    with open(adapter_cfg.config_path) as f:
        adapter_yaml = yaml.safe_load(f)
    pipeline_entities.add(adapter_yaml['target_entity'])

# 匹配规则：target_entity 在清单中即可
target_rules = [
    rule for rule in all_rules
    if rule.get('target_entity') in pipeline_entities
]
```

---

**User**

1.同意你的决策 2.我认为主实体就可以，但是需要你帮忙对比一下两种方案 3.先实现anti_join和compare 4.有第二张实体表（如图），记录的是每辆车每天的里程数，我需要对比这张表每辆车的里程数(mileage_distance）和主表（名为cycle的entity）每辆车每天的里程数是否相等

---

**Cursor**

好的！我看到代码中其实已经包含了 `cross_entity` 的核心实现（在上轮讨论中已完成），包括 `rule_engine.py`、`rule_scanner.py`、`vehicle_mileage_adapter.yaml`、`p0_mileage_cross_check.yaml` 和 `daily_cycle_etl.yaml` 的更新。

让我先对比两种方案，然后进行端到端验证，确保一切正确连接。

---

## 两种方案对比

### 方案 A：主实体方案（单表合并）

**做法**：在 Adapter 阶段，将第二张表（vehicle_mileage）通过 join 合并到主实体（Cycle），规则仍在单表上操作。

```yaml
# cycle_adapter.yaml 中增加 join
input_schema:
  primary_source: "raw_cycle_section"
  joins:
    - join_source: "raw_vehicle_mileage"
      type: "left"
      left_on: ["vehicle_id", "shift_date"]
      right_on: ["vehicle_id", "shift_date"]
      suffix: "_mileage"
# 然后在 fields 中映射 mileage_distance 字段
```

| 维度 | 评价 |
|------|------|
| **粒度匹配** | ❌ **致命问题**：Cycle 是 per-cycle 粒度（一天一辆车有多条），mileage 是 per-vehicle-per-day 粒度。join 后 mileage_distance 会被重复到每条 cycle 行 → 数据膨胀、语义失真 |
| **规则复杂度** | ⚠️ 需要先 pre_compute 做 `over()` 窗口聚合才能比较，逻辑不直观 |
| **灵活性** | ❌ 如果还有第三张表，adapter YAML 会越来越复杂 |
| **简洁性** | ✅ 不需要新的规则类型，filter 即可 |
| **性能** | ⚠️ 数据膨胀导致内存占用增大 |

### 方案 B：多实体方案（跨表校验 `cross_entity`）✅ 推荐

**做法**：每张表通过独立 Adapter 生成独立实体，规则阶段通过 `cross_entity` 类型引用两个实体进行比对。

```yaml
# p0_mileage_cross_check.yaml
input:
  entity_keys:
    left: "entity_cycle"
    right: "entity_vehicle_mileage"
logic:
  type: cross_entity
  operation: compare
  left_pre_agg:            # 左表按 (vehicle_id, shift_date) 先聚合
    group_by: [vehicle_id, shift_date]
    agg_exprs:
      - name: total_mileage
        expr: "col('mileage').sum()"
  join_on:
    left: [vehicle_id, shift_date]
    right: [vehicle_id, shift_date]
  assertion: "col('total_mileage') != col('mileage_distance')"
```

| 维度 | 评价 |
|------|------|
| **粒度匹配** | ✅ 通过 `left_pre_agg` 将左表聚合到与右表相同的粒度后再 join，语义清晰 |
| **规则复杂度** | ✅ YAML 中直观表达"聚合 → join → 断言"，可读性好 |
| **灵活性** | ✅ 支持 `anti_join`（找缺失行）和 `compare`（找不一致行），可组合使用 |
| **简洁性** | ⚠️ 需要新的 rule type，但已实现 |
| **性能** | ✅ 先聚合再 join，数据量小，内存效率高 |

**结论**：你的场景（cycle per-cycle 粒度 vs mileage per-day 粒度）**方案 B（cross_entity）明显更合适**。方案 A 在粒度不一致时会导致数据重复和语义错误。

---

现在让我验证现有实现是否端到端正确：

```yaml
# cycle_adapter.yaml 中增加 join
input_schema:
  primary_source: "raw_cycle_section"
  joins:
    - join_source: "raw_vehicle_mileage"
      type: "left"
      left_on: ["vehicle_id", "shift_date"]
      right_on: ["vehicle_id", "shift_date"]
      suffix: "_mileage"
# 然后在 fields 中映射 mileage_distance 字段
```

```yaml
# p0_mileage_cross_check.yaml
input:
  entity_keys:
    left: "entity_cycle"
    right: "entity_vehicle_mileage"
logic:
  type: cross_entity
  operation: compare
  left_pre_agg:            # 左表按 (vehicle_id, shift_date) 先聚合
    group_by: [vehicle_id, shift_date]
    agg_exprs:
      - name: total_mileage
        expr: "col('mileage').sum()"
  join_on:
    left: [vehicle_id, shift_date]
    right: [vehicle_id, shift_date]
  assertion: "col('total_mileage') != col('mileage_distance')"
```

The file `p0_box_per_vehicle.yaml` is empty, causing the scanner to crash. Let me fix the scanner to handle empty files gracefully:

```python
"""
orchestration/rule_scanner.py
规则扫描器

职责：
- 扫描规则配置文件（configs/rules/*.yaml）
- 解析规则依赖关系
- 拓扑排序（处理依赖顺序）
- 验证配置完整性（防御性解析）
- 支持 V2 规则格式（filter / aggregate 类型）

运行时机：
- DAG 文件顶层代码（解析时）
- Aggregator 执行时（加载规则元数据）
- 不能有耗时 I/O 操作（只读本地文件）
"""
import os
import glob
import yaml
from typing import List, Dict, Any, Optional


class RuleScanner:
    """
    规则扫描器
    
    使用示例:
    ```python
    # 在 DAG 文件顶层代码中
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    
    # 动态生成 Rule Tasks
    for rule in rules:
        GenericRuleOperator(
            task_id=rule['rule_id'],
            config_path=rule['config_path'],
            ...
        )
    ```
    """
    
    def __init__(self, rules_dir: str = None):
        """
        初始化
        
        Args:
            rules_dir: 规则目录路径（相对于 $AIRFLOW_HOME/plugins/）
        """
        self.rules_dir = rules_dir or 'configs/rules'
    
    def scan_rules(self) -> List[Dict[str, Any]]:
        """
        扫描规则并返回排序后的列表
        
        Returns:
            规则配置列表（已按依赖关系拓扑排序）
            
        示例:
            [
                {
                    'rule_id': 'rule_p0_time_check',
                    'config_path': 'configs/rules/p0_time_check.yaml',
                    'depends_on': [],
                    'severity': 'P0',
                    'config': {...}
                },
                ...
            ]
        
        Raises:
            ValueError: 配置验证失败、循环依赖等
        """
        # 1. 扫描目录
        rule_files = self._scan_rule_files()
        
        if not rule_files:
            raise ValueError(f"No rule files found in {self.rules_dir}")
        
        print(f"📋 Found {len(rule_files)} rule files")
        
        # 2. 加载并验证规则
        rules = []
        for file_path in rule_files:
            try:
                rule_config = self._load_and_validate_rule(file_path)
                if rule_config is None:
                    # 空文件，跳过
                    print(f"⏭️ Skipping empty rule file: {file_path}")
                    continue
                rules.append(rule_config)
            except Exception as e:
                # 配置错误：拒绝加载 DAG
                raise ValueError(f"Failed to load rule from {file_path}: {e}")
        
        # 3. 拓扑排序（处理依赖关系）
        sorted_rules = self._topological_sort(rules)
        
        print(f"✅ Loaded {len(sorted_rules)} rules (topologically sorted)")
        
        return sorted_rules
    
    def get_rule_configs_by_id(self, target_entity: Optional[str] = None) -> Dict[str, Dict]:
        """
        获取规则配置字典（按 rule_id 索引）
        
        用于 Aggregator 在执行时获取规则的完整配置，
        以驱动数据驱动的 alert/asset 处理逻辑。
        
        Args:
            target_entity: 可选，按 target_entity 过滤
        
        Returns:
            {rule_id: full_config_dict}
        """
        rules = self.scan_rules()
        
        result = {}
        for rule in rules:
            if target_entity and rule.get('target_entity') != target_entity:
                continue
            result[rule['rule_id']] = rule['config']
        
        return result
    
    def _scan_rule_files(self) -> List[str]:
        """
        扫描规则目录，返回所有 YAML 文件路径
        
        支持绝对路径和相对路径（相对于 $AIRFLOW_HOME/plugins/）
        
        Returns:
            文件路径列表（已排序）
        """
        if os.path.isabs(self.rules_dir):
            rules_path = self.rules_dir
        else:
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            rules_path = os.path.join(airflow_home, 'plugins', self.rules_dir)
        
        # 扫描 *.yaml 文件
        pattern = os.path.join(rules_path, '*.yaml')
        files = glob.glob(pattern)
        
        # 排序保证稳定性
        return sorted(files)
    
    def _load_and_validate_rule(self, file_path: str) -> Dict[str, Any]:
        """
        加载并验证单个规则配置
        
        Args:
            file_path: 规则文件路径
        
        Returns:
            规则配置字典
        
        Raises:
            ValueError: 配置验证失败
        """
        # 1. 加载 YAML
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 2. 防御性验证（Schema Validation）
        self._validate_rule_config(config, file_path)
        
        # 3. 提取关键信息
        # 计算相对路径（相对于 plugins/）
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        plugins_dir = os.path.join(airflow_home, 'plugins')
        relative_path = os.path.relpath(file_path, plugins_dir)
        
        return {
            'rule_id': config['meta']['rule_id'],
            'config_path': relative_path,  # 相对路径
            'depends_on': config.get('depends_on', []),
            'severity': config['meta']['severity'],
            'description': config['meta'].get('description', ''),
            'target_entity': config.get('target_entity'),
            'config': config  # 完整配置（供调试和 Aggregator 使用）
        }
    
    def _validate_rule_config(self, config: Dict, file_path: str) -> None:
        """
        验证规则配置完整性（防御性解析，支持 V2 格式）
        
        V2 格式新增支持：
        - logic.type: filter（默认） | aggregate
        - logic.pre_compute: 预计算步骤（可选）
        - logic.group_by / agg_exprs / having_expr: 聚合型规则必需
        - input.entity_key: 明确的实体读取键（可选）
        - alert.content_template: Jinja 模板（可选）
        - alert.trigger_id_field: 触发 ID 字段名（可选）
        - asset.field_mapping: 资产字段映射（可选）
        
        必需字段：
        - meta
        - meta.rule_id
        - meta.severity
        - target_entity
        - logic
        - logic.filter_expr（当 type=filter 时）
        - logic.group_by / agg_exprs / having_expr（当 type=aggregate 时）
        
        Args:
            config: 规则配置字典
            file_path: 文件路径（用于错误提示）
        
        Raises:
            ValueError: 配置验证失败
        """
        errors = []
        
        # ========== meta 校验 ==========
        if 'meta' not in config:
            errors.append("Missing 'meta' field")
        else:
            if 'rule_id' not in config['meta']:
                errors.append("Missing 'meta.rule_id'")
            if 'severity' not in config['meta']:
                errors.append("Missing 'meta.severity'")
            else:
                # 验证 severity 取值
                valid_severities = ['P0', 'P1', 'P2', 'INFO']
                if config['meta']['severity'] not in valid_severities:
                    errors.append(f"Invalid severity: {config['meta']['severity']} (must be one of {valid_severities})")
        
        # ========== target_entity 校验 ==========
        if 'target_entity' not in config:
            errors.append("Missing 'target_entity'")
        
        # ========== logic 校验（支持 V2 多类型） ==========
        if 'logic' not in config:
            errors.append("Missing 'logic' field")
        else:
            logic = config['logic']
            logic_type = logic.get('type', 'filter')  # 默认 filter（向后兼容）
            
            if logic_type == 'filter':
                # filter 类型必须有 filter_expr
                if 'filter_expr' not in logic:
                    errors.append("Missing 'logic.filter_expr' (required for type=filter)")
            
            elif logic_type == 'aggregate':
                # aggregate 类型必须有 group_by, agg_exprs, having_expr
                if 'group_by' not in logic:
                    errors.append("Missing 'logic.group_by' (required for type=aggregate)")
                elif not isinstance(logic['group_by'], list):
                    errors.append("'logic.group_by' must be a list of column names")
                
                if 'agg_exprs' not in logic:
                    errors.append("Missing 'logic.agg_exprs' (required for type=aggregate)")
                elif not isinstance(logic['agg_exprs'], list):
                    errors.append("'logic.agg_exprs' must be a list")
                else:
                    for idx, agg in enumerate(logic['agg_exprs']):
                        if 'name' not in agg:
                            errors.append(f"Missing 'name' in logic.agg_exprs[{idx}]")
                        if 'expr' not in agg:
                            errors.append(f"Missing 'expr' in logic.agg_exprs[{idx}]")
                
                if 'having_expr' not in logic:
                    errors.append("Missing 'logic.having_expr' (required for type=aggregate)")
            
            elif logic_type == 'cross_entity':
                # cross_entity 类型：跨表校验（anti_join / compare）
                # 必须有 input.entity_keys、logic.operation、logic.join_on
                
                # 验证 input.entity_keys
                entity_keys = config.get('input', {}).get('entity_keys')
                if not entity_keys:
                    errors.append("Missing 'input.entity_keys' (required for type=cross_entity)")
                else:
                    if 'left' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.left'")
                    if 'right' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.right'")
                
                # 验证 operation
                operation = logic.get('operation')
                if not operation:
                    errors.append("Missing 'logic.operation' (required for type=cross_entity)")
                elif operation not in ('anti_join', 'compare'):
                    errors.append(f"Invalid operation: '{operation}' (must be 'anti_join' or 'compare')")
                
                # 验证 join_on
                join_on = logic.get('join_on')
                if not join_on:
                    errors.append("Missing 'logic.join_on' (required for type=cross_entity)")
                else:
                    if 'left' not in join_on:
                        errors.append("Missing 'logic.join_on.left'")
                    elif not isinstance(join_on['left'], list):
                        errors.append("'logic.join_on.left' must be a list of column names")
                    if 'right' not in join_on:
                        errors.append("Missing 'logic.join_on.right'")
                    elif not isinstance(join_on['right'], list):
                        errors.append("'logic.join_on.right' must be a list of column names")
                
                # compare 需要 assertion
                if operation == 'compare' and 'assertion' not in logic:
                    errors.append("Missing 'logic.assertion' (required for operation=compare)")
                
                # 验证 left_pre_agg / right_pre_agg（可选）
                for side in ('left_pre_agg', 'right_pre_agg'):
                    if side in logic:
                        pre_agg = logic[side]
                        if 'group_by' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.group_by'")
                        if 'agg_exprs' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.agg_exprs'")
                        elif isinstance(pre_agg['agg_exprs'], list):
                            for idx, agg in enumerate(pre_agg['agg_exprs']):
                                if 'name' not in agg:
                                    errors.append(f"Missing 'name' in logic.{side}.agg_exprs[{idx}]")
                                if 'expr' not in agg:
                                    errors.append(f"Missing 'expr' in logic.{side}.agg_exprs[{idx}]")
            
            else:
                errors.append(f"Unknown logic type: '{logic_type}' (must be 'filter', 'aggregate', or 'cross_entity')")
            
            # pre_compute 校验（可选，两种类型都支持）
            if 'pre_compute' in logic:
                if not isinstance(logic['pre_compute'], list):
                    errors.append("'logic.pre_compute' must be a list")
                else:
                    for idx, step in enumerate(logic['pre_compute']):
                        if 'name' not in step:
                            errors.append(f"Missing 'name' in logic.pre_compute[{idx}]")
                        if 'expr' not in step:
                            errors.append(f"Missing 'expr' in logic.pre_compute[{idx}]")
        
        # ========== asset 校验（可选） ==========
        if 'asset' in config:
            asset = config['asset']
            if asset.get('enabled', False) and 'field_mapping' in asset:
                field_map = asset['field_mapping']
                required_mapping_fields = ['asset_id', 'vehicle_id', 'start_ts', 'end_ts']
                for f in required_mapping_fields:
                    if f not in field_map:
                        errors.append(f"Missing 'asset.field_mapping.{f}' (required when asset.enabled=true)")
        
        # ========== 汇总错误 ==========
        if errors:
            error_msg = f"Invalid rule config in {file_path}:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
    
    def _topological_sort(self, rules: List[Dict]) -> List[Dict]:
        """
        拓扑排序（Kahn 算法）
        
        处理规则依赖关系，确保：
        - 被依赖的规则先执行
        - 检测循环依赖
        
        Args:
            rules: 规则列表
        
        Returns:
            排序后的规则列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 构建依赖图和规则字典
        graph = {}
        rule_dict = {}
        
        for rule in rules:
            rule_id = rule['rule_id']
            graph[rule_id] = rule['depends_on']
            rule_dict[rule_id] = rule
        
        # 验证依赖是否存在
        for rule_id, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise ValueError(
                        f"Rule '{rule_id}' depends on '{dep}', but '{dep}' not found. "
                        f"Available rules: {list(graph.keys())}"
                    )
        
        # 计算入度（我依赖多少个规则）
        # depends_on 的含义：我依赖谁（即存在边 dep -> me）
        # 入度 = len(depends_on)
        in_degree = {rule_id: len(deps) for rule_id, deps in graph.items()}
        
        # Kahn 算法：从入度为 0 的节点开始
        queue = [rule_id for rule_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # 取出一个入度为 0 的节点（没有依赖的规则，可以先执行）
            rule_id = queue.pop(0)
            result.append(rule_dict[rule_id])
            
            # 遍历所有规则，找出"依赖于当前规则"的规则
            for other_rule_id, deps in graph.items():
                if rule_id in deps:
                    # 当前规则已完成，依赖它的规则入度减 1
                    in_degree[other_rule_id] -= 1
                    # 如果入度变为 0，说明所有依赖都已满足，可以执行了
                    if in_degree[other_rule_id] == 0:
                        queue.append(other_rule_id)
        
        # 检测循环依赖
        if len(result) != len(rules):
            # 找出参与循环依赖的规则
            unprocessed = [r['rule_id'] for r in rules if r not in result]
            raise ValueError(
                f"Circular dependency detected in rules! "
                f"Unprocessed rules: {unprocessed}. "
                f"Check the 'depends_on' fields."
            )
        
        return result
    
    def get_rule_summary(self, rules: List[Dict]) -> str:
        """
        生成规则摘要（用于日志）
        
        Args:
            rules: 规则列表
        
        Returns:
            摘要字符串
        """
        lines = ["Rule Execution Order:"]
        for idx, rule in enumerate(rules, 1):
            deps_str = f" (depends on: {', '.join(rule['depends_on'])})" if rule['depends_on'] else ""
            logic_type = rule['config'].get('logic', {}).get('type', 'filter')
            lines.append(f"  {idx}. [{rule['severity']}] {rule['rule_id']} (type={logic_type}){deps_str}")
        
        return "\n".join(lines)

```

```python
"""
orchestration/rule_scanner.py
规则扫描器

职责：
- 扫描规则配置文件（configs/rules/*.yaml）
- 解析规则依赖关系
- 拓扑排序（处理依赖顺序）
- 验证配置完整性（防御性解析）
- 支持 V2 规则格式（filter / aggregate 类型）

运行时机：
- DAG 文件顶层代码（解析时）
- Aggregator 执行时（加载规则元数据）
- 不能有耗时 I/O 操作（只读本地文件）
"""
import os
import glob
import yaml
from typing import List, Dict, Any, Optional


class RuleScanner:
    """
    规则扫描器
    
    使用示例:
    ```python
    # 在 DAG 文件顶层代码中
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    
    # 动态生成 Rule Tasks
    for rule in rules:
        GenericRuleOperator(
            task_id=rule['rule_id'],
            config_path=rule['config_path'],
            ...
        )
    ```
    """
    
    def __init__(self, rules_dir: str = None):
        """
        初始化
        
        Args:
            rules_dir: 规则目录路径（相对于 $AIRFLOW_HOME/plugins/）
        """
        self.rules_dir = rules_dir or 'configs/rules'
    
    def scan_rules(self) -> List[Dict[str, Any]]:
        """
        扫描规则并返回排序后的列表
        
        Returns:
            规则配置列表（已按依赖关系拓扑排序）
            
        示例:
            [
                {
                    'rule_id': 'rule_p0_time_check',
                    'config_path': 'configs/rules/p0_time_check.yaml',
                    'depends_on': [],
                    'severity': 'P0',
                    'config': {...}
                },
                ...
            ]
        
        Raises:
            ValueError: 配置验证失败、循环依赖等
        """
        # 1. 扫描目录
        rule_files = self._scan_rule_files()
        
        if not rule_files:
            raise ValueError(f"No rule files found in {self.rules_dir}")
        
        print(f"📋 Found {len(rule_files)} rule files")
        
        # 2. 加载并验证规则
        rules = []
        for file_path in rule_files:
            try:
                rule_config = self._load_and_validate_rule(file_path)
                if rule_config is None:
                    # 空文件，跳过
                    print(f"⏭️ Skipping empty rule file: {file_path}")
                    continue
                rules.append(rule_config)
            except Exception as e:
                # 配置错误：拒绝加载 DAG
                raise ValueError(f"Failed to load rule from {file_path}: {e}")
        
        # 3. 拓扑排序（处理依赖关系）
        sorted_rules = self._topological_sort(rules)
        
        print(f"✅ Loaded {len(sorted_rules)} rules (topologically sorted)")
        
        return sorted_rules
    
    def get_rule_configs_by_id(self, target_entity: Optional[str] = None) -> Dict[str, Dict]:
        """
        获取规则配置字典（按 rule_id 索引）
        
        用于 Aggregator 在执行时获取规则的完整配置，
        以驱动数据驱动的 alert/asset 处理逻辑。
        
        Args:
            target_entity: 可选，按 target_entity 过滤
        
        Returns:
            {rule_id: full_config_dict}
        """
        rules = self.scan_rules()
        
        result = {}
        for rule in rules:
            if target_entity and rule.get('target_entity') != target_entity:
                continue
            result[rule['rule_id']] = rule['config']
        
        return result
    
    def _scan_rule_files(self) -> List[str]:
        """
        扫描规则目录，返回所有 YAML 文件路径
        
        支持绝对路径和相对路径（相对于 $AIRFLOW_HOME/plugins/）
        
        Returns:
            文件路径列表（已排序）
        """
        if os.path.isabs(self.rules_dir):
            rules_path = self.rules_dir
        else:
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            rules_path = os.path.join(airflow_home, 'plugins', self.rules_dir)
        
        # 扫描 *.yaml 文件
        pattern = os.path.join(rules_path, '*.yaml')
        files = glob.glob(pattern)
        
        # 排序保证稳定性
        return sorted(files)
    
    def _load_and_validate_rule(self, file_path: str) -> Dict[str, Any]:
        """
        加载并验证单个规则配置
        
        Args:
            file_path: 规则文件路径
        
        Returns:
            规则配置字典
        
        Raises:
            ValueError: 配置验证失败
        """
        # 1. 加载 YAML
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 空文件或空内容：跳过
        if config is None:
            return None
        
        # 2. 防御性验证（Schema Validation）
        self._validate_rule_config(config, file_path)
        
        # 3. 提取关键信息
        # 计算相对路径（相对于 plugins/）
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        plugins_dir = os.path.join(airflow_home, 'plugins')
        relative_path = os.path.relpath(file_path, plugins_dir)
        
        return {
            'rule_id': config['meta']['rule_id'],
            'config_path': relative_path,  # 相对路径
            'depends_on': config.get('depends_on', []),
            'severity': config['meta']['severity'],
            'description': config['meta'].get('description', ''),
            'target_entity': config.get('target_entity'),
            'config': config  # 完整配置（供调试和 Aggregator 使用）
        }
    
    def _validate_rule_config(self, config: Dict, file_path: str) -> None:
        """
        验证规则配置完整性（防御性解析，支持 V2 格式）
        
        V2 格式新增支持：
        - logic.type: filter（默认） | aggregate
        - logic.pre_compute: 预计算步骤（可选）
        - logic.group_by / agg_exprs / having_expr: 聚合型规则必需
        - input.entity_key: 明确的实体读取键（可选）
        - alert.content_template: Jinja 模板（可选）
        - alert.trigger_id_field: 触发 ID 字段名（可选）
        - asset.field_mapping: 资产字段映射（可选）
        
        必需字段：
        - meta
        - meta.rule_id
        - meta.severity
        - target_entity
        - logic
        - logic.filter_expr（当 type=filter 时）
        - logic.group_by / agg_exprs / having_expr（当 type=aggregate 时）
        
        Args:
            config: 规则配置字典
            file_path: 文件路径（用于错误提示）
        
        Raises:
            ValueError: 配置验证失败
        """
        errors = []
        
        # ========== meta 校验 ==========
        if 'meta' not in config:
            errors.append("Missing 'meta' field")
        else:
            if 'rule_id' not in config['meta']:
                errors.append("Missing 'meta.rule_id'")
            if 'severity' not in config['meta']:
                errors.append("Missing 'meta.severity'")
            else:
                # 验证 severity 取值
                valid_severities = ['P0', 'P1', 'P2', 'INFO']
                if config['meta']['severity'] not in valid_severities:
                    errors.append(f"Invalid severity: {config['meta']['severity']} (must be one of {valid_severities})")
        
        # ========== target_entity 校验 ==========
        if 'target_entity' not in config:
            errors.append("Missing 'target_entity'")
        
        # ========== logic 校验（支持 V2 多类型） ==========
        if 'logic' not in config:
            errors.append("Missing 'logic' field")
        else:
            logic = config['logic']
            logic_type = logic.get('type', 'filter')  # 默认 filter（向后兼容）
            
            if logic_type == 'filter':
                # filter 类型必须有 filter_expr
                if 'filter_expr' not in logic:
                    errors.append("Missing 'logic.filter_expr' (required for type=filter)")
            
            elif logic_type == 'aggregate':
                # aggregate 类型必须有 group_by, agg_exprs, having_expr
                if 'group_by' not in logic:
                    errors.append("Missing 'logic.group_by' (required for type=aggregate)")
                elif not isinstance(logic['group_by'], list):
                    errors.append("'logic.group_by' must be a list of column names")
                
                if 'agg_exprs' not in logic:
                    errors.append("Missing 'logic.agg_exprs' (required for type=aggregate)")
                elif not isinstance(logic['agg_exprs'], list):
                    errors.append("'logic.agg_exprs' must be a list")
                else:
                    for idx, agg in enumerate(logic['agg_exprs']):
                        if 'name' not in agg:
                            errors.append(f"Missing 'name' in logic.agg_exprs[{idx}]")
                        if 'expr' not in agg:
                            errors.append(f"Missing 'expr' in logic.agg_exprs[{idx}]")
                
                if 'having_expr' not in logic:
                    errors.append("Missing 'logic.having_expr' (required for type=aggregate)")
            
            elif logic_type == 'cross_entity':
                # cross_entity 类型：跨表校验（anti_join / compare）
                # 必须有 input.entity_keys、logic.operation、logic.join_on
                
                # 验证 input.entity_keys
                entity_keys = config.get('input', {}).get('entity_keys')
                if not entity_keys:
                    errors.append("Missing 'input.entity_keys' (required for type=cross_entity)")
                else:
                    if 'left' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.left'")
                    if 'right' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.right'")
                
                # 验证 operation
                operation = logic.get('operation')
                if not operation:
                    errors.append("Missing 'logic.operation' (required for type=cross_entity)")
                elif operation not in ('anti_join', 'compare'):
                    errors.append(f"Invalid operation: '{operation}' (must be 'anti_join' or 'compare')")
                
                # 验证 join_on
                join_on = logic.get('join_on')
                if not join_on:
                    errors.append("Missing 'logic.join_on' (required for type=cross_entity)")
                else:
                    if 'left' not in join_on:
                        errors.append("Missing 'logic.join_on.left'")
                    elif not isinstance(join_on['left'], list):
                        errors.append("'logic.join_on.left' must be a list of column names")
                    if 'right' not in join_on:
                        errors.append("Missing 'logic.join_on.right'")
                    elif not isinstance(join_on['right'], list):
                        errors.append("'logic.join_on.right' must be a list of column names")
                
                # compare 需要 assertion
                if operation == 'compare' and 'assertion' not in logic:
                    errors.append("Missing 'logic.assertion' (required for operation=compare)")
                
                # 验证 left_pre_agg / right_pre_agg（可选）
                for side in ('left_pre_agg', 'right_pre_agg'):
                    if side in logic:
                        pre_agg = logic[side]
                        if 'group_by' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.group_by'")
                        if 'agg_exprs' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.agg_exprs'")
                        elif isinstance(pre_agg['agg_exprs'], list):
                            for idx, agg in enumerate(pre_agg['agg_exprs']):
                                if 'name' not in agg:
                                    errors.append(f"Missing 'name' in logic.{side}.agg_exprs[{idx}]")
                                if 'expr' not in agg:
                                    errors.append(f"Missing 'expr' in logic.{side}.agg_exprs[{idx}]")
            
            else:
                errors.append(f"Unknown logic type: '{logic_type}' (must be 'filter', 'aggregate', or 'cross_entity')")
            
            # pre_compute 校验（可选，两种类型都支持）
            if 'pre_compute' in logic:
                if not isinstance(logic['pre_compute'], list):
                    errors.append("'logic.pre_compute' must be a list")
                else:
                    for idx, step in enumerate(logic['pre_compute']):
                        if 'name' not in step:
                            errors.append(f"Missing 'name' in logic.pre_compute[{idx}]")
                        if 'expr' not in step:
                            errors.append(f"Missing 'expr' in logic.pre_compute[{idx}]")
        
        # ========== asset 校验（可选） ==========
        if 'asset' in config:
            asset = config['asset']
            if asset.get('enabled', False) and 'field_mapping' in asset:
                field_map = asset['field_mapping']
                required_mapping_fields = ['asset_id', 'vehicle_id', 'start_ts', 'end_ts']
                for f in required_mapping_fields:
                    if f not in field_map:
                        errors.append(f"Missing 'asset.field_mapping.{f}' (required when asset.enabled=true)")
        
        # ========== 汇总错误 ==========
        if errors:
            error_msg = f"Invalid rule config in {file_path}:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
    
    def _topological_sort(self, rules: List[Dict]) -> List[Dict]:
        """
        拓扑排序（Kahn 算法）
        
        处理规则依赖关系，确保：
        - 被依赖的规则先执行
        - 检测循环依赖
        
        Args:
            rules: 规则列表
        
        Returns:
            排序后的规则列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 构建依赖图和规则字典
        graph = {}
        rule_dict = {}
        
        for rule in rules:
            rule_id = rule['rule_id']
            graph[rule_id] = rule['depends_on']
            rule_dict[rule_id] = rule
        
        # 验证依赖是否存在
        for rule_id, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise ValueError(
                        f"Rule '{rule_id}' depends on '{dep}', but '{dep}' not found. "
                        f"Available rules: {list(graph.keys())}"
                    )
        
        # 计算入度（我依赖多少个规则）
        # depends_on 的含义：我依赖谁（即存在边 dep -> me）
        # 入度 = len(depends_on)
        in_degree = {rule_id: len(deps) for rule_id, deps in graph.items()}
        
        # Kahn 算法：从入度为 0 的节点开始
        queue = [rule_id for rule_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # 取出一个入度为 0 的节点（没有依赖的规则，可以先执行）
            rule_id = queue.pop(0)
            result.append(rule_dict[rule_id])
            
            # 遍历所有规则，找出"依赖于当前规则"的规则
            for other_rule_id, deps in graph.items():
                if rule_id in deps:
                    # 当前规则已完成，依赖它的规则入度减 1
                    in_degree[other_rule_id] -= 1
                    # 如果入度变为 0，说明所有依赖都已满足，可以执行了
                    if in_degree[other_rule_id] == 0:
                        queue.append(other_rule_id)
        
        # 检测循环依赖
        if len(result) != len(rules):
            # 找出参与循环依赖的规则
            unprocessed = [r['rule_id'] for r in rules if r not in result]
            raise ValueError(
                f"Circular dependency detected in rules! "
                f"Unprocessed rules: {unprocessed}. "
                f"Check the 'depends_on' fields."
            )
        
        return result
    
    def get_rule_summary(self, rules: List[Dict]) -> str:
        """
        生成规则摘要（用于日志）
        
        Args:
            rules: 规则列表
        
        Returns:
            摘要字符串
        """
        lines = ["Rule Execution Order:"]
        for idx, rule in enumerate(rules, 1):
            deps_str = f" (depends on: {', '.join(rule['depends_on'])})" if rule['depends_on'] else ""
            logic_type = rule['config'].get('logic', {}).get('type', 'filter')
            lines.append(f"  {idx}. [{rule['severity']}] {rule['rule_id']} (type={logic_type}){deps_str}")
        
        return "\n".join(lines)

```

Now let me also update the return type hint:

```python
"""
orchestration/rule_scanner.py
规则扫描器

职责：
- 扫描规则配置文件（configs/rules/*.yaml）
- 解析规则依赖关系
- 拓扑排序（处理依赖顺序）
- 验证配置完整性（防御性解析）
- 支持 V2 规则格式（filter / aggregate 类型）

运行时机：
- DAG 文件顶层代码（解析时）
- Aggregator 执行时（加载规则元数据）
- 不能有耗时 I/O 操作（只读本地文件）
"""
import os
import glob
import yaml
from typing import List, Dict, Any, Optional


class RuleScanner:
    """
    规则扫描器
    
    使用示例:
    ```python
    # 在 DAG 文件顶层代码中
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    
    # 动态生成 Rule Tasks
    for rule in rules:
        GenericRuleOperator(
            task_id=rule['rule_id'],
            config_path=rule['config_path'],
            ...
        )
    ```
    """
    
    def __init__(self, rules_dir: str = None):
        """
        初始化
        
        Args:
            rules_dir: 规则目录路径（相对于 $AIRFLOW_HOME/plugins/）
        """
        self.rules_dir = rules_dir or 'configs/rules'
    
    def scan_rules(self) -> List[Dict[str, Any]]:
        """
        扫描规则并返回排序后的列表
        
        Returns:
            规则配置列表（已按依赖关系拓扑排序）
            
        示例:
            [
                {
                    'rule_id': 'rule_p0_time_check',
                    'config_path': 'configs/rules/p0_time_check.yaml',
                    'depends_on': [],
                    'severity': 'P0',
                    'config': {...}
                },
                ...
            ]
        
        Raises:
            ValueError: 配置验证失败、循环依赖等
        """
        # 1. 扫描目录
        rule_files = self._scan_rule_files()
        
        if not rule_files:
            raise ValueError(f"No rule files found in {self.rules_dir}")
        
        print(f"📋 Found {len(rule_files)} rule files")
        
        # 2. 加载并验证规则
        rules = []
        for file_path in rule_files:
            try:
                rule_config = self._load_and_validate_rule(file_path)
                if rule_config is None:
                    # 空文件，跳过
                    print(f"⏭️ Skipping empty rule file: {file_path}")
                    continue
                rules.append(rule_config)
            except Exception as e:
                # 配置错误：拒绝加载 DAG
                raise ValueError(f"Failed to load rule from {file_path}: {e}")
        
        # 3. 拓扑排序（处理依赖关系）
        sorted_rules = self._topological_sort(rules)
        
        print(f"✅ Loaded {len(sorted_rules)} rules (topologically sorted)")
        
        return sorted_rules
    
    def get_rule_configs_by_id(self, target_entity: Optional[str] = None) -> Dict[str, Dict]:
        """
        获取规则配置字典（按 rule_id 索引）
        
        用于 Aggregator 在执行时获取规则的完整配置，
        以驱动数据驱动的 alert/asset 处理逻辑。
        
        Args:
            target_entity: 可选，按 target_entity 过滤
        
        Returns:
            {rule_id: full_config_dict}
        """
        rules = self.scan_rules()
        
        result = {}
        for rule in rules:
            if target_entity and rule.get('target_entity') != target_entity:
                continue
            result[rule['rule_id']] = rule['config']
        
        return result
    
    def _scan_rule_files(self) -> List[str]:
        """
        扫描规则目录，返回所有 YAML 文件路径
        
        支持绝对路径和相对路径（相对于 $AIRFLOW_HOME/plugins/）
        
        Returns:
            文件路径列表（已排序）
        """
        if os.path.isabs(self.rules_dir):
            rules_path = self.rules_dir
        else:
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        rules_path = os.path.join(airflow_home, 'plugins', self.rules_dir)
        
        # 扫描 *.yaml 文件
        pattern = os.path.join(rules_path, '*.yaml')
        files = glob.glob(pattern)
        
        # 排序保证稳定性
        return sorted(files)
    
    def _load_and_validate_rule(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        加载并验证单个规则配置
        
        Args:
            file_path: 规则文件路径
        
        Returns:
            规则配置字典
        
        Raises:
            ValueError: 配置验证失败
        """
        # 1. 加载 YAML
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 空文件或空内容：跳过
        if config is None:
            return None
        
        # 2. 防御性验证（Schema Validation）
        self._validate_rule_config(config, file_path)
        
        # 3. 提取关键信息
        # 计算相对路径（相对于 plugins/）
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        plugins_dir = os.path.join(airflow_home, 'plugins')
        relative_path = os.path.relpath(file_path, plugins_dir)
        
        return {
            'rule_id': config['meta']['rule_id'],
            'config_path': relative_path,  # 相对路径
            'depends_on': config.get('depends_on', []),
            'severity': config['meta']['severity'],
            'description': config['meta'].get('description', ''),
            'target_entity': config.get('target_entity'),
            'config': config  # 完整配置（供调试和 Aggregator 使用）
        }
    
    def _validate_rule_config(self, config: Dict, file_path: str) -> None:
        """
        验证规则配置完整性（防御性解析，支持 V2 格式）
        
        V2 格式新增支持：
        - logic.type: filter（默认） | aggregate
        - logic.pre_compute: 预计算步骤（可选）
        - logic.group_by / agg_exprs / having_expr: 聚合型规则必需
        - input.entity_key: 明确的实体读取键（可选）
        - alert.content_template: Jinja 模板（可选）
        - alert.trigger_id_field: 触发 ID 字段名（可选）
        - asset.field_mapping: 资产字段映射（可选）
        
        必需字段：
        - meta
        - meta.rule_id
        - meta.severity
        - target_entity
        - logic
        - logic.filter_expr（当 type=filter 时）
        - logic.group_by / agg_exprs / having_expr（当 type=aggregate 时）
        
        Args:
            config: 规则配置字典
            file_path: 文件路径（用于错误提示）
        
        Raises:
            ValueError: 配置验证失败
        """
        errors = []
        
        # ========== meta 校验 ==========
        if 'meta' not in config:
            errors.append("Missing 'meta' field")
        else:
            if 'rule_id' not in config['meta']:
                errors.append("Missing 'meta.rule_id'")
            if 'severity' not in config['meta']:
                errors.append("Missing 'meta.severity'")
            else:
                # 验证 severity 取值
                valid_severities = ['P0', 'P1', 'P2', 'INFO']
                if config['meta']['severity'] not in valid_severities:
                    errors.append(f"Invalid severity: {config['meta']['severity']} (must be one of {valid_severities})")
        
        # ========== target_entity 校验 ==========
        if 'target_entity' not in config:
            errors.append("Missing 'target_entity'")
        
        # ========== logic 校验（支持 V2 多类型） ==========
        if 'logic' not in config:
            errors.append("Missing 'logic' field")
        else:
            logic = config['logic']
            logic_type = logic.get('type', 'filter')  # 默认 filter（向后兼容）
            
            if logic_type == 'filter':
                # filter 类型必须有 filter_expr
                if 'filter_expr' not in logic:
                    errors.append("Missing 'logic.filter_expr' (required for type=filter)")
            
            elif logic_type == 'aggregate':
                # aggregate 类型必须有 group_by, agg_exprs, having_expr
                if 'group_by' not in logic:
                    errors.append("Missing 'logic.group_by' (required for type=aggregate)")
                elif not isinstance(logic['group_by'], list):
                    errors.append("'logic.group_by' must be a list of column names")
                
                if 'agg_exprs' not in logic:
                    errors.append("Missing 'logic.agg_exprs' (required for type=aggregate)")
                elif not isinstance(logic['agg_exprs'], list):
                    errors.append("'logic.agg_exprs' must be a list")
                else:
                    for idx, agg in enumerate(logic['agg_exprs']):
                        if 'name' not in agg:
                            errors.append(f"Missing 'name' in logic.agg_exprs[{idx}]")
                        if 'expr' not in agg:
                            errors.append(f"Missing 'expr' in logic.agg_exprs[{idx}]")
                
                if 'having_expr' not in logic:
                    errors.append("Missing 'logic.having_expr' (required for type=aggregate)")
            
            elif logic_type == 'cross_entity':
                # cross_entity 类型：跨表校验（anti_join / compare）
                # 必须有 input.entity_keys、logic.operation、logic.join_on
                
                # 验证 input.entity_keys
                entity_keys = config.get('input', {}).get('entity_keys')
                if not entity_keys:
                    errors.append("Missing 'input.entity_keys' (required for type=cross_entity)")
                else:
                    if 'left' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.left'")
                    if 'right' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.right'")
                
                # 验证 operation
                operation = logic.get('operation')
                if not operation:
                    errors.append("Missing 'logic.operation' (required for type=cross_entity)")
                elif operation not in ('anti_join', 'compare'):
                    errors.append(f"Invalid operation: '{operation}' (must be 'anti_join' or 'compare')")
                
                # 验证 join_on
                join_on = logic.get('join_on')
                if not join_on:
                    errors.append("Missing 'logic.join_on' (required for type=cross_entity)")
                else:
                    if 'left' not in join_on:
                        errors.append("Missing 'logic.join_on.left'")
                    elif not isinstance(join_on['left'], list):
                        errors.append("'logic.join_on.left' must be a list of column names")
                    if 'right' not in join_on:
                        errors.append("Missing 'logic.join_on.right'")
                    elif not isinstance(join_on['right'], list):
                        errors.append("'logic.join_on.right' must be a list of column names")
                
                # compare 需要 assertion
                if operation == 'compare' and 'assertion' not in logic:
                    errors.append("Missing 'logic.assertion' (required for operation=compare)")
                
                # 验证 left_pre_agg / right_pre_agg（可选）
                for side in ('left_pre_agg', 'right_pre_agg'):
                    if side in logic:
                        pre_agg = logic[side]
                        if 'group_by' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.group_by'")
                        if 'agg_exprs' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.agg_exprs'")
                        elif isinstance(pre_agg['agg_exprs'], list):
                            for idx, agg in enumerate(pre_agg['agg_exprs']):
                                if 'name' not in agg:
                                    errors.append(f"Missing 'name' in logic.{side}.agg_exprs[{idx}]")
                                if 'expr' not in agg:
                                    errors.append(f"Missing 'expr' in logic.{side}.agg_exprs[{idx}]")
            
            else:
                errors.append(f"Unknown logic type: '{logic_type}' (must be 'filter', 'aggregate', or 'cross_entity')")
            
            # pre_compute 校验（可选，两种类型都支持）
            if 'pre_compute' in logic:
                if not isinstance(logic['pre_compute'], list):
                    errors.append("'logic.pre_compute' must be a list")
                else:
                    for idx, step in enumerate(logic['pre_compute']):
                        if 'name' not in step:
                            errors.append(f"Missing 'name' in logic.pre_compute[{idx}]")
                        if 'expr' not in step:
                            errors.append(f"Missing 'expr' in logic.pre_compute[{idx}]")
        
        # ========== asset 校验（可选） ==========
        if 'asset' in config:
            asset = config['asset']
            if asset.get('enabled', False) and 'field_mapping' in asset:
                field_map = asset['field_mapping']
                required_mapping_fields = ['asset_id', 'vehicle_id', 'start_ts', 'end_ts']
                for f in required_mapping_fields:
                    if f not in field_map:
                        errors.append(f"Missing 'asset.field_mapping.{f}' (required when asset.enabled=true)")
        
        # ========== 汇总错误 ==========
        if errors:
            error_msg = f"Invalid rule config in {file_path}:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
    
    def _topological_sort(self, rules: List[Dict]) -> List[Dict]:
        """
        拓扑排序（Kahn 算法）
        
        处理规则依赖关系，确保：
        - 被依赖的规则先执行
        - 检测循环依赖
        
        Args:
            rules: 规则列表
        
        Returns:
            排序后的规则列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 构建依赖图和规则字典
        graph = {}
        rule_dict = {}
        
        for rule in rules:
            rule_id = rule['rule_id']
            graph[rule_id] = rule['depends_on']
            rule_dict[rule_id] = rule
        
        # 验证依赖是否存在
        for rule_id, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise ValueError(
                        f"Rule '{rule_id}' depends on '{dep}', but '{dep}' not found. "
                        f"Available rules: {list(graph.keys())}"
                    )
        
        # 计算入度（我依赖多少个规则）
        # depends_on 的含义：我依赖谁（即存在边 dep -> me）
        # 入度 = len(depends_on)
        in_degree = {rule_id: len(deps) for rule_id, deps in graph.items()}
        
        # Kahn 算法：从入度为 0 的节点开始
        queue = [rule_id for rule_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # 取出一个入度为 0 的节点（没有依赖的规则，可以先执行）
            rule_id = queue.pop(0)
            result.append(rule_dict[rule_id])
            
            # 遍历所有规则，找出"依赖于当前规则"的规则
            for other_rule_id, deps in graph.items():
                if rule_id in deps:
                    # 当前规则已完成，依赖它的规则入度减 1
                    in_degree[other_rule_id] -= 1
                    # 如果入度变为 0，说明所有依赖都已满足，可以执行了
                    if in_degree[other_rule_id] == 0:
                        queue.append(other_rule_id)
        
        # 检测循环依赖
        if len(result) != len(rules):
            # 找出参与循环依赖的规则
            unprocessed = [r['rule_id'] for r in rules if r not in result]
            raise ValueError(
                f"Circular dependency detected in rules! "
                f"Unprocessed rules: {unprocessed}. "
                f"Check the 'depends_on' fields."
            )
        
        return result
    
    def get_rule_summary(self, rules: List[Dict]) -> str:
        """
        生成规则摘要（用于日志）
        
        Args:
            rules: 规则列表
        
        Returns:
            摘要字符串
        """
        lines = ["Rule Execution Order:"]
        for idx, rule in enumerate(rules, 1):
            deps_str = f" (depends on: {', '.join(rule['depends_on'])})" if rule['depends_on'] else ""
            logic_type = rule['config'].get('logic', {}).get('type', 'filter')
            lines.append(f"  {idx}. [{rule['severity']}] {rule['rule_id']} (type={logic_type}){deps_str}")
        
        return "\n".join(lines)

```

```python
"""
orchestration/rule_scanner.py
规则扫描器

职责：
- 扫描规则配置文件（configs/rules/*.yaml）
- 解析规则依赖关系
- 拓扑排序（处理依赖顺序）
- 验证配置完整性（防御性解析）
- 支持 V2 规则格式（filter / aggregate 类型）

运行时机：
- DAG 文件顶层代码（解析时）
- Aggregator 执行时（加载规则元数据）
- 不能有耗时 I/O 操作（只读本地文件）
"""
import os
import glob
import yaml
from typing import List, Dict, Any, Optional


class RuleScanner:
    """
    规则扫描器
    
    使用示例:
    ```python
    # 在 DAG 文件顶层代码中
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    
    # 动态生成 Rule Tasks
    for rule in rules:
        GenericRuleOperator(
            task_id=rule['rule_id'],
            config_path=rule['config_path'],
            ...
        )
    ```
    """
    
    def __init__(self, rules_dir: str = None):
        """
        初始化
        
        Args:
            rules_dir: 规则目录路径（相对于 $AIRFLOW_HOME/plugins/）
        """
        self.rules_dir = rules_dir or 'configs/rules'
    
    def scan_rules(self) -> List[Dict[str, Any]]:
        """
        扫描规则并返回排序后的列表
        
        Returns:
            规则配置列表（已按依赖关系拓扑排序）
            
        示例:
            [
                {
                    'rule_id': 'rule_p0_time_check',
                    'config_path': 'configs/rules/p0_time_check.yaml',
                    'depends_on': [],
                    'severity': 'P0',
                    'config': {...}
                },
                ...
            ]
        
        Raises:
            ValueError: 配置验证失败、循环依赖等
        """
        # 1. 扫描目录
        rule_files = self._scan_rule_files()
        
        if not rule_files:
            raise ValueError(f"No rule files found in {self.rules_dir}")
        
        print(f"📋 Found {len(rule_files)} rule files")
        
        # 2. 加载并验证规则
        rules = []
        for file_path in rule_files:
            try:
                rule_config = self._load_and_validate_rule(file_path)
                if rule_config is None:
                    # 空文件，跳过
                    print(f"⏭️ Skipping empty rule file: {file_path}")
                    continue
                rules.append(rule_config)
            except Exception as e:
                # 配置错误：拒绝加载 DAG
                raise ValueError(f"Failed to load rule from {file_path}: {e}")
        
        # 3. 拓扑排序（处理依赖关系）
        sorted_rules = self._topological_sort(rules)
        
        print(f"✅ Loaded {len(sorted_rules)} rules (topologically sorted)")
        
        return sorted_rules
    
    def get_rule_configs_by_id(self, target_entity: Optional[str] = None) -> Dict[str, Dict]:
        """
        获取规则配置字典（按 rule_id 索引）
        
        用于 Aggregator 在执行时获取规则的完整配置，
        以驱动数据驱动的 alert/asset 处理逻辑。
        
        Args:
            target_entity: 可选，按 target_entity 过滤
        
        Returns:
            {rule_id: full_config_dict}
        """
        rules = self.scan_rules()
        
        result = {}
        for rule in rules:
            if target_entity and rule.get('target_entity') != target_entity:
                continue
            result[rule['rule_id']] = rule['config']
        
        return result
    
    def _scan_rule_files(self) -> List[str]:
        """
        扫描规则目录，返回所有 YAML 文件路径
        
        支持绝对路径和相对路径（相对于 $AIRFLOW_HOME/plugins/）
        
        Returns:
            文件路径列表（已排序）
        """
        if os.path.isabs(self.rules_dir):
            rules_path = self.rules_dir
        else:
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            rules_path = os.path.join(airflow_home, 'plugins', self.rules_dir)
        
        # 扫描 *.yaml 文件
        pattern = os.path.join(rules_path, '*.yaml')
        files = glob.glob(pattern)
        
        # 排序保证稳定性
        return sorted(files)
    
    def _load_and_validate_rule(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        加载并验证单个规则配置
        
        Args:
            file_path: 规则文件路径
        
        Returns:
            规则配置字典
        
        Raises:
            ValueError: 配置验证失败
        """
        # 1. 加载 YAML
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 空文件或空内容：跳过
        if config is None:
            return None
        
        # 2. 防御性验证（Schema Validation）
        self._validate_rule_config(config, file_path)
        
        # 3. 提取关键信息
        # 计算相对路径（相对于 plugins/）
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        plugins_dir = os.path.join(airflow_home, 'plugins')
        relative_path = os.path.relpath(file_path, plugins_dir)
        
        return {
            'rule_id': config['meta']['rule_id'],
            'config_path': relative_path,  # 相对路径
            'depends_on': config.get('depends_on', []),
            'severity': config['meta']['severity'],
            'description': config['meta'].get('description', ''),
            'target_entity': config.get('target_entity'),
            'config': config  # 完整配置（供调试和 Aggregator 使用）
        }
    
    def _validate_rule_config(self, config: Dict, file_path: str) -> None:
        """
        验证规则配置完整性（防御性解析，支持 V2 格式）
        
        V2 格式新增支持：
        - logic.type: filter（默认） | aggregate
        - logic.pre_compute: 预计算步骤（可选）
        - logic.group_by / agg_exprs / having_expr: 聚合型规则必需
        - input.entity_key: 明确的实体读取键（可选）
        - alert.content_template: Jinja 模板（可选）
        - alert.trigger_id_field: 触发 ID 字段名（可选）
        - asset.field_mapping: 资产字段映射（可选）
        
        必需字段：
        - meta
        - meta.rule_id
        - meta.severity
        - target_entity
        - logic
        - logic.filter_expr（当 type=filter 时）
        - logic.group_by / agg_exprs / having_expr（当 type=aggregate 时）
        
        Args:
            config: 规则配置字典
            file_path: 文件路径（用于错误提示）
        
        Raises:
            ValueError: 配置验证失败
        """
        errors = []
        
        # ========== meta 校验 ==========
        if 'meta' not in config:
            errors.append("Missing 'meta' field")
        else:
            if 'rule_id' not in config['meta']:
                errors.append("Missing 'meta.rule_id'")
            if 'severity' not in config['meta']:
                errors.append("Missing 'meta.severity'")
            else:
                # 验证 severity 取值
                valid_severities = ['P0', 'P1', 'P2', 'INFO']
                if config['meta']['severity'] not in valid_severities:
                    errors.append(f"Invalid severity: {config['meta']['severity']} (must be one of {valid_severities})")
        
        # ========== target_entity 校验 ==========
        if 'target_entity' not in config:
            errors.append("Missing 'target_entity'")
        
        # ========== logic 校验（支持 V2 多类型） ==========
        if 'logic' not in config:
            errors.append("Missing 'logic' field")
        else:
            logic = config['logic']
            logic_type = logic.get('type', 'filter')  # 默认 filter（向后兼容）
            
            if logic_type == 'filter':
                # filter 类型必须有 filter_expr
                if 'filter_expr' not in logic:
                    errors.append("Missing 'logic.filter_expr' (required for type=filter)")
            
            elif logic_type == 'aggregate':
                # aggregate 类型必须有 group_by, agg_exprs, having_expr
                if 'group_by' not in logic:
                    errors.append("Missing 'logic.group_by' (required for type=aggregate)")
                elif not isinstance(logic['group_by'], list):
                    errors.append("'logic.group_by' must be a list of column names")
                
                if 'agg_exprs' not in logic:
                    errors.append("Missing 'logic.agg_exprs' (required for type=aggregate)")
                elif not isinstance(logic['agg_exprs'], list):
                    errors.append("'logic.agg_exprs' must be a list")
                else:
                    for idx, agg in enumerate(logic['agg_exprs']):
                        if 'name' not in agg:
                            errors.append(f"Missing 'name' in logic.agg_exprs[{idx}]")
                        if 'expr' not in agg:
                            errors.append(f"Missing 'expr' in logic.agg_exprs[{idx}]")
                
                if 'having_expr' not in logic:
                    errors.append("Missing 'logic.having_expr' (required for type=aggregate)")
            
            elif logic_type == 'cross_entity':
                # cross_entity 类型：跨表校验（anti_join / compare）
                # 必须有 input.entity_keys、logic.operation、logic.join_on
                
                # 验证 input.entity_keys
                entity_keys = config.get('input', {}).get('entity_keys')
                if not entity_keys:
                    errors.append("Missing 'input.entity_keys' (required for type=cross_entity)")
                else:
                    if 'left' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.left'")
                    if 'right' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.right'")
                
                # 验证 operation
                operation = logic.get('operation')
                if not operation:
                    errors.append("Missing 'logic.operation' (required for type=cross_entity)")
                elif operation not in ('anti_join', 'compare'):
                    errors.append(f"Invalid operation: '{operation}' (must be 'anti_join' or 'compare')")
                
                # 验证 join_on
                join_on = logic.get('join_on')
                if not join_on:
                    errors.append("Missing 'logic.join_on' (required for type=cross_entity)")
                else:
                    if 'left' not in join_on:
                        errors.append("Missing 'logic.join_on.left'")
                    elif not isinstance(join_on['left'], list):
                        errors.append("'logic.join_on.left' must be a list of column names")
                    if 'right' not in join_on:
                        errors.append("Missing 'logic.join_on.right'")
                    elif not isinstance(join_on['right'], list):
                        errors.append("'logic.join_on.right' must be a list of column names")
                
                # compare 需要 assertion
                if operation == 'compare' and 'assertion' not in logic:
                    errors.append("Missing 'logic.assertion' (required for operation=compare)")
                
                # 验证 left_pre_agg / right_pre_agg（可选）
                for side in ('left_pre_agg', 'right_pre_agg'):
                    if side in logic:
                        pre_agg = logic[side]
                        if 'group_by' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.group_by'")
                        if 'agg_exprs' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.agg_exprs'")
                        elif isinstance(pre_agg['agg_exprs'], list):
                            for idx, agg in enumerate(pre_agg['agg_exprs']):
                                if 'name' not in agg:
                                    errors.append(f"Missing 'name' in logic.{side}.agg_exprs[{idx}]")
                                if 'expr' not in agg:
                                    errors.append(f"Missing 'expr' in logic.{side}.agg_exprs[{idx}]")
            
            else:
                errors.append(f"Unknown logic type: '{logic_type}' (must be 'filter', 'aggregate', or 'cross_entity')")
            
            # pre_compute 校验（可选，两种类型都支持）
            if 'pre_compute' in logic:
                if not isinstance(logic['pre_compute'], list):
                    errors.append("'logic.pre_compute' must be a list")
                else:
                    for idx, step in enumerate(logic['pre_compute']):
                        if 'name' not in step:
                            errors.append(f"Missing 'name' in logic.pre_compute[{idx}]")
                        if 'expr' not in step:
                            errors.append(f"Missing 'expr' in logic.pre_compute[{idx}]")
        
        # ========== asset 校验（可选） ==========
        if 'asset' in config:
            asset = config['asset']
            if asset.get('enabled', False) and 'field_mapping' in asset:
                field_map = asset['field_mapping']
                required_mapping_fields = ['asset_id', 'vehicle_id', 'start_ts', 'end_ts']
                for f in required_mapping_fields:
                    if f not in field_map:
                        errors.append(f"Missing 'asset.field_mapping.{f}' (required when asset.enabled=true)")
        
        # ========== 汇总错误 ==========
        if errors:
            error_msg = f"Invalid rule config in {file_path}:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
    
    def _topological_sort(self, rules: List[Dict]) -> List[Dict]:
        """
        拓扑排序（Kahn 算法）
        
        处理规则依赖关系，确保：
        - 被依赖的规则先执行
        - 检测循环依赖
        
        Args:
            rules: 规则列表
        
        Returns:
            排序后的规则列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 构建依赖图和规则字典
        graph = {}
        rule_dict = {}
        
        for rule in rules:
            rule_id = rule['rule_id']
            graph[rule_id] = rule['depends_on']
            rule_dict[rule_id] = rule
        
        # 验证依赖是否存在
        for rule_id, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise ValueError(
                        f"Rule '{rule_id}' depends on '{dep}', but '{dep}' not found. "
                        f"Available rules: {list(graph.keys())}"
                    )
        
        # 计算入度（我依赖多少个规则）
        # depends_on 的含义：我依赖谁（即存在边 dep -> me）
        # 入度 = len(depends_on)
        in_degree = {rule_id: len(deps) for rule_id, deps in graph.items()}
        
        # Kahn 算法：从入度为 0 的节点开始
        queue = [rule_id for rule_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # 取出一个入度为 0 的节点（没有依赖的规则，可以先执行）
            rule_id = queue.pop(0)
            result.append(rule_dict[rule_id])
            
            # 遍历所有规则，找出"依赖于当前规则"的规则
            for other_rule_id, deps in graph.items():
                if rule_id in deps:
                    # 当前规则已完成，依赖它的规则入度减 1
                    in_degree[other_rule_id] -= 1
                    # 如果入度变为 0，说明所有依赖都已满足，可以执行了
                    if in_degree[other_rule_id] == 0:
                        queue.append(other_rule_id)
        
        # 检测循环依赖
        if len(result) != len(rules):
            # 找出参与循环依赖的规则
            unprocessed = [r['rule_id'] for r in rules if r not in result]
            raise ValueError(
                f"Circular dependency detected in rules! "
                f"Unprocessed rules: {unprocessed}. "
                f"Check the 'depends_on' fields."
            )
        
        return result
    
    def get_rule_summary(self, rules: List[Dict]) -> str:
        """
        生成规则摘要（用于日志）
        
        Args:
            rules: 规则列表
        
        Returns:
            摘要字符串
        """
        lines = ["Rule Execution Order:"]
        for idx, rule in enumerate(rules, 1):
            deps_str = f" (depends on: {', '.join(rule['depends_on'])})" if rule['depends_on'] else ""
            logic_type = rule['config'].get('logic', {}).get('type', 'filter')
            lines.append(f"  {idx}. [{rule['severity']}] {rule['rule_id']} (type={logic_type}){deps_str}")
        
        return "\n".join(lines)

```

The edit didn't seem to apply correctly. Let me try again:

```python
"""
orchestration/rule_scanner.py
规则扫描器

职责：
- 扫描规则配置文件（configs/rules/*.yaml）
- 解析规则依赖关系
- 拓扑排序（处理依赖顺序）
- 验证配置完整性（防御性解析）
- 支持 V2 规则格式（filter / aggregate 类型）

运行时机：
- DAG 文件顶层代码（解析时）
- Aggregator 执行时（加载规则元数据）
- 不能有耗时 I/O 操作（只读本地文件）
"""
import os
import glob
import yaml
from typing import List, Dict, Any, Optional


class RuleScanner:
    """
    规则扫描器
    
    使用示例:
    ```python
    # 在 DAG 文件顶层代码中
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    
    # 动态生成 Rule Tasks
    for rule in rules:
        GenericRuleOperator(
            task_id=rule['rule_id'],
            config_path=rule['config_path'],
            ...
        )
    ```
    """
    
    def __init__(self, rules_dir: str = None):
        """
        初始化
        
        Args:
            rules_dir: 规则目录路径（相对于 $AIRFLOW_HOME/plugins/）
        """
        self.rules_dir = rules_dir or 'configs/rules'
    
    def scan_rules(self) -> List[Dict[str, Any]]:
        """
        扫描规则并返回排序后的列表
        
        Returns:
            规则配置列表（已按依赖关系拓扑排序）
            
        示例:
            [
                {
                    'rule_id': 'rule_p0_time_check',
                    'config_path': 'configs/rules/p0_time_check.yaml',
                    'depends_on': [],
                    'severity': 'P0',
                    'config': {...}
                },
                ...
            ]
        
        Raises:
            ValueError: 配置验证失败、循环依赖等
        """
        # 1. 扫描目录
        rule_files = self._scan_rule_files()
        
        if not rule_files:
            raise ValueError(f"No rule files found in {self.rules_dir}")
        
        print(f"📋 Found {len(rule_files)} rule files")
        
        # 2. 加载并验证规则
        rules = []
        for file_path in rule_files:
            try:
                rule_config = self._load_and_validate_rule(file_path)
                if rule_config is None:
                    # 空文件，跳过
                    print(f"⏭️ Skipping empty rule file: {file_path}")
                    continue
                rules.append(rule_config)
            except Exception as e:
                # 配置错误：拒绝加载 DAG
                raise ValueError(f"Failed to load rule from {file_path}: {e}")
        
        # 3. 拓扑排序（处理依赖关系）
        sorted_rules = self._topological_sort(rules)
        
        print(f"✅ Loaded {len(sorted_rules)} rules (topologically sorted)")
        
        return sorted_rules
    
    def get_rule_configs_by_id(self, target_entity: Optional[str] = None) -> Dict[str, Dict]:
        """
        获取规则配置字典（按 rule_id 索引）
        
        用于 Aggregator 在执行时获取规则的完整配置，
        以驱动数据驱动的 alert/asset 处理逻辑。
        
        Args:
            target_entity: 可选，按 target_entity 过滤
        
        Returns:
            {rule_id: full_config_dict}
        """
        rules = self.scan_rules()
        
        result = {}
        for rule in rules:
            if target_entity and rule.get('target_entity') != target_entity:
                continue
            result[rule['rule_id']] = rule['config']
        
        return result
    
    def _scan_rule_files(self) -> List[str]:
        """
        扫描规则目录，返回所有 YAML 文件路径
        
        支持绝对路径和相对路径（相对于 $AIRFLOW_HOME/plugins/）
        
        Returns:
            文件路径列表（已排序）
        """
        if os.path.isabs(self.rules_dir):
            rules_path = self.rules_dir
        else:
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            rules_path = os.path.join(airflow_home, 'plugins', self.rules_dir)
        
        # 扫描 *.yaml 文件
        pattern = os.path.join(rules_path, '*.yaml')
        files = glob.glob(pattern)
        
        # 排序保证稳定性
        return sorted(files)
    
    def _load_and_validate_rule(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        加载并验证单个规则配置
        
        Args:
            file_path: 规则文件路径
        
        Returns:
            规则配置字典
        
        Raises:
            ValueError: 配置验证失败
        """
        # 1. 加载 YAML
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 空文件或空内容：跳过
        if config is None:
            return None
        
        # 2. 防御性验证（Schema Validation）
        self._validate_rule_config(config, file_path)
        
        # 3. 提取关键信息
        # 计算相对路径（相对于 plugins/）
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        plugins_dir = os.path.join(airflow_home, 'plugins')
        relative_path = os.path.relpath(file_path, plugins_dir)
        
        return {
            'rule_id': config['meta']['rule_id'],
            'config_path': relative_path,  # 相对路径
            'depends_on': config.get('depends_on', []),
            'severity': config['meta']['severity'],
            'description': config['meta'].get('description', ''),
            'target_entity': config.get('target_entity'),
            'config': config  # 完整配置（供调试和 Aggregator 使用）
        }
    
    def _validate_rule_config(self, config: Dict, file_path: str) -> None:
        """
        验证规则配置完整性（防御性解析，支持 V2 格式）
        
        V2 格式新增支持：
        - logic.type: filter（默认） | aggregate
        - logic.pre_compute: 预计算步骤（可选）
        - logic.group_by / agg_exprs / having_expr: 聚合型规则必需
        - input.entity_key: 明确的实体读取键（可选）
        - alert.content_template: Jinja 模板（可选）
        - alert.trigger_id_field: 触发 ID 字段名（可选）
        - asset.field_mapping: 资产字段映射（可选）
        
        必需字段：
        - meta
        - meta.rule_id
        - meta.severity
        - target_entity
        - logic
        - logic.filter_expr（当 type=filter 时）
        - logic.group_by / agg_exprs / having_expr（当 type=aggregate 时）
        
        Args:
            config: 规则配置字典
            file_path: 文件路径（用于错误提示）
        
        Raises:
            ValueError: 配置验证失败
        """
        errors = []
        
        # ========== meta 校验 ==========
        if 'meta' not in config:
            errors.append("Missing 'meta' field")
        else:
            if 'rule_id' not in config['meta']:
                errors.append("Missing 'meta.rule_id'")
            if 'severity' not in config['meta']:
                errors.append("Missing 'meta.severity'")
            else:
                # 验证 severity 取值
                valid_severities = ['P0', 'P1', 'P2', 'INFO']
                if config['meta']['severity'] not in valid_severities:
                    errors.append(f"Invalid severity: {config['meta']['severity']} (must be one of {valid_severities})")
        
        # ========== target_entity 校验 ==========
        if 'target_entity' not in config:
            errors.append("Missing 'target_entity'")
        
        # ========== logic 校验（支持 V2 多类型） ==========
        if 'logic' not in config:
            errors.append("Missing 'logic' field")
        else:
            logic = config['logic']
            logic_type = logic.get('type', 'filter')  # 默认 filter（向后兼容）
            
            if logic_type == 'filter':
                # filter 类型必须有 filter_expr
                if 'filter_expr' not in logic:
                    errors.append("Missing 'logic.filter_expr' (required for type=filter)")
            
            elif logic_type == 'aggregate':
                # aggregate 类型必须有 group_by, agg_exprs, having_expr
                if 'group_by' not in logic:
                    errors.append("Missing 'logic.group_by' (required for type=aggregate)")
                elif not isinstance(logic['group_by'], list):
                    errors.append("'logic.group_by' must be a list of column names")
                
                if 'agg_exprs' not in logic:
                    errors.append("Missing 'logic.agg_exprs' (required for type=aggregate)")
                elif not isinstance(logic['agg_exprs'], list):
                    errors.append("'logic.agg_exprs' must be a list")
                else:
                    for idx, agg in enumerate(logic['agg_exprs']):
                        if 'name' not in agg:
                            errors.append(f"Missing 'name' in logic.agg_exprs[{idx}]")
                        if 'expr' not in agg:
                            errors.append(f"Missing 'expr' in logic.agg_exprs[{idx}]")
                
                if 'having_expr' not in logic:
                    errors.append("Missing 'logic.having_expr' (required for type=aggregate)")
            
            elif logic_type == 'cross_entity':
                # cross_entity 类型：跨表校验（anti_join / compare）
                # 必须有 input.entity_keys、logic.operation、logic.join_on
                
                # 验证 input.entity_keys
                entity_keys = config.get('input', {}).get('entity_keys')
                if not entity_keys:
                    errors.append("Missing 'input.entity_keys' (required for type=cross_entity)")
                else:
                    if 'left' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.left'")
                    if 'right' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.right'")
                
                # 验证 operation
                operation = logic.get('operation')
                if not operation:
                    errors.append("Missing 'logic.operation' (required for type=cross_entity)")
                elif operation not in ('anti_join', 'compare'):
                    errors.append(f"Invalid operation: '{operation}' (must be 'anti_join' or 'compare')")
                
                # 验证 join_on
                join_on = logic.get('join_on')
                if not join_on:
                    errors.append("Missing 'logic.join_on' (required for type=cross_entity)")
                else:
                    if 'left' not in join_on:
                        errors.append("Missing 'logic.join_on.left'")
                    elif not isinstance(join_on['left'], list):
                        errors.append("'logic.join_on.left' must be a list of column names")
                    if 'right' not in join_on:
                        errors.append("Missing 'logic.join_on.right'")
                    elif not isinstance(join_on['right'], list):
                        errors.append("'logic.join_on.right' must be a list of column names")
                
                # compare 需要 assertion
                if operation == 'compare' and 'assertion' not in logic:
                    errors.append("Missing 'logic.assertion' (required for operation=compare)")
                
                # 验证 left_pre_agg / right_pre_agg（可选）
                for side in ('left_pre_agg', 'right_pre_agg'):
                    if side in logic:
                        pre_agg = logic[side]
                        if 'group_by' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.group_by'")
                        if 'agg_exprs' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.agg_exprs'")
                        elif isinstance(pre_agg['agg_exprs'], list):
                            for idx, agg in enumerate(pre_agg['agg_exprs']):
                                if 'name' not in agg:
                                    errors.append(f"Missing 'name' in logic.{side}.agg_exprs[{idx}]")
                                if 'expr' not in agg:
                                    errors.append(f"Missing 'expr' in logic.{side}.agg_exprs[{idx}]")
            
            else:
                errors.append(f"Unknown logic type: '{logic_type}' (must be 'filter', 'aggregate', or 'cross_entity')")
            
            # pre_compute 校验（可选，两种类型都支持）
            if 'pre_compute' in logic:
                if not isinstance(logic['pre_compute'], list):
                    errors.append("'logic.pre_compute' must be a list")
                else:
                    for idx, step in enumerate(logic['pre_compute']):
                        if 'name' not in step:
                            errors.append(f"Missing 'name' in logic.pre_compute[{idx}]")
                        if 'expr' not in step:
                            errors.append(f"Missing 'expr' in logic.pre_compute[{idx}]")
        
        # ========== asset 校验（可选） ==========
        if 'asset' in config:
            asset = config['asset']
            if asset.get('enabled', False) and 'field_mapping' in asset:
                field_map = asset['field_mapping']
                required_mapping_fields = ['asset_id', 'vehicle_id', 'start_ts', 'end_ts']
                for f in required_mapping_fields:
                    if f not in field_map:
                        errors.append(f"Missing 'asset.field_mapping.{f}' (required when asset.enabled=true)")
        
        # ========== 汇总错误 ==========
        if errors:
            error_msg = f"Invalid rule config in {file_path}:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
    
    def _topological_sort(self, rules: List[Dict]) -> List[Dict]:
        """
        拓扑排序（Kahn 算法）
        
        处理规则依赖关系，确保：
        - 被依赖的规则先执行
        - 检测循环依赖
        
        Args:
            rules: 规则列表
        
        Returns:
            排序后的规则列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 构建依赖图和规则字典
        graph = {}
        rule_dict = {}
        
        for rule in rules:
            rule_id = rule['rule_id']
            graph[rule_id] = rule['depends_on']
            rule_dict[rule_id] = rule
        
        # 验证依赖是否存在
        for rule_id, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise ValueError(
                        f"Rule '{rule_id}' depends on '{dep}', but '{dep}' not found. "
                        f"Available rules: {list(graph.keys())}"
                    )
        
        # 计算入度（我依赖多少个规则）
        # depends_on 的含义：我依赖谁（即存在边 dep -> me）
        # 入度 = len(depends_on)
        in_degree = {rule_id: len(deps) for rule_id, deps in graph.items()}
        
        # Kahn 算法：从入度为 0 的节点开始
        queue = [rule_id for rule_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # 取出一个入度为 0 的节点（没有依赖的规则，可以先执行）
            rule_id = queue.pop(0)
            result.append(rule_dict[rule_id])
            
            # 遍历所有规则，找出"依赖于当前规则"的规则
            for other_rule_id, deps in graph.items():
                if rule_id in deps:
                    # 当前规则已完成，依赖它的规则入度减 1
                    in_degree[other_rule_id] -= 1
                    # 如果入度变为 0，说明所有依赖都已满足，可以执行了
                    if in_degree[other_rule_id] == 0:
                        queue.append(other_rule_id)
        
        # 检测循环依赖
        if len(result) != len(rules):
            # 找出参与循环依赖的规则
            unprocessed = [r['rule_id'] for r in rules if r not in result]
            raise ValueError(
                f"Circular dependency detected in rules! "
                f"Unprocessed rules: {unprocessed}. "
                f"Check the 'depends_on' fields."
            )
        
        return result
    
    def get_rule_summary(self, rules: List[Dict]) -> str:
        """
        生成规则摘要（用于日志）
        
        Args:
            rules: 规则列表
        
        Returns:
            摘要字符串
        """
        lines = ["Rule Execution Order:"]
        for idx, rule in enumerate(rules, 1):
            deps_str = f" (depends on: {', '.join(rule['depends_on'])})" if rule['depends_on'] else ""
            logic_type = rule['config'].get('logic', {}).get('type', 'filter')
            lines.append(f"  {idx}. [{rule['severity']}] {rule['rule_id']} (type={logic_type}){deps_str}")
        
        return "\n".join(lines)

```

The file still has wrong indentation. Let me use a more precise fix:

Good, indentation is now correct. Let me re-run the verification:

现在让我验证 DAG Factory 的多 Adapter 流水线完整性：

Now let me verify the dag_factory's multi-adapter handling:

所有验证通过。以下是完整总结：

---

## 方案对比总结

### 方案 A：主实体方案（单表合并） — 不推荐 ❌

在 Adapter 阶段将 vehicle_mileage 数据 join 到 Cycle 表，规则仍在单表上操作。

- **致命问题**：Cycle 是 per-cycle 粒度（一天一车多条），mileage 是 per-vehicle-per-day 粒度。join 后 `mileage_distance` 会被重复到每条 cycle 行 → **数据膨胀 + 语义失真**
- 需要 `pre_compute` + `over()` 窗口函数才能做对比，逻辑不直观
- 第二张表、第三张表... Adapter YAML 会越来越臃肿

### 方案 B：多实体方案（cross_entity 跨表校验） — 推荐 ✅

每张表独立 Adapter → 独立实体 → 规则阶段通过 `cross_entity` 类型引用两个实体做对比。

- 通过 `left_pre_agg` 把左表聚合到与右表相同粒度后再 join，**语义清晰**
- 支持 `anti_join`（找缺失行）和 `compare`（找不一致行）
- 先聚合再 join，**内存效率高**
- 新增表只需加 Adapter + Rule YAML，**扩展性好**

---

## 已实现的完整架构

整个多实体跨表校验功能已经端到端实现并验证通过，涉及以下文件：

### 流水线拓扑
```
[Sensor] → [Loader] → [Adapter: cycle]           → [merge_entity_contexts]
                     → [Adapter: vehicle_mileage] ↗
                                                    ↓
                                             [Rule Tasks Group]
                                               ├── 10 × filter rules (单表)
                                               └── 1 × cross_entity rule (跨表)
                                                    ↓
                                             [Aggregator] → [Dispatcher] → [Save Assets]
```

### 修改/新增的文件

| 文件 | 变更 |
|------|------|
| `rule_engine.py` | V3：新增 `_execute_cross_entity()`、`_pre_aggregate()`，支持 `anti_join` 和 `compare` 操作 |
| `rule_scanner.py` | 新增 `cross_entity` 类型验证（entity_keys、operation、join_on、assertion、pre_agg），修复空文件跳过和缩进 bug |
| `dag_factory.py` | 已支持多 Adapter 并行 + `merge_entity_contexts` 合并任务 + `_collect_pipeline_entities` 收集多实体 |
| `daily_cycle_etl.yaml` | 新增 `raw_vehicle_mileage` 提取 + `adapters:` 双 Adapter 声明 |
| `vehicle_mileage_adapter.yaml` | 新增：VehicleMileage 实体适配器 |
| `p0_mileage_cross_check.yaml` | 新增：跨表里程校验规则（compare + left_pre_agg） |

### 跨表校验规则示例

`p0_mileage_cross_check.yaml` 的核心配置：

```1:56:plugins/configs/rules/p0_mileage_cross_check.yaml
# p0_mileage_cross_check.yaml
# 跨表里程校验规则
# ...
input:
  entity_keys:
    left: "entity_cycle"
    right: "entity_vehicle_mileage"

logic:
  type: cross_entity
  operation: compare
  left_pre_agg:
    group_by: ["vehicle_id", "shift_date"]
    agg_exprs:
      - name: "total_mileage"
        expr: "col('mileage').sum()"
  join_on:
    left: ["vehicle_id", "shift_date"]
    right: ["vehicle_id", "shift_date"]
  assertion: "col('total_mileage') != col('mileage_distance')"
```

**逻辑说明**：
1. **左表**（entity_cycle）先按 `(vehicle_id, shift_date)` 聚合，求 `SUM(mileage)` → `total_mileage`
2. 与**右表**（entity_vehicle_mileage）按 `(vehicle_id, shift_date)` LEFT JOIN
3. 过滤出 `total_mileage ≠ mileage_distance` 的行 → 即里程不一致的车辆

这正好满足你的需求：**对比主表（Cycle）每辆车每天的里程数与第二张表（vehicle_daily_mileage）的里程数是否相等**。

```plaintext
[Sensor] → [Loader] → [Adapter: cycle]           → [merge_entity_contexts]
                     → [Adapter: vehicle_mileage] ↗
                                                    ↓
                                             [Rule Tasks Group]
                                               ├── 10 × filter rules (单表)
                                               └── 1 × cross_entity rule (跨表)
                                                    ↓
                                             [Aggregator] → [Dispatcher] → [Save Assets]
```

```yaml
# p0_mileage_cross_check.yaml
# 跨表里程校验规则
# ...
input:
  entity_keys:
    left: "entity_cycle"
    right: "entity_vehicle_mileage"

logic:
  type: cross_entity
  operation: compare
  left_pre_agg:
    group_by: ["vehicle_id", "shift_date"]
    agg_exprs:
      - name: "total_mileage"
        expr: "col('mileage').sum()"
  join_on:
    left: ["vehicle_id", "shift_date"]
    right: ["vehicle_id", "shift_date"]
  assertion: "col('total_mileage') != col('mileage_distance')"
```

---

**User**

你修改的代码已经测试通过，下一步你觉得应该干什么呢

---

**Cursor**

测试通过了很好。让我站在整体项目角度梳理一下当前状态和下一步方向。

## 当前已完成的能力

| 能力 | 状态 |
|------|:----:|
| 多数据源并行加载（MySQL） | ✅ |
| 单实体 Adapter（Join + 字段映射） | ✅ |
| 多实体并行 Adapter + Context 合并 | ✅ |
| 规则引擎 V2（filter / aggregate / cross_entity） | ✅ |
| 跨表校验（anti_join / compare + pre_agg） | ✅ |
| 数据驱动的 Aggregator（方案B） | ✅ |
| Jinja 告警模板 | ✅ |
| DAG Factory 动态生成 | ✅ |
| P1 资产打包 + Dataset 事件驱动 | ✅ |

## 尚未解决的问题（按优先级排列）

我认为接下来有 **3 个方向**值得考虑，它们解决的问题层次不同：

### 方向 1：结果持久化与可观测性

**问题**：当前每次 DAG 跑完后，规则结果只存在于 Parquet 文件和邮件通知中。没有一个集中的地方可以回答"上周哪些规则命中最多？趋势是怎样的？哪些车辆反复出现异常？"

**做法**：在 Aggregator 或 Dispatcher 之后增加一个 `ReportWriter` 步骤，将每次运行的规则结果摘要写入一张 MySQL 表（如 `governance_run_summary`），字段包括 `batch_id`、`rule_id`、`hit_count`、`run_date` 等。这样你可以用任何 BI 工具（Grafana / Metabase）接这张表做趋势看板。

**改动量**：小。新增一个 Operator + 一张表 + DAG Factory 加一个 Task。

### 方向 2：漏斗式数据提取（Loader 升级）

**问题**：我们之前详细讨论过——当前 Loader 的多个 extraction 是独立并行的，无法做到"根据 A 的结果去查 B"。你提到的 InfluxDB 时序数据和 Map API JSON 数据需要这种能力。

**做法**：extraction 之间支持 `depends_on` + `input_ref`，Loader 按拓扑排序执行，后续 extraction 的查询模板可以引用前一步的 DataFrame。同时实现 InfluxDB 和 HTTP 数据源 extractor。

**改动量**：中。改 Loader + 新增 Extractor 类 + 升级 Source YAML Schema。

### 方向 3：规则结果的闭环处理（修复/回写）

**问题**：当前发现异常后只能告警，不能对原数据进行修复或标记。你最初的目标中提到"对于错误的数据我要对原数据进行修改"，这个能力在主治理流水线中还是缺失的。

**做法**：在规则 YAML 中增加 `action` 配置（如 `action: mark_invalid` 或 `action: write_back`），在 Aggregator 之后新增一个 `RemediationOperator`，根据 action 配置执行回写操作。

**改动量**：中到大。涉及新 Operator + 回写安全机制 + 审计日志。

---

## 我的建议

**先做方向 1（结果持久化）**，原因：

1. **投入产出比最高** — 改动小，但立刻让你能看到历史趋势，这对一个"数据质量测试平台"来说是基础设施级的能力
2. **为方向 2 和 3 打基础** — 有了结果存储，漏斗式提取和回写的效果才能被追踪和验证
3. **演示价值** — 如果你需要向其他人展示这个平台的价值，一张 Grafana 看板比 Airflow 日志有说服力得多

你觉得哪个方向更优先？还是你有其他想法？

---

**User**

同意你的意见，先做方向1 是不是需要先设计表结构

---

**Cursor**

对，表结构设计应该先行。让我从"这些表需要回答什么问题"出发来设计。

## 需要回答的核心问题

先明确持久化的数据要支撑哪些分析场景：

| 分析场景 | 示例问题 |
|---------|---------|
| **趋势监控** | 过去 30 天，P0 告警数量是上升还是下降？ |
| **规则热力图** | 哪条规则命中最频繁？是否需要调整阈值？ |
| **车辆画像** | 哪些车辆反复出现在异常中？ |
| **执行监控** | 哪条规则执行最慢？是否有性能瓶颈？ |
| **运行概览** | 今天这批数据整体质量如何？和昨天比怎样？ |
| **失败追溯** | 某条规则上周三为什么 FAILED？错误信息是什么？ |

## 表结构方案

我建议用 **两张表**：一张运行级别、一张规则级别。不需要第三张告警明细表——因为 `trigger_cycle_ids` 可以作为 JSON 存在规则结果表中，避免过早范式化。

### 表 1：`governance_run_summary` — 批次级概览

每次 DAG 运行写一条记录，用于宏观仪表盘。

```sql
CREATE TABLE governance_run_summary (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- 身份标识
    batch_id        VARCHAR(128) NOT NULL,
    dag_id          VARCHAR(128) NOT NULL,
    run_date        DATE NOT NULL,
    
    -- 实体统计
    entity_name     VARCHAR(64),           -- 主实体名 (如 'Cycle')
    total_rows      INT DEFAULT 0,         -- 实体总行数
    
    -- 规则执行统计
    total_rules     INT DEFAULT 0,         -- 总规则数
    success_rules   INT DEFAULT 0,         -- 成功规则数
    failed_rules    INT DEFAULT 0,         -- 失败规则数
    skipped_rules   INT DEFAULT 0,         -- 跳过规则数
    total_hits      INT DEFAULT 0,         -- 总命中行数
    
    -- 信号统计
    alert_count     INT DEFAULT 0,         -- 告警数
    asset_count     INT DEFAULT 0,         -- 资产数
    
    -- 时间
    started_at      DATETIME,              -- DAG 开始时间
    completed_at    DATETIME,              -- DAG 完成时间
    duration_sec    FLOAT DEFAULT 0,       -- 总耗时(秒)
    
    -- 状态
    status          VARCHAR(16) DEFAULT 'SUCCESS',  -- SUCCESS / PARTIAL_FAILURE / FAILED
    
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- 索引
    UNIQUE KEY uk_batch (batch_id),
    INDEX idx_run_date (run_date),
    INDEX idx_dag_id (dag_id, run_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**示例数据**：

| batch_id | dag_id | run_date | total_rules | total_hits | alert_count | status |
|----------|--------|----------|:-----------:|:----------:|:-----------:|--------|
| BATCH_20260306 | gov_daily_cycle_etl | 2026-03-06 | 12 | 47 | 5 | SUCCESS |
| BATCH_20260305 | gov_daily_cycle_etl | 2026-03-05 | 12 | 23 | 3 | SUCCESS |

### 表 2：`governance_rule_results` — 规则级明细

每次运行中每条规则写一条记录，用于趋势分析和下钻。

```sql
CREATE TABLE governance_rule_results (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- 关联
    batch_id        VARCHAR(128) NOT NULL,
    run_date        DATE NOT NULL,
    
    -- 规则信息
    rule_id         VARCHAR(128) NOT NULL,
    severity        VARCHAR(8) NOT NULL,       -- P0 / P1 / P2 / INFO
    logic_type      VARCHAR(32) DEFAULT 'filter', -- filter / aggregate / cross_entity
    description     VARCHAR(512),              -- 规则描述（来自 YAML）
    
    -- 执行结果
    status          VARCHAR(16) NOT NULL,      -- SUCCESS / FAILED / SKIPPED
    hit_count       INT DEFAULT 0,             -- 命中行数
    execution_time_sec FLOAT DEFAULT 0,        -- 规则执行耗时(秒)
    error_message   TEXT,                      -- 失败原因（如果 FAILED）
    
    -- 告警信息（命中时填充）
    alert_title     VARCHAR(256),              -- 告警标题
    trigger_ids     JSON,                      -- 触发的 ID 列表 (如 cycle_ids)
    
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- 索引
    UNIQUE KEY uk_batch_rule (batch_id, rule_id),
    INDEX idx_run_date (run_date),
    INDEX idx_rule_id (rule_id, run_date),
    INDEX idx_severity (severity, run_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**示例数据**：

| batch_id | rule_id | severity | status | hit_count | execution_time_sec |
|----------|---------|:--------:|--------|:---------:|:------------------:|
| BATCH_20260306 | rule_p0_time_check | P0 | SUCCESS | 0 | 0.12 |
| BATCH_20260306 | rule_p0_box_count | P0 | SUCCESS | 15 | 0.35 |
| BATCH_20260306 | rule_p0_mileage_cross_check | P0 | SUCCESS | 3 | 0.48 |
| BATCH_20260306 | rule_p1_twin_lift | P1 | SUCCESS | 29 | 0.18 |
| BATCH_20260306 | rule_p2_timeout | P2 | SUCCESS | 8 | 0.22 |

## 设计决策点

几个需要确认的问题：

**1. 写入时机**：在 pipeline 的哪个阶段写入？

我建议在 **Aggregator 之后、Dispatcher 之前**新增一个 `ReportWriterOperator`。这样它能拿到完整的 `GovernanceContext`（含所有规则结果和告警），流水线变为：

```
... → Aggregator → ReportWriter → Dispatcher → ...
```

**2. 写入哪个数据库**：用现有的 `qa_mysql_conn` 还是单独的连接？

建议复用 `qa_mysql_conn`（和 asset_packing 用同一个库），在 Source YAML 中配置 `conn_id`。

**3. `trigger_ids` 存储策略**：JSON 列 vs 单独表？

建议用 **JSON 列**。原因：
- 大多数查询只需要 `hit_count`，不需要逐条追溯
- 需要追溯时 `JSON_EXTRACT` 足够
- 避免过早建一张可能有几万行的明细表

**4. 这两张表放在 Source YAML 的哪里配置？**

```yaml
# daily_cycle_etl.yaml 中新增
report:
  enabled: true
  conn_id: "qa_mysql_conn"
```

或者也可以不放在 Source YAML 中——让 ReportWriter 读环境变量，所有 pipeline 共享同一个报告库。你倾向哪种？

请确认这些设计是否符合你的预期，然后我开始实现。

---

**User**

关于决策点的回复：1.同意你的意见 2.用现有的qa_mysql_conn 3.用JSON列  3.放在Source YAML中配置

---

**Cursor**

好，所有决策点已确认。让我先读取相关文件，然后开始实现。

Now let me also read the existing YAML config and the dispatcher to understand how they connect.

Good, I now have a complete picture. Let me implement all the changes.

Now let me implement all the changes. Starting with the schema, SQL migration, and the operator in parallel.

Now create the SQL migration script and the ReportWriterOperator.

Now update `dag_factory.py` to insert the ReportWriter between Aggregator and Dispatcher.

Now update the pipeline comment and the logic between Aggregator and Dispatcher.

Now replace the section between Aggregator and Dispatcher to insert the ReportWriter.

Update the step number for Save Assets (was Step 7, now Step 8).

Now update the `daily_cycle_etl.yaml` to add the report config.

Now let me verify the changes by checking for lint errors and running tests.

No test files for the main pipeline. Let me verify all modified files look correct by reading the final versions.

All the changes look correct. Let me also verify the `report_writer.py` syntax by running a quick Python syntax check.

所有语法检查通过。以下是本次实现的完整总结：

---

## 实现总结：结果持久化（Result Persistence）

### 修改/新增的文件

**1. `plugins/schemas/source_config_schema.py`** — 新增 `ReportConfig` 模型

```128:131:plugins/schemas/source_config_schema.py
class ReportConfig(BaseModel):
    """结果持久化配置"""
    enabled: bool = True
    conn_id: str = Field(default="qa_mysql_conn", description="数据库连接ID")
```

在 `SourceYAMLConfig` 中添加了 `report` 字段（默认启用）。

**2. `plugins/migrations/001_governance_report_tables.sql`** — 建表 DDL

两张表：
- `governance_run_summary`：批次级运行汇总（batch_id 唯一键），记录规则执行统计、信号统计、耗时、状态
- `governance_rule_results`：规则级执行明细（batch_id + rule_id 唯一键），记录 severity、logic_type、hit_count、trigger_ids（JSON）

**3. `plugins/operators/report_writer.py`** — 新增 `ReportWriterOperator`

核心逻辑：
- 从 `GovernanceContext` 读取所有 `rule_outputs` 和 `alerts`
- 通过 `RuleScanner` 加载规则元数据（severity、logic_type、description）
- 构建 alert 索引，关联 trigger_ids 到每条规则
- 使用 `MySqlHook` 执行幂等 UPSERT（`ON DUPLICATE KEY UPDATE`）
- `fail_on_error=False`（默认），写入失败不中断 pipeline

**4. `plugins/orchestration/dag_factory.py`** — 流水线调整

新流水线：
```
... → Aggregator → [ReportWriter] → Dispatcher → ...
```

当 `report.enabled=true` 时，ReportWriter 插入在 Aggregator 和 Dispatcher 之间；Dispatcher 的 `upstream_task_id` 动态切换为 `"report_writer"`。向后兼容：当 `report.enabled=false` 或未配置时，跳过 ReportWriter。

**5. `plugins/configs/sources/daily_cycle_etl.yaml`** — 添加 report 配置

```154:157:plugins/configs/sources/daily_cycle_etl.yaml
report:
  enabled: true
  conn_id: "qa_mysql_conn"
```

### 部署步骤

在部署前，需要在 `qa_mysql_conn` 对应的数据库中执行建表脚本：

```bash
mysql -h <host> -u <user> -p <database> < plugins/migrations/001_governance_report_tables.sql
```

之后 DAG 会自动在每次运行时将结果写入这两张表。

---

**User**

目前方向一的改造已经完全实施和测试完成了，接下来我想进入方向二的讨论和实施

---

**Cursor**

让我先回顾一下之前讨论中提到的各个方向，确认"方向二"的具体内容。

Let me read more details about the three directions that were previously proposed.

好，方向二是**漏斗式数据提取（Loader 升级）**。这是一个涉及 Loader 架构核心改造的中等规模任务，有多种实现路径和需要权衡的设计决策，我建议我们先讨论方案再动手。

好，那我们直接在这里讨论方案。让我先全面审视当前 Loader 的现状，然后给出设计方案。

Good, I now have the full picture. Let me系统地分析方向二的设计方案。

---

## 方向二：漏斗式数据提取（Loader 升级）

### 当前问题

当前 `UniversalLoaderOperator` 的核心循环是扁平的：

```75:78:plugins/operators/loader.py
        for task in extractions:
            self._execute_extraction_task(task, source_meta, ctx, context)
```

每个 extraction 完全独立，SQL 模板只能引用 Airflow 宏（`{{ ds }}`），无法引用其他 extraction 的结果。你之前描述的典型场景是：

```
MySQL(A) → 根据A的vehicle_id和时间窗口去查 InfluxDB(B) → 根据B的GPS坐标去查 Map API(C)
```

### 需要解决的四个子问题

| # | 子问题 | 说明 |
|---|--------|------|
| 1 | **执行依赖** | extraction 之间声明 `depends_on`，Loader 按拓扑排序执行 |
| 2 | **数据传递** | 下游 extraction 的查询模板能引用上游的 DataFrame |
| 3 | **迭代模式** | 支持 `batch`（WHERE IN 批量）和 `per_row`（逐行查询）两种模式 |
| 4 | **多源 Extractor** | 统一 Extractor 接口，实现 MySQL / InfluxDB / HTTP API |

### 设计方案

#### 1. YAML Schema 扩展

`ExtractionConfig` 新增三个字段：

```yaml
extractions:
  # Step 1: 普通提取（无依赖）
  - id: "raw_cycles"
    source_type: "mysql"
    conn_id: "datalog_mysql_conn"
    query: |
      SELECT cycle_id, vehicle_id, start_time, end_time
      FROM cycle_section_summary
      WHERE DATE(shift_date) = '{{ ds }}'
    output_key: "raw_cycles"

  # Step 2: 依赖 Step 1，使用其结果
  - id: "raw_telemetry"
    source_type: "influxdb"
    conn_id: "influxdb_conn"
    depends_on: ["raw_cycles"]          # 必须在 raw_cycles 之后执行
    input_ref: "raw_cycles"             # 引用上游 DataFrame
    iteration_mode: "per_row"           # 逐行迭代上游数据
    query: |
      from(bucket: "vehicle_telemetry")
        |> range(start: {{ row.start_time }}, stop: {{ row.end_time }})
        |> filter(fn: (r) => r.vehicle_id == "{{ row.vehicle_id }}")
    output_key: "raw_telemetry"

  # Step 3: 依赖 Step 2，批量模式
  - id: "raw_routes"
    source_type: "http_api"
    conn_id: "map_api_conn"
    depends_on: ["raw_telemetry"]
    input_ref: "raw_telemetry"
    iteration_mode: "per_row"
    request:
      method: "GET"
      url_template: "https://api.map.com/v1/route"
      params:
        lat: "{{ row.latitude }}"
        lng: "{{ row.longitude }}"
    output_key: "raw_routes"
```

两种迭代模式：

| 模式 | 模板变量 | 适用场景 | 示例 |
|------|---------|---------|------|
| `batch`（默认） | `{{ ref_ids }}`、`{{ ref_values('col') }}` | SQL WHERE IN | `WHERE vehicle_id IN ({{ ref_values('vehicle_id') }})` |
| `per_row` | `{{ row.col_name }}` | API 调用、时序查询 | `range(start: {{ row.start_time }})` |

#### 2. 执行引擎改造

Loader 内部从简单循环升级为**拓扑排序执行**：

```
extractions: [A, B, C, D]
depends_on:  B→A, C→A, D→[B,C]

拓扑层级:
  Layer 0: [A]          ← 先执行
  Layer 1: [B, C]       ← A 完成后执行（可并行）
  Layer 2: [D]          ← B,C 都完成后执行
```

无 `depends_on` 的 extraction 保持原有行为（Layer 0，向后兼容）。

#### 3. Extractor 接口

统一抽象：

```python
class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, config: dict, context: dict,
                upstream_df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
        ...

class MySQLExtractor(BaseExtractor): ...
class InfluxDBExtractor(BaseExtractor): ...
class HTTPAPIExtractor(BaseExtractor): ...
```

#### 4. 分阶段实施

| 阶段 | 内容 | 改动 |
|------|------|------|
| **Phase 1** | `depends_on` + `input_ref` + `batch` 模式 + 拓扑排序 | Loader + Schema |
| **Phase 2** | `per_row` 迭代模式 | Loader |
| **Phase 3** | InfluxDB Extractor | 新文件 |
| **Phase 4** | HTTP API Extractor | 新文件 |

---

### 需要你确认的决策点

1. **实施范围**：先做 Phase 1（`depends_on` + `batch` 模式，仅 MySQL），还是四个阶段一起做？

2. **是否有具体的漏斗式业务场景**需要立即实现？比如你现在是否已经有 InfluxDB 数据源需要接入，还是先把框架搭好？

3. **per_row 的并发控制**：逐行迭代上游数据时，如果上游有 1000 行，意味着 1000 次 API 调用。你倾向于串行执行（简单安全）还是支持并发（`max_concurrent: 10`）？

4. **Extractor 放置位置**：是放在 `plugins/operators/extractors/` 目录下，还是放在 `plugins/infra/extractors/` 下？

---

**User**

需要你先评估这种业务上的需求和数据治理测试平台的设计在业内是否常见

---

**Cursor**

这是一个值得深入讨论的架构问题。让我从业界实践出发做一个系统评估。

---

## 一、数据质量平台的业界格局

先看主流工具的定位和它们各自"管到哪一层"：

| 工具 | 定位 | 是否管数据提取 | 是否支持跨源关联 | 漏斗式提取 |
|------|------|:---:|:---:|:---:|
| **Great Expectations** | DataFrame 级校验 | 不管（你自己加载） | 不支持 | 不支持 |
| **Soda Core** | SQL 级检查 | 不管（直接连数据库） | 有限（check 级别） | 不支持 |
| **dbt tests** | 模型级测试 | 不管（数据已在仓库中） | SQL JOIN 即可 | 不适用 |
| **Apache Griffin** | Hadoop/Spark DQ | 不管 | Spark SQL | 不支持 |
| **Monte Carlo** | 数据可观测性 | 不管（元数据采集） | 异常关联 | 不适用 |
| **Deequ (Amazon)** | Spark 统计检查 | 不管 | 不支持 | 不支持 |

**关键发现：业界主流数据质量工具几乎都不管数据提取。** 它们的假设是：数据已经在某个地方了（DataFrame、数据仓库、数据湖），你只需要对它做校验。

## 二、你的平台的实际定位

你的平台不是一个纯粹的"数据质量工具"，而是一个 **数据治理流水线（Data Governance Pipeline）**，它实际上横跨了三个层次：

```
┌──────────────────────────────────────────────────────┐
│  Layer 1: 数据集成层    Loader + Adapter             │  ← ETL 工具的领域
├──────────────────────────────────────────────────────┤
│  Layer 2: 质量检测层    Rules + Aggregator           │  ← DQ 工具的领域
├──────────────────────────────────────────────────────┤
│  Layer 3: 响应动作层    Dispatcher + AssetPacking    │  ← 告警/运维工具的领域
└──────────────────────────────────────────────────────┘
```

大多数 DQ 工具只做 Layer 2。你做了全部三层。这不是缺点——**对于车队 IoT 这种垂直场景，这种一体化设计反而是合理的**，因为你的数据源高度异构（MySQL 关系型 + InfluxDB 时序 + Map API），不可能期望所有数据都提前 ETL 到一个统一的仓库里。

## 三、漏斗式提取在业界的位置

**漏斗式提取本身是一个典型的 ETL/数据集成需求**，在以下场景中非常常见：

| 场景 | 漏斗模式 | 举例 |
|------|----------|------|
| **IoT 数据分析** | 关系库 → 时序库 → 地图API | 你的场景 |
| **反欺诈** | 交易库 → 用户画像 → 外部黑名单API | 银行 |
| **供应链** | 订单库 → 物流追踪API → 天气API | 电商 |
| **安全审计** | 日志库 → CMDB → 威胁情报API | 安全 |

支持这种模式的工具：

- **Apache NiFi**：原生支持 Processor 链式数据流
- **Apache Airflow**：通过多个 Task 实现（但每个 Task 是独立的 Operator，数据通过 XCom 或文件传递）
- **Prefect / Dagster**：通过函数式编排实现
- **自定义 Spark Pipeline**：代码级链式操作

**但注意：这些都不是 DQ 工具，而是数据编排工具。**

## 四、核心评估

### 你的设计做对了什么

1. **YAML 驱动 + DAG 动态生成** — 这和 dbt、Soda 的设计哲学一致，是业界公认的好做法
2. **规则引擎支持多种逻辑类型（filter/aggregate/cross_entity）** — 表达力已经超过 Soda/GE 的声明式规则
3. **Airflow 作为编排器** — 业界标准选择
4. **P0/P1/P2 分级响应** — 成熟的告警分级实践
5. **结果持久化** — 方向一做完后，已经具备了可观测性基础

### 漏斗式提取的定位问题

这里是核心判断：**漏斗式提取是一个有效需求，但它属于 Layer 1（数据集成层）的能力扩展，不属于 Layer 2（质量检测层）的核心关注点。**

业界的通常做法是**两种路径**：

**路径 A：平台内一体化**
```
Loader（漏斗式提取） → Adapter → Rules → ...
```
优点：单一 pipeline 端到端，配置统一
缺点：Loader 变得很重，职责不单一

**路径 B：分层解耦**
```
独立 ETL DAG（漏斗式提取 → 写入中间表/文件）
     ↓ (Dataset 事件触发)
治理 DAG（Loader 简单读取 → Rules → ...）
```
优点：关注点分离，Loader 保持轻量
缺点：需要维护两套 DAG，中间存储开销

## 五、我的建议

对于你的车队 IoT 场景，**路径 A（平台内一体化）更合适**，理由是：

1. 你的 InfluxDB 查询的时间窗口来自 MySQL 里的 cycle 数据——**提取逻辑和治理逻辑是紧密耦合的**，拆成两个 DAG 反而会增加复杂度和维护成本
2. 你的 DAG Factory 已经能动态生成 pipeline，加漏斗式提取只是增强 Loader 的能力
3. 这种一体化设计在 **垂直领域的自建平台** 中其实很常见（只是不会开源出来），尤其是 IoT、自动驾驶、金融等对数据实时性要求高的行业

但要注意：**不需要也不应该让 Loader 变成通用 ETL 引擎。** 只支持你实际业务需要的模式就够了。

---

### 你需要考虑的问题

在决定实施范围之前：

1. **你目前是否有一个具体的漏斗式提取场景需要上线？** 比如你现在是不是已经有 InfluxDB 数据需要接入？还是先把框架能力搭好备用？

2. **如果当前没有 InfluxDB 场景，是否有纯 MySQL 的漏斗式需求？** 比如"先查 A 表拿到 vehicle_id 列表，再去 B 表查这些 vehicle_id 的详细数据"——这种 MySQL → MySQL 的链式场景可能更快产生价值。

3. **你倾向路径 A 还是路径 B？**

这些问题的答案会直接决定实施的范围和优先级。

---

**User**

我倾向于路径A，并且对于你所说的漏斗式场景，我有纯mysql的漏斗式需求，也有我之前向你描述的那种接入influx的场景

---

**Cursor**

好，确认路径 A + 纯 MySQL 漏斗 + InfluxDB 场景。让我开始实施。

Step 1: Update schema. Step 2-4: Create extractors. Let me do all of these in parallel.

Now create the base extractor, MySQL extractor, and InfluxDB extractor.

Now the core piece — refactor the Loader with topological sort, template rendering, and iteration modes.

Now let me verify all files have correct syntax.

所有代码语法检查通过，零 lint 错误。以下是完整的改动总结：

---

## 实现总结：漏斗式数据提取（Loader V2）

### 架构变化

```
Before:  Loader 内部 → for task in extractions → 各自独立执行
After:   Loader 内部 → 拓扑排序 → 按依赖链顺序执行 → 上游数据注入下游模板
```

### 新增文件（4 个）

**`plugins/infra/extractors/base.py`** — 抽象基类 + `UpstreamRef` 模板助手

- `BaseExtractor.extract_single()` — 子类只需实现单条查询执行
- `UpstreamRef` — batch 模式下封装上游 DataFrame，提供 Jinja 模板方法：

| 模板表达式 | 输出示例 | 用途 |
|-----------|---------|------|
| `{{ ref.values('vehicle_id') }}` | `'V001','V002','V003'` | SQL WHERE IN |
| `{{ ref.values('id', quote=False) }}` | `1,2,3` | 数值型 IN |
| `{{ ref.count }}` | `42` | 条件判断 |
| `{{ ref.min('shift_date') }}` | `'2026-01-01'` | 时间范围 |
| `{{ ref.max('shift_date') }}` | `'2026-01-31'` | 时间范围 |

**`plugins/infra/extractors/mysql_extractor.py`** — MySQL 提取器（从原 Loader 重构）

**`plugins/infra/extractors/influxdb_extractor.py`** — InfluxDB 2.x 提取器

- 使用 `influxdb-client` 官方 SDK
- 从 Airflow Connection 读取 url / org / token
- 自动清理 InfluxDB 内部元数据列（`result`, `table`, `_start`, `_stop`）

**`plugins/infra/extractors/__init__.py`** — 提取器注册中心

- `get_extractor(source_type, conn_id, logger)` 工厂方法

### 修改文件（2 个）

**`plugins/operators/loader.py`** — 核心重构

- **拓扑排序**：`_topological_sort()` 按 `depends_on` 关系排序 extraction，检测环路
- **上游解析**：`_resolve_upstream()` 从已完成的 extraction 或 Context 中获取上游 DataFrame
- **batch 模式**：整体注入 `ref`，一次查询拿到结果
- **per_row 模式**：逐行迭代，每行渲染查询并执行，结果拼接
- **空上游保护**：上游 0 行时自动跳过，不执行无效查询
- **完全向后兼容**：无 `depends_on` 的 extraction 行为和 V1 完全一致

**`plugins/schemas/source_config_schema.py`** — Schema 扩展

- `depends_on: Optional[List[str]]` — 依赖声明
- `input_ref: Optional[str]` — 上游数据引用
- `iteration_mode: 'batch' | 'per_row'` — 迭代模式
- `source_type` 新增 `'http_api'` 选项

### YAML 使用示例

**纯 MySQL 漏斗：**

```yaml
extractions:
  - id: "target_vehicles"
    source_type: "mysql"
    conn_id: "datalog_mysql_conn"
    query: |
      SELECT DISTINCT vehicle_id, shift_date
      FROM cycle_section_summary
      WHERE DATE(shift_date) = '{{ ds }}' AND site_type = 'PORT_A'
    output_key: "raw_target_vehicles"

  - id: "vehicle_details"
    source_type: "mysql"
    conn_id: "another_mysql_conn"
    depends_on: ["target_vehicles"]
    input_ref: "raw_target_vehicles"
    iteration_mode: "batch"
    query: |
      SELECT * FROM vehicle_registry
      WHERE vehicle_id IN ({{ ref.values('vehicle_id') }})
    output_key: "raw_vehicle_details"
```

**MySQL → InfluxDB 漏斗：**

```yaml
extractions:
  - id: "raw_cycles"
    source_type: "mysql"
    conn_id: "datalog_mysql_conn"
    query: |
      SELECT cycle_id, vehicle_id, start_time, end_time
      FROM cycle_section_summary
      WHERE DATE(shift_date) = '{{ ds }}'
    output_key: "raw_cycles"

  - id: "raw_telemetry"
    source_type: "influxdb"
    conn_id: "influxdb_conn"
    depends_on: ["raw_cycles"]
    input_ref: "raw_cycles"
    iteration_mode: "per_row"
    query: |
      from(bucket: "vehicle_telemetry")
        |> range(start: {{ row.start_time }}, stop: {{ row.end_time }})
        |> filter(fn: (r) => r.vehicle_id == "{{ row.vehicle_id }}")
    output_key: "raw_telemetry"
```

### 部署注意

1. 现有 `daily_cycle_etl.yaml` **不需要任何修改**——无 `depends_on` 的 extraction 完全向后兼容
2. 如需使用 InfluxDB，需安装依赖：`pip install influxdb-client`
3. InfluxDB Airflow Connection 配置：host=URL, login=org, password=token

---

**User**

请你查看archive/dag中的DatalogLogicCheck_worker_dag 它是一个漏斗式提取数据的场景，它所实现的业务逻辑，目前平台是否可以支持

---

**Cursor**

这是一个很好的对照基准。让我逐步分析这个 DAG 的业务逻辑和当前平台的支持程度。

---

## 业务逻辑拆解

这个 DAG 是一个典型的四步漏斗式验证流水线：

```
Step 1: MySQL → 提取+Unpivot+筛选 → claims (vehicle_id, cycle_id, unix_timestamp)
   ↓
Step 2: InfluxDB → 逐行查物理层真相 → 追加 (actual_x, actual_y, actual_speed)
   ↓
Step 3: Map API → 按车分组批量查语义层 → 追加 (map_road_type)
   ↓
Step 4: Pandera → 验证规则 → 写入结果表
```

## 逐步能力对照

### Step 1: MySQL 提取 + 复杂转换

**业务逻辑：**
1. 从 `subtarget_vehicle_cycle` 表按 shift_date + vehicle_ids 查询
2. Unpivot 宽表（8 对 SUBTASK_TYPE / ALIGN_STA_TIME 列）→ 长表
3. 字符串时间 → Unix Timestamp 转换
4. 过滤 `subtask_type == "To QC"` 且有时间戳
5. 自定义"连续 QC 筛选"逻辑（`_apply_consecutive_qc_filter`）

**平台支持度：**

| 子步骤 | 支持 | 说明 |
|--------|:---:|------|
| SQL 查询 | ✅ | Loader 直接支持 |
| Unpivot 宽→长 | ❌ | 超出 Adapter YAML 的 `fields` 映射能力，需要 `pl.melt()` |
| 时间解析/转换 | ❌ | Adapter 的 `source_expr` 能做简单表达式，但 `.str.strptime().dt.epoch()` 链太复杂 |
| 连续 QC 筛选 | ❌ | 纯自定义 Python 逻辑，无法用 YAML 表达 |

**结论：提取部分 ✅，但转换逻辑远超当前 Adapter 的表达力。** 当前 Adapter 适合做简单的列映射/重命名，无法做 unpivot + 复杂过滤。

### Step 2: InfluxDB 逐行查询

**业务逻辑：**
```python
for row in df.iter_rows(named=True):
    query_influxdb(vehicle_id=row["vehicle_id"],
                   time_range=[unix_timestamp - 1s, unix_timestamp + 1s])
    → actual_x, actual_y, actual_speed
```

**平台支持度：**

| 子步骤 | 支持 | 说明 |
|--------|:---:|------|
| depends_on + input_ref | ✅ | Loader V2 新增 |
| per_row 迭代 | ✅ | Loader V2 新增 |
| InfluxDB Flux 查询 | ✅ | 新 InfluxDBExtractor |
| 时间窗口计算 `timestamp ± 1` | ⚠️ | Jinja 模板不支持算术运算，需在 Step 1 预计算 `start_ts` / `end_ts` 列 |
| 结果追加回原 DataFrame | ❌ | 当前 per_row 是拼接所有行的结果，不是"追加列"到上游 DataFrame |

**关键 gap：** 原 DAG 的模式是"逐行查 InfluxDB，把结果列追加回同一个 DataFrame"。而平台的 per_row 模式是"逐行查，把所有结果纵向拼接成新 DataFrame"。这是**不同的数据流模式**：

```
原DAG:    df → for each row → query → df.with_columns([result_col])  ← 横向追加
平台V2:   df → for each row → query → pl.concat(results)            ← 纵向拼接
```

### Step 3: Map API 批量查询

**业务逻辑：**
1. 按 `vehicle_id` 分组
2. 每组构建 `{x, y, timestamp}` 点列表
3. 调用自定义 `MapClient.annotate_multiple_vehicles()` 批量 API
4. 将 `road_type` 映射回每行

**平台支持度：**

| 子步骤 | 支持 | 说明 |
|--------|:---:|------|
| HTTP API 调用 | ❌ | 无 HTTP Extractor（且此 API 的请求/响应格式是自定义的） |
| 按车分组批量 | ❌ | 平台无此概念 |
| MapClient | ❌ | 私有业务 SDK，无法泛化 |

**结论：完全不支持。** 这是一个高度定制的业务 API 调用，无法用通用 Extractor 覆盖。

### Step 4: Pandera 验证 + 写入

**业务逻辑：**
- `map_road_type` 必须包含 "QC"
- `actual_speed` <= 0.5
- 按车辆统计 pass/fail
- 写入结果表

**平台支持度：**

| 子步骤 | 支持 | 说明 |
|--------|:---:|------|
| 规则校验 | ✅ | 可用 filter 规则表达等价逻辑 |
| 按车辆统计 | ✅ | 可用 aggregate 规则 |
| 结果写入 | ✅ | ReportWriter 已实现 |

**结论：验证逻辑可以用当前规则引擎等价表达。**

---

## 总结：平台 vs 原 DAG

| 能力 | 平台现状 | 原 DAG 需要 |
|------|:---:|:---:|
| 参数化 SQL 提取 | ✅ | ✅ |
| 漏斗式依赖（A→B→C） | ✅ (V2) | ✅ |
| InfluxDB per_row 查询 | ✅ (V2) | ✅ |
| **Unpivot 宽表转长表** | ❌ | ✅ |
| **复杂 Python 转换逻辑** | ❌ | ✅（连续QC筛选） |
| **横向列追加（enrichment）** | ❌ | ✅（Step 2/3 追加列） |
| **自定义 API 客户端** | ❌ | ✅（MapClient） |
| 规则校验 | ✅ | ✅ |
| 结果持久化 | ✅ | ✅ |

### 核心差距有 3 个：

1. **Adapter 表达力不足** — 无法做 unpivot、复杂时间解析、自定义 Python 过滤逻辑。当前 Adapter 只能做简单字段映射。

2. **缺少"横向 enrichment"模式** — 原 DAG 的模式是"拿着一个 DataFrame 逐步追加列"（Step 1 → Step 2 追加 x/y/speed → Step 3 追加 road_type）。平台的 Loader 是"每个 extraction 输出独立 DataFrame"，没有"在原 DataFrame 上追加列"的概念。

3. **无法集成自定义业务 SDK** — MapClient 是一个私有 API 封装，无法用泛化的 HTTP Extractor 替代（请求格式和响应解析都是定制的）。

### 我的判断

**这个 DAG 的业务场景，即使把所有方向二的功能都做完，平台仍然无法原生支持。** 原因不在于漏斗式提取能力（这已经解决了），而在于：

- **这个 DAG 本质上是一个 enrichment pipeline**（逐步给数据追加信息），不是一个 extraction pipeline（从不同源拉数据然后做规则校验）
- 它依赖高度定制的 Python 逻辑（unpivot、连续QC、MapClient），无法通过声明式 YAML 配置表达

### 可选路径

1. **路径 A：扩展平台支持"自定义 Python 步骤"** — 在 Adapter 或 Loader 中新增一种 `type: python_script` 的钩子，允许用户写 Python 脚本做任意转换。这能覆盖所有定制逻辑，但本质上等于回到了手写代码。

2. **路径 B：保持这个 DAG 独立，不纳入平台** — 它作为一个专用的验证 DAG 继续存在，平台只管主治理流水线（Cycle/Mileage 等结构化数据的规则校验）。两者并行运行。

3. **路径 C：部分迁移** — 把 Step 4 的验证逻辑迁移到平台（利用规则引擎），但 Step 1-3 的数据准备仍然用独立 DAG/Operator 完成。数据准备 DAG 输出一个"已 enrich"的 DataFrame 到 Parquet/MySQL，平台从该处接手做规则校验。

你倾向哪个方向？

---

**User**

我更倾向B和C，但是最终的选择是不是应该取决于同类场景出现的频率？

---

**Cursor**

你问对了核心问题。是的，**频率决定投资回报**。让我用一个简单的决策框架帮你判断。

## 决策矩阵

| 同类场景数量 | 推荐路径 | 理由 |
|:---:|:---:|------|
| **1 个**（仅此 DAG） | **B**（独立运行） | 工程投入最小，不改平台 |
| **2-3 个** | **C**（部分迁移） | 规则校验部分标准化到平台，enrichment 各自独立 |
| **5+ 个** | **A**（扩展平台） | 模式稳定，值得抽象为平台能力 |

## 关键判断依据

你可以盘点一下：在你的业务中，有多少场景属于这种 **"enrichment 型验证"** 模式——即需要拿着一批数据，逐步去不同系统查物理层/语义层信息，然后做交叉验证？

具体来说，看以下模式是否反复出现：

| 模式 | 例子 | 是否已有/计划中？ |
|------|------|:---:|
| MySQL → InfluxDB（用时间窗口查时序数据） | 本 DAG 的 Step 2 | ✅ 已有 |
| MySQL → 外部 API（用坐标/ID 查属性） | 本 DAG 的 Step 3（MapClient） | ✅ 已有 |
| MySQL → MySQL 漏斗（用 A 表结果筛 B 表） | 你提到的纯 MySQL 漏斗需求 | ✅ 有需求 |
| InfluxDB → MySQL（用传感器事件查业务记录） | 如充电事件反查充电订单 | ? |
| MySQL → InfluxDB → InfluxDB（多级时序关联） | 如轨迹验证后查充电桩数据 | ? |

如果后两个也在你的规划中，那说明 enrichment 模式是高频的，值得考虑 A。

## 我的建议：先 C，留 A 的口子

**当下**：选路径 C
- 这个 DAG 的 Step 1-3 保持独立（因为 enrichment 逻辑高度定制）
- Step 1-3 执行完后，把最终的"已 enrich"DataFrame 写入 Parquet 或中间表
- 平台从该处接手：Loader 读取已 enrich 的数据 → 规则引擎校验 → ReportWriter 持久化 → Dispatcher 告警

这样你**立刻**就能获得平台的规则引擎、结果持久化和告警能力，而不需要重写 enrichment 逻辑。

**未来**：如果发现 3 个以上类似 DAG，再把 enrichment 模式抽象为平台能力。到时候最小化的扩展方式不是做通用 ETL，而是在 Adapter 层增加一个 **`type: python_callable`** 的钩子——用户写一个 Python 函数，平台负责调用它并管理输入输出。这比重新发明 ETL 框架务实得多。

---

你觉得这个判断框架合理吗？你目前除了这个 Ground Truth DAG 之外，还有几个类似的 enrichment 场景？

---

**User**

你所举例的后两种模式也有可能出现（虽然目前还没有具体的场景）。你提到重新发明ETL架构，是指你认为当前的平台架构有根本性的问题吗

---

**Cursor**

不是。平台架构没有根本性问题。让我澄清我的意思。

## 平台的设计定位是清晰的

当前平台的核心能力是：**声明式数据质量校验**。

```
YAML 配置 → 自动生成 DAG → 加载 → 适配 → 规则校验 → 告警/持久化
```

这个流程对 `daily_cycle_etl` 这类场景是完美匹配的：数据已经存在于 MySQL 中，你只需要声明"加载哪些表、怎么映射成实体、用什么规则检查"。整个过程不需要写一行 Python 代码，这正是平台的价值。

## "重新发明 ETL"是指一种风险，不是现状

我的意思是：如果为了支持 Ground Truth DAG 这类场景，我们往平台里加越来越多的能力——

- Adapter 支持 unpivot / pivot
- Adapter 支持任意 Python 表达式链
- Loader 支持"横向追加列"模式
- Loader 支持自定义 SDK 集成
- 支持 per_row enrichment 并回写原 DataFrame

加到最后，平台就不再是一个"数据质量测试平台"，而变成了一个"通用数据处理框架"。那时候用户写 YAML 的复杂度和写 Python 代码差不多，YAML 的声明式优势就消失了。

**这才是我说的风险——不是架构有问题，而是不要让架构试图解决所有问题。**

## 两类问题，两种范式

| | 声明式质量校验（平台擅长的） | 命令式 enrichment 流水线（Ground Truth DAG） |
|---|---|---|
| **数据流** | 多源 → 合并/映射 → 规则校验 | 逐步查询 → 追加列 → 交叉验证 |
| **配置方式** | YAML | Python 代码 |
| **拓扑** | 固定（Loader→Adapter→Rules→...） | 每个场景不同 |
| **转换复杂度** | 低（字段映射、简单表达式） | 高（unpivot、自定义SDK、分组批量API） |
| **代表场景** | 每日 Cycle 数据校验 | Ground Truth 物理层/语义层验证 |

这两类不是一个比另一个好，而是解决的问题不同。**好的架构应该有清晰的边界，而不是试图覆盖一切。**

## 真正需要关注的

与其担心平台架构有没有根本性问题，更值得关注的是：**平台的边界画在哪里？**

我认为合理的边界是：

```
平台负责的：
  ✅ 声明式数据源接入（包括漏斗式 MySQL/InfluxDB 提取）
  ✅ 实体适配（字段映射、简单计算列）
  ✅ 规则引擎（filter / aggregate / cross_entity）
  ✅ 结果持久化 + 告警 + 资产打包

平台不负责的（由独立 DAG / Operator 处理）：
  ❌ 复杂的数据转换（unpivot、窗口函数链、自定义聚合）
  ❌ 自定义外部 SDK 集成（MapClient 等）
  ❌ 命令式 enrichment 流水线
```

当 enrichment 场景确实积累到 3-5 个时，正确的做法不是扩大平台的边界，而是在 Adapter 层加一个轻量的 **`python_callable` 钩子**——本质上是说"这一步超出了 YAML 的表达范围，你写个 Python 函数，平台帮你编排它"。这既保持了平台的声明式核心，又留了一个安全的逃生口。

你觉得这个边界划分合理吗？

