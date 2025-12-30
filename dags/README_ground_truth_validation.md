# Ground Truth Validation Worker DAG

## 概述

本 Worker DAG 实现了"三层真相对账"验证逻辑，用于验证 MySQL 业务声明（Business Claims）与物理真相（InfluxDB）和语义真相（地图服务）的一致性。

## 架构说明

### Controller-Worker 模式

```
Controller DAG (dq_v1_controller)
    ↓
    ├─ 数据就绪检查（Bucket Brigade）
    ├─ 动态分片（每 5-10 辆车一组）
    └─ 触发 Worker DAG
            ↓
        Worker DAG (dq_v1_worker_ground_truth)
            ├─ Step 1: 提取 MySQL 声明
            ├─ Step 2: 获取 InfluxDB 物理真相
            ├─ Step 3: 获取地图语义真相
            └─ Step 4: Pandera 验证 + 持久化
```

### 三层真相对账

1. **业务层声明（MySQL）**: `subtarget_vehicle_cycle` 表记录车辆声称的作业类型和时间
2. **物理层真相（InfluxDB）**: GPS 坐标和速度（实际车辆位置）
3. **语义层真相（Map API）**: 道路类型（实际区域语义）

## 文件结构

```
cactus-box/
├── dags/
│   └── dag_worker_ground_truth.py      # Worker DAG 主逻辑
├── plugins/
│   └── dq_lib/
│       └── ground_truth_utils.py       # InfluxClient & MapClient
└── deploy/
    └── schema_qa_ground_truth_result.sql  # 结果表 Schema (已重命名为 datalog_logic_check_result)
```

## 输入参数

Worker DAG 通过 `dag_run.conf` 接收 Controller 传递的参数：

```python
{
    "target_shift_date": "2025-11-02",      # 班次日期
    "vehicle_list": ["AT01", "AT02", "AT05"]  # 车辆 ID 列表（已分片）
}
```

## 四步验证逻辑

### Step 1: 提取 MySQL 声明（Polars）

**功能**: 从 `subtarget_vehicle_cycle` 表提取目标车辆的作业声明

**关键技术**:
- 动态 SQL 构建（使用 `WHERE IN` 过滤 vehicle_ids）
- Polars Unpivot：将 8 对 `(subtask_type_N, ALIGN_STA_TIME_N_SUBTASK)` 列转为长表
- 时间转换：`String -> Datetime -> Unix Timestamp (Int64)`

**SQL 示例**:
```sql
SELECT
    vehicle_id,
    cycle_id,
    subtask_type_1, ALIGN_STA_TIME_1_SUBTASK,
    subtask_type_2, ALIGN_STA_TIME_2_SUBTASK,
    ...,
    subtask_type_8, ALIGN_STA_TIME_8_SUBTASK
FROM dagster_pipelines.subtarget_vehicle_cycle
WHERE shift_date = '2025-11-02'
  AND vehicle_id IN ('AT01', 'AT02', 'AT05');
```

**Polars 时间转换**:
```python
df = df.with_columns(
    pl.col("align_time_str")
    .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
    .dt.epoch(time_unit="s")
    .cast(pl.Int64)
    .alias("unix_timestamp")
)
```

**输出**:
```
┌────────────┬──────────┬────────────────┐
│ vehicle_id │ cycle_id │ unix_timestamp │
├────────────┼──────────┼────────────────┤
│ AT01       │ C123     │ 1703064552     │
│ AT02       │ C124     │ 1703064560     │
└────────────┴──────────┴────────────────┘
```

### Step 2: 获取物理真相（InfluxDB）

**功能**: 查询 InfluxDB 获取车辆在指定时间戳的实际位置和速度

**查询窗口**: `[unix_timestamp - 1s, unix_timestamp + 1s]`

**聚合方式**: MEAN（降噪）

**InfluxDB Flux 查询示例**:
```flux
from(bucket: "vehicle_telemetry")
  |> range(start: 2025-12-21T09:32:01Z, stop: 2025-12-21T09:32:03Z)
  |> filter(fn: (r) => r["_measurement"] == "vehicle_position")
  |> filter(fn: (r) => r["vehicle_id"] == "AT01")
  |> filter(fn: (r) => r["_field"] == "x" or r["_field"] == "y" or r["_field"] == "speed")
  |> mean()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
```

**输出**:
```
┌────────────┬──────────┬────────────────┬──────────┬──────────┬──────────────┐
│ vehicle_id │ cycle_id │ unix_timestamp │ actual_x │ actual_y │ actual_speed │
├────────────┼──────────┼────────────────┼──────────┼──────────┼──────────────┤
│ AT01       │ C123     │ 1703064552     │ 548.2    │ 594.1    │ 0.3          │
│ AT02       │ C124     │ 1703064560     │ 550.5    │ 600.7    │ 0.4          │
└────────────┴──────────┴────────────────┴──────────┴──────────┴──────────────┘
```

### Step 3: 获取语义真相（Map API）

**功能**: 批量查询地图服务获取坐标对应的道路类型

**API 端点**: `POST http://10.105.66.20:1234/api/v1/annotate/batch`

**请求格式**:
```json
{
  "port": "AQCTMap_20251121V1.0",
  "format": "json",
  "vehicle_id": "AT01",
  "points": [
    { "x": 548.2, "y": 594.1, "timestamp": 1703064552 }
  ],
  "use_cache": true
}
```

**响应格式**:
```json
{
  "results": [
    {
      "attributes": {
        "road_type": {
          "road_type": "QC"
        }
      }
    }
  ]
}
```

**优化策略**: 按 `vehicle_id` 分组，每个车辆发送一次批量请求

**输出**:
```
┌────────────┬──────────┬────────────────┬──────────┬──────────┬──────────────┬───────────────┐
│ vehicle_id │ cycle_id │ unix_timestamp │ actual_x │ actual_y │ actual_speed │ map_road_type │
├────────────┼──────────┼────────────────┼──────────┼──────────┼──────────────┼───────────────┤
│ AT01       │ C123     │ 1703064552     │ 548.2    │ 594.1    │ 0.3          │ QC            │
│ AT02       │ C124     │ 1703064560     │ 550.5    │ 600.7    │ 0.4          │ Road          │
└────────────┴──────────┴────────────────┴──────────┴──────────┴──────────────┴───────────────┘
```

### Step 4: Pandera 验证 + 持久化

**验证规则**:

1. **位置验证**: 如果 `subtask_type == "To QC"`，则 `map_road_type` 必须包含 "QC"
2. **静止性验证**: `actual_speed <= 0.5 m/s`

**Pandera Schema**:
```python
schema = DataFrameSchema(
    {
        "map_road_type": Column(
            pl.Utf8,
            nullable=True,
            checks=[
                Check.str_contains("QC", name="location_must_be_qc"),
            ],
        ),
    },
    checks=[
        Check(
            lambda df: (df["actual_speed"] <= 0.5) | df["actual_speed"].is_null(),
            name="stationarity_check",
        ),
    ],
)
```

**结果持久化**:
- 表名: `dagster_pipelines.datalog_logic_check_result`
- 字段: `shift_date`, `vehicle_id`, `total_records`, `passed_records`, `failed_records`
- Controller 通过此表追踪完成进度

## 环境变量配置

```bash
# MySQL 连接
export DATALOG_CONN_ID="datalog_mysql_conn"

# InfluxDB 配置
export INFLUX_URL="http://10.105.66.20:8086"
export INFLUX_TOKEN="your_influx_token"
export INFLUX_ORG="your_org"
export INFLUX_BUCKET="vehicle_telemetry"

# Map API 配置
export MAP_API_URL="http://10.105.66.20:1234/api/v1/annotate/batch"
export MAP_PORT="AQCTMap_20251121V1.0"
```

## 部署步骤

### 1. 创建结果表

```bash
mysql -h <host> -u <user> -p < database/schemas/schema_qa_ground_truth_result.sql
```

### 2. 配置 Airflow Connections

**MySQL (DataLog)**:
```bash
airflow connections add 'datalog_mysql_conn' \
    --conn-type 'mysql' \
    --conn-host '10.105.66.20' \
    --conn-schema 'dagster_pipelines' \
    --conn-login 'your_user' \
    --conn-password 'your_password' \
    --conn-port 3306
```

### 3. 安装 Python 依赖

```bash
pip install polars influxdb-client pandera requests
```

### 4. 部署 DAG 文件

```bash
# Worker DAG
cp dags/dag_worker_ground_truth.py /path/to/airflow/dags/

# Utils
cp plugins/dq_lib/ground_truth_utils.py /path/to/airflow/plugins/dq_lib/
```

### 5. 验证部署

```bash
# 检查 DAG 是否加载成功
airflow dags list | grep dq_v1_worker_ground_truth

# 测试 DAG
airflow dags test dq_v1_worker_ground_truth 2025-12-25 \
    --conf '{"target_shift_date": "2025-11-02", "vehicle_list": ["AT01"]}'
```

## 监控和调试

### 查看任务日志

```bash
airflow tasks logs dq_v1_worker_ground_truth extract_claims <execution_date>
```

### 查询结果统计

```sql
SELECT 
    shift_date,
    COUNT(DISTINCT vehicle_id) AS vehicles,
    SUM(total_records) AS total,
    SUM(passed_records) AS passed,
    SUM(failed_records) AS failed,
    ROUND(SUM(passed_records) * 100.0 / SUM(total_records), 2) AS pass_rate
FROM dagster_pipelines.datalog_logic_check_result
WHERE shift_date = '2025-11-02'
GROUP BY shift_date;
```

### 查看失败记录详情

```sql
SELECT *
FROM dagster_pipelines.datalog_logic_check_result
WHERE shift_date = '2025-11-02'
  AND failed_records > 0
ORDER BY failed_records DESC;
```

## 性能优化建议

1. **InfluxDB 查询优化**:
   - 当前实现为逐条查询，建议优化为批量查询
   - 考虑使用 InfluxDB 的批量查询 API

2. **Map API 优化**:
   - 已实现按 `vehicle_id` 批量查询
   - 可进一步考虑缓存热点坐标的道路类型

3. **Polars 内存优化**:
   - 对于超大数据集，考虑使用 Polars 的 `scan_` 系列函数（延迟执行）
   - 启用流式处理（streaming mode）

4. **并行化**:
   - 可考虑将 Step 2 和 Step 3 并行执行（需要先完成 Step 1 的坐标提取）

## 常见问题

### Q1: Worker DAG 失败，但 Controller 一直等待

**原因**: Worker 未写入结果表，导致 Controller 无法判断完成状态

**解决方案**: 
- 检查 Worker 日志，确认失败原因
- 手动写入空结果（避免 Controller 卡死）：
```sql
INSERT INTO dagster_pipelines.datalog_logic_check_result 
(shift_date, vehicle_id, total_records, passed_records, failed_records)
VALUES ('2025-11-02', 'AT01', 0, 0, 0);
```

### Q2: InfluxDB 连接超时

**原因**: 网络不通或 Token 失效

**解决方案**:
- 检查网络连通性：`curl http://10.105.66.20:8086/health`
- 验证 Token 有效性
- 增加超时时间：`InfluxClient(timeout=60000)`

### Q3: Map API 返回 None

**原因**: 坐标超出地图范围或服务异常

**解决方案**:
- 检查坐标是否合理
- 验证 Map API 可用性：`curl -X POST http://10.105.66.20:1234/api/v1/annotate/batch`
- 查看 Worker 日志中的详细错误信息

## 扩展建议

1. **支持更多验证规则**:
   - 添加更多 Pandera checks（例如：坐标合理性、速度变化率）
   - 支持自定义验证规则配置

2. **详细错误报告**:
   - 将验证失败的详细信息（failure_cases）写入单独的错误表
   - 支持错误分类和聚合分析

3. **实时告警**:
   - 集成 Slack/Email 告警（当验证失败率超过阈值时）
   - 支持自定义告警规则

4. **可视化 Dashboard**:
   - 使用 Grafana/Tableau 展示验证结果趋势
   - 支持按车辆、班次、验证规则维度的下钻分析

## 贡献者

Data Engineering Team - Ground Truth Validation Project

## 许可证

内部使用 - Confidential

