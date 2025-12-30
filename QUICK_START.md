# Ground Truth Validation - Quick Start Guide

## 🚀 5 分钟快速启动

### 前置条件检查

```bash
# 1. 检查 Python 依赖
python -c "import polars, pandera, influxdb_client; print('✓ Dependencies OK')"

# 2. 检查 MySQL 连接
airflow connections get datalog_mysql_conn

# 3. 检查 InfluxDB 可达性
curl -I http://10.105.66.20:8086/health

# 4. 检查 Map API 可达性
curl -I http://10.105.66.20:1234
```

---

## 📋 部署步骤

### Step 1: 创建结果表（1 分钟）

```bash
cd /home/ubuntu/cactus_box/cactus-box
mysql -h <your_host> -u <your_user> -p dagster_pipelines < database/schemas/schema_qa_ground_truth_result.sql
```

### Step 2: 配置环境变量（2 分钟）

```bash
# 编辑配置文件
cp config/config_ground_truth.env .env
nano .env  # 填入实际的 INFLUX_TOKEN 等配置

# 加载配置
source .env

# 验证配置
echo $INFLUX_TOKEN
```

### Step 3: 部署 DAG 文件（1 分钟）

```bash
# Worker DAG 已经在 dags/ 目录中
# Utils 已经在 plugins/dq_lib/ 目录中
# Airflow 会自动扫描并加载

# 验证 DAG 加载
airflow dags list | grep ground_truth
```

### Step 4: 测试运行（1 分钟）

```bash
# 方法 1: 独立测试（推荐）
cd dags
python test_ground_truth.py

# 方法 2: Airflow 测试
airflow dags test dq_v1_worker_ground_truth 2025-12-25 \
    --conf '{"target_shift_date": "2025-11-02", "vehicle_list": ["AT01"]}'
```

---

## 🎯 验证部署成功

### 检查 DAG 状态

```bash
# 查看 DAG 列表
airflow dags list | grep -E "dq_v1_(controller|worker_ground_truth)"

# 预期输出:
# dq_v1_controller              | ... | True
# dq_v1_worker_ground_truth     | ... | False (由 Controller 触发)
```

### 查看日志

```bash
# Controller 日志
airflow tasks logs dq_v1_controller compute_plan <date>

# Worker 日志
airflow tasks logs dq_v1_worker_ground_truth extract_claims <date>
```

### 查询结果

```sql
-- 查看最新验证结果
SELECT 
    shift_date,
    vehicle_id,
    total_records,
    passed_records,
    failed_records,
    ROUND(passed_records * 100.0 / NULLIF(total_records, 0), 2) AS pass_rate_pct,
    created_at
FROM dagster_pipelines.qa_ground_truth_result
ORDER BY created_at DESC
LIMIT 10;
```

---

## 🔧 常见问题排查

### 问题 1: DAG 未显示在 Airflow UI

**症状**: `airflow dags list` 找不到 `dq_v1_worker_ground_truth`

**排查步骤**:
```bash
# 1. 检查文件路径
ls -la dags/dag_worker_ground_truth.py
ls -la plugins/dq_lib/ground_truth_utils.py

# 2. 检查语法错误
python dags/dag_worker_ground_truth.py

# 3. 查看 Airflow 日志
tail -f $AIRFLOW_HOME/logs/scheduler/latest/*.log
```

**解决方案**:
- 确保文件在正确的目录
- 检查 Python 语法错误
- 重启 Airflow Scheduler: `airflow scheduler`

---

### 问题 2: InfluxDB 连接失败

**症状**: `[InfluxClient] Query error: ...`

**排查步骤**:
```bash
# 1. 检查网络连通性
ping 10.105.66.20
curl -I http://10.105.66.20:8086/health

# 2. 验证 Token
echo $INFLUX_TOKEN

# 3. 测试查询
influx query --host http://10.105.66.20:8086 --org $INFLUX_ORG --token $INFLUX_TOKEN 'from(bucket:"vehicle_telemetry") |> range(start: -1h) |> limit(n:1)'
```

**解决方案**:
- 检查 Token 是否正确
- 确认 Org 和 Bucket 名称
- 增加超时时间: `INFLUX_TIMEOUT=60000`

---

### 问题 3: Map API 返回空结果

**症状**: `map_road_type` 列全为 `None`

**排查步骤**:
```bash
# 1. 测试 API 可用性
curl -X POST http://10.105.66.20:1234/api/v1/annotate/batch \
  -H "Content-Type: application/json" \
  -d '{
    "port": "AQCTMap_20251121V1.0",
    "format": "json",
    "vehicle_id": "AT01",
    "points": [{"x": 548, "y": 594, "timestamp": 1703064552}],
    "use_cache": true
  }'

# 2. 检查坐标范围
# 确保坐标在地图范围内
```

**解决方案**:
- 验证 API 端点和端口号
- 检查坐标是否合理（不能超出地图范围）
- 查看 Worker 日志中的详细错误信息

---

### 问题 4: Pandera 验证全部失败

**症状**: `failed_records` 数量等于 `total_records`

**排查步骤**:
```sql
-- 查看原始数据
SELECT 
    vehicle_id,
    map_road_type,
    actual_speed
FROM dagster_pipelines.subtarget_vehicle_cycle
WHERE shift_date = '2025-11-02'
LIMIT 10;
```

**解决方案**:
- 检查 `map_road_type` 是否包含 "QC"
- 检查 `actual_speed` 是否 <= 0.5
- 如需调整阈值，修改 `MAX_SPEED_FOR_STATIONARY`

---

### 问题 5: Controller 一直等待 Worker

**症状**: `VAR_INFLIGHT` 一直存在，Controller 不推进

**排查步骤**:
```sql
-- 检查结果表是否有数据
SELECT 
    COUNT(DISTINCT vehicle_id) AS done_cnt
FROM dagster_pipelines.qa_ground_truth_result
WHERE shift_date = '2025-11-02';

-- 检查预期车辆数
SELECT 
    COUNT(DISTINCT vehicle_id) AS expected_cnt
FROM dagster_pipelines.daily_cycle_overall
WHERE shift_date = '2025-11-02';
```

**解决方案**:
- 手动写入空结果（避免卡死）:
```sql
INSERT INTO dagster_pipelines.qa_ground_truth_result 
(shift_date, vehicle_id, total_records, passed_records, failed_records)
SELECT shift_date, vehicle_id, 0, 0, 0
FROM dagster_pipelines.daily_cycle_overall
WHERE shift_date = '2025-11-02'
ON DUPLICATE KEY UPDATE total_records = 0;
```

---

## 📞 获取帮助

### 1. 查看日志

```bash
# Scheduler 日志
tail -f $AIRFLOW_HOME/logs/scheduler/latest/*.log

# Task 日志
airflow tasks logs dq_v1_worker_ground_truth extract_claims <date> -s <task_instance>
```

### 2. 运行独立测试

```bash
cd dags
python test_ground_truth.py
```

### 3. 查看详细文档

```bash
# 完整文档
cat README_ground_truth_validation.md

# 实现总结
cat ../IMPLEMENTATION_SUMMARY.md
```

### 4. 联系团队

**Data Engineering Team**  
Slack: `#data-engineering`  
Email: `data-engineering@company.com`

---

## ✅ 检查清单

部署完成后，请确认以下项目：

- [ ] 结果表已创建（`qa_ground_truth_result`）
- [ ] 环境变量已配置（`.env` 文件）
- [ ] DAG 已加载（Airflow UI 可见）
- [ ] 独立测试通过（`test_ground_truth.py`）
- [ ] InfluxDB 连接正常
- [ ] Map API 连接正常
- [ ] Controller DAG 已启用
- [ ] Worker DAG 触发成功
- [ ] 结果表有数据写入
- [ ] 验证规则正常工作

---

**祝部署顺利！** 🎉

