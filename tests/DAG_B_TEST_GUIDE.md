# DAG B 测试指南

## 📋 测试准备

### 前置条件检查

```bash
# 1. 确认 Airflow 正在运行
docker ps | grep airflow

# 2. 确认 MySQL 正在运行
docker ps | grep mysql

# 3. 确认数据库表已升级
docker exec -it deploy-mysql-1 mysql -u root -p -e "USE your_database; DESC auto_test_case_catalog;" | grep triggered_rule_id
```

---

## 🚀 测试方法

### 方法 1: 快速测试脚本（推荐）

**适用场景**: 快速验证 DAG B 基本功能

```bash
# 1. 进入测试目录
cd /home/ubuntu/cactus_box/cactus-box/tests

# 2. 运行快速测试
./quick_test_dag_b.sh
```

**测试内容**:
- ✅ 检查表结构（`triggered_rule_id` 字段）
- ✅ 插入测试数据
- ✅ 验证 PENDING 状态
- ✅ 触发 DAG B
- ✅ 检查处理结果
- ✅ 清理测试数据

---

### 方法 2: Python 测试套件（完整）

**适用场景**: 完整的端到端测试

```bash
# 1. 进入 Airflow 容器
docker exec -it deploy-airflow-1 bash

# 2. 运行 Python 测试
cd /opt/airflow
python3 tests/test_dag_b_single_table.py
```

**测试内容**:
- ✅ 插入 3 条测试数据
- ✅ 验证 PENDING 状态
- ✅ 触发 DAG B
- ✅ 检查 PROCESSING 状态
- ✅ 等待并检查最终结果
- ✅ 自动清理测试数据

---

### 方法 3: 手动测试（详细）

**适用场景**: 调试和深入分析

#### Step 1: 插入测试数据

```sql
-- 连接到 MySQL
docker exec -it deploy-mysql-1 mysql -u root -p your_database

-- 插入测试数据
INSERT INTO auto_test_case_catalog 
(batch_id, cycle_id, vehicle_id, shift_date, rule_version, 
 category, case_tags, severity, 
 trigger_timestamp, time_window_start, time_window_end, 
 triggered_rule_id, process_status, pack_base_path, created_at)
VALUES 
('MANUAL_TEST_001', 'CYCLE_TEST_001', 'V001', CURDATE(), 'v1.0',
 'CornerCase', '["TWIN_LIFT","MANUAL_TEST"]', 'P1',
 NOW(), NOW(), NOW() + INTERVAL 2 HOUR,
 'rule_p1_twin_lift', 'PENDING', '/data/assets/test/', NOW());

-- 验证插入
SELECT cycle_id, triggered_rule_id, process_status 
FROM auto_test_case_catalog 
WHERE batch_id = 'MANUAL_TEST_001';
```

#### Step 2: 触发 DAG B

```bash
# 手动触发
docker exec deploy-airflow-1 airflow dags trigger asset_packing_dag
```

#### Step 3: 监控日志

```bash
# 实时查看日志
docker logs -f deploy-airflow-1 | grep -E "(asset_packing_dag|MANUAL_TEST)"

# 查看特定任务日志
docker exec deploy-airflow-1 airflow tasks logs asset_packing_dag get_pending_assets <execution_date>
```

#### Step 4: 检查结果

```sql
-- 查看状态变化
SELECT 
    cycle_id,
    process_status,
    pack_key,
    pack_retry_count,
    pack_error_message,
    updated_at
FROM auto_test_case_catalog
WHERE batch_id = 'MANUAL_TEST_001';

-- 查看状态分布
SELECT 
    process_status,
    COUNT(*) as count
FROM auto_test_case_catalog
WHERE batch_id = 'MANUAL_TEST_001'
GROUP BY process_status;
```

#### Step 5: 清理

```sql
-- 清理测试数据
DELETE FROM auto_test_case_catalog
WHERE batch_id = 'MANUAL_TEST_001';
```

---

## 🔍 验证清单

### 数据库层面
- [ ] `auto_test_case_catalog` 表有 `triggered_rule_id` 字段
- [ ] 测试数据成功插入，状态为 `PENDING`
- [ ] DAG B 执行后，状态变为 `PROCESSING` 或 `POLLING`
- [ ] 最终状态为 `PACKAGED` 或 `FAILED`

### DAG 层面
- [ ] `asset_packing_dag` 在 Airflow UI 中显示为 Active
- [ ] DAG B 可以成功触发
- [ ] `cleanup_zombie_tasks` 任务执行成功
- [ ] `get_pending_assets` 任务获取到测试数据
- [ ] `pack_assets` 任务执行（可能失败，因为是 mock 接口）

### 日志层面
- [ ] 日志中显示 "Found X pending assets"
- [ ] 日志中显示 "Processing asset: CYCLE_TEST_001"
- [ ] 如有错误，`pack_error_message` 字段记录了详细信息

---

## 📊 预期结果

### 正常流程

```
PENDING (初始状态)
    ↓
PROCESSING (DAG B 开始处理)
    ↓
POLLING (调用打包接口，等待完成)
    ↓
PACKAGED (打包完成) ✅
```

### 打包服务不可用

```
PENDING
    ↓
PROCESSING
    ↓
FAILED (打包接口调用失败)
    ↓
PENDING (自动重置，retry_count +1)
    ↓
... 重试 3 次 ...
    ↓
ABANDONED (超过重试次数) ❌
```

### 僵尸任务

```
PENDING
    ↓
PROCESSING (DAG B 崩溃)
    ↓
... 2 小时后 ...
    ↓
PENDING (僵尸任务重置，retry_count +1) ♻️
```

---

## 🐛 常见问题

### 问题 1: IndexError: tuple index out of range

**原因**: `triggered_rule_id` 字段未添加

**解决**:
```bash
# 执行升级脚本
docker exec -i deploy-mysql-1 mysql -u root -p your_database < \
    database/schemas/schema_auto_test_case_catalog_v3_single_table.sql
```

### 问题 2: No pending assets found

**原因**: 测试数据未正确插入或状态不是 PENDING

**检查**:
```sql
SELECT * FROM auto_test_case_catalog 
WHERE batch_id LIKE 'TEST_%' 
ORDER BY created_at DESC LIMIT 10;
```

### 问题 3: DAG B 未自动触发

**原因**: Dataset 未正确配置或 DAG A 未执行

**解决**:
- 检查 Dataset: `docker exec deploy-airflow-1 airflow datasets list`
- 手动触发测试: `docker exec deploy-airflow-1 airflow dags trigger asset_packing_dag`

### 问题 4: 打包失败（Mock 接口）

**原因**: 使用的是 mock 接口，可能返回错误

**预期**: 这是正常的！Mock 接口用于测试流程，不是真实打包

**验证要点**:
- ✅ 状态流转正确（PENDING → PROCESSING → POLLING）
- ✅ 错误信息记录在 `pack_error_message`
- ✅ 重试机制生效（`pack_retry_count` 增加）

---

## 📈 性能测试

### 批量测试

```sql
-- 插入 50 条测试数据
INSERT INTO auto_test_case_catalog 
(batch_id, cycle_id, vehicle_id, shift_date, rule_version, 
 category, case_tags, severity, 
 trigger_timestamp, time_window_start, time_window_end, 
 triggered_rule_id, process_status, pack_base_path, created_at)
SELECT 
    'PERF_TEST_001' as batch_id,
    CONCAT('CYCLE_', LPAD(n, 5, '0')) as cycle_id,
    CONCAT('V', LPAD(MOD(n, 10), 3, '0')) as vehicle_id,
    CURDATE() as shift_date,
    'v1.0' as rule_version,
    'CornerCase' as category,
    '["TWIN_LIFT"]' as case_tags,
    'P1' as severity,
    NOW() as trigger_timestamp,
    NOW() as time_window_start,
    NOW() + INTERVAL 2 HOUR as time_window_end,
    'rule_p1_twin_lift' as triggered_rule_id,
    'PENDING' as process_status,
    '/data/assets/test/' as pack_base_path,
    NOW() as created_at
FROM (
    SELECT @row := @row + 1 as n
    FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
         (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) t2,
         (SELECT @row:=0) t3
    LIMIT 50
) numbers;
```

---

## 📞 支持

遇到问题？
- **文档**: [DAG_B_DEPLOYMENT_GUIDE.md](../DAG_B_DEPLOYMENT_GUIDE.md)
- **Email**: data-governance@example.com
- **Slack**: #data-governance-testing

---

**测试愉快！** 🚀
