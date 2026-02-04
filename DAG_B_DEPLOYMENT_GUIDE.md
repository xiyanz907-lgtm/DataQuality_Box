# DAG B 部署与测试指南

## 📋 目录
1. [功能概述](#功能概述)
2. [架构设计](#架构设计)
3. [前置条件](#前置条件)
4. [部署步骤](#部署步骤)
5. [测试验证](#测试验证)
6. [监控与告警](#监控与告警)
7. [故障排查](#故障排查)
8. [运维手册](#运维手册)

---

## 🎯 功能概述

**DAG B (Asset Packing DAG)** 是数据治理平台的资产打包模块，负责将 DAG A 识别的 P1 资产（高价值场景数据）打包并上传到存储系统。

### 核心特性
- ✅ **事件驱动**：由 DAG A 通过 Airflow Dataset 自动触发
- ✅ **异步处理**：支持异步打包接口 + 轮询机制
- ✅ **僵尸任务处理**：自动检测并重置超时任务
- ✅ **重试机制**：失败自动重试 3 次，超过则永久放弃
- ✅ **批量处理**：每次最多处理 50 条资产
- ✅ **行锁防并发**：使用 `FOR UPDATE SKIP LOCKED` 防止冲突

---

## 🏗️ 架构设计

### 数据流转
```
DAG A (识别资产) 
    ↓ 写入队列 
    ↓ 触发 Dataset
DAG B (打包资产)
    ├─ 清理僵尸任务
    ├─ 获取待处理资产 (PENDING)
    ├─ 调用打包服务 (异步)
    ├─ 轮询打包状态 (最多 60 次)
    ├─ 更新元数据表 (process_status = PACKAGED)
    └─ 发送失败汇总邮件
```

### 状态机
```
PENDING → PROCESSING → POLLING → SUCCESS
                     ↓          ↓
                   FAILED ← ABANDONED
```

- **PENDING**: 待处理（初始状态）
- **PROCESSING**: 打包中（已调用打包接口）
- **POLLING**: 轮询中（等待打包完成）
- **SUCCESS**: 成功
- **FAILED**: 失败（可重试，`retry_count < 3`）
- **ABANDONED**: 已放弃（`retry_count >= 3`）

---

## ✅ 前置条件

### 1. 数据库表
需要创建以下表：

```bash
# 1. 创建打包队列表
mysql -u root -p < database/schemas/schema_governance_asset_packing_queue.sql

# 2. 升级元数据表
mysql -u root -p < database/schemas/schema_auto_test_case_catalog_v2_migration.sql
```

### 2. 打包服务
确认打包服务 API 可访问：

```bash
# 测试打包接口
curl -X POST https://mock.apipost.net/mock/34a21a/api/launcher/queryInfluxData \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "startTime": "2026-01-01T10:00:00Z",
    "endTime": "2026-01-01T12:00:00Z",
    "vehicleId": "V001",
    "basePath": "/data/assets/"
  }'

# 测试查询接口
curl -X GET "https://mock.apipost.net/mock/34a21a/api/launcher/querySyncCacheResult?key=YOUR_PACK_KEY" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### 3. Airflow 版本
- **Airflow >= 2.4.0** (支持 Dataset 功能)

检查版本：
```bash
docker exec deploy-airflow-1 airflow version
```

### 4. MySQL 连接
确认 Airflow 中已配置 `datalog_mysql_conn` 连接：

```bash
# 查看连接列表
docker exec deploy-airflow-1 airflow connections list | grep datalog_mysql_conn
```

---

## 🚀 部署步骤

### Step 1: 拉取最新代码
```bash
cd /home/ubuntu/cactus_box/cactus-box
git pull origin main
```

### Step 2: 检查文件清单
确认以下文件已存在：

```bash
# 核心文件
✅ plugins/datasets.py                                      # Dataset 定义
✅ plugins/services/packing_service.py                      # 打包服务客户端
✅ dags/asset_packing_dag.py                                # DAG B
✅ dags/governance_main_dag.py                              # DAG A (已修改)
✅ plugins/domian/context.py                                # AssetItem 添加 rule_id
✅ plugins/operators/aggregator.py                          # add_asset 添加 rule_id

# 数据库文件
✅ database/schemas/schema_governance_asset_packing_queue.sql
✅ database/schemas/schema_auto_test_case_catalog_v2_migration.sql
✅ database/monitoring/asset_packing_monitor.sql
```

### Step 3: 创建数据库表
```bash
# 连接到 MySQL
docker exec -it deploy-mysql-1 mysql -u root -p

# 切换数据库
USE your_database_name;

# 执行建表 SQL
SOURCE /path/to/schema_governance_asset_packing_queue.sql;
SOURCE /path/to/schema_auto_test_case_catalog_v2_migration.sql;

# 验证表结构
DESC governance_asset_packing_queue;
DESC auto_test_case_catalog;
```

### Step 4: 重启 Airflow
```bash
cd /home/ubuntu/cactus_box/cactus-box/deploy
docker-compose restart airflow
```

### Step 5: 检查 DAG 状态
```bash
# 查看 DAG 列表
docker exec deploy-airflow-1 airflow dags list | grep -E "(governance_main_dag|asset_packing_dag)"

# 查看 Dataset
docker exec deploy-airflow-1 airflow datasets list
```

预期输出：
```
✅ governance_main_dag (Active)
✅ asset_packing_dag (Active, Schedule: Dataset)
```

---

## 🧪 测试验证

### Test 1: 手动触发 DAG A
```bash
# 触发 DAG A
docker exec deploy-airflow-1 airflow dags trigger governance_main_dag

# 查看运行状态
docker exec deploy-airflow-1 airflow dags state governance_main_dag
```

### Test 2: 验证队列写入
```sql
-- 查看队列中的资产
SELECT * FROM governance_asset_packing_queue 
WHERE batch_id = 'BATCH_XXXXXX'
ORDER BY created_at DESC;

-- 预期结果：
-- 如果 DAG A 识别到 P1 资产，队列中应该有对应记录
-- status = 'PENDING'
```

### Test 3: 验证 Dataset 触发
```bash
# 查看 Dataset 历史
docker exec deploy-airflow-1 airflow datasets list-events

# 预期：应该有 GOVERNANCE_ASSET_DATASET 的触发记录
```

### Test 4: 验证 DAG B 自动触发
```bash
# 查看 DAG B 运行历史
docker exec deploy-airflow-1 airflow dags list-runs -d asset_packing_dag

# 预期：DAG B 应该在 DAG A 完成后自动触发
```

### Test 5: 验证打包结果
```sql
-- 查看打包成功的资产
SELECT * FROM governance_asset_packing_queue 
WHERE status = 'SUCCESS'
ORDER BY processed_at DESC
LIMIT 10;

-- 查看元数据表更新
SELECT * FROM auto_test_case_catalog
WHERE process_status = 'PACKAGED'
ORDER BY updated_at DESC
LIMIT 10;
```

### Test 6: 测试僵尸任务处理
```sql
-- 手动模拟僵尸任务
UPDATE governance_asset_packing_queue
SET status = 'PROCESSING',
    updated_at = NOW() - INTERVAL 3 HOUR
WHERE id = <某个ID>;

-- 触发 DAG B（或等待自动触发）
-- 验证僵尸任务被重置
SELECT * FROM governance_asset_packing_queue WHERE id = <某个ID>;
-- 预期：status 应该变为 'PENDING'，retry_count +1
```

### Test 7: 测试重试次数限制
```sql
-- 模拟超过重试次数的任务
UPDATE governance_asset_packing_queue
SET status = 'PROCESSING',
    retry_count = 3,
    updated_at = NOW() - INTERVAL 3 HOUR
WHERE id = <某个ID>;

-- 触发 DAG B
-- 验证任务被标记为 ABANDONED
SELECT * FROM governance_asset_packing_queue WHERE id = <某个ID>;
-- 预期：status = 'ABANDONED'
```

---

## 📊 监控与告警

### 1. 队列积压监控
```sql
-- 每分钟执行
SELECT COUNT(*) AS pending_count
FROM governance_asset_packing_queue
WHERE status = 'PENDING';

-- 告警阈值：pending_count > 500
```

### 2. 僵尸任务监控
```sql
-- 每 10 分钟执行
SELECT COUNT(*) AS zombie_count
FROM governance_asset_packing_queue
WHERE status IN ('PROCESSING', 'POLLING')
  AND updated_at < NOW() - INTERVAL 2 HOUR;

-- 告警阈值：zombie_count > 0
```

### 3. 打包成功率监控
```sql
-- 每 15 分钟执行
SELECT 
    ROUND(
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS success_rate_pct
FROM governance_asset_packing_queue
WHERE processed_at >= NOW() - INTERVAL 1 HOUR;

-- 告警阈值：success_rate_pct < 80%
```

### 4. 处理耗时监控
```sql
-- 每 30 分钟执行
SELECT 
    AVG(TIMESTAMPDIFF(SECOND, pack_started_at, processed_at)) AS avg_duration_sec,
    MAX(TIMESTAMPDIFF(SECOND, pack_started_at, processed_at)) AS max_duration_sec
FROM governance_asset_packing_queue
WHERE status = 'SUCCESS'
  AND processed_at >= NOW() - INTERVAL 1 HOUR;

-- 告警阈值：max_duration_sec > 600 (10分钟)
```

---

## 🔧 故障排查

### 问题 1：DAG B 未自动触发

**症状**：DAG A 完成后，DAG B 没有启动

**排查步骤**：
```bash
# 1. 检查 Dataset 是否正确定义
docker exec deploy-airflow-1 airflow datasets list | grep governance_asset_packing_queue

# 2. 检查 DAG A 是否声明了 outlets
docker exec deploy-airflow-1 python3 -c "
from dags.governance_main_dag import dag
print(dag.tasks[-1].outlets)
"

# 3. 检查 DAG B 是否监听了 schedule
docker exec deploy-airflow-1 python3 -c "
from dags.asset_packing_dag import dag
print(dag.schedule_interval)
"

# 4. 查看 Dataset 触发历史
docker exec deploy-airflow-1 airflow datasets list-events
```

**解决方案**：
- 确认 `plugins/datasets.py` 被正确加载
- 确认 DAG A 的 `save_assets_task` 有 `outlets=[GOVERNANCE_ASSET_DATASET]`
- 确认 DAG B 的 `schedule=[GOVERNANCE_ASSET_DATASET]`

### 问题 2：打包服务调用失败

**症状**：所有资产都标记为 FAILED，错误信息显示 HTTP 错误

**排查步骤**：
```bash
# 1. 手动测试打包接口
curl -X POST https://mock.apipost.net/mock/34a21a/api/launcher/queryInfluxData \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"startTime": "2026-01-01T10:00:00Z", "endTime": "2026-01-01T12:00:00Z", "vehicleId": "V001", "basePath": "/data/"}'

# 2. 检查 Airflow 日志
docker logs deploy-airflow-1 | grep "PackingServiceClient"
```

**解决方案**：
- 检查打包服务是否在同一网络
- 检查 Authorization Token 是否过期
- 修改 `plugins/services/packing_service.py` 中的 `BASE_URL` 和 `AUTH_TOKEN`

### 问题 3：僵尸任务未被清理

**症状**：PROCESSING 状态的任务超过 2 小时仍未重置

**排查步骤**：
```sql
-- 1. 查询僵尸任务
SELECT * FROM governance_asset_packing_queue
WHERE status IN ('PROCESSING', 'POLLING')
  AND updated_at < NOW() - INTERVAL 2 HOUR;

-- 2. 检查 cleanup_zombie_tasks 是否执行
-- 查看 Airflow 日志
```

**解决方案**：
- 手动触发 DAG B
- 检查 `cleanup_zombie_tasks` 任务的执行日志
- 如果必要，手动重置：
```sql
UPDATE governance_asset_packing_queue
SET status = 'PENDING',
    retry_count = retry_count + 1
WHERE id IN (...);
```

### 问题 4：元数据表未更新

**症状**：队列中显示 SUCCESS，但 `auto_test_case_catalog` 表的 `process_status` 仍是 `IDENTIFIED`

**排查步骤**：
```bash
# 1. 检查 update_metadata 任务日志
docker exec deploy-airflow-1 airflow tasks logs asset_packing_dag update_metadata <execution_date>

# 2. 手动测试更新逻辑
docker exec -it deploy-mysql-1 mysql -u root -p -e "
UPDATE auto_test_case_catalog
SET process_status = 'PACKAGED'
WHERE id IN (
    SELECT asset_id FROM governance_asset_packing_queue WHERE status = 'SUCCESS'
);
"
```

**解决方案**：
- 确认 `auto_test_case_catalog` 表中有对应的 `id` 记录
- 检查 asset_id 是否正确映射到 meta 表的主键

---

## 🛠️ 运维手册

### 手动重试失败任务
```sql
-- 将 FAILED 任务重置为 PENDING（会在下次 DAG B 触发时重试）
UPDATE governance_asset_packing_queue
SET status = 'PENDING',
    error_message = NULL,
    updated_at = NOW()
WHERE status = 'FAILED'
  AND retry_count < 3;
```

### 手动放弃任务
```sql
-- 将某个任务标记为 ABANDONED（不再重试）
UPDATE governance_asset_packing_queue
SET status = 'ABANDONED',
    error_message = 'Manually abandoned by admin',
    updated_at = NOW()
WHERE id = <任务ID>;
```

### 查看队列统计
```sql
SELECT 
    status,
    COUNT(*) AS count
FROM governance_asset_packing_queue
GROUP BY status;
```

### 清理历史数据（保留 7 天）
```sql
DELETE FROM governance_asset_packing_queue
WHERE created_at < NOW() - INTERVAL 7 DAY;
```

### 手动触发 DAG B
```bash
docker exec deploy-airflow-1 airflow dags trigger asset_packing_dag
```

---

## 📞 联系方式
- **Owner**: data-governance@example.com
- **Slack**: #data-governance-alerts
- **On-Call**: 查看 PagerDuty 排班表

---

## ✅ 部署清单

部署前请确认：

- [ ] 数据库表已创建（`governance_asset_packing_queue` + `auto_test_case_catalog` 升级）
- [ ] 打包服务 API 可访问
- [ ] Airflow 版本 >= 2.4.0
- [ ] MySQL 连接 `datalog_mysql_conn` 已配置
- [ ] DAG A 和 DAG B 在 Airflow UI 中显示为 Active
- [ ] Dataset `GOVERNANCE_ASSET_DATASET` 在 Airflow UI 中可见
- [ ] 手动触发 DAG A 测试通过
- [ ] 队列写入测试通过
- [ ] DAG B 自动触发测试通过
- [ ] 打包结果验证通过
- [ ] 僵尸任务处理测试通过
- [ ] 监控 SQL 已集成到监控系统
- [ ] 告警规则已配置

---

**部署日期**: 2026-02-02  
**版本**: v1.0  
**文档维护**: Data Governance Team
