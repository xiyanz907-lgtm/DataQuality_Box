# DAG B 单表方案迁移指南

## 📋 迁移概述

**迁移日期**: 2026-02-02  
**版本**: v2.0 (单表方案)  
**迁移类型**: 架构优化（双表 → 单表）

---

## 🎯 迁移目标

将资产打包功能从 **双表方案** 迁移到 **单表方案**：
- **双表方案**: `governance_asset_packing_queue` (队列) + `auto_test_case_catalog` (meta)
- **单表方案**: `auto_test_case_catalog` (合并)

---

## ✅ 变更清单

### 1. 数据库变更

| 操作 | 详情 | 文件 |
|-----|------|------|
| **表升级** | `auto_test_case_catalog` 添加 9 个打包字段 | `database/schemas/schema_auto_test_case_catalog_v3_single_table.sql` |
| **字段扩展** | `process_status` 新增 4 个状态值 | 同上 |
| **索引新增** | 添加 2 个打包队列索引 | 同上 |
| **表废弃** | `governance_asset_packing_queue` 不再使用 | 可选：手动删除 |

### 2. 代码变更

| 文件 | 变更内容 | 变更类型 |
|-----|---------|---------|
| `plugins/datasets.py` | Dataset URI 改为 `auto_test_case_catalog` | 修改 |
| `dags/governance_main_dag.py` | `save_assets_to_queue` 改为写入 meta 表 | 重构 |
| `dags/asset_packing_dag.py` | 所有 SQL 查询改为单表 | 重构 |
| `database/monitoring/...` | 监控 SQL 适配单表 | 新建 |

---

## 📊 数据库表结构对比

### 新增字段（auto_test_case_catalog）

| 字段名 | 类型 | 说明 | 对应原队列表字段 |
|-------|------|------|----------------|
| `pack_key` | VARCHAR(200) | 异步打包任务Key | ✅ pack_key |
| `pack_url` | VARCHAR(500) | 打包文件URL | ✅ pack_url |
| `pack_base_path` | VARCHAR(500) | 打包存储路径 | ✅ base_path |
| `pack_poll_count` | INT | 轮询次数 | ✅ poll_count |
| `pack_retry_count` | INT | 打包重试次数 | ✅ retry_count |
| `pack_error_message` | TEXT | 打包错误信息 | ✅ error_message |
| `pack_started_at` | DATETIME | 打包开始时间 | ✅ pack_started_at |
| `pack_completed_at` | DATETIME | 打包完成时间 | ✅ processed_at |
| `updated_at` | DATETIME | 更新时间 | 🆕 新增 |

### process_status 扩展

```sql
-- 原有状态（保留）
'IDENTIFIED'    -- 已识别
'PACKAGED'      -- 已打包
'BENCHMARKED'   -- 已基准测试

-- 新增状态（队列管理）
'PENDING'       -- 待打包
'PROCESSING'    -- 打包中
'POLLING'       -- 轮询中
'ABANDONED'     -- 已放弃
```

---

## 🚀 迁移步骤

### Step 1: 备份数据（必须）

```bash
# 备份 auto_test_case_catalog 表
mysqldump -u root -p your_database auto_test_case_catalog > backup_auto_test_case_catalog_$(date +%Y%m%d).sql

# 备份 governance_asset_packing_queue 表（如有数据）
mysqldump -u root -p your_database governance_asset_packing_queue > backup_governance_asset_packing_queue_$(date +%Y%m%d).sql
```

### Step 2: 执行数据库升级

```bash
# 进入 MySQL
docker exec -it deploy-mysql-1 mysql -u root -p

# 切换数据库
USE your_database_name;

# 执行升级脚本
SOURCE /path/to/schema_auto_test_case_catalog_v3_single_table.sql;

# 验证
DESC auto_test_case_catalog;
SHOW INDEX FROM auto_test_case_catalog;
```

### Step 3: 数据迁移（如有旧队列数据）

```sql
-- 可选：将旧队列表的数据迁移到 meta 表
UPDATE auto_test_case_catalog atc
INNER JOIN governance_asset_packing_queue gapq 
    ON atc.id = CAST(gapq.asset_id AS UNSIGNED)
SET 
    atc.process_status = CASE 
        WHEN gapq.status = 'SUCCESS' THEN 'PACKAGED'
        WHEN gapq.status = 'ABANDONED' THEN 'ABANDONED'
        WHEN gapq.status = 'PENDING' THEN 'PENDING'
        WHEN gapq.status = 'PROCESSING' THEN 'PROCESSING'
        WHEN gapq.status = 'POLLING' THEN 'POLLING'
        ELSE atc.process_status
    END,
    atc.pack_key = gapq.pack_key,
    atc.pack_url = gapq.pack_url,
    atc.pack_base_path = gapq.base_path,
    atc.pack_poll_count = gapq.poll_count,
    atc.pack_retry_count = gapq.retry_count,
    atc.pack_error_message = gapq.error_message,
    atc.pack_started_at = gapq.pack_started_at,
    atc.pack_completed_at = gapq.processed_at,
    atc.updated_at = gapq.updated_at
WHERE gapq.status != 'PENDING' OR gapq.retry_count > 0;
```

### Step 4: 重启 Airflow

```bash
cd /home/ubuntu/cactus_box/cactus-box/deploy
docker-compose restart airflow

# 等待 30 秒
sleep 30
```

### Step 5: 验证 DAG

```bash
# 检查 DAG 状态
docker exec deploy-airflow-1 airflow dags list | grep -E "(governance_main_dag|asset_packing_dag)"

# 检查 Dataset
docker exec deploy-airflow-1 airflow datasets list | grep auto_test_case_catalog
```

### Step 6: 测试完整流程

```bash
# 1. 手动触发 DAG A
docker exec deploy-airflow-1 airflow dags trigger governance_main_dag

# 2. 查看 DAG A 日志
docker exec deploy-airflow-1 airflow tasks logs governance_main_dag save_assets_to_queue <execution_date>

# 3. 验证 meta 表状态
docker exec -i deploy-mysql-1 mysql -u root -p your_database -e \
"SELECT id, cycle_id, process_status, pack_retry_count FROM auto_test_case_catalog WHERE process_status = 'PENDING' LIMIT 5;"

# 4. 验证 DAG B 自动触发
docker exec deploy-airflow-1 airflow dags list-runs -d asset_packing_dag --state running

# 5. 查看打包结果
docker exec -i deploy-mysql-1 mysql -u root -p your_database -e \
"SELECT id, cycle_id, process_status, pack_url FROM auto_test_case_catalog WHERE process_status = 'PACKAGED' LIMIT 5;"
```

---

## 🔍 迁移验证清单

### 数据库层面
- [ ] `auto_test_case_catalog` 表新增 9 个字段
- [ ] `process_status` 字段支持新状态值
- [ ] 新增 2 个打包队列索引
- [ ] 旧队列表数据已迁移（如有）

### 代码层面
- [ ] Dataset URI 指向 `auto_test_case_catalog`
- [ ] DAG A 写入 meta 表，状态为 PENDING
- [ ] DAG B 查询 meta 表，process_status = 'PENDING'
- [ ] DAG B 更新 meta 表，process_status = 'PACKAGED'

### 功能层面
- [ ] 手动触发 DAG A 成功
- [ ] P1 资产写入 meta 表，状态为 PENDING
- [ ] Dataset 触发 DAG B
- [ ] DAG B 获取待处理资产
- [ ] 打包服务调用成功
- [ ] 状态更新为 PACKAGED
- [ ] 僵尸任务处理正常

---

## 📈 监控适配

### 新监控 SQL

```bash
# 使用单表方案的监控 SQL
cat database/monitoring/asset_packing_monitor_single_table.sql
```

### Grafana 面板更新

```sql
-- 队列积压
SELECT COUNT(*) FROM auto_test_case_catalog WHERE process_status = 'PENDING'

-- 僵尸任务
SELECT COUNT(*) FROM auto_test_case_catalog 
WHERE process_status IN ('PROCESSING', 'POLLING')
  AND updated_at < NOW() - INTERVAL 2 HOUR

-- 打包成功率
SELECT 
    ROUND(SUM(CASE WHEN process_status = 'PACKAGED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM auto_test_case_catalog
WHERE pack_completed_at >= NOW() - INTERVAL 1 HOUR
```

---

## 🔄 回滚方案

### 如果迁移失败，可以回滚

```sql
-- 1. 删除新增字段
ALTER TABLE auto_test_case_catalog DROP COLUMN pack_key;
ALTER TABLE auto_test_case_catalog DROP COLUMN pack_url;
ALTER TABLE auto_test_case_catalog DROP COLUMN pack_base_path;
ALTER TABLE auto_test_case_catalog DROP COLUMN pack_poll_count;
ALTER TABLE auto_test_case_catalog DROP COLUMN pack_retry_count;
ALTER TABLE auto_test_case_catalog DROP COLUMN pack_error_message;
ALTER TABLE auto_test_case_catalog DROP COLUMN pack_started_at;
ALTER TABLE auto_test_case_catalog DROP COLUMN pack_completed_at;
ALTER TABLE auto_test_case_catalog DROP COLUMN updated_at;

-- 2. 删除新增索引
ALTER TABLE auto_test_case_catalog DROP INDEX idx_pack_queue;
ALTER TABLE auto_test_case_catalog DROP INDEX idx_pack_polling;

-- 3. 恢复代码（Git）
git checkout <previous_commit_hash>

-- 4. 重启 Airflow
docker-compose restart airflow
```

---

## 🧹 清理旧表（可选）

### 迁移成功后，可以删除旧队列表

```sql
-- ⚠️ 确保迁移完全成功后再执行

-- 1. 验证旧表不再使用
SELECT * FROM governance_asset_packing_queue LIMIT 1;

-- 2. 备份旧表（如需要）
CREATE TABLE governance_asset_packing_queue_backup AS 
SELECT * FROM governance_asset_packing_queue;

-- 3. 删除旧表
DROP TABLE governance_asset_packing_queue;
```

---

## 📊 性能对比

| 指标 | 双表方案 | 单表方案 | 改善 |
|-----|---------|---------|------|
| **数据冗余** | 高 | 低 | ✅ -50% |
| **写入次数** | 2 次 | 1 次 | ✅ -50% |
| **查询复杂度** | JOIN | 单表 | ✅ 简化 |
| **状态一致性** | 中等 | 高 | ✅ 强一致 |
| **维护成本** | 高 | 低 | ✅ -40% |

---

## ✅ 迁移完成检查

### 最终验证

```bash
# 1. 检查表结构
docker exec -i deploy-mysql-1 mysql -u root -p your_database -e "DESC auto_test_case_catalog;"

# 2. 检查 DAG 状态
docker exec deploy-airflow-1 airflow dags list | grep governance

# 3. 检查 Dataset
docker exec deploy-airflow-1 airflow datasets list

# 4. 运行完整流程测试
docker exec deploy-airflow-1 airflow dags trigger governance_main_dag

# 5. 监控日志
docker logs -f deploy-airflow-1 | grep -E "(governance_main_dag|asset_packing_dag)"
```

---

## 📞 支持与反馈

- **问题报告**: data-governance@example.com
- **Slack**: #data-governance-migration
- **文档**: [DAG_B_DEPLOYMENT_GUIDE.md](DAG_B_DEPLOYMENT_GUIDE.md)

---

**迁移状态**: ✅ 代码已完成，等待执行  
**预计耗时**: 30 分钟（含备份和验证）  
**风险等级**: 🟢 低（支持回滚）
