# DAG B 快速启动指南

## 🚀 5 分钟快速部署

### Step 1: 创建数据库表（2 分钟）

```bash
# 进入 MySQL 容器
docker exec -it deploy-mysql-1 mysql -u root -p

# 切换数据库（请替换为实际数据库名）
USE your_database_name;

# 复制粘贴以下 SQL 并执行
```

```sql
-- 1. 创建打包队列表
CREATE TABLE IF NOT EXISTS governance_asset_packing_queue (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL,
    asset_id VARCHAR(100) NOT NULL,
    rule_id VARCHAR(100) NOT NULL,
    vehicle_id VARCHAR(50) NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    base_path VARCHAR(500) NOT NULL,
    status ENUM('PENDING', 'PROCESSING', 'POLLING', 'SUCCESS', 'FAILED', 'ABANDONED') DEFAULT 'PENDING',
    pack_key VARCHAR(200),
    pack_url VARCHAR(500),
    poll_count INT DEFAULT 0,
    error_message TEXT,
    retry_count INT DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    pack_started_at DATETIME,
    processed_at DATETIME,
    UNIQUE KEY uk_asset (batch_id, asset_id),
    INDEX idx_status_created (status, created_at),
    INDEX idx_status_updated (status, updated_at),
    INDEX idx_status_polling (status, poll_count),
    INDEX idx_batch_id (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. 升级 meta 表
ALTER TABLE auto_test_case_catalog 
ADD COLUMN retry_count INT DEFAULT 0 
COMMENT '打包重试次数' 
AFTER process_status;

-- 3. 验证
DESC governance_asset_packing_queue;
DESC auto_test_case_catalog;
```

### Step 2: 重启 Airflow（1 分钟）

```bash
cd /home/ubuntu/cactus_box/cactus-box/deploy
docker-compose restart airflow

# 等待 30 秒让 Airflow 完全启动
sleep 30
```

### Step 3: 验证 DAG（1 分钟）

```bash
# 检查 DAG A 和 DAG B
docker exec deploy-airflow-1 airflow dags list | grep -E "(governance_main_dag|asset_packing_dag)"

# 预期输出：
# governance_main_dag    | ...  | True
# asset_packing_dag      | ...  | True

# 检查 Dataset
docker exec deploy-airflow-1 airflow datasets list | grep governance_asset_packing_queue

# 预期输出：
# mysql://datalog_mysql_conn/governance_asset_packing_queue
```

### Step 4: 手动测试（1 分钟）

```bash
# 1. 插入测试数据
docker exec -i deploy-mysql-1 mysql -u root -p your_database_name <<EOF
INSERT INTO governance_asset_packing_queue 
(batch_id, asset_id, rule_id, vehicle_id, start_time, end_time, base_path, status)
VALUES 
('TEST_BATCH_001', 'TEST_ASSET_001', 'rule_p1_twin_lift', 'V001', 
 NOW(), NOW() + INTERVAL 2 HOUR, '/data/assets/test/', 'PENDING');
EOF

# 2. 触发 DAG B
docker exec deploy-airflow-1 airflow dags trigger asset_packing_dag

# 3. 等待 10 秒后查看结果
sleep 10

docker exec -i deploy-mysql-1 mysql -u root -p your_database_name -e \
"SELECT asset_id, status, retry_count, error_message FROM governance_asset_packing_queue WHERE asset_id = 'TEST_ASSET_001';"

# 4. 清理测试数据
docker exec -i deploy-mysql-1 mysql -u root -p your_database_name -e \
"DELETE FROM governance_asset_packing_queue WHERE asset_id = 'TEST_ASSET_001';"
```

---

## ✅ 验证清单

完成以上步骤后，请确认：

- [ ] `governance_asset_packing_queue` 表已创建
- [ ] `auto_test_case_catalog` 表有 `retry_count` 字段
- [ ] DAG A (`governance_main_dag`) 在 Airflow UI 中显示为 Active
- [ ] DAG B (`asset_packing_dag`) 在 Airflow UI 中显示为 Active
- [ ] Dataset `GOVERNANCE_ASSET_DATASET` 在 Airflow UI 中可见
- [ ] 手动测试通过（测试数据被成功处理）

---

## 🎉 完成！

现在可以：

1. **查看 Airflow UI**: http://localhost:8080
   - 导航到 `Datasets` 页面，查看 `GOVERNANCE_ASSET_DATASET`
   - 查看 `asset_packing_dag` 的 Schedule 配置

2. **触发完整流程**:
   ```bash
   docker exec deploy-airflow-1 airflow dags trigger governance_main_dag
   ```
   
   预期：
   - DAG A 完成后，自动触发 DAG B
   - P1 资产被自动打包

3. **监控队列状态**:
   ```sql
   SELECT status, COUNT(*) AS count
   FROM governance_asset_packing_queue
   GROUP BY status;
   ```

---

## 📚 进一步阅读

- [完整部署指南](DAG_B_DEPLOYMENT_GUIDE.md)
- [实施总结](DAG_B_IMPLEMENTATION_SUMMARY.md)
- [监控 SQL](database/monitoring/asset_packing_monitor.sql)

---

## 🆘 遇到问题？

### 问题 1: DAG 未显示
```bash
# 检查日志
docker logs deploy-airflow-1 | tail -50

# 检查 DAG 文件语法
docker exec deploy-airflow-1 python3 -m py_compile /opt/airflow/dags/asset_packing_dag.py
```

### 问题 2: Dataset 未触发
```bash
# 检查 Dataset 事件
docker exec deploy-airflow-1 airflow datasets list-events

# 手动触发 DAG B 测试
docker exec deploy-airflow-1 airflow dags trigger asset_packing_dag
```

### 问题 3: 打包服务连接失败
```bash
# 测试打包服务
curl -X GET "https://mock.apipost.net/mock/34a21a/api/launcher/querySyncCacheResult?key=test" \
  -H "Authorization: Bearer YOUR_TOKEN"

# 修改 plugins/services/packing_service.py 中的 BASE_URL 和 AUTH_TOKEN
```

---

**需要帮助？** 联系 data-governance@example.com
