#!/bin/bash
# ============================================================
# DAG B 集成测试脚本
# 验证 DAG A -> Dataset -> DAG B 的完整流程
# ============================================================

set -e  # 遇到错误立即退出

echo "🚀 Starting DAG B Integration Test..."
echo "=================================================="

# ============================================================
# 配置参数
# ============================================================
MYSQL_CONTAINER="deploy-mysql-1"
AIRFLOW_CONTAINER="deploy-airflow-1"
MYSQL_USER="root"
MYSQL_PASS="your_password"  # 请修改为实际密码
DATABASE="your_database"     # 请修改为实际数据库名

# ============================================================
# Test 1: 检查数据库表
# ============================================================
echo ""
echo "📋 Test 1: Checking database tables..."

# 检查队列表
docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    CASE 
        WHEN COUNT(*) = 1 THEN '✅ governance_asset_packing_queue exists'
        ELSE '❌ governance_asset_packing_queue NOT FOUND'
    END AS status
FROM information_schema.tables
WHERE table_schema = '$DATABASE'
  AND table_name = 'governance_asset_packing_queue';
EOF

# 检查 meta 表的 retry_count 字段
docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    CASE 
        WHEN COUNT(*) = 1 THEN '✅ retry_count column exists'
        ELSE '❌ retry_count column NOT FOUND'
    END AS status
FROM information_schema.columns
WHERE table_schema = '$DATABASE'
  AND table_name = 'auto_test_case_catalog'
  AND column_name = 'retry_count';
EOF

# ============================================================
# Test 2: 检查 Airflow DAG
# ============================================================
echo ""
echo "📋 Test 2: Checking Airflow DAGs..."

# 检查 DAG A
docker exec $AIRFLOW_CONTAINER airflow dags list | grep -q "governance_main_dag" && \
    echo "✅ DAG A (governance_main_dag) found" || \
    echo "❌ DAG A NOT FOUND"

# 检查 DAG B
docker exec $AIRFLOW_CONTAINER airflow dags list | grep -q "asset_packing_dag" && \
    echo "✅ DAG B (asset_packing_dag) found" || \
    echo "❌ DAG B NOT FOUND"

# ============================================================
# Test 3: 检查 Dataset
# ============================================================
echo ""
echo "📋 Test 3: Checking Airflow Dataset..."

docker exec $AIRFLOW_CONTAINER airflow datasets list | grep -q "governance_asset_packing_queue" && \
    echo "✅ Dataset (GOVERNANCE_ASSET_DATASET) found" || \
    echo "❌ Dataset NOT FOUND"

# ============================================================
# Test 4: 检查打包服务连通性
# ============================================================
echo ""
echo "📋 Test 4: Testing packing service connectivity..."

curl -s -X GET "https://mock.apipost.net/mock/34a21a/api/launcher/querySyncCacheResult?key=test" \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MSwidXNlcm5hbWUiOiJ3YW5nZGVmYSIsImV4cCI6MTc2MjUwODE5Nywic3ViIjoiQUNDRVNTIn0.W0b7YmmokSPw1GYb1hQb2AxdHjtKFPsIDaQeUOxPg2w" \
    > /dev/null && \
    echo "✅ Packing service reachable" || \
    echo "❌ Packing service UNREACHABLE"

# ============================================================
# Test 5: 插入测试数据到队列
# ============================================================
echo ""
echo "📋 Test 5: Inserting test data..."

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
INSERT INTO governance_asset_packing_queue 
(batch_id, asset_id, rule_id, vehicle_id, start_time, end_time, base_path, status)
VALUES 
('TEST_BATCH_$(date +%Y%m%d_%H%M%S)', 
 'TEST_ASSET_001', 
 'rule_p1_twin_lift', 
 'V001', 
 NOW(), 
 NOW() + INTERVAL 2 HOUR, 
 '/data/assets/test/', 
 'PENDING')
ON DUPLICATE KEY UPDATE updated_at = NOW();

SELECT '✅ Test data inserted' AS status;
EOF

# ============================================================
# Test 6: 手动触发 DAG B
# ============================================================
echo ""
echo "📋 Test 6: Manually triggering DAG B..."

docker exec $AIRFLOW_CONTAINER airflow dags trigger asset_packing_dag && \
    echo "✅ DAG B triggered successfully" || \
    echo "❌ Failed to trigger DAG B"

# ============================================================
# Test 7: 等待 10 秒后检查队列状态
# ============================================================
echo ""
echo "📋 Test 7: Checking queue status after 10 seconds..."
sleep 10

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    asset_id,
    status,
    retry_count,
    error_message,
    updated_at
FROM governance_asset_packing_queue
WHERE asset_id = 'TEST_ASSET_001'
ORDER BY updated_at DESC
LIMIT 1;
EOF

# ============================================================
# Test 8: 清理测试数据
# ============================================================
echo ""
echo "📋 Test 8: Cleaning up test data..."

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
DELETE FROM governance_asset_packing_queue
WHERE asset_id = 'TEST_ASSET_001';

SELECT '✅ Test data cleaned' AS status;
EOF

# ============================================================
# 完成
# ============================================================
echo ""
echo "=================================================="
echo "✅ Integration test completed!"
echo ""
echo "📝 Next Steps:"
echo "  1. Check Airflow UI for DAG B run status"
echo "  2. Review logs: docker logs deploy-airflow-1 | tail -100"
echo "  3. If all tests pass, proceed with production deployment"
echo ""
