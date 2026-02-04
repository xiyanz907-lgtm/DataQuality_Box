#!/bin/bash
# 快速测试 DAG B
set -e

echo "🚀 DAG B 快速测试"
echo "=================================================="

# 配置
MYSQL_CONTAINER="cactus_db_container"
AIRFLOW_CONTAINER="deploy-airflow-1"
MYSQL_USER="root"
MYSQL_PASS="root"
DATABASE="qa_meta"

# Test 1: 检查表结构
echo ""
echo "📋 Test 1: 检查表结构"
echo "--------------------------------------------------"
docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE -e \
"SELECT COLUMN_NAME FROM information_schema.columns 
WHERE table_schema = '$DATABASE' 
AND table_name = 'auto_test_case_catalog' 
AND column_name = 'triggered_rule_id';"

# Test 2: 插入测试数据
echo ""
echo "📋 Test 2: 插入测试数据"
echo "--------------------------------------------------"
TEST_BATCH_ID="TEST_$(date +%Y%m%d_%H%M%S)"
TEST_CYCLE_ID="CYCLE_TEST_$(date +%Y%m%d_%H%M%S)"

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
INSERT INTO auto_test_case_catalog 
(batch_id, cycle_id, vehicle_id, shift_date, rule_version, 
 category, case_tags, severity, 
 trigger_timestamp, time_window_start, time_window_end, 
 triggered_rule_id, process_status, pack_base_path, created_at)
VALUES 
('$TEST_BATCH_ID', '$TEST_CYCLE_ID', 'V_TEST', CURDATE(), 'v1.0',
 'CornerCase', '["TWIN_LIFT","TEST"]', 'P1',
 NOW(), NOW(), NOW() + INTERVAL 2 HOUR,
 'rule_p1_twin_lift', 'PENDING', '/data/assets/test/', NOW());

SELECT CONCAT('✅ Test data inserted: ', cycle_id) AS status 
FROM auto_test_case_catalog WHERE cycle_id = '$TEST_CYCLE_ID';
EOF

# Test 3: 验证 PENDING 状态
echo ""
echo "📋 Test 3: 验证 PENDING 状态"
echo "--------------------------------------------------"
docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE -e \
"SELECT cycle_id, triggered_rule_id, vehicle_id, process_status 
FROM auto_test_case_catalog 
WHERE batch_id = '$TEST_BATCH_ID';"

# Test 4: 触发 DAG B
echo ""
echo "📋 Test 4: 触发 DAG B"
echo "--------------------------------------------------"
docker exec $AIRFLOW_CONTAINER airflow dags trigger asset_packing_dag
echo "✅ DAG B triggered"

# Test 5: 等待并检查状态
echo ""
echo "📋 Test 5: 等待 20 秒后检查状态"
echo "--------------------------------------------------"
sleep 20

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE -e \
"SELECT cycle_id, process_status, pack_retry_count, 
LEFT(pack_error_message, 50) as error_preview 
FROM auto_test_case_catalog 
WHERE batch_id = '$TEST_BATCH_ID';"

# Test 6: 状态分布
echo ""
echo "📋 Test 6: 状态分布统计"
echo "--------------------------------------------------"
docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE -e \
"SELECT process_status, COUNT(*) as count 
FROM auto_test_case_catalog 
WHERE batch_id = '$TEST_BATCH_ID' 
GROUP BY process_status;"

# 清理选项
echo ""
echo "=================================================="
echo "测试完成！"
echo "=================================================="
echo ""
echo "测试批次ID: $TEST_BATCH_ID"
echo "测试资产ID: $TEST_CYCLE_ID"
echo ""
echo "清理测试数据："
echo "  docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE -e \"DELETE FROM auto_test_case_catalog WHERE batch_id = '$TEST_BATCH_ID';\""
echo ""
