#!/bin/bash
# ============================================================
# DAG B 快速测试脚本
# 用于快速验证 DAG B 的基本功能
# ============================================================

set -e

echo "🚀 DAG B 快速测试"
echo "=================================================="

# ============================================================
# 配置
# ============================================================
MYSQL_CONTAINER="deploy-mysql-1"
AIRFLOW_CONTAINER="deploy-airflow-1"
MYSQL_USER="root"
MYSQL_PASS="${MYSQL_ROOT_PASSWORD:-your_password}"
DATABASE="${QA_DB_NAME:-your_database}"

# ============================================================
# 颜色定义
# ============================================================
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================
# Test 1: 检查表结构
# ============================================================
echo ""
echo "📋 Test 1: 检查表结构"
echo "--------------------------------------------------"

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN '✅ triggered_rule_id field exists'
        ELSE '❌ triggered_rule_id field NOT FOUND'
    END AS status
FROM information_schema.columns
WHERE table_schema = '$DATABASE'
  AND table_name = 'auto_test_case_catalog'
  AND column_name = 'triggered_rule_id';
EOF

# ============================================================
# Test 2: 插入测试数据
# ============================================================
echo ""
echo "📋 Test 2: 插入测试数据"
echo "--------------------------------------------------"

TEST_BATCH_ID="TEST_BATCH_$(date +%Y%m%d_%H%M%S)"
TEST_CYCLE_ID="TEST_CYCLE_$(date +%Y%m%d_%H%M%S)"

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

SELECT '✅ Test data inserted' AS status;
EOF

# ============================================================
# Test 3: 验证 PENDING 状态
# ============================================================
echo ""
echo "📋 Test 3: 验证 PENDING 状态"
echo "--------------------------------------------------"

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    cycle_id,
    triggered_rule_id,
    vehicle_id,
    process_status,
    pack_base_path
FROM auto_test_case_catalog
WHERE batch_id = '$TEST_BATCH_ID'
  AND process_status = 'PENDING';
EOF

# ============================================================
# Test 4: 检查 DAG B 状态
# ============================================================
echo ""
echo "📋 Test 4: 检查 DAG B 状态"
echo "--------------------------------------------------"

docker exec $AIRFLOW_CONTAINER airflow dags list | grep asset_packing_dag && \
    echo -e "${GREEN}✅ DAG B exists${NC}" || \
    echo -e "${RED}❌ DAG B NOT FOUND${NC}"

# ============================================================
# Test 5: 手动触发 DAG B
# ============================================================
echo ""
echo "📋 Test 5: 手动触发 DAG B"
echo "--------------------------------------------------"

docker exec $AIRFLOW_CONTAINER airflow dags trigger asset_packing_dag && \
    echo -e "${GREEN}✅ DAG B triggered${NC}" || \
    echo -e "${RED}❌ Failed to trigger DAG B${NC}"

# ============================================================
# Test 6: 等待并检查结果
# ============================================================
echo ""
echo "📋 Test 6: 等待 20 秒后检查结果"
echo "--------------------------------------------------"

sleep 20

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    process_status,
    COUNT(*) as count
FROM auto_test_case_catalog
WHERE batch_id = '$TEST_BATCH_ID'
GROUP BY process_status;
EOF

# ============================================================
# Test 7: 查看详细状态
# ============================================================
echo ""
echo "📋 Test 7: 查看详细状态"
echo "--------------------------------------------------"

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    cycle_id,
    process_status,
    pack_key,
    pack_retry_count,
    pack_error_message,
    updated_at
FROM auto_test_case_catalog
WHERE batch_id = '$TEST_BATCH_ID';
EOF

# ============================================================
# Test 8: 清理测试数据
# ============================================================
echo ""
echo "📋 Test 8: 清理测试数据"
echo "--------------------------------------------------"

read -p "是否清理测试数据? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
DELETE FROM auto_test_case_catalog
WHERE batch_id = '$TEST_BATCH_ID';

SELECT '✅ Test data cleaned' AS status;
EOF
else
    echo "⏭️  Skipped cleanup (batch_id: $TEST_BATCH_ID)"
fi

# ============================================================
# 完成
# ============================================================
echo ""
echo "=================================================="
echo -e "${GREEN}✅ 测试完成！${NC}"
echo "=================================================="
echo ""
echo "📝 下一步:"
echo "  1. 查看 Airflow UI: http://localhost:8081"
echo "  2. 检查 DAG B 运行日志"
echo "  3. 如有失败，查看 pack_error_message"
echo ""
