#!/bin/bash
# ============================================================
# DAG B 单表方案测试脚本
# 功能：验证完整的资产打包流程
# ============================================================

set -e  # 遇到错误立即退出

echo "🧪 Starting DAG B Single Table Test..."
echo "=================================================="

# ============================================================
# 配置参数（请根据实际情况修改）
# ============================================================
MYSQL_CONTAINER="deploy-mysql-1"
AIRFLOW_CONTAINER="deploy-airflow-1"
MYSQL_USER="root"
MYSQL_PASS="your_password"  # 请修改
DATABASE="qa"                # 请修改

# ============================================================
# Test 1: 检查数据库表结构
# ============================================================
echo ""
echo "📋 Test 1: Checking table structure..."

echo "Checking triggered_rule_id field..."
docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    CASE 
        WHEN COUNT(*) = 1 THEN '✅ triggered_rule_id field exists'
        ELSE '❌ triggered_rule_id field NOT FOUND'
    END AS status
FROM information_schema.columns
WHERE table_schema = '$DATABASE'
  AND table_name = 'auto_test_case_catalog'
  AND column_name = 'triggered_rule_id';
EOF

echo "Checking pack_* fields..."
docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    column_name,
    CASE 
        WHEN column_name LIKE 'pack_%' THEN '✅ EXISTS'
        ELSE '✅ EXISTS'
    END AS status
FROM information_schema.columns
WHERE table_schema = '$DATABASE'
  AND table_name = 'auto_test_case_catalog'
  AND column_name IN ('pack_key', 'pack_url', 'pack_base_path', 'pack_poll_count', 
                      'pack_retry_count', 'pack_error_message', 'pack_started_at', 
                      'pack_completed_at', 'updated_at')
ORDER BY column_name;
EOF

# ============================================================
# Test 2: 检查 Airflow DAG 状态
# ============================================================
echo ""
echo "📋 Test 2: Checking Airflow DAG status..."

# 检查 DAG B
docker exec $AIRFLOW_CONTAINER airflow dags list | grep -q "asset_packing_dag" && \
    echo "✅ DAG B (asset_packing_dag) found" || \
    echo "❌ DAG B NOT FOUND"

# 检查 Dataset
docker exec $AIRFLOW_CONTAINER airflow datasets list | grep -q "auto_test_case_catalog" && \
    echo "✅ Dataset (auto_test_case_catalog) found" || \
    echo "❌ Dataset NOT FOUND"

# ============================================================
# Test 3: 清理旧测试数据
# ============================================================
echo ""
echo "📋 Test 3: Cleaning up old test data..."

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
DELETE FROM auto_test_case_catalog 
WHERE cycle_id LIKE 'TEST_%' 
   OR batch_id LIKE 'TEST_%';

SELECT '✅ Old test data cleaned' AS status;
EOF

# ============================================================
# Test 4: 插入测试数据（模拟 DAG A 写入）
# ============================================================
echo ""
echo "📋 Test 4: Inserting test data..."

BATCH_ID="TEST_BATCH_$(date +%Y%m%d_%H%M%S)"
CYCLE_ID_1="TEST_CYCLE_001_$(date +%s)"
CYCLE_ID_2="TEST_CYCLE_002_$(date +%s)"
CYCLE_ID_3="TEST_CYCLE_003_$(date +%s)"

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
-- 插入 3 条测试数据
INSERT INTO auto_test_case_catalog 
(batch_id, cycle_id, vehicle_id, shift_date, rule_version, 
 category, case_tags, severity, 
 trigger_timestamp, time_window_start, time_window_end, 
 triggered_rule_id, process_status, pack_base_path, created_at)
VALUES 
-- 测试数据 1
('$BATCH_ID', '$CYCLE_ID_1', 'TEST_V001', CURDATE(), 'v1.0', 
 'CornerCase', '["TWIN_LIFT", "TEST"]', 'P1', 
 NOW(), NOW(), NOW() + INTERVAL 2 HOUR, 
 'rule_p1_twin_lift', 'PENDING', '/data/assets/test/', NOW()),

-- 测试数据 2
('$BATCH_ID', '$CYCLE_ID_2', 'TEST_V002', CURDATE(), 'v1.0', 
 'CornerCase', '["OVERTAKE", "TEST"]', 'P1', 
 NOW(), NOW(), NOW() + INTERVAL 1 HOUR, 
 'rule_p1_overtake', 'PENDING', '/data/assets/test/', NOW()),

-- 测试数据 3
('$BATCH_ID', '$CYCLE_ID_3', 'TEST_V003', CURDATE(), 'v1.0', 
 'CornerCase', '["EMERGENCY_STOP", "TEST"]', 'P1', 
 NOW(), NOW(), NOW() + INTERVAL 3 HOUR, 
 'rule_p1_emergency', 'PENDING', '/data/assets/test/', NOW());

SELECT '✅ Inserted 3 test records' AS status;

-- 验证插入
SELECT 
    id,
    cycle_id,
    triggered_rule_id,
    process_status,
    pack_base_path
FROM auto_test_case_catalog
WHERE batch_id = '$BATCH_ID';
EOF

# ============================================================
# Test 5: 验证 PENDING 状态的数据
# ============================================================
echo ""
echo "📋 Test 5: Verifying PENDING status records..."

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    COUNT(*) AS pending_count,
    CASE 
        WHEN COUNT(*) >= 3 THEN '✅ Test data ready'
        ELSE '❌ Insufficient test data'
    END AS status
FROM auto_test_case_catalog
WHERE batch_id = '$BATCH_ID'
  AND process_status = 'PENDING';
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
# Test 7: 等待并查看执行状态
# ============================================================
echo ""
echo "📋 Test 7: Waiting for DAG B execution (30 seconds)..."
sleep 30

echo "Checking DAG B run status..."
docker exec $AIRFLOW_CONTAINER airflow dags list-runs -d asset_packing_dag --state running,success,failed | head -5

# ============================================================
# Test 8: 查看 cleanup_zombie_tasks 日志
# ============================================================
echo ""
echo "📋 Test 8: Checking cleanup_zombie_tasks logs..."
echo "(最近一次运行)"

docker exec $AIRFLOW_CONTAINER bash -c "
    airflow tasks list asset_packing_dag | grep cleanup_zombie_tasks > /dev/null && \
    airflow dags list-runs -d asset_packing_dag --state success,running,failed --output table | head -5
" || echo "⚠️ Task not found yet"

# ============================================================
# Test 9: 查看 get_pending_assets 日志
# ============================================================
echo ""
echo "📋 Test 9: Checking get_pending_assets execution..."

docker exec $AIRFLOW_CONTAINER bash -c "
    LATEST_RUN=\$(airflow dags list-runs -d asset_packing_dag --output json | python3 -c 'import json, sys; runs = json.load(sys.stdin); print(runs[0][\"run_id\"] if runs else \"\")' 2>/dev/null || echo '')
    if [ ! -z \"\$LATEST_RUN\" ]; then
        echo \"Latest run ID: \$LATEST_RUN\"
        echo \"Fetching logs...\"
        airflow tasks logs asset_packing_dag get_pending_assets \$LATEST_RUN 2>/dev/null | tail -20 || echo '⚠️ Logs not available yet'
    else
        echo '⚠️ No runs found yet'
    fi
"

# ============================================================
# Test 10: 查看数据库状态变化
# ============================================================
echo ""
echo "📋 Test 10: Checking database status after DAG B execution..."

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    cycle_id,
    triggered_rule_id,
    process_status,
    pack_retry_count,
    SUBSTRING(pack_error_message, 1, 50) AS error_preview,
    pack_started_at,
    pack_completed_at
FROM auto_test_case_catalog
WHERE batch_id = '$BATCH_ID'
ORDER BY created_at;
EOF

# ============================================================
# Test 11: 统计结果
# ============================================================
echo ""
echo "📋 Test 11: Summary statistics..."

docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE <<EOF
SELECT 
    process_status,
    COUNT(*) AS count
FROM auto_test_case_catalog
WHERE batch_id = '$BATCH_ID'
GROUP BY process_status;
EOF

# ============================================================
# Test 12: 检查打包服务调用（查看日志中是否有打包服务相关信息）
# ============================================================
echo ""
echo "📋 Test 12: Checking packing service calls in Airflow logs..."

docker logs --tail 50 $AIRFLOW_CONTAINER 2>&1 | grep -E "(PackingServiceClient|Packing started|pack_key)" | tail -10 || \
    echo "⚠️ No packing service logs found (may not have reached pack_assets task yet)"

# ============================================================
# 完成
# ============================================================
echo ""
echo "=================================================="
echo "✅ DAG B Test completed!"
echo ""
echo "📝 Next Steps:"
echo "  1. Review the test results above"
echo "  2. Check Airflow UI: http://localhost:8080"
echo "  3. View DAG B graph: http://localhost:8080/dags/asset_packing_dag/grid"
echo "  4. If pack_assets task is running, check its logs in Airflow UI"
echo ""
echo "🧹 Cleanup test data (optional):"
echo "  docker exec -i $MYSQL_CONTAINER mysql -u$MYSQL_USER -p$MYSQL_PASS $DATABASE -e \"DELETE FROM auto_test_case_catalog WHERE batch_id = '$BATCH_ID';\""
echo ""
