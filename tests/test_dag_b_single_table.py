#!/usr/bin/env python3
"""
DAG B 单表方案测试脚本
用于验证资产打包流程的完整性

Author: Data Governance Team
Date: 2026-02-02
"""
import os
import sys
import json
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 添加项目路径
sys.path.insert(0, '/opt/airflow')

# ============================================================
# 测试配置
# ============================================================
MYSQL_CONN_ID = 'qa_mysql_conn'
META_TABLE = 'auto_test_case_catalog'
TEST_BATCH_ID = f'TEST_BATCH_{datetime.now().strftime("%Y%m%d_%H%M%S")}'

# ============================================================
# 测试数据
# ============================================================
TEST_ASSETS = [
    {
        'cycle_id': f'TEST_CYCLE_001_{datetime.now().strftime("%H%M%S")}',
        'vehicle_id': 'V001',
        'rule_id': 'rule_p1_twin_lift',
        'tags': ['TWIN_LIFT', 'HIGH_VALUE'],
        'time_window': {
            'start': datetime.now(),
            'end': datetime.now() + timedelta(hours=2)
        },
        'base_path': '/data/assets/test_twin_lift/'
    },
    {
        'cycle_id': f'TEST_CYCLE_002_{datetime.now().strftime("%H%M%S")}',
        'vehicle_id': 'V002',
        'rule_id': 'rule_p1_twin_lift',
        'tags': ['TWIN_LIFT'],
        'time_window': {
            'start': datetime.now(),
            'end': datetime.now() + timedelta(hours=1)
        },
        'base_path': '/data/assets/test_twin_lift/'
    },
    {
        'cycle_id': f'TEST_CYCLE_003_{datetime.now().strftime("%H%M%S")}',
        'vehicle_id': 'V003',
        'rule_id': 'rule_p1_twin_lift',
        'tags': ['TWIN_LIFT', 'EDGE_CASE'],
        'time_window': {
            'start': datetime.now(),
            'end': datetime.now() + timedelta(hours=3)
        },
        'base_path': '/data/assets/test_twin_lift/'
    },
]


# ============================================================
# 测试函数
# ============================================================

def test_1_insert_test_data():
    """测试1: 插入测试数据"""
    print("\n" + "="*60)
    print("🧪 Test 1: 插入测试数据")
    print("="*60)
    
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    insert_sql = f"""
        INSERT INTO {META_TABLE} 
        (batch_id, cycle_id, vehicle_id, shift_date, rule_version, 
         category, case_tags, severity, 
         trigger_timestamp, time_window_start, time_window_end, 
         triggered_rule_id, process_status, pack_base_path, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING', %s, NOW())
        ON DUPLICATE KEY UPDATE 
            process_status = 'PENDING',
            pack_retry_count = 0,
            updated_at = NOW()
    """
    
    success_count = 0
    for asset in TEST_ASSETS:
        try:
            hook.run(insert_sql, parameters=(
                TEST_BATCH_ID,                              # batch_id
                asset['cycle_id'],                          # cycle_id
                asset['vehicle_id'],                        # vehicle_id
                asset['time_window']['start'].date(),       # shift_date
                'v1.0',                                     # rule_version
                'CornerCase',                               # category
                json.dumps(asset['tags']),                  # case_tags (JSON)
                'P1',                                       # severity
                asset['time_window']['start'],              # trigger_timestamp
                asset['time_window']['start'],              # time_window_start
                asset['time_window']['end'],                # time_window_end
                asset['rule_id'],                           # triggered_rule_id
                asset['base_path'],                         # pack_base_path
            ))
            success_count += 1
            print(f"  ✅ Inserted: {asset['cycle_id']}")
        except Exception as e:
            print(f"  ❌ Failed to insert {asset['cycle_id']}: {str(e)}")
    
    print(f"\n📊 Result: {success_count}/{len(TEST_ASSETS)} assets inserted")
    return success_count == len(TEST_ASSETS)


def test_2_verify_pending_status():
    """测试2: 验证 PENDING 状态"""
    print("\n" + "="*60)
    print("🧪 Test 2: 验证 PENDING 状态")
    print("="*60)
    
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    select_sql = f"""
        SELECT cycle_id, triggered_rule_id, vehicle_id, process_status, pack_base_path
        FROM {META_TABLE}
        WHERE batch_id = %s
          AND process_status = 'PENDING'
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(select_sql, (TEST_BATCH_ID,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if not rows:
        print("  ❌ No PENDING assets found!")
        return False
    
    print(f"  ✅ Found {len(rows)} PENDING assets:")
    for row in rows:
        print(f"     - cycle_id: {row[0]}, rule_id: {row[1]}, vehicle: {row[2]}, status: {row[3]}")
    
    return len(rows) == len(TEST_ASSETS)


def test_3_trigger_dag_b():
    """测试3: 手动触发 DAG B"""
    print("\n" + "="*60)
    print("🧪 Test 3: 触发 DAG B")
    print("="*60)
    
    import subprocess
    
    try:
        result = subprocess.run(
            ['airflow', 'dags', 'trigger', 'asset_packing_dag'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("  ✅ DAG B triggered successfully")
            print(f"     Output: {result.stdout.strip()}")
            return True
        else:
            print(f"  ❌ Failed to trigger DAG B: {result.stderr}")
            return False
    except Exception as e:
        print(f"  ❌ Exception: {str(e)}")
        return False


def test_4_check_processing_status():
    """测试4: 检查 PROCESSING 状态"""
    print("\n" + "="*60)
    print("🧪 Test 4: 检查 PROCESSING 状态")
    print("="*60)
    print("  ⏱️  Waiting 10 seconds for DAG B to process...")
    
    import time
    time.sleep(10)
    
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    select_sql = f"""
        SELECT cycle_id, process_status, pack_retry_count, pack_error_message
        FROM {META_TABLE}
        WHERE batch_id = %s
          AND process_status IN ('PROCESSING', 'POLLING', 'PACKAGED', 'FAILED')
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(select_sql, (TEST_BATCH_ID,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if not rows:
        print("  ⚠️  Assets still in PENDING state (DAG B may not have started)")
        return False
    
    print(f"  ✅ Found {len(rows)} assets being processed:")
    for row in rows:
        status_icon = "✅" if row[1] == 'PACKAGED' else "🔄" if row[1] in ('PROCESSING', 'POLLING') else "❌"
        print(f"     {status_icon} cycle_id: {row[0]}, status: {row[1]}, retry: {row[2]}")
        if row[3]:  # error_message
            print(f"        Error: {row[3][:100]}")
    
    return True


def test_5_check_final_results():
    """测试5: 检查最终结果"""
    print("\n" + "="*60)
    print("🧪 Test 5: 检查最终结果")
    print("="*60)
    print("  ⏱️  Waiting 30 seconds for DAG B to complete...")
    
    import time
    time.sleep(30)
    
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    stats_sql = f"""
        SELECT 
            process_status,
            COUNT(*) as count
        FROM {META_TABLE}
        WHERE batch_id = %s
        GROUP BY process_status
    """
    
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(stats_sql, (TEST_BATCH_ID,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    
    print("\n  📊 Status Distribution:")
    total = 0
    packaged = 0
    for row in rows:
        status, count = row
        total += count
        if status == 'PACKAGED':
            packaged = count
            print(f"     ✅ {status}: {count}")
        elif status in ('PROCESSING', 'POLLING', 'PENDING'):
            print(f"     🔄 {status}: {count}")
        else:
            print(f"     ❌ {status}: {count}")
    
    success_rate = (packaged / total * 100) if total > 0 else 0
    print(f"\n  📈 Success Rate: {packaged}/{total} ({success_rate:.1f}%)")
    
    return success_rate >= 50  # 至少 50% 成功


def test_6_cleanup():
    """测试6: 清理测试数据"""
    print("\n" + "="*60)
    print("🧪 Test 6: 清理测试数据")
    print("="*60)
    
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    delete_sql = f"""
        DELETE FROM {META_TABLE}
        WHERE batch_id = %s
    """
    
    try:
        affected_rows = hook.run(delete_sql, parameters=(TEST_BATCH_ID,), handler=lambda cursor: cursor.rowcount)
        print(f"  ✅ Deleted {affected_rows} test records")
        return True
    except Exception as e:
        print(f"  ❌ Cleanup failed: {str(e)}")
        return False


# ============================================================
# 主测试流程
# ============================================================

def run_all_tests():
    """运行所有测试"""
    print("\n" + "="*60)
    print("🚀 DAG B 单表方案测试套件")
    print("="*60)
    print(f"📋 Test Batch ID: {TEST_BATCH_ID}")
    print(f"📋 MySQL Connection: {MYSQL_CONN_ID}")
    print(f"📋 Meta Table: {META_TABLE}")
    
    tests = [
        ("插入测试数据", test_1_insert_test_data),
        ("验证 PENDING 状态", test_2_verify_pending_status),
        ("触发 DAG B", test_3_trigger_dag_b),
        ("检查 PROCESSING 状态", test_4_check_processing_status),
        ("检查最终结果", test_5_check_final_results),
        ("清理测试数据", test_6_cleanup),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n  💥 Test Exception: {str(e)}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # 打印测试结果汇总
    print("\n" + "="*60)
    print("📊 测试结果汇总")
    print("="*60)
    
    passed = 0
    failed = 0
    for test_name, result in results:
        if result:
            print(f"  ✅ {test_name}: PASSED")
            passed += 1
        else:
            print(f"  ❌ {test_name}: FAILED")
            failed += 1
    
    print("\n" + "="*60)
    print(f"📈 总计: {passed} 通过, {failed} 失败 (共 {len(results)} 个测试)")
    print("="*60)
    
    return passed == len(results)


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
