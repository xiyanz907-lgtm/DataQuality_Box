#!/usr/bin/env python3
"""
测试 Hook → Pandas → Polars 转换
用于验证方案 A 的可行性
"""
import sys
sys.path.insert(0, '/opt/airflow')

from airflow.providers.mysql.hooks.mysql import MySqlHook
import polars as pl


def test_hook_conversion():
    """测试数据库连接和转换"""
    try:
        # 1. 测试 Hook 连接
        print("📝 Step 1: Testing MySqlHook connection...")
        hook = MySqlHook(mysql_conn_id="datalog_mysql_conn")
        
        # 2. 执行简单查询
        print("📝 Step 2: Executing test query...")
        test_sql = "SELECT 1 AS id, 'test' AS name, NOW() AS timestamp"
        pandas_df = hook.get_pandas_df(sql=test_sql)
        
        print(f"✅ Pandas DataFrame shape: {pandas_df.shape}")
        print(f"   Columns: {list(pandas_df.columns)}")
        print(f"   Data:\n{pandas_df}")
        
        # 3. 转换为 Polars
        print("\n📝 Step 3: Converting to Polars...")
        polars_df = pl.from_pandas(pandas_df)
        
        print(f"✅ Polars DataFrame shape: {polars_df.shape}")
        print(f"   Schema: {polars_df.schema}")
        print(f"   Data:\n{polars_df}")
        
        # 4. 验证数据一致性
        print("\n📝 Step 4: Verifying data consistency...")
        assert pandas_df.shape == (polars_df.height, polars_df.width), "Shape mismatch!"
        print("✅ Data consistency verified!")
        
        print("\n" + "="*60)
        print("🎉 All tests passed! Hook → Pandas → Polars works!")
        print("="*60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_hook_conversion()
    sys.exit(0 if success else 1)
