"""
测试脚本：验证 Phase 1 基础设施层

测试内容：
1. Context 的序列化/反序列化
2. IOStrategy 的本地读写
3. Config 的配置读取
"""
import sys
import os

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import polars as pl
from plugins.domian.context import GovernanceContext, DataRef, AlertItem, AssetItem
from plugins.infra.io_strategy import IOStrategy
from plugins.infra.config import Config


def test_context_basic():
    """测试 Context 基本功能"""
    print("\n=== 测试 1: Context 基本功能 ===")
    
    # 创建 Context
    ctx = GovernanceContext(
        batch_id="BATCH_TEST_001",
        run_date="2026-01-26",
        storage_type="local"
    )
    
    # 添加日志
    ctx.log("Test started")
    
    # 注册数据
    ctx.register_data(
        key="test_data",
        uri="file:///tmp/test/",
        stage="RAW",
        count=100,
        storage_type="local",
        alt_key="TestData"
    )
    
    # 添加告警
    ctx.add_alert(
        rule_id="test_rule",
        severity="P0",
        title="Test Alert",
        content="This is a test",
        trigger_cycle_ids=["C001", "C002"]
    )
    
    # 添加资产
    ctx.add_asset(
        asset_id="C001",
        asset_type="TEST",
        vehicle_id="V001",
        start_ts="2026-01-26T10:00:00Z",
        end_ts="2026-01-26T11:00:00Z",
        tags=["TEST_TAG"],
        target_path="/tmp/assets/C001/"
    )
    
    # 打印摘要
    print("Context Summary:")
    for key, value in ctx.summary().items():
        print(f"  {key}: {value}")
    
    print("✅ Context 基本功能测试通过")
    return ctx


def test_context_serialization(ctx):
    """测试 Context 序列化"""
    print("\n=== 测试 2: Context 序列化/反序列化 ===")
    
    # 序列化
    json_str = ctx.to_json()
    print(f"序列化后大小: {len(json_str)} bytes")
    
    # 反序列化
    ctx_restored = GovernanceContext.from_json(json_str)
    
    # 验证
    assert ctx_restored.batch_id == ctx.batch_id
    assert len(ctx_restored.data_registry) == len(ctx.data_registry)
    assert len(ctx_restored.alerts) == len(ctx.alerts)
    assert len(ctx_restored.assets) == len(ctx.assets)
    
    print(f"✅ 序列化/反序列化测试通过")
    print(f"  - 数据注册表: {len(ctx_restored.data_registry)} 项")
    print(f"  - 告警: {len(ctx_restored.alerts)} 个")
    print(f"  - 资产: {len(ctx_restored.assets)} 个")


def test_io_strategy():
    """测试 IOStrategy"""
    print("\n=== 测试 3: IOStrategy 读写 ===")
    
    # 创建测试数据
    df = pl.DataFrame({
        "cycle_id": ["C001", "C002", "C003"],
        "vehicle_id": ["V001", "V001", "V002"],
        "duration": [100, 200, 150]
    })
    
    # 测试目录
    test_uri = "file:///tmp/governance_test/batch_id=TEST_001/stage=RAW/key=test_data/"
    
    # 清理旧数据
    IOStrategy.clean_directory(test_uri, "local")
    
    # 写入
    print("写入测试数据...")
    file_path = IOStrategy.write_parquet(df, test_uri, "local", stage="RAW")
    print(f"  文件路径: {file_path}")
    
    # 读取
    print("读取测试数据...")
    df_read = IOStrategy.read_parquet(test_uri, "local")
    print(f"  读取行数: {df_read.height}")
    
    # 验证
    assert df_read.height == df.height
    assert df_read.columns == df.columns
    
    print("✅ IOStrategy 读写测试通过")
    
    # 清理
    IOStrategy.clean_directory(test_uri, "local")
    print("  测试数据已清理")


def test_config():
    """测试 Config"""
    print("\n=== 测试 4: Config 配置管理 ===")
    
    # 设置测试配置
    Config.set('TEST_KEY', 'test_value')
    Config.set('TEST_INT', '123')
    Config.set('TEST_BOOL', 'true')
    
    # 读取
    assert Config.get('TEST_KEY') == 'test_value'
    assert Config.get_int('TEST_INT') == 123
    assert Config.get_bool('TEST_BOOL') == True
    
    # 获取预定义配置
    storage_type = Config.get_storage_type()
    print(f"  存储类型: {storage_type}")
    
    compression_cfg = Config.get_compression_config()
    print(f"  压缩算法: {compression_cfg['global']}")
    
    print("✅ Config 配置管理测试通过")


def test_context_dataframe_integration():
    """测试 Context 与 IOStrategy 的集成"""
    print("\n=== 测试 5: Context + IOStrategy 集成 ===")
    
    # 创建 Context
    ctx = GovernanceContext(
        batch_id="BATCH_TEST_002",
        run_date="2026-01-26",
        storage_type="local"
    )
    
    # 创建测试数据
    df = pl.DataFrame({
        "cycle_id": ["C001", "C002", "C003"],
        "start_time": ["2026-01-26T10:00:00Z", "2026-01-26T11:00:00Z", "2026-01-26T12:00:00Z"],
        "end_time": ["2026-01-26T10:30:00Z", "2026-01-26T11:30:00Z", "2026-01-26T12:30:00Z"]
    })
    
    # 使用 Context 写入
    print("使用 Context.put_dataframe() 写入...")
    ctx.put_dataframe(key="entity_cycle", df=df, stage="ENTITY", alt_key="Cycle")
    
    # 使用 Context 读取（使用别名）
    print("使用 Context.get_dataframe() 读取（别名）...")
    df_read = ctx.get_dataframe(key="Cycle", use_alt_key=True)
    
    # 验证
    assert df_read.height == df.height
    print(f"  读取行数: {df_read.height}")
    
    # 获取 URI
    uri = ctx.get_data_uri("Cycle", use_alt_key=True)
    print(f"  数据 URI: {uri}")
    
    print("✅ Context + IOStrategy 集成测试通过")
    
    # 清理
    IOStrategy.clean_directory(uri, "local")


def main():
    """运行所有测试"""
    print("=" * 60)
    print("Phase 1 基础设施层测试")
    print("=" * 60)
    
    try:
        # 测试 1: Context 基本功能
        ctx = test_context_basic()
        
        # 测试 2: Context 序列化
        test_context_serialization(ctx)
        
        # 测试 3: IOStrategy
        test_io_strategy()
        
        # 测试 4: Config
        test_config()
        
        # 测试 5: 集成测试
        test_context_dataframe_integration()
        
        print("\n" + "=" * 60)
        print("✅ 所有测试通过！Phase 1 基础设施层实现完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
