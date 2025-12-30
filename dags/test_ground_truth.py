"""
Ground Truth Validation - 示例测试脚本

此脚本演示如何独立测试各个组件（不依赖 Airflow）

使用方法:
    python test_ground_truth.py
"""

import os
import sys
from datetime import datetime

# 添加 plugins 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plugins"))

from dq_lib.ground_truth_utils import InfluxClient, MapClient


def test_influx_client():
    """测试 InfluxDB 客户端"""
    print("\n=== Testing InfluxDB Client ===")
    
    # 配置（从环境变量读取）
    url = os.getenv("INFLUX_URL", "http://10.105.66.20:8086")
    token = os.getenv("INFLUX_TOKEN", "your_token")
    org = os.getenv("INFLUX_ORG", "your_org")
    bucket = os.getenv("INFLUX_BUCKET", "vehicle_telemetry")
    
    try:
        with InfluxClient(url=url, token=token, org=org, bucket=bucket) as client:
            # 测试查询（使用当前时间戳）
            unix_timestamp = int(datetime.now().timestamp())
            vehicle_id = "AT01"
            
            print(f"Querying vehicle={vehicle_id}, timestamp={unix_timestamp}")
            result = client.query_position_at_timestamp(
                vehicle_id=vehicle_id,
                unix_timestamp=unix_timestamp,
                window_seconds=1,
            )
            
            if result:
                print(f"✓ Result: {result}")
            else:
                print("✗ No data found (this is expected for test data)")
        
        print("✓ InfluxDB Client test passed")
        
    except Exception as e:
        print(f"✗ InfluxDB Client test failed: {e}")


def test_map_client():
    """测试地图服务客户端"""
    print("\n=== Testing Map Client ===")
    
    # 配置
    base_url = os.getenv("MAP_API_URL", "http://10.105.66.20:1234/api/v1/annotate/batch")
    port = os.getenv("MAP_PORT", "AQCTMap_20251121V1.0")
    
    try:
        client = MapClient(base_url=base_url, port=port)
        
        # 测试查询（使用示例坐标）
        vehicle_id = "AT01"
        points = [
            {"x": 548.0, "y": 594.0, "timestamp": 1703064552},
            {"x": 550.0, "y": 600.0, "timestamp": 1703064560},
        ]
        
        print(f"Querying vehicle={vehicle_id}, points={len(points)}")
        results = client.annotate_batch(vehicle_id=vehicle_id, points=points)
        
        if results:
            print(f"✓ Results: {results}")
        else:
            print("✗ No results (check API availability)")
        
        print("✓ Map Client test passed")
        
    except Exception as e:
        print(f"✗ Map Client test failed: {e}")


def test_polars_transformation():
    """测试 Polars 数据转换逻辑"""
    print("\n=== Testing Polars Transformation ===")
    
    import polars as pl
    
    # 模拟 MySQL 数据（宽表格式）
    data = {
        "vehicle_id": ["AT01", "AT02"],
        "cycle_id": ["C123", "C124"],
        "subtask_type_1": ["To QC", "Loading"],
        "ALIGN_STA_TIME_1_SUBTASK": ["2025-12-21 09:32:02", "2025-12-21 10:15:30"],
        "subtask_type_2": ["Loading", "To QC"],
        "ALIGN_STA_TIME_2_SUBTASK": ["2025-12-21 09:45:10", "2025-12-21 10:30:45"],
    }
    
    df = pl.DataFrame(data)
    print(f"Original DataFrame:\n{df}")
    
    # Unpivot subtask_type
    df_long = df.unpivot(
        index=["vehicle_id", "cycle_id"],
        on=["subtask_type_1", "subtask_type_2"],
        variable_name="subtask_index",
        value_name="subtask_type",
    ).with_columns(
        pl.col("subtask_index").str.extract(r"(\d+)$", 1).cast(pl.Int32).alias("idx")
    )
    
    # Unpivot time
    df_time = df.unpivot(
        index=["vehicle_id", "cycle_id"],
        on=["ALIGN_STA_TIME_1_SUBTASK", "ALIGN_STA_TIME_2_SUBTASK"],
        variable_name="time_index",
        value_name="align_time_str",
    ).with_columns(
        pl.col("time_index").str.extract(r"(\d+)", 1).cast(pl.Int32).alias("idx")
    )
    
    # Join
    df_merged = df_long.join(
        df_time.select(["vehicle_id", "cycle_id", "idx", "align_time_str"]),
        on=["vehicle_id", "cycle_id", "idx"],
        how="left",
    )
    
    print(f"\nAfter Unpivot and Join:\n{df_merged}")
    
    # 时间转换
    df_merged = df_merged.with_columns(
        pl.col("align_time_str")
        .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
        .dt.epoch(time_unit="s")
        .cast(pl.Int64)
        .alias("unix_timestamp")
    )
    
    print(f"\nAfter Time Conversion:\n{df_merged}")
    
    # 过滤
    df_filtered = df_merged.filter(
        (pl.col("subtask_type") == "To QC") & (pl.col("unix_timestamp").is_not_null())
    ).select([
        "vehicle_id",
        "cycle_id",
        "unix_timestamp",
    ])
    
    print(f"\nFiltered (To QC only):\n{df_filtered}")
    print("✓ Polars transformation test passed")


def test_pandera_validation():
    """测试 Pandera 验证逻辑"""
    print("\n=== Testing Pandera Validation ===")
    
    import polars as pl
    import pandera.polars as pa
    from pandera import Column, DataFrameSchema, Check
    
    # 模拟完整数据
    data = {
        "vehicle_id": ["AT01", "AT02", "AT03"],
        "cycle_id": ["C123", "C124", "C125"],
        "unix_timestamp": [1703064552, 1703064560, 1703064568],
        "actual_x": [548.0, 550.0, 552.0],
        "actual_y": [594.0, 600.0, 606.0],
        "actual_speed": [0.3, 0.4, 1.5],  # AT03 超速（应该失败）
        "map_road_type": ["QC", "QC", "Road"],  # AT03 不在 QC 区域（应该失败）
    }
    
    df = pl.DataFrame(data)
    print(f"Test DataFrame:\n{df}")
    
    # 定义验证规则
    schema = DataFrameSchema(
        {
            "vehicle_id": Column(pl.Utf8, nullable=False),
            "cycle_id": Column(pl.Utf8, nullable=True),
            "unix_timestamp": Column(pl.Int64, nullable=False),
            "actual_x": Column(pl.Float64, nullable=True),
            "actual_y": Column(pl.Float64, nullable=True),
            "actual_speed": Column(pl.Float64, nullable=True),
            "map_road_type": Column(
                pl.Utf8,
                nullable=True,
                checks=[
                    Check.str_contains("QC", name="location_must_be_qc"),
                ],
            ),
        },
        checks=[
            Check(
                lambda df: (df["actual_speed"] <= 0.5) | df["actual_speed"].is_null(),
                name="stationarity_check",
                description="actual_speed must be <= 0.5",
            ),
        ],
        strict=False,
    )
    
    # 执行验证
    try:
        validated_df = schema.validate(df, lazy=True)
        print(f"\n✓ All records passed validation:\n{validated_df}")
    except pa.errors.SchemaErrors as e:
        print(f"\n✗ Validation failed (expected):")
        print(f"Failure cases:\n{e.failure_cases}")
        
        # 统计
        total = len(df)
        failed = len(e.failure_cases) if e.failure_cases is not None else 0
        passed = total - failed
        print(f"\nSummary: Total={total}, Passed={passed}, Failed={failed}")
    
    print("✓ Pandera validation test passed")


def main():
    """运行所有测试"""
    print("=" * 60)
    print("Ground Truth Validation - Component Tests")
    print("=" * 60)
    
    # 测试 Polars 转换（不需要外部依赖）
    test_polars_transformation()
    
    # 测试 Pandera 验证（不需要外部依赖）
    test_pandera_validation()
    
    # 测试 InfluxDB 客户端（需要配置）
    if os.getenv("INFLUX_TOKEN"):
        test_influx_client()
    else:
        print("\n⚠ Skipping InfluxDB test (INFLUX_TOKEN not set)")
    
    # 测试地图客户端（需要网络）
    test_map_client()
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()

