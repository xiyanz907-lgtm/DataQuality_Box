"""
DQ v1 Worker DAG - Ground Truth Validation

职责：
1) 接收 Controller 传递的参数（shift_date, vehicle_ids）
2) 四步验证逻辑：
   Step 1: 从 MySQL 提取目标车辆的作业声明（subtarget_vehicle_cycle）
   Step 2: 从 InfluxDB 获取物理层真相（实际位置/速度）
   Step 3: 从地图服务获取语义层真相（道路类型）
   Step 4: 使用 Pandera 验证规则，写入结果表

输入参数（dag_run.conf）：
- shift_date: str (e.g., "2025-11-02")
- vehicle_list: list[str] (e.g., ["AT01", "AT02", "AT05"])

关键技术：
- Polars: 高性能数据转换（Unpivot + 时间解析）
- InfluxDB: 物理层真相（GPS 位置/速度）
- Map API: 语义层真相（道路类型）
- Pandera: 数据质量验证
"""

import os
import sys
import logging
from datetime import timedelta
from typing import Dict, List, Optional
from io import StringIO

import pendulum
import polars as pl
import pandera.polars as pa
from pandera import Column, DataFrameSchema, Check

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException

# 统一复用项目内的 Connection ID 配置
from services.config import CONN_ID_DATALOG as CONFIG_CONN_ID_DATALOG

# 确保 plugins 路径可导入
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
plugins_dir = os.path.join(project_root, "plugins")
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

# Ground Truth 验证工具
from dq_lib.ground_truth_utils import InfluxClient, MapClient

# -----------------------------
# 配置区
# -----------------------------
DAG_ID = "DatalogLogicCheck_worker"

# MySQL 连接（DataLog 开发库）
CONN_ID_DATALOG = os.getenv("DATALOG_CONN_ID", CONFIG_CONN_ID_DATALOG)

# 表名（库名固定为 dagster_pipelines）
TBL_SUBTARGET = "dagster_pipelines.subtarget_vehicle_cycle"
TBL_RESULT = "dagster_pipelines.datalog_logic_check_result"
TBL_FAILURE_DETAIL = "dagster_pipelines.datalog_logic_check_failed_detail"

# InfluxDB 配置
INFLUX_URL = os.getenv("INFLUX_URL", "http://10.105.66.20:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "your_token_here")
INFLUX_ORG = os.getenv("INFLUX_ORG", "your_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "vehicle_telemetry")

# Map API 配置
MAP_API_URL = os.getenv("MAP_API_URL", "http://10.105.66.20:1234/api/v1/annotate/batch")
MAP_PORT = os.getenv("MAP_PORT", "AQCTMap_20251121V1.0")

# 验证规则常量
MAX_SPEED_FOR_STATIONARY = 0.5  # 静止阈值（m/s）

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


# -----------------------------
# 辅助函数：连续 'To QC' 筛选
# -----------------------------
def _apply_consecutive_qc_filter(df_all: pl.DataFrame, df_valid: pl.DataFrame, logger) -> pl.DataFrame:
    """
    应用连续 'To QC' 筛选规则（改进版）：
    - 基于所有 'To QC'（包括 align_time 为空的）判断连续性
    - 但只保留有 timestamp 的记录
    - 如果一个 cycle_id 有连续的 'To QC'（索引相邻），只保留有值的最后一个
    - 如果都不连续，保留所有有值的记录
    
    Args:
        df_all: 所有 'To QC' 记录（用于判断连续性）
        df_valid: 只包含有 timestamp 的 'To QC' 记录（用于最终输出）
        logger: Airflow logger
    
    Returns:
        过滤后的 DataFrame
    """
    import pandas as pd
    
    # 转换为 Pandas 方便分组处理
    df_all_pandas = df_all.to_pandas()
    df_valid_pandas = df_valid.to_pandas()
    
    result_records = []
    
    for cycle_id in df_all_pandas['cycle_id'].unique():
        # 从 df_all 获取所有 'To QC' 的索引（包括 align_time 为空的）
        all_indices = sorted(df_all_pandas[df_all_pandas['cycle_id'] == cycle_id]['idx'].tolist())
        
        # 从 df_valid 获取有 timestamp 的记录
        valid_group = df_valid_pandas[df_valid_pandas['cycle_id'] == cycle_id]
        
        if len(valid_group) == 0:
            # 没有有效记录，跳过
            continue
        
        valid_indices = sorted(valid_group['idx'].tolist())
        
        logger.info(f"[Consecutive Filter] cycle_id={cycle_id}, all QC indices={all_indices}, valid indices={valid_indices}")
        
        # 检查是否有连续索引（基于所有 'To QC'，包括 align_time 为空的）
        has_consecutive = False
        if len(all_indices) >= 2:
            for i in range(len(all_indices) - 1):
                if all_indices[i+1] - all_indices[i] == 1:
                    has_consecutive = True
                    break
        
        if has_consecutive:
            # 找出有值的最大索引（从 valid_indices 中选择最大的）
            max_valid_idx = max(valid_indices)
            filtered = valid_group[valid_group['idx'] == max_valid_idx]
            logger.info(f"[Consecutive Filter] cycle_id={cycle_id}: Has consecutive, keeping only valid idx={max_valid_idx}")
        else:
            # 都不连续，保留所有有值的记录
            filtered = valid_group
            logger.info(f"[Consecutive Filter] cycle_id={cycle_id}: No consecutive, keeping all {len(valid_indices)} valid records")
        
        result_records.append(filtered)
    
    # 合并所有结果
    if result_records:
        df_result_pandas = pd.concat(result_records, ignore_index=True)
        df_result = pl.from_pandas(df_result_pandas)
        return df_result
    else:
        # 返回空 DataFrame（保持相同的 schema）
        return df.head(0)


with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,  # 由 Controller 触发
    start_date=pendulum.today("UTC").add(days=-1),
    catchup=False,
    default_args=default_args,
    tags=["dq", "worker", "ground_truth", "validation"],
    is_paused_upon_creation=False,
) as dag:

    @task
    def extract_claims(**context) -> Optional[str]:
        """
        Step 1: 从 MySQL 提取目标车辆的作业声明
        
        核心逻辑：
        1. 使用 shift_date 和 vehicle_ids 动态构建 SQL WHERE 条件
        2. 提取 subtask_type_1~8 和 ALIGN_STA_TIME_1~8_SUBTASK 列
        3. 使用 Polars Unpivot 将宽表转为长表（1 行/子任务）
        4. 时间转换：字符串 -> Datetime -> Unix Timestamp (Int64)
        5. 过滤：subtask_type == "To QC" AND timestamp IS NOT NULL
        
        Args:
            shift_date: 班次日期 (e.g., "2025-11-02")
            vehicle_ids: 车辆 ID 列表 (e.g., ["AT01", "AT02"])
        
        Returns:
            str: Polars DataFrame 序列化为 JSON（供下游任务使用）
        """
        logger = logging.getLogger("airflow.task")
        
        # 从 dag_run.conf 读取参数
        dag_run = context['dag_run']
        shift_date = dag_run.conf.get('target_shift_date', '')
        vehicle_ids = dag_run.conf.get('vehicle_list', [])
        
        logger.info(f"[Step 1] Extracting claims for shift_date={shift_date}, vehicles={vehicle_ids}")
        
        if not vehicle_ids:
            logger.warning("[Step 1] Empty vehicle_ids, skipping.")
            return None
        
        hook = MySqlHook(mysql_conn_id=CONN_ID_DATALOG)
        
        # 构建动态 SQL（使用 WHERE IN 子句）
        # 注意：vehicle_ids 已经是列表，需要转换为 SQL IN 格式
        vehicle_ids_sql = ", ".join([f"'{vid}'" for vid in vehicle_ids])
        
        sql = f"""
        SELECT
            vehicle_id,
            TRACTOR_CYCLE_ID as cycle_id,
            SUBTASK_TYPE_1, ALIGN_STA_TIME_1_SUBTASK,
            SUBTASK_TYPE_2, ALIGN_STA_TIME_2_SUBTASK,
            SUBTASK_TYPE_3, ALIGN_STA_TIME_3_SUBTASK,
            SUBTASK_TYPE_4, ALIGN_STA_TIME_4_SUBTASK,
            SUBTASK_TYPE_5, ALIGN_STA_TIME_5_SUBTASK,
            SUBTASK_TYPE_6, ALIGN_STA_TIME_6_SUBTASK,
            SUBTASK_TYPE_7, ALIGN_STA_TIME_7_SUBTASK,
            SUBTASK_TYPE_8, ALIGN_STA_TIME_8_SUBTASK
        FROM {TBL_SUBTARGET}
        WHERE shift_date = '{shift_date}'
          AND vehicle_id IN ({vehicle_ids_sql})
        """
        
        logger.info(f"[Step 1] SQL Query:\n{sql}")
        
        # 使用 Pandas 读取，然后转为 Polars（Airflow MySQL Hook 不直接支持 Polars）
        df_pandas = hook.get_pandas_df(sql)
        
        if df_pandas.empty:
            logger.warning(f"[Step 1] No data found for shift_date={shift_date}, vehicles={vehicle_ids}")
            return None
        
        logger.info(f"[Step 1] Fetched {len(df_pandas)} rows from MySQL")
        
        # 转换为 Polars
        df = pl.from_pandas(df_pandas)
        
        # Unpivot/Melt: 将 8 对 (SUBTASK_TYPE_N, ALIGN_STA_TIME_N_SUBTASK) 转为长表
        # 目标：每行一个子任务
        # 注意：Polars 0.19.x 使用 melt，0.20+ 使用 unpivot
        df_long = df.melt(
            id_vars=["vehicle_id", "cycle_id"],
            value_vars=[f"SUBTASK_TYPE_{i}" for i in range(1, 9)],
            variable_name="subtask_index",
            value_name="subtask_type",
        ).with_columns(
            # 提取数字索引（例如 "SUBTASK_TYPE_3" -> 3）
            pl.col("subtask_index").str.extract(r"(\d+)$", 1).cast(pl.Int32).alias("idx")
        )
        
        # 将对应的时间列 Join 回来
        # 构建时间列的长表
        df_time = df.melt(
            id_vars=["vehicle_id", "cycle_id"],
            value_vars=[f"ALIGN_STA_TIME_{i}_SUBTASK" for i in range(1, 9)],
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
        
        # 🔍 调试：查看 melt 后的数据
        logger.info(f"[DEBUG] df_merged shape: {df_merged.shape}")
        logger.info(f"[DEBUG] df_merged columns: {df_merged.columns}")
        logger.info(f"[DEBUG] subtask_type unique values: {df_merged['subtask_type'].unique().to_list()}")
        logger.info(f"[DEBUG] Sample rows (first 3):\n{df_merged.head(3)}")
        
        # 时间转换：字符串 -> Datetime -> Unix Timestamp (Int64, 秒)
        # MySQL 格式: '2025-12-21 09:32:02'
        df_merged = df_merged.with_columns(
            pl.col("align_time_str")
            .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False)
            .dt.epoch(time_unit="s")
            .cast(pl.Int64)
            .alias("unix_timestamp")
        )
        
        # 🔍 调试：查看时间转换后的数据
        logger.info(f"[DEBUG] Non-null unix_timestamp count: {df_merged.filter(pl.col('unix_timestamp').is_not_null()).shape[0]}")
        logger.info(f"[DEBUG] 'To QC' count: {df_merged.filter(pl.col('subtask_type') == 'To QC').shape[0]}")
        
        # 先筛选所有 'To QC'（包括 align_time 为空的，用于判断连续性）
        df_all_qc = df_merged.filter(pl.col("subtask_type") == "To QC")
        
        # 再筛选有 timestamp 的 'To QC'
        df_valid_qc = df_merged.filter(
            (pl.col("subtask_type") == "To QC") & (pl.col("unix_timestamp").is_not_null())
        )
        
        logger.info(f"[Step 1] Found {len(df_all_qc)} 'To QC' tasks (including null timestamps)")
        logger.info(f"[Step 1] Found {len(df_valid_qc)} 'To QC' tasks with valid timestamps")
        
        if df_valid_qc.is_empty():
            logger.warning("[Step 1] No valid 'To QC' tasks found after filtering")
            return None
        
        # 应用连续 'To QC' 筛选规则
        # 规则：基于所有 'To QC' 判断连续性，但只保留有 timestamp 的记录
        df_filtered = _apply_consecutive_qc_filter(df_all_qc, df_valid_qc, logger)
        
        # 选择需要的列
        df_filtered = df_filtered.select([
            "vehicle_id",
            "cycle_id",
            "unix_timestamp",
        ])
        
        logger.info(f"[Step 1] After consecutive filter: {len(df_filtered)} records")
        
        if df_filtered.is_empty():
            logger.warning("[Step 1] No records after consecutive filter")
            return None
        
        # 序列化为 JSON（供 XCom 传递）
        return df_filtered.write_json()

    @task
    def fetch_physical_truth(claims_json: Optional[str]) -> Optional[str]:
        """
        Step 2: 从 InfluxDB 获取物理层真相（实际位置/速度）
        
        核心逻辑：
        1. 反序列化 Step 1 的 DataFrame
        2. 遍历每行，查询 InfluxDB：[unix_timestamp - 1s, unix_timestamp + 1s]
        3. 聚合（MEAN）以降噪
        4. 将 actual_x, actual_y, actual_speed 追加到 DataFrame
        
        Args:
            claims_json: Step 1 输出的 JSON 字符串
        
        Returns:
            str: 更新后的 DataFrame (JSON)
        """
        logger = logging.getLogger("airflow.task")
        logger.info("[Step 2] Fetching physical truth from InfluxDB")
        
        if not claims_json:
            logger.warning("[Step 2] No claims data, skipping.")
            return None
        
        # 反序列化（使用 StringIO 因为 claims_json 是字符串，不是文件路径）
        df = pl.read_json(StringIO(claims_json))
        logger.info(f"[Step 2] Processing {len(df)} claims")
        
        # 🔍 调试：打印 InfluxDB 配置（不包括完整 Token）
        logger.info(f"[DEBUG] INFLUX_URL={INFLUX_URL}")
        logger.info(f"[DEBUG] INFLUX_ORG={INFLUX_ORG}")
        logger.info(f"[DEBUG] INFLUX_BUCKET={INFLUX_BUCKET}")
        logger.info(f"[DEBUG] INFLUX_TOKEN length={len(INFLUX_TOKEN)}, first 10 chars={INFLUX_TOKEN[:10]}")
        
        # 初始化 InfluxDB 客户端
        with InfluxClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
            bucket=INFLUX_BUCKET,
        ) as influx_client:
            
            # 构建查询列表
            queries = [
                (row["vehicle_id"], row["unix_timestamp"])
                for row in df.iter_rows(named=True)
            ]
            
            # 批量查询
            results = influx_client.query_batch(queries, window_seconds=1)
            
            # 提取结果
            actual_x = []
            actual_y = []
            actual_speed = []
            
            for result in results:
                if result:
                    actual_x.append(result.get("actual_x"))
                    actual_y.append(result.get("actual_y"))
                    actual_speed.append(result.get("actual_speed"))
                else:
                    actual_x.append(None)
                    actual_y.append(None)
                    actual_speed.append(None)
            
            # 追加列
            df = df.with_columns([
                pl.Series("actual_x", actual_x, dtype=pl.Float64),
                pl.Series("actual_y", actual_y, dtype=pl.Float64),
                pl.Series("actual_speed", actual_speed, dtype=pl.Float64),
            ])
        
        logger.info(f"[Step 2] Fetched physical truth for {len(df)} records")
        
        # 🔍 调试日志：打印有 InfluxDB 数据的样本记录
        df_with_data = df.filter(pl.col("actual_x").is_not_null())
        logger.info(f"[Step 2] Records with InfluxDB data: {len(df_with_data)} / {len(df)}")
        if len(df_with_data) > 0:
            logger.info(f"[DEBUG] Sample records with InfluxDB data (first 5):")
            logger.info(f"\n{df_with_data.head(5)}")
        
        return df.write_json()

    @task
    def fetch_semantic_truth(df_json: Optional[str]) -> Optional[str]:
        """
        Step 3: 从地图服务获取语义层真相（道路类型）
        
        核心逻辑：
        1. 反序列化 Step 2 的 DataFrame
        2. 按 vehicle_id 分组，构建批量请求
        3. 调用 Map API（批量模式）
        4. 将 map_road_type 追加到 DataFrame
        
        Args:
            df_json: Step 2 输出的 JSON 字符串
        
        Returns:
            str: 更新后的 DataFrame (JSON)
        """
        logger = logging.getLogger("airflow.task")
        logger.info("[Step 3] Fetching semantic truth from Map API")
        
        if not df_json:
            logger.warning("[Step 3] No data from Step 2, skipping.")
            return None
        
        # 反序列化（使用 StringIO 因为 df_json 是字符串，不是文件路径）
        df = pl.read_json(StringIO(df_json))
        logger.info(f"[Step 3] Processing {len(df)} records")
        
        # 初始化 Map 客户端
        map_client = MapClient(
            base_url=MAP_API_URL,
            port=MAP_PORT,
        )
        
        # 按 vehicle_id 分组，构建批量请求
        # 格式: {vehicle_id: [{"x": float, "y": float, "timestamp": int}, ...]}
        vehicle_points = {}
        
        for row in df.iter_rows(named=True):
            vehicle_id = row["vehicle_id"]
            
            # 跳过缺失坐标的记录
            if row["actual_x"] is None or row["actual_y"] is None:
                continue
            
            if vehicle_id not in vehicle_points:
                vehicle_points[vehicle_id] = []
            
            vehicle_points[vehicle_id].append({
                "x": row["actual_x"],
                "y": row["actual_y"],
                "timestamp": row["unix_timestamp"],
            })
        
        logger.info(f"[Step 3] Grouped into {len(vehicle_points)} vehicles for batch query")
        
        # 批量查询
        map_results = map_client.annotate_multiple_vehicles(vehicle_points)
        
        # 将结果映射回 DataFrame
        # 构建 (vehicle_id, unix_timestamp) -> road_type 的字典
        road_type_map = {}
        for vehicle_id, points in vehicle_points.items():
            road_types = map_results.get(vehicle_id, [])
            for i, point in enumerate(points):
                timestamp = point["timestamp"]
                road_type = road_types[i] if i < len(road_types) else None
                road_type_map[(vehicle_id, timestamp)] = road_type
        
        # 追加到 DataFrame
        map_road_types = [
            road_type_map.get((row["vehicle_id"], row["unix_timestamp"]))
            for row in df.iter_rows(named=True)
        ]
        
        df = df.with_columns(
            pl.Series("map_road_type", map_road_types, dtype=pl.Utf8)
        )
        
        logger.info(f"[Step 3] Fetched semantic truth for {len(df)} records")
        
        # 🔍 调试日志：打印有 Map API 数据的样本记录
        df_with_map = df.filter(pl.col("map_road_type").is_not_null())
        logger.info(f"[Step 3] Records with Map API data: {len(df_with_map)} / {len(df)}")
        if len(df_with_map) > 0:
            logger.info(f"[DEBUG] Sample records with Map API results (first 5):")
            logger.info(f"\n{df_with_map.head(5)}")
        
        return df.write_json()

    @task
    def validate_and_persist(df_json: Optional[str], **context) -> None:
        """
        Step 4: 使用 Pandera 验证规则并写入结果表
        
        核心逻辑：
        1. 反序列化 Step 3 的 DataFrame
        2. 定义 Pandera Schema（验证规则）：
           - Location: map_road_type MUST contain "QC"
           - Stationarity: actual_speed <= 0.5
        3. 执行验证
        4. 统计 Pass/Fail
        5. 写入结果表（包含 shift_date, vehicle_id）
        
        Args:
            df_json: Step 3 输出的 JSON 字符串
            shift_date: 班次日期（用于写入结果表）
            vehicle_ids: 车辆 ID 列表（用于写入结果表）
        """
        logger = logging.getLogger("airflow.task")
        logger.info("[Step 4] Validating and persisting results")
        
        # 从 dag_run.conf 读取参数
        dag_run = context['dag_run']
        shift_date = dag_run.conf.get('target_shift_date', '')
        vehicle_ids = dag_run.conf.get('vehicle_list', [])
        
        if not df_json:
            logger.warning("[Step 4] No data from Step 3, skipping.")
            # 写入空结果（避免 Controller 卡死）
            _write_empty_results(shift_date, vehicle_ids)
            return
        
        # 反序列化（使用 StringIO 因为 df_json 是字符串，不是文件路径）
        df_polars = pl.read_json(StringIO(df_json))
        logger.info(f"[Step 4] Validating {len(df_polars)} records")
        
        # 🔍 调试日志：打印进入验证阶段的样本数据
        logger.info(f"[DEBUG] Sample records entering validation (first 5):")
        logger.info(f"\n{df_polars.head(5)}")
        
        # 将 Polars DataFrame 转换为 Pandas DataFrame（Pandera 0.19.3 只支持 Pandas）
        df = df_polars.to_pandas()
        logger.info(f"[DEBUG] Converted to Pandas DataFrame for validation")
        
        # 定义 Pandera Schema
        schema = DataFrameSchema(
            {
                "vehicle_id": Column(str, nullable=False),
                "cycle_id": Column(str, nullable=True),
                "unix_timestamp": Column(int, nullable=False),
                "actual_x": Column(float, nullable=True),
                "actual_y": Column(float, nullable=True),
                "actual_speed": Column(
                    float,
                    nullable=True,
                    checks=[
                        # 静止性检查：actual_speed <= 0.5（仅对非 null 值检查）
                        Check.less_than_or_equal_to(MAX_SPEED_FOR_STATIONARY, name="stationarity_check"),
                    ],
                ),
                "map_road_type": Column(
                    str,
                    nullable=True,
                    checks=[
                        Check.str_contains("QC", name="location_must_be_qc"),
                    ],
                ),
            },
            strict=False,
        )
        
        # 执行验证（捕获失败记录）
        failed_indices_set = set()
        try:
            validated_df = schema.validate(df, lazy=True)
            logger.info(f"[Step 4] Validation passed for {len(validated_df)} records")
            
        except pa.errors.SchemaErrors as e:
            logger.warning(f"[Step 4] Validation failed: {e}")
            
            # 解析失败记录
            failure_cases = e.failure_cases
            logger.info(f"[Step 4] Failure cases summary:\n{failure_cases}")
            
            # 🔍 调试日志：显示失败记录的详细信息并写入明细表
            if failure_cases is not None and len(failure_cases) > 0:
                # 获取失败的行索引
                if "index" in failure_cases.columns:
                    failed_indices_set = set(failure_cases["index"].unique().tolist())
                    if failed_indices_set:
                        df_failed = df.loc[df.index.isin(failed_indices_set)]
                        logger.warning(f"[DEBUG] Failed records details (first 10):")
                        logger.warning(f"\n{df_failed.head(10)}")
                        
                        # 写入失败明细表
                        _write_failure_details(shift_date, df_failed, failure_cases)
        
        # 按 vehicle_id 分组统计
        import pandas as pd
        vehicle_stats = {}
        
        for vehicle_id in df['vehicle_id'].unique():
            df_vehicle = df[df['vehicle_id'] == vehicle_id]
            total_vehicle = len(df_vehicle)
            
            # 计算该车辆的失败记录数
            vehicle_failed_count = len([idx for idx in df_vehicle.index if idx in failed_indices_set])
            vehicle_passed_count = total_vehicle - vehicle_failed_count
            
            vehicle_stats[vehicle_id] = {
                'total': total_vehicle,
                'passed': vehicle_passed_count,
                'failed': vehicle_failed_count
            }
            
            logger.info(f"[Step 4] Vehicle {vehicle_id}: Total={total_vehicle}, Passed={vehicle_passed_count}, Failed={vehicle_failed_count}")
        
        # 总体统计（用于日志）
        total_all = len(df)
        failed_all = len(failed_indices_set)
        passed_all = total_all - failed_all
        logger.info(f"[Step 4] Overall summary: Total={total_all}, Passed={passed_all}, Failed={failed_all}")
        
        # 写入结果表（按车辆）
        _write_results(shift_date, vehicle_stats)

    def _write_empty_results(shift_date: str, vehicle_ids: List[str]) -> None:
        """写入空结果（避免 Controller 卡死）"""
        logger = logging.getLogger("airflow.task")
        hook = MySqlHook(mysql_conn_id=CONN_ID_DATALOG)
        
        for vehicle_id in vehicle_ids:
            sql = f"""
            INSERT INTO {TBL_RESULT} 
            (shift_date, vehicle_id, total_records, passed_records, failed_records, created_at)
            VALUES ('{shift_date}', '{vehicle_id}', 0, 0, 0, NOW())
            ON DUPLICATE KEY UPDATE
                total_records = 0,
                passed_records = 0,
                failed_records = 0,
                created_at = NOW()
            """
            hook.run(sql)
        
        logger.info(f"[Step 4] Written empty results for {len(vehicle_ids)} vehicles")

    def _write_results(shift_date: str, vehicle_stats: Dict[str, Dict[str, int]]) -> None:
        """
        写入验证结果到 MySQL（按车辆分组）
        
        Args:
            shift_date: 班次日期
            vehicle_stats: 车辆统计字典，格式：
                {
                    'AT01': {'total': 10, 'passed': 8, 'failed': 2},
                    'AT02': {'total': 15, 'passed': 15, 'failed': 0},
                    ...
                }
        """
        logger = logging.getLogger("airflow.task")
        hook = MySqlHook(mysql_conn_id=CONN_ID_DATALOG)
        
        for vehicle_id, stats in vehicle_stats.items():
            total = stats['total']
            passed = stats['passed']
            failed = stats['failed']
            
            sql = f"""
            INSERT INTO {TBL_RESULT} 
            (shift_date, vehicle_id, total_records, passed_records, failed_records, created_at)
            VALUES ('{shift_date}', '{vehicle_id}', {total}, {passed}, {failed}, NOW())
            ON DUPLICATE KEY UPDATE
                total_records = {total},
                passed_records = {passed},
                failed_records = {failed},
                updated_at = NOW()
            """
            hook.run(sql)
        
        logger.info(f"[Step 4] Written results for {len(vehicle_stats)} vehicles")

    def _write_failure_details(shift_date: str, df_failed, failure_cases) -> None:
        """写入验证失败的明细记录到 MySQL"""
        import pandas as pd
        logger = logging.getLogger("airflow.task")
        hook = MySqlHook(mysql_conn_id=CONN_ID_DATALOG)
        
        # 合并失败记录和失败原因
        # failure_cases 包含：index, column, check, failure_case
        # df_failed 包含：vehicle_id, cycle_id, unix_timestamp, actual_x, actual_y, actual_speed, map_road_type
        
        # 为 df_failed 添加索引作为列（用于 merge）
        df_failed_with_idx = df_failed.copy()
        df_failed_with_idx['_idx'] = df_failed_with_idx.index
        
        # 为 failure_cases 添加索引列
        failure_cases_with_idx = failure_cases.copy()
        failure_cases_with_idx.rename(columns={'index': '_idx'}, inplace=True)
        
        # 合并
        df_merged = pd.merge(
            df_failed_with_idx,
            failure_cases_with_idx[['_idx', 'column', 'check']],
            on='_idx',
            how='left'
        )
        
        logger.info(f"[Step 4] Writing {len(df_merged)} failure detail records")
        
        # 逐行写入明细表
        for _, row in df_merged.iterrows():
            # 判断失败类型和期望条件
            check_name = row.get('check', 'unknown')
            column_name = row.get('column', 'unknown')
            
            if check_name == 'location_must_be_qc':
                failure_type = 'road_type_mismatch'
                expected_condition = 'map_road_type 应包含 "QC"'
                actual_value = str(row.get('map_road_type', 'NULL'))
            elif check_name == 'stationarity_check':
                failure_type = 'speed_violation'
                expected_condition = f'actual_speed 应 <= {MAX_SPEED_FOR_STATIONARY}'
                actual_value = str(row.get('actual_speed', 'NULL'))
            else:
                failure_type = f'unknown_{column_name}'
                expected_condition = f'{column_name} 验证失败（{check_name}）'
                actual_value = str(row.get(column_name, 'NULL'))
            
            # SQL 转义处理
            def escape_sql(value):
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    return 'NULL'
                return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'"  # 单引号转义
            
            vehicle_id = escape_sql(row.get('vehicle_id'))
            cycle_id = escape_sql(row.get('cycle_id'))
            unix_timestamp = row.get('unix_timestamp', 0)
            actual_x = row.get('actual_x') if pd.notna(row.get('actual_x')) else None
            actual_y = row.get('actual_y') if pd.notna(row.get('actual_y')) else None
            actual_speed = row.get('actual_speed') if pd.notna(row.get('actual_speed')) else None
            map_road_type = escape_sql(row.get('map_road_type'))
            
            sql = f"""
            INSERT INTO {TBL_FAILURE_DETAIL}
            (shift_date, vehicle_id, cycle_id, unix_timestamp, 
             failure_type, expected_condition, actual_value,
             actual_x, actual_y, actual_speed, map_road_type, created_at)
            VALUES (
                '{shift_date}', 
                {vehicle_id}, 
                {cycle_id}, 
                {unix_timestamp},
                '{failure_type}',
                {escape_sql(expected_condition)},
                {escape_sql(actual_value)},
                {actual_x if actual_x is not None else 'NULL'},
                {actual_y if actual_y is not None else 'NULL'},
                {actual_speed if actual_speed is not None else 'NULL'},
                {map_road_type},
                NOW()
            )
            """
            
            try:
                hook.run(sql)
            except Exception as e:
                logger.error(f"[Step 4] Failed to write failure detail: {e}")
                logger.error(f"[Step 4] SQL: {sql}")
        
        logger.info(f"[Step 4] Finished writing failure details")

    # DAG 任务流
    # 四步验证流程（参数在 Task 内部从 context 读取）
    claims_json = extract_claims()
    physical_json = fetch_physical_truth(claims_json)
    semantic_json = fetch_semantic_truth(physical_json)
    validate_and_persist(semantic_json)

