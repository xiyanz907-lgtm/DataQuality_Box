import polars as pl
import logging

# 设置日志记录器
logger = logging.getLogger(__name__)

class ReconciliationEngine:
    """
    负责 nGen 系统与 Cactus 系统的数据清洗、聚合与匹配的核心算法类。
    """

    @staticmethod
    def aggregate_ngen(df_ngen: pl.DataFrame) -> pl.DataFrame:
        """
        清洗并聚合 nGen 数据。

        逻辑:
        1. 类型转换: Tractor_Cycle_Id -> Int64, On/Off_Chasis_Datetime -> Datetime
        2. 聚合: 按 Tractor_Cycle_Id 分组, 计算 Start/End Time, Vehicle ID, Container Nos

        Args:
            df_ngen: nGen 原始数据 DataFrame

        Returns:
            pl.DataFrame: 聚合后的 nGen 数据
        """
        # 防御性编程: 检查输入是否为空
        if df_ngen is None or df_ngen.is_empty():
            logger.warning("aggregate_ngen: 输入 DataFrame 为空")
            return pl.DataFrame()

        logger.info(f"开始处理 nGen 数据, 输入行数: {df_ngen.height}")

        try:
            # 1. 数据清洗 (Data Cleaning)
            # 使用 lazy 模式可能更高效，但在数据量不大时 eager 也可以。这里保持 eager 以便于调试和错误捕获。
            
            # 定义时间解析格式: "11/12/2025 21:58:59" -> %d/%m/%Y %H:%M:%S
            time_fmt = "%d/%m/%Y %H:%M:%S"

            df_clean = df_ngen.with_columns([
                # Tractor_Cycle_Id 转为 Int64, 转换失败设为 null (strict=False)
                pl.col("Tractor_Cycle_Id").cast(pl.Int64, strict=False),
                
                # 解析时间字段
                pl.col("On_Chasis_Datetime")
                  .str.strptime(pl.Datetime, format=time_fmt, strict=False)
                  .alias("start_time_parsed"),
                
                pl.col("Off_Chasis_Datetime")
                  .str.strptime(pl.Datetime, format=time_fmt, strict=False)
                  .alias("end_time_parsed"),
                  
                # Tractor_No 清洗 (Normalization)
                # 提取数字 -> Int -> String -> zfill(2) -> "AT" + ...
                # 步骤:
                # 1. 替换非数字字符为空
                # 2. cast Int64 (自动去除前导0)
                # 3. cast Utf8
                # 4. zfill(2)
                # 5. format "AT{}"
                pl.format("AT{}", 
                    pl.col("Tractor_No").str.replace_all(r"[^0-9]", "")
                      .cast(pl.Int64, strict=False)
                      .cast(pl.Utf8)
                      .str.zfill(2)
                ).alias("normalized_vehicle_id")
            ])

            # 2. 聚合逻辑 (Aggregation)
            # 按 Tractor_Cycle_Id 分组
            df_agg = df_clean.group_by("Tractor_Cycle_Id").agg([
                # cycle_id: 取 Key (这里其实就是 Tractor_Cycle_Id，重命名即可)
                pl.col("Tractor_Cycle_Id").first().alias("cycle_id"),

                # real_start_time: Min(Start Time)
                pl.col("start_time_parsed").min().alias("real_start_time"),

                # real_end_time: Max(End Time)
                pl.col("end_time_parsed").max().alias("real_end_time"),

                # vehicle_id: 使用清洗后的 normalized_vehicle_id
                pl.col("normalized_vehicle_id").first().alias("vehicle_id"),

                # container_nos: Join(Container_No) -> 使用逗号连接
                # 假设 Container_No 是字符串，使用 implode().list.join(",")
                pl.col("Container_No").implode().list.join(",").alias("container_nos")
            ])

            # 过滤掉 Key 为空的行 (如果 Tractor_Cycle_Id 转换失败)
            df_agg = df_agg.filter(pl.col("cycle_id").is_not_null())

            logger.info(f"nGen 数据聚合完成, 输出行数: {df_agg.height}")
            return df_agg

        except Exception as e:
            logger.error(f"aggregate_ngen 执行失败: {str(e)}")
            # 根据 "不要删除" 和 "防御性编程" 原则，这里可以抛出异常或者返回空表
            # 既然是核心算法，抛出异常让上层处理可能更合适
            raise e

    @staticmethod
    def match_data(df_ngen_agg: pl.DataFrame, df_cactus: pl.DataFrame) -> dict:
        """
        nGen 聚合数据与 Cactus 数据进行匹配 (Reconciliation)。

        逻辑:
        1. Cactus 清洗: 时间截取与转换
        2. 排序: 两表均按 [vehicle_id, end_time] 排序
        3. 匹配: join_asof (nearest, tolerance=5m)

        Args:
            df_ngen_agg: nGen 聚合后的 DataFrame (Left)
            df_cactus: Cactus 原始数据 DataFrame (Right)

        Returns:
            dict: {'matched': df, 'unmatched': df}
        """
        # 防御性编程
        if df_ngen_agg is None or df_ngen_agg.is_empty():
            logger.warning("match_data: nGen 聚合数据为空")
            return {'matched': pl.DataFrame(), 'unmatched': pl.DataFrame()}
        
        if df_cactus is None or df_cactus.is_empty():
            logger.warning("match_data: Cactus 数据为空")
            # 如果 Right 表为空，则所有 Left 表数据都未匹配
            return {'matched': pl.DataFrame(), 'unmatched': df_ngen_agg}

        logger.info(f"开始数据匹配: nGen({df_ngen_agg.height}) vs Cactus({df_cactus.height})")

        try:
            # 1. Cactus 清洗 (Data Cleaning)
            # _time (结束时间) 和 _time_begin: 截取前19位 ("2025-11-01 00:00:22.187015" -> "2025-11-01 00:00:22")
            # 然后转为 Datetime
            
            # 注意: 这里假设是 ISO 格式 (YYYY-MM-DD HH:MM:SS)
            iso_fmt = "%Y-%m-%d %H:%M:%S"

            df_cactus_clean = df_cactus.with_columns([
                pl.col("_time").str.slice(0, 19)
                  .str.strptime(pl.Datetime, format=iso_fmt, strict=False)
                  .alias("_time_cleaned"),
                
                pl.col("_time_begin").str.slice(0, 19)
                  .str.strptime(pl.Datetime, format=iso_fmt, strict=False)
                  .alias("_time_begin_cleaned")
            ])

            # 2. 排序 (Sorting)
            # nGen: [vehicle_id, real_end_time]
            # Cactus: [vehicleId, _time]
            # join_asof 要求 on 字段必须排序
            
            df_ngen_sorted = df_ngen_agg.sort(["vehicle_id", "real_end_time"])
            df_cactus_sorted = df_cactus_clean.sort(["vehicleId", "_time_cleaned"])

            # 3. 匹配 (Matching - join_asof)
            # Left: df_ngen_agg
            # Right: df_cactus
            # By: vehicle_id vs vehicleId
            # On: real_end_time vs _time_cleaned
            # Strategy: nearest
            # Tolerance: 5m
            
            # 注意: join_asof 默认是 left join
            df_matched_raw = df_ngen_sorted.join_asof(
                df_cactus_sorted,
                left_on="real_end_time",
                right_on="_time_cleaned",
                left_by="vehicle_id",
                right_by="vehicleId",
                strategy="nearest",
                tolerance="5m"
            )

            # 4. 分离 Matched 和 Unmatched
            # 如果匹配成功，来自 Right 表的字段 (如 cycleId, _time_cleaned) 不会为空
            # 我们可以用 Right 表的一个非空字段来判断，比如 cycleId (int) 或者 _time_cleaned
            
            # 获取 Right 表的列名，以便检查
            # 注意: join_asof 后，列名可能会有后缀如果冲突，但这里 left_on/right_on 不同，by 不同
            # Right columns should be present.
            
            # 使用 _time_cleaned 来判断是否匹配成功
            matched_mask = pl.col("_time_cleaned").is_not_null()
            
            df_matched = df_matched_raw.filter(matched_mask)
            df_unmatched = df_matched_raw.filter(matched_mask.not_())

            logger.info(f"匹配完成: 成功 {df_matched.height}, 失败 {df_unmatched.height}")

            return {
                'matched': df_matched,
                'unmatched': df_unmatched
            }

        except Exception as e:
            logger.error(f"match_data 执行失败: {str(e)}")
            raise e

