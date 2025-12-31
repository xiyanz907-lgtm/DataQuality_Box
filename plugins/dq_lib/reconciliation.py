import polars as pl
import logging

# 设置日志记录器
logger = logging.getLogger(__name__)

class ReconciliationEngine:
    """
    负责 nGen 系统与 Cactus 系统的数据清洗、聚合与匹配的核心算法类。
    """

    @staticmethod
    def aggregate_ngen(df_ngen: pl.DataFrame, timezone: str = "Asia/Hong_Kong") -> pl.DataFrame:
        """
        清洗并聚合 nGen 数据。

        逻辑:
        1. 类型转换: Tractor_Cycle_Id -> Int64, On/Off_Chasis_Datetime -> Datetime
        2. 时区转换: Local Time -> UTC (标准化)
        3. 聚合: 按 Tractor_Cycle_Id 分组, 计算 Start/End Time, Vehicle ID, Container Nos

        Args:
            df_ngen: nGen 原始数据 DataFrame
            timezone: nGen 数据所在的本地时区 (e.g. "Asia/Hong_Kong", "Africa/Cairo")

        Returns:
            pl.DataFrame: 聚合后的 nGen 数据 (时间均为 UTC)
        """
        # 定义 nGen 聚合后的标准 Schema
        agg_schema = {
            "cycle_id": pl.Int64,
            "real_start_time": pl.Datetime(time_zone="UTC"),
            "real_end_time": pl.Datetime(time_zone="UTC"),
            "vehicle_id": pl.Utf8,
            "ref_cnt_large": pl.Utf8,
            "ref_cnt_small_list": pl.List(pl.Utf8)
        }

        # 防御性编程: 检查输入是否为空
        if df_ngen is None or df_ngen.is_empty():
            logger.warning("aggregate_ngen: 输入 DataFrame 为空")
            # 返回带有正确 Schema 的空 DataFrame，防止后续步骤因找不到列报错
            return pl.DataFrame(schema=agg_schema)

        logger.info(f"开始处理 nGen 数据, 输入行数: {df_ngen.height}")

        try:
            # 1. 数据清洗 (Data Cleaning)
            # 使用 lazy 模式可能更高效，但在数据量不大时 eager 也可以。这里保持 eager 以便于调试和错误捕获。
            
            # 定义时间解析格式: "11/12/2025 21:58:59" -> %d/%m/%Y %H:%M:%S
            time_fmt = "%d/%m/%Y %H:%M:%S"

            df_clean = df_ngen.with_columns([
                # Tractor_Cycle_Id 转为 Int64, 转换失败设为 null (strict=False)
                pl.col("Tractor_Cycle_Id").cast(pl.Int64, strict=False),
                
                # 解析时间字段 (Naive -> Local Aware -> UTC)
                pl.col("On_Chasis_Datetime")
                  .str.strptime(pl.Datetime, format=time_fmt, strict=False)
                  .dt.replace_time_zone(timezone) # 标记为本地时区
                  .dt.convert_time_zone("UTC")    # 转换为 UTC
                  .alias("start_time_parsed"),
                
                pl.col("Off_Chasis_Datetime")
                  .str.strptime(pl.Datetime, format=time_fmt, strict=False)
                  .dt.replace_time_zone(timezone)
                  .dt.convert_time_zone("UTC")
                  .alias("end_time_parsed"),
                  
                # Tractor_No 清洗 (Normalization)
                # 提取数字 -> Int -> String -> zfill(2) -> "AT" + ...
                pl.format("AT{}", 
                    pl.col("Tractor_No").str.replace_all(r"[^0-9]", "")
                      .cast(pl.Int64, strict=False)
                      .cast(pl.Utf8)
                      .str.zfill(2)
                ).alias("normalized_vehicle_id"),

                # [新增] Cntr_Id 清洗 (Normalization)
                # 1. 转大写
                # 2. 去除所有非字母和数字的字符 (去除空格、横杠、乱码符号)
                # 结果将用于后续聚合
                pl.col("Cntr_Id")
                  .str.to_uppercase()
                  .str.replace_all(r"[^A-Z0-9]", "")
                  .alias("clean_cntr_id")
            ])

            # 2. 聚合逻辑 (Aggregation)
            # 按 Tractor_Cycle_Id 分组
            # 逻辑调整:
            # 1. 计算 min_start, max_end
            # 2. 收集 ref_cnt_large (40/45尺)
            # 3. 收集 ref_cnt_small_list (20尺列表)
            
            # [修改] 防御性编程: 箱号校验
            # 使用 clean_cntr_id。
            # 校验逻辑: 长度必须为 10 或 11 位 (ISO标准是11位，兼容10位)。
            # 同时确保前4位是字母 (ISO标准)。
            
            valid_cntr_expr = (
                (pl.col("clean_cntr_id").str.len_chars().is_in([10, 11])) &
                (pl.col("clean_cntr_id").str.contains(r"^[A-Z]{4}"))
            )

            df_agg = df_clean.group_by("Tractor_Cycle_Id").agg([
                # cycle_id: 取 Key
                pl.col("Tractor_Cycle_Id").first().alias("cycle_id"),

                # real_start_time: Min(On_Chasis_Datetime)
                pl.col("start_time_parsed").min().alias("real_start_time"),

                # real_end_time: Max(Off_Chasis_Datetime)
                pl.col("end_time_parsed").max().alias("real_end_time"),

                # vehicle_id: 使用清洗后的 normalized_vehicle_id
                pl.col("normalized_vehicle_id").first().alias("vehicle_id"),

                # ref_cnt_large: 40/45尺, 取第1个 (业务上应该只有1个)
                pl.col("clean_cntr_id")
                  .filter(
                      (pl.col("Cntr_Length_In_Feet").is_in(["40", "45"])) & 
                      valid_cntr_expr
                  )
                  .unique()
                  .sort()
                  .first()
                  .alias("ref_cnt_large"),

                # ref_cnt_small_list: 20尺列表, 去重排序
                pl.col("clean_cntr_id")
                  .filter(
                      (pl.col("Cntr_Length_In_Feet") == "20") & 
                      valid_cntr_expr
                  )
                  .unique()
                  .sort()
                  .alias("ref_cnt_small_list")
            ])

            # 过滤掉 Key 为空的行 (如果 Tractor_Cycle_Id 转换失败)
            df_agg = df_agg.filter(pl.col("cycle_id").is_not_null())

            # [新增] 再次防御: 如果过滤后变为空，确保返回正确的 Schema
            if df_agg.is_empty():
                 # 使用之前定义的 agg_schema 重新构造空 DataFrame，以防 filter 导致 Schema 丢失 (虽不常见但保险)
                 # 注意: pl.DataFrame(schema=...) 会创建一个空的且带类型的 DF
                 # 只要列名匹配即可
                 return pl.DataFrame(schema=agg_schema)

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
        nGen 聚合数据与 Cactus 数据进行分级匹配 (Reconciliation)。

        逻辑:
        1. 清洗 Cactus 数据
        2. Level 1: 严格匹配 (Strict Match)
           - join_asof, tolerance=5m
        3. Level 2: 宽松匹配 (Loose Match) - 针对 Level 1 未匹配的数据
           - join (on vehicle_id), time_diff <= 3h, container_check (any match)
        4. 识别 Cactus Only (sync_status=2) 和 Orphan nGen (Log only)

        Args:
            df_ngen_agg: nGen 聚合后的 DataFrame (Left)
            df_cactus: Cactus 原始数据 DataFrame (Right)

        Returns:
            dict: {
                'matched': pl.DataFrame (包含 matched_status=1 和 matched_status=2/4 的记录),
                'orphaned_ngen': pl.DataFrame (仅做记录)
            }
        """
        if df_ngen_agg is None or df_ngen_agg.is_empty():
            logger.warning("match_data: nGen 聚合数据为空")
            # 如果没有 nGen 数据，所有 Cactus 数据都无法验证，理论上如果这里是为了 verify Cactus，应该全标记为 2？
            # 但如果 nGen 没拉到数据，可能是提取问题。暂时返回空。
            return {'matched': pl.DataFrame(), 'orphaned_ngen': pl.DataFrame()}
        
        if "vehicle_id" not in df_ngen_agg.columns:
             logger.error("match_data: nGen 数据缺少 vehicle_id 列")
             return {'matched': pl.DataFrame(), 'orphaned_ngen': pl.DataFrame()}

        if df_cactus is None or df_cactus.is_empty():
            logger.warning("match_data: Cactus 数据为空")
            # Cactus 为空，所有 nGen 都是 orphans
            return {'matched': pl.DataFrame(), 'orphaned_ngen': df_ngen_agg}

        logger.info(f"开始数据匹配: nGen({df_ngen_agg.height}) vs Cactus({df_cactus.height})")

        try:
            # 1. Cactus 清洗 (Data Cleaning)
            iso_fmt = "%Y-%m-%d %H:%M:%S"
            df_cactus_clean = df_cactus.with_columns([
                pl.col("_time").str.slice(0, 19)
                  .str.strptime(pl.Datetime, format=iso_fmt, strict=False)
                  .dt.replace_time_zone("UTC") # 假设 Cactus 已经是 UTC Naive，标记为 UTC
                  .alias("_time_cleaned"),
                pl.col("_time_begin").str.slice(0, 19)
                  .str.strptime(pl.Datetime, format=iso_fmt, strict=False)
                  .dt.replace_time_zone("UTC")
                  .alias("_time_begin_cleaned"),
                pl.col("cycleId").cast(pl.Int64, strict=False)
            ]).rename({"vehicleId": "vehicle_id"})

            # -------------------------------------------------------------------------
            # Level 1: 严格匹配 (Strict Match, 5 mins)
            # -------------------------------------------------------------------------
            df_ngen_sorted = df_ngen_agg.sort(["vehicle_id", "real_end_time"])
            df_cactus_sorted = df_cactus_clean.sort(["vehicle_id", "_time_cleaned"])

            # Left Join: nGen -> Cactus
            df_l1_raw = df_ngen_sorted.join_asof(
                df_cactus_sorted,
                left_on="real_end_time",
                right_on="_time_cleaned",
                by="vehicle_id",
                strategy="nearest",
                tolerance="5m"
            )

            # 分离 L1 成功和失败
            matched_mask_l1 = pl.col("_time_cleaned").is_not_null()
            df_l1_success = df_l1_raw.filter(matched_mask_l1).with_columns(pl.lit(1).alias("matched_status"))
            
            # L1 失败的 nGen 数据 (保留左表原始列，去除右表 join 产生的 null 列)
            # 这里的 drop 需要只 drop 右表的列。
            # 简单起见，我们重新 select 左表的列。
            ngen_cols = df_ngen_agg.columns
            df_l1_failed = df_l1_raw.filter(matched_mask_l1.not_()).select(ngen_cols)

            logger.info(f"Level 1 (Strict) 匹配结果: 成功 {df_l1_success.height}, 待处理 {df_l1_failed.height}")

            if df_l1_failed.is_empty():
                return {'matched': df_l1_success, 'orphaned_ngen': pl.DataFrame()}

            # -------------------------------------------------------------------------
            # Level 2: 宽松匹配 (Loose Match, 3 hours + Container Check)
            # -------------------------------------------------------------------------
            # Cross Join / Join on Vehicle ID (因为时间范围宽，不能用 asof)
            # 注意：Cactus 数据可能在 L1 已经被用过了，是否允许复用？
            # 业务上：一个 Cactus 记录只能对应一个 nGen 记录。
            # 严谨做法：应从 df_cactus_sorted 中排除掉已经被 L1 匹配掉的 ID。
            
            l1_matched_cactus_ids = df_l1_success.select("id").unique()
            df_cactus_remain = df_cactus_sorted.join(l1_matched_cactus_ids, on="id", how="anti")
            
            if df_cactus_remain.is_empty():
                 return {'matched': df_l1_success, 'orphaned_ngen': df_l1_failed}

            # 执行 Join on vehicle_id
            # 警告：如果不加时间限制直接 join，会产生笛卡尔积。
            # Polars 建议：Join 后 filter
            
            df_l2_candidates = df_l1_failed.join(df_cactus_remain, on="vehicle_id", how="inner")
            
            # 计算时间差 (abs)
            df_l2_candidates = df_l2_candidates.with_columns(
                (pl.col("real_end_time") - pl.col("_time_cleaned")).abs().alias("time_diff")
            )
            
            # 筛选 1: 时间窗口 <= 3小时
            df_l2_time_ok = df_l2_candidates.filter(pl.col("time_diff") <= pl.duration(hours=3))
            
            # 筛选 2: 箱号校验
            # nGen: ref_cnt_large, ref_cnt_small_list
            # Cactus: cnt01, cnt02, cnt03
            # 逻辑: 任意一边非空且相等
            
            def check_container_match_expr():
                # 辅助: 构造 Cactus 箱号列表列
                return (
                    pl.col("ref_cnt_large").is_in([pl.col("cnt01"), pl.col("cnt02"), pl.col("cnt03")]) |
                    pl.col("ref_cnt_small_list").list.set_intersection(
                        pl.concat_list([pl.col("cnt01"), pl.col("cnt02"), pl.col("cnt03")])
                    ).list.len() > 0
                )

            # 由于 Polars 列表操作较复杂，这里简化逻辑：
            # 只要 nGen 的 ref_cnt_large 等于 Cactus 的任一 cnt，或者 ref_cnt_small_list 中包含 Cactus 的任一 cnt
            # 考虑到数据清洗质量，这里做简单的字符包含或相等检查
            
            # 暂时简化：只要时间对上，就认为是 Candidate，然后按时间最近排序取 Top 1
            # 补充需求明确提到需要“箱号匹配”。
            
            # 实现箱号匹配过滤
            # 1. 构造 Cactus 侧的箱号集合 (Array)
            # 2. 检查交集
            # [Fix] 预先处理 null 值，避免 concat_list 后 fill_null("") 导致的类型错误
            
            df_l2_checked = df_l2_time_ok.with_columns([
                 pl.concat_list([
                     pl.col("cnt01").fill_null(""), 
                     pl.col("cnt02").fill_null(""), 
                     pl.col("cnt03").fill_null("")
                 ]).alias("cactus_cnts")
            ]).filter(
                (pl.col("ref_cnt_large").is_in(pl.col("cactus_cnts"))) |
                (pl.col("ref_cnt_small_list").list.set_intersection(pl.col("cactus_cnts")).list.len() > 0)
            )

            # 选出最佳匹配 (每个 nGen 记录只能匹配一个 Cactus，取时间最近的)
            df_l2_best = df_l2_checked.sort(["cycle_id", "time_diff"]).unique(subset=["cycle_id"], keep="first")
            
            # 添加状态标记 4 (Loose Match)
            df_l2_success = df_l2_best.with_columns(pl.lit(4).alias("matched_status"))
            
            # -------------------------------------------------------------------------
            # 结果合并与处理
            # -------------------------------------------------------------------------
            
            # 1. L1 Success
            # 2. L2 Success (Drop temp columns)
            cols_to_keep = df_l1_success.columns # 保持一致
            df_l2_final = df_l2_success.select([c for c in cols_to_keep if c in df_l2_success.columns])
            
            # 合并所有匹配
            df_matched_all = pl.concat([df_l1_success, df_l2_final], how="diagonal")
            
            # 找出 Orphaned nGen (L1 Failed 且不在 L2 Success 中)
            # 使用 Anti Join
            matched_cycle_ids = df_l2_final.select("cycle_id")
            df_orphaned_ngen = df_l1_failed.join(matched_cycle_ids, on="cycle_id", how="anti")
            
            # 找出 Cactus Only (在输入 Cactus 中，但未被任何 nGen 匹配)
            # 这里的输入 df_cactus 是本次 Reconcile 范围内的所有 Cactus 数据
            # [Fix] 使用 df_cactus_clean 而不是原始 df_cactus，以确保包含 _time_cleaned 等列
            all_matched_cactus_ids = df_matched_all.select("id").unique()
            df_cactus_unmatched = df_cactus_clean.join(all_matched_cactus_ids, on="id", how="anti")
            
            # 将 Cactus Only 标记为 sync_status=2 (Target Only)
            # 为了返回统一格式，这里需要构造一个 DataFrame，包含 update 所需的列
            # 对于 sync_status=2，我们需要 update set sync_status=2。
            # 它没有 cycle_id (nGen ID)，所以 cycle_id 为 null
            
            df_cactus_only_update = df_cactus_unmatched.select([
                pl.col("id"),
                pl.lit(None).cast(pl.Int64).alias("cycle_id"),
                pl.lit(None).alias("ref_cnt_large"), # 不回填箱号
                pl.lit(None).alias("ref_cnt_small_list"),
                pl.lit(2).alias("matched_status")
            ])
            
            # 最终合并 Matched (1/4) 和 Cactus Only (2)
            # [修改] 保留更多列用于 Metrics 计算
            # 注意: df_matched_all 来自 L1/L2，包含 nGen 和 Cactus 列
            # df_cactus_unmatched (Cactus Only) 只有 Cactus 列
            
            metrics_cols = [
                "id", "cycle_id", "matched_status", 
                "ref_cnt_large", "ref_cnt_small_list",
                "real_end_time", "_time_cleaned", 
                "cnt01", "cnt02", "cnt03", 
                "vehicle_id"
            ]
            
            # 1. Matched 部分: 直接选择 (列都存在)
            df_matched_updates = df_matched_all.select(
                [c for c in metrics_cols if c in df_matched_all.columns]
            )
            
            # 2. Cactus Only 部分: 需要构造 nGen 相关的空列
            # 它来自 df_cactus_unmatched (包含 _time_cleaned, cnt01... 和 id)
            # 需要补充: cycle_id, ref_cnt_large, ref_cnt_small_list, real_end_time, matched_status=2
            
            df_cactus_only_update = df_cactus_unmatched.select([
                pl.col("id"),
                pl.col("vehicle_id"),
                pl.col("_time_cleaned"),
                pl.col("cnt01"), pl.col("cnt02"), pl.col("cnt03"),
                
                # 补充 nGen 空列
                pl.lit(None).cast(pl.Int64).alias("cycle_id"),
                pl.lit(None).cast(pl.Utf8).alias("ref_cnt_large"),
                pl.lit(None).cast(pl.List(pl.Utf8)).alias("ref_cnt_small_list"),
                pl.lit(None).cast(pl.Datetime(time_zone="UTC")).alias("real_end_time"), # 注意时区
                
                pl.lit(2).alias("matched_status")
            ])
            
            # 确保列顺序一致以便 concat (diagonal 模式其实不强求顺序，但为了 metrics_cols 完整性)
            df_final_updates = pl.concat(
                [df_matched_updates, df_cactus_only_update], 
                how="diagonal"
            )

            logger.info(f"匹配综述: L1(Strict)={df_l1_success.height}, L2(Loose)={df_l2_final.height}, CactusOnly(2)={df_cactus_only_update.height}")

            return {
                'matched': df_final_updates,
                'orphaned_ngen': df_orphaned_ngen
            }

        except Exception as e:
            logger.error(f"match_data 执行失败: {str(e)}")
            raise e

