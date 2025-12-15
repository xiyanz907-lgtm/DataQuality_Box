import polars as pl
import logging

logger = logging.getLogger(__name__)

class MetricsEngine:
    """
    负责计算 Reconciliation 后的数据质量指标，并生成轻量级报告。
    不涉及数据库写入。
    """

    @staticmethod
    def calculate_report(df_matched: pl.DataFrame) -> dict:
        """
        基于匹配后的数据计算质量指标。

        Args:
            df_matched: 包含 nGen 和 Cactus 列的 DataFrame。
                        必须包含: 'real_end_time', '_time_cleaned', 
                        'ref_cnt_large', 'ref_cnt_small_list', 'cnt01', 'cnt02', 'cnt03', 'matched_status'

        Returns:
            dict: 统计摘要和异常明细
        """
        if df_matched is None or df_matched.is_empty():
            return {
                "total_count": 0,
                "perfect_count": 0,
                "avg_time_diff_sec": 0,
                "consistency_stats": {},
                "errors": []
            }

        try:
            # 1. 预处理：计算时间差 (秒)
            # 使用 UTC 时间列进行比较
            # real_end_time 和 _time_cleaned 应该已经是 Datetime 类型 (UTC)
            
            # [Defensive] 确保列存在
            required_cols = ["real_end_time", "_time_cleaned"]
            for col in required_cols:
                if col not in df_matched.columns:
                    logger.warning(f"MetricsEngine: Missing column {col}, skipping metrics.")
                    return {}

            # 计算 Time Diff (seconds)
            # 注意: matched=2 (Target Only) 的行 real_end_time 为 null，diff 为 null
            df_calcs = df_matched.with_columns([
                (pl.col("real_end_time") - pl.col("_time_cleaned")).dt.total_seconds().abs().alias("time_diff_sec"),
            ])

            # 2. 箱号一致性判定
            # 逻辑: 
            # - 构造 Cactus 箱号列表 (去除空值)
            # - 构造 nGen 箱号列表 (ref_cnt_large + ref_cnt_small_list)
            # - 计算交集
            
            # 辅助: 处理 null 列表
            # 如果 matched=2, nGen 列可能为 null
            
            df_calcs = df_calcs.with_columns([
                pl.concat_list([
                    pl.col("cnt01").fill_null(""), 
                    pl.col("cnt02").fill_null(""), 
                    pl.col("cnt03").fill_null("")
                ]).list.eval(pl.element().filter(pl.element() != "")).alias("cactus_cnts"),
                
                # nGen List: ref_cnt_large (str) -> list, + ref_cnt_small_list (list)
                pl.concat_list([
                    pl.col("ref_cnt_large").fill_null(""),
                ]).list.eval(pl.element().filter(pl.element() != "")).alias("ngen_large_list")
            ])
            
            # 合并 nGen 列表
            # 注意: ref_cnt_small_list 可能是 null
            df_calcs = df_calcs.with_columns([
                 pl.concat_list([
                     pl.col("ngen_large_list"),
                     pl.col("ref_cnt_small_list").fill_null([])
                 ]).list.eval(pl.element().flatten()).alias("ngen_cnts")
            ])
            
            # 判定状态
            # MATCH: nGen 和 Cactus 集合有交集 (简化逻辑: 只要有一个箱号对上就算 Match? 还是必须全对?)
            # 业务通常要求: 只要 nGen 记录的箱号在 Cactus 里找到了，或者 Cactus 的箱号在 nGen 里找到了。
            # 严格一点: set(ngen) == set(cactus)
            # 宽松一点(Intersection): len(intersection) > 0
            
            # 定义状态:
            # - MATCH: 交集 > 0
            # - MISMATCH: 交集 = 0 且 两边都不为空
            # - NULL_NGEN: nGen 为空 (且 matched != 2)
            # - NULL_CACTUS: Cactus 为空
            # - TARGET_ONLY: matched=2
            
            def get_status_expr():
                return (
                    pl.when(pl.col("matched_status") == 2).then(pl.lit("TARGET_ONLY"))
                    .when(pl.col("ngen_cnts").list.len() == 0).then(pl.lit("NULL_NGEN"))
                    .when(pl.col("cactus_cnts").list.len() == 0).then(pl.lit("NULL_CACTUS"))
                    .when(
                        pl.col("ngen_cnts").list.set_intersection(pl.col("cactus_cnts")).list.len() > 0
                    ).then(pl.lit("MATCH"))
                    .otherwise(pl.lit("MISMATCH"))
                ).alias("cntr_status")

            df_calcs = df_calcs.with_columns(get_status_expr())
            
            # 3. 判定 Perfect
            # Perfect = (Status == MATCH) AND (Time Diff <= 300s)
            df_calcs = df_calcs.with_columns(
                ((pl.col("cntr_status") == "MATCH") & (pl.col("time_diff_sec") <= 300)).alias("is_perfect")
            )
            
            # --- 统计汇总 ---
            
            total_count = df_calcs.height
            perfect_count = df_calcs.filter(pl.col("is_perfect")).height
            
            # 平均时间差 (只统计 matched=1/4 的)
            avg_diff = df_calcs.filter(pl.col("matched_status").is_in([1, 4])) \
                               .select(pl.col("time_diff_sec").mean()).item()
            
            # 箱号状态分布
            stats_df = df_calcs.group_by("cntr_status").count()
            consistency_stats = {row[0]: row[1] for row in stats_df.iter_rows()}
            
            # --- 提取异常明细 (Top 20) ---
            # 优先级: MISMATCH > Time Diff > NULL
            
            # 筛选非 Perfect 且非 Target Only (Target Only 太多且已知，可视情况是否放进异常)
            # 假设我们只关心 "本来应该匹配上但出了问题" 的
            
            error_df = df_calcs.filter(
                (pl.col("is_perfect").not_()) & (pl.col("matched_status") != 2)
            ).sort("time_diff_sec", descending=True).head(20)
            
            errors = []
            for row in error_df.iter_rows(named=True):
                issue = []
                if row["cntr_status"] != "MATCH":
                    issue.append(f"Cntr {row['cntr_status']}")
                if row["time_diff_sec"] is not None and row["time_diff_sec"] > 300:
                    issue.append(f"TimeDiff {int(row['time_diff_sec'])}s")
                
                errors.append({
                    "id": row.get("id"), # Cactus ID
                    "vehicle": row.get("vehicle_id"),
                    "issue": ", ".join(issue),
                    "ngen_cnts": str(row.get("ngen_cnts")),
                    "cactus_cnts": str(row.get("cactus_cnts"))
                })

            return {
                "total_count": total_count,
                "perfect_count": perfect_count,
                "avg_time_diff_sec": round(avg_diff, 2) if avg_diff is not None else 0,
                "consistency_stats": consistency_stats,
                "errors": errors
            }

        except Exception as e:
            logger.error(f"Metrics calculation failed: {e}")
            # 即使统计失败，不要阻断主流程
            return {"error": str(e)}
