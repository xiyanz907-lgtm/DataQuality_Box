import polars as pl

class ConsistencyChecks:
    
    @staticmethod
    def check_time_consistency(df_left, df_right, left_key, right_key, left_col, right_col, threshold=300):
        """
        通用：双表时间差检查 (跨库/跨表)
        包含业务逻辑：强制对齐年份
        
        参数:
        df_left: 左表（通常是自采数据，作为基准）
        df_right: 右表（nGen数据）
        left_key/right_key: 关联键 (如 cycleId / Tractor_Cycle_Id)
        left_col/right_col: 时间列 (如 _time_end / Off_Chasis_Datetime)
        threshold: 时间差阈值（秒）
        """
        # 1. 连接两个 DataFrame
        joined = df_left.join(
            df_right, left_on=left_key, right_on=right_key, how="inner", suffix="_right"
        )
        
        total_matched = joined.height

        if total_matched == 0:
            return {
                "type": "consistency_time_diff",
                "passed": True, # 没有数据也算通过，或者根据业务定为 False
                "total_matched": 0,
                "failed": 0,
                "failed_samples": []
            }

        # 2. 计算时间差的绝对值
        # 核心逻辑：nGen年份可能错误，强制使用左表年份替换右表年份
        result_df = joined.with_columns(
            (pl.col(left_col) - pl.col(right_col).dt.replace(year=pl.col(left_col).dt.year()))
            .dt.total_seconds().abs().alias("time_diff_sec")
        )
        
        # 3. 过滤出时间差超过阈值的记录
        violations = result_df.filter(
            pl.col("time_diff_sec") > threshold
        )
        
        failed_count = violations.height
        passed_count = total_matched - failed_count

        return {
            "type": "consistency_time_diff",
            "total_matched": total_matched,
            "passed": failed_count == 0,
            "passed_count": passed_count,
            "failed": failed_count,
            # 日志打印异常样本 TOP10
            # 注意：如果 right_col 和 left_col 同名，Polars 会给右边的加 suffix="_right"
            # 这里假设传入的列名不同，或者你只关心原始列名
            "failed_samples": violations.select([left_key, "time_diff_sec", left_col, right_col]).head(10).to_dicts()
        }

    @staticmethod
    def check_chain_continuity(df, group_col, sort_col, current_time_col, prev_time_col, id_col=None):
        """
        通用：单表时间链连续性检查
        
        参数:
        df: 数据表
        group_col: 分组字段（如 vehicleId）
        sort_col: 排序字段（通常是 本条记录的开始时间）
        current_time_col: 用于显示的当前时间字段
        prev_time_col: 上一条记录的结束时间字段 (用于和 sort_col 比较)
        id_col: (可选) 用于追踪上一条记录的 ID
        """
        if df.height == 0:
            return {
                "type": "continuity_chain_check",
                "passed": True,
                "total_matched": 0,
                "failed": 0
            }

        # 1. 对每个分组进行排序
        df_sorted = df.sort([group_col, sort_col])
        
        # 2. 计算逻辑：获取上一行的结束时间
        # shift(1) 获取上一行数据，over(group_col) 保证不跨车计算
        cols_to_add = [
            pl.col(prev_time_col).shift(1).over(group_col).alias("prev_end_time")
        ]
        
        # 如果需要追踪上一行的 ID
        if id_col:
            cols_to_add.append(pl.col(id_col).shift(1).over(group_col).alias("prev_id"))
            
        result_df = df_sorted.with_columns(cols_to_add)

        # 3. 识别不连续点
        # 异常条件：
        # A. 上一行有数据 (is_not_null，排除每辆车的第一条记录)
        # B. 当前行开始时间 (sort_col) != 上一行结束时间 (prev_end_time)
        violation_count = result_df.filter(
            (pl.col("prev_end_time").is_not_null()) &
            (pl.col(sort_col) != pl.col("prev_end_time"))
        )

        failed_count = violation_count.height
        total_count = df.height

        # 4. 构造返回列，方便排查
        select_cols = [group_col]
        if id_col:
            select_cols.append(id_col)
            select_cols.append("prev_id")

        # 把当前开始时间、上一条结束时间都打出来对比
        select_cols.extend([current_time_col, "prev_end_time"])

        # 返回结果
        return {
            "type": "continuity_chain_check",
            "total_matched": total_count,
            "passed": failed_count == 0,
            "failed": failed_count,
            "failed_samples": violation_count.select(select_cols).head(10).to_dicts()
        }