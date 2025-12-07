import polars as pl

class ConsistencyChecks:
    @staticmethod
    def check_time_consistency(df_left,df_right,left_key,right_key,left_col,right_col,threshold=300):
        """
        通用：双表时间差检查
        
        参数:
        df_left: 左表（自采）
        df_right: 右表（nGen）
        left/right_key: 关联键
        left/right_col: 时间列
        threshold: 时间差阈值（秒），超过该值视为不一致
        
        返回:
        包含不一致记录的DataFrame
        """
        # 连接两个DataFrame
        joined = df_left.join(
            df_right, left_on=left_key, right_on=right_key, how="inner", suffix="_right"
        )
        
        total_matched = joined.height

        # 计算时间差的绝对值
        # result_df = joined.with_columns([
        #     (pl.col(left_col) - pl.col(right_col)).dt.total_seconds().abs().alias("time_diff_sec")
        # ])
        result_df = joined.with_columns(
            (pl.col(left_col) - pl.col(right_col).dt.replace(year=pl.col(left_col).dt.year()))
            .dt.total_seconds().abs().alias("time_diff_sec")
        )
        
        # 过滤出时间差超过阈值的记录
        violations = result_df.filter(
            pl.col("time_diff_sec") > threshold
        )
        
        failed_count = violations.height
        passed_count = total_matched - failed_count

        return{
            "type" : "consistency_time_diff",
            "total_matched" : total_matched,
            "passed" : passed_count,
            "failed" : failed_count,
            #日志打印异常样本TOP10
            "failed_samples" : violations.select([left_key,"time_diff_sec",left_col,right_col]).head(10)
        }

    @staticmethod
    def check_chain_continuity(df,group_col,sort_col,current_time_col,prev_time_col,id_col=None):
        """
        通用：单表时间链连续性检查
        
        参数:
        df: 数据表
        group_col: 分组字段（如vehicleID）
        sort_col: 排序字段（如开始时间）
        prev_time_col: 结束时间字段
        
        返回:
        包含不连续记录的DataFrame
        """
        # 对每个分组进行排序并计算前一条记录的结束时间
        df_sorted = df.sort([group_col, sort_col])
        
        # 计算逻辑：获取上一行的结束时间
        # shift(1) 获取上一行数据
        cols_to_add = [
            pl.col(prev_time_col).shift(1).over(group_col).alias("prev_end_time")
        ]
        # 拿上一行的ID
        if id_col:
            cols_to_add.append(pl.col(id_col).shift(1).over(group_col).alias("prev_id"))
        # over(group_col) = 按group_col分组计算（如vehicleID，不跨车）
        result_df = df_sorted.with_columns(
            cols_to_add
        )

        # 识别不连续点
        # 异常条件（cnt_newcycles为例）：A.上一行有数据 is_not_null  B.当前行开始时间 ！= 上一行结束时间
        violation_count = result_df.filter(
            (pl.col("prev_end_time").is_not_null()) &
            (pl.col(sort_col) != pl.col("prev_end_time"))
        )

        failed_count = violation_count.height
        total_count = df.height

        # 构造返回列
        select_cols = [group_col]
        if id_col:
            select_cols.append(id_col)
            select_cols.append("prev_id")

        select_cols.extend([current_time_col, "prev_end_time"])

        # 返回结果
        return {
            "type": "cotinuity_chain_check",
            "total_matched": total_count,
            "passed": total_count - failed_count,
            "failed": failed_count,
            "failed_samples": violation_count.select(select_cols).head(10)
        }