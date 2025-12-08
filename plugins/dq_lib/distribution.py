import polars as pl

class DistributionChecks:
    
    @staticmethod
    def check_3sigma_outliers(df, col_name, threshold_sigma=3):
        """
        基于 3-Sigma 准则检测异常值 (Outliers)
        
        :param df: Polars DataFrame
        :param col_name: 数值列名
        :param threshold_sigma: 几倍标准差？默认 3 (覆盖 99.7% 的数据)
        """
        # 1. 预处理：剔除空值
        clean_df = df.select([pl.col("cycleId"), pl.col(col_name)]).drop_nulls()
        
        if clean_df.height < 2:
            return {
                "type": f"distribution_3sigma_{col_name}",
                "passed": True,
                "msg": "样本过少，无法计算标准差"
            }

        # 2. 计算统计量 (平均值 Mean, 标准差 StdDev)
        stats = clean_df.select([
            pl.col(col_name).mean().alias("mean"),
            pl.col(col_name).std().alias("std")
        ]).to_dicts()[0]
        
        mean_val = stats['mean']
        std_val = stats['std']
        
        # 3. 计算 3-Sigma 上下界
        # 对于作业时长，我们通常只关心“太慢”(Upper Bound)，不太关心“太快”(Lower Bound)
        # 但为了通用，我们还是算出界限
        upper_bound = mean_val + (threshold_sigma * std_val)
        lower_bound = mean_val - (threshold_sigma * std_val)

        # 4. 找出异常值 (Outliers)
        outliers = clean_df.filter(
            (pl.col(col_name) > upper_bound) | 
            (pl.col(col_name) < lower_bound)
        )
        
        outlier_count = outliers.height
        total_count = clean_df.height
        outlier_ratio = outlier_count / total_count
        
        # 5. 判定结果
        # 工业经验：如果有超过 5% 的数据都是 3-Sigma 异常，说明系统极不稳定
        # 或者你也可以仅仅把它作为 Info 输出，永远返回 True (Pass)
        is_passed = outlier_ratio < 0.05 

        return {
            "type": f"distribution_3sigma_{col_name}",
            "passed": is_passed,
            "total_samples": total_count,
            "mean": round(mean_val, 2),
            "std_dev": round(std_val, 2),
            "upper_limit": round(upper_bound, 2), # 超过这个值的都算异常慢
            "outlier_count": outlier_count,
            "outlier_ratio": f"{outlier_ratio:.2%}",
            # 打印 Top 5 最慢的作业
            "failed_samples": outliers.sort(col_name, descending=True).head(5).to_dicts()
        }