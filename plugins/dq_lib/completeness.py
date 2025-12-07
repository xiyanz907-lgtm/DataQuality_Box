import polars as pl

class CompletenessChecks:
    
    @staticmethod
    def check_matching_rate(df_target, df_source, target_key, source_key):
        """
        通用：计算匹配率/丢失率 (Completeness)
        逻辑：检查 df_target (如 nGen) 中的 ID 是否都存在于 df_source (如 自采) 中
        
        参数:
        df_target: 基准表 (nGen - 真值)
        df_source: 待测表 (Cactus - 自采)
        target_key: 基准表的ID列名
        source_key: 待测表的ID列名
        """
        # 找出 Target 有，但 Source 没有的记录 (Anti Join)
        missing_df = df_target.join(
            df_source, left_on=target_key, right_on=source_key, how="anti"
        )
        
        total_rows = df_target.height
        missing_count = missing_df.height
        
        # 计算丢失率 (保留4位小数)
        loss_rate = round(missing_count / total_rows, 4) if total_rows > 0 else 0.0
        
        # 判定标准：比如丢失率必须 < 1% (这里写死或者后续做成参数)
        is_passed = loss_rate < 0.01 

        return {
            "type": "completeness_matching_rate",
            "passed": is_passed,
            "total_target_rows": total_rows,
            "missing_count": missing_count,
            "loss_rate": loss_rate,
            # 打印前 5 个丢失的 ID，方便排查
            "missing_samples": missing_df.select([target_key]).head(5).to_dicts()
        }