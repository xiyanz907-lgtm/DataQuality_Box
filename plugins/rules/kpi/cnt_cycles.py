import os
import polars as pl
# 导入我们在 L1 层写好的通用工具
from dq_lib.completeness import CompletenessChecks
from dq_lib.consistency import ConsistencyChecks
from dq_lib.distribution import DistributionChecks

# 1. 定义配置信息 (给 Runner 看的)
# Runner 看到这个 CONFIG，就知道运行前要去拉取 nGen 的数据
CONFIG = {
    "need_reference": True,           # 需要参考数据
    "reference_source": "ngen",       # 参考源是 nGen
    "reference_table": "ngen",        # nGen 里的表名 (数据库是 hutchisonports)
    "join_key_target": "Tractor_Cycle_Id", # nGen 的关联键
    "join_key_source": "cycleId"           # 自采表的关联键
}

# 从环境变量读取阈值，默认 300秒
THRESHOLD = int(os.getenv("THRESHOLD_TIME_DIFF", 300))

# 2. 定义业务规则逻辑 (给 Runner 调用的入口函数)
def get_logic_rules(df_self, df_ref=None):
    """
    组装 cnt_cycles 的所有逻辑校验规则
    :param df_self: 自采数据 (cnt_cycles)
    :param df_ref:  参考数据 (nGen) - 由 Runner 根据 CONFIG 自动拉取并传入
    """
    results = []

    # --- 场景 A: 跨表校验 ---
    if df_ref is not None and df_ref.height > 0:
        
        # 【规则 1】完整性检查：匹配率/丢失率
        # 使用工具库里的 check_matching_rate
        res_comp = CompletenessChecks.check_matching_rate(
            df_target=df_ref,       # 基准：nGen
            df_source=df_self,      # 待测：自采
            target_key='Tractor_Cycle_Id',
            source_key='cycleId'
        )
        results.append(res_comp)

        # 【规则 2】一致性检查：时间差是否在阈值内
        # 使用工具库里的 check_time_consistency
        res_cons = ConsistencyChecks.check_time_consistency(
            df_left=df_self,
            df_right=df_ref,
            left_key='cycleId',
            right_key='Tractor_Cycle_Id',
            left_col='_time_end',
            right_col='Off_Chasis_Datetime',
            threshold=THRESHOLD
        )
        results.append(res_cons)

        #  # 3. 一致性 - 箱号 (Container No)
        #
        # res_cnt = ConsistencyChecks.check_field_equality(
        #     df_left=df_self,
        #     df_right=df_ref,
        #     left_key='cycleId',
        #     right_key='Tractor_Cycle_Id',
        #     left_col='cnt01',        # 自采箱号字段
        #     right_col='Container_No' # nGen箱号字段
        # )
        # results.append(res_cnt)

    else:
        # 如果 nGen 没数据，或者没传进来，记录一条警告
        results.append({
            "type": "meta_check", 
            "passed": False, 
            "msg": "Reference data (nGen) is empty or missing, skipping cross-checks."
        })

    # --- 场景 B: 单表逻辑  ---
    # 3-Sigma 异常检测
    if df_self is not None and df_self.height > 0:
        # 计算作业时长（确保时间字段为 datetime）
        try:
            df_typed = df_self.with_columns(
                pl.col('_time_end').cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False),
                pl.col('_time_begin').cast(pl.String).str.slice(0, 19).str.to_datetime(strict=False),
            )
        except Exception as e:
            results.append({
                "type": "distribution_3sigma_duration_sec",
                "passed": False,
                "msg": f"时间字段解析失败: {e}"
            })
            return results

        df_calc = df_typed.with_columns(
            (pl.col('_time_end') - pl.col('_time_begin')).dt.total_seconds().alias('duration_sec')
        ).filter(pl.col('duration_sec') > 0)

        if df_calc.height > 5:
            res_dist = DistributionChecks.check_3sigma_outliers(
                df=df_calc,
                col_name='duration_sec',
                threshold_sigma=3
            )
            results.append(res_dist)
        else:
            # 样本不足时也返回说明，便于日志可见
            results.append({
                "type": "distribution_3sigma_duration_sec",
                "passed": True,
                "msg": f"样本数不足以计算3-sigma（有效行: {df_calc.height}）"
            })

    return results