import os
import polars as pl

# 导入我们写好的工具库
from dq_lib.consistency import ConsistencyChecks
from dq_lib.completeness import CompletenessChecks

# 从环境变量读取阈值，如果没有则默认 300秒
THRESHOLD = int(os.getenv("THRESHOLD_TIME_DIFF", 300))

def get_logic_rules(df_self, df_ngen=None):
    """
    cnt_cycles 表的逻辑校验规则集合
    
    参数:
    df_self: 自采数据 (cnt_cycles)
    df_ngen: nGen数据 (已在 Runner 层做过白名单过滤和时间清洗)
    """
    results = []

    # =========================================================
    # 场景 A: 必须有 nGen 数据才能做的对比 (跨表校验)
    # =========================================================
    if df_ngen is not None and df_ngen.height > 0:
        
        # 1. 【完整性检查】: nGen 的作业，我们漏采了吗？
        # 关注点：nGen 的 Tractor_Cycle_Id 是否都在我们的 cycleId 里
        res_comp = CompletenessChecks.check_matching_rate(
            df_target=df_ngen,      # 基准：nGen
            df_source=df_self,      # 待测：自采
            target_key='Tractor_Cycle_Id',
            source_key='cycleId'
        )
        results.append(res_comp)

        # 2. 【一致性检查】: 匹配上的作业，结束时间差是否在阈值内？
        # 关注点：_time_end vs Off_Chasis_Datetime
        # 注意：这里调用的是我们刚才确认过的包含 year replace 的逻辑
        res_cons = ConsistencyChecks.check_time_consistency(
            df_left=df_self,
            df_right=df_ngen,
            left_key='cycleId',
            right_key='Tractor_Cycle_Id',
            left_col='_time_end',           # 自采结束时间
            right_col='Off_Chasis_Datetime', # nGen结束时间
            threshold=THRESHOLD
        )
        results.append(res_cons)

    else:
        # 如果 nGen 没数据 (可能是 Runner 没取到，或者当天真没作业)
        # 我们可以返回一个警告，或者跳过
        print("⚠️ Warning: nGen 数据为空，跳过跨表对比规则。")

    # =========================================================
    # 场景 B: 只需要自己就能做的逻辑 (单表逻辑校验)
    # =========================================================
    # 比如：虽然 cnt_cycles 是一张离散表，但如果你想检查单车作业是否时序倒流
    # 也可以在这里调用 check_chain_continuity
    # (根据你的需求，cnt_cycles 好像暂时不需要测连续性，这里先留空)
    
    # 示例：如果要测
    # res_chain = ConsistencyChecks.check_chain_continuity(
    #     df=df_self,
    #     group_col='vehicleId',
    #     sort_col='_time_begin',
    #     current_time_col='_time_begin',
    #     prev_time_col='_time_end'
    # )
    # results.append(res_chain)

    return results