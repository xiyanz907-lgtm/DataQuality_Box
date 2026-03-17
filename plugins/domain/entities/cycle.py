from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

@dataclass
class Cycle:
    """
    标准作业圈实体 (Standard Domain Entity)
    所有源数据 (MySQL/Influx) 最终都必须清洗成这个样子

    定位：领域契约文档 + 可选的 Schema 校验基准

    - 本文件定义 Adapter 输出的核心字段、类型和业务含义
    - Adapter 可以产出本文件未定义的派生字段（如聚合指标、窗口计算列）
    - 编写 Rule YAML 时，以本文件为字段参考
    - 未来可作为 Adapter 输出的 Schema 校验依据（安检层）

    相关文件：
    - 字段映射配置：configs/adapters/cycle_adapter.yaml
    - 数据提取配置：configs/sources/daily_cycle_etl.yaml
    """
    # ================= 原始字段（直接从数据库映射） =================
    vehicle_id: str         # 车辆号
    cycle_id: str           # 全局唯一ID
    box: int          # 箱数
    shift_date: datetime    # 批次日期
    start_time: datetime    # 统一 UTC
    end_time: datetime
    duration: float         # 单位: 秒 (起止时间预计算字段)
    task_type: str          # 作业类型 (VSLD/VSDS)
    running_time: float     # 单圈作业时长（min）
    mileage: float          # 单圈作业里程（km）
    power_consumption: float          # 单圈电耗
    ckp_3to4_total_time: float          # checkpoint 3to4段总时间
    ckp_7to8_total_time: float          # checkpoint 7to8段总时间
    is_twin_lift: bool      # 是否双箱 (True/False)
    subtask_type_1: str          # 子任务1
    page_time_1_subtask: float          # 子任务1开始时间
    subtask_type_2: str          # 子任务2
    page_time_2_subtask: float          # 子任务2开始时间
    subtask_type_3: str          # 子任务3
    page_time_3_subtask: float          # 子任务3开始时间
    subtask_type_4: str          # 子任务4
    page_time_4_subtask: float          # 子任务4开始时间
    subtask_type_5: str          # 子任务5
    page_time_5_subtask: float          # 子任务5开始时间
    subtask_type_6: str          # 子任务6
    page_time_6_subtask: float          # 子任务6开始时间
    subtask_type_7: str          # 子任务7
    page_time_7_subtask: float          # 子任务7开始时间
    subtask_type_8: str          # 子任务8
    page_time_8_subtask: float          # 子任务8开始时间
    total_speed: float          # 行驶速度
    total_travel_speed: float          # 行驶速度
    total_size: float       # 单圈作业时长（s）
    mileage_all: float          # 累计里程
    power_per_km: float          # 每公里电耗
    power_per_box: float          # 每个box电耗
    idle_time: float          # 空闲时长（h）

    # ================= 行级派生字段 =================
    non_null_subtask_count: int          # 非空子任务数（统计 subtask_type_1~8 中非空的个数）
    non_null_page_time_count: int        # 非空子任务开始时间数（统计 page_time_1~8 中非空的个数）

    # ================= 组级聚合字段 (跨行计算，同组内所有行值相同） =================
    # 由 Adapter 通过 Polars .over() 窗口函数计算，广播回每一行
    # daily_total_box: int               # SUM(box) OVER (vehicle_id, shift_date) — 同车同天总箱数
    # daily_total_mileage: float         # SUM(mileage) OVER (vehicle_id, shift_date) — 同车同天总里程
    daily_total_box: int               # SUM(box) OVER (shift_date) — 同天总箱数
    daily_total_mileage: float         # SUM(mileage) OVER (shift_date) — 同天总里程
    daily_total_mileage_all: float         # SUM(mileage_all) OVER (shift_date) — 同天总累计里程
    daily_total_idle_time: float         # SUM(idle_time) OVER (shift_date) — 同天总空闲时长
    daily_avg_power_per_km: float         # AVG(power_per_km) OVER (shift_date) — 同天平均每公里电耗
    daily_avg_power_per_box: float         # AVG(power_per_box) OVER (shift_date) — 同天平均每个box电耗

    # --- 差异字段 ---
    diff_box_count_ngen: int          # Checkpoint log和ngen箱数差异
    diff_box_count1: int          # Checkpoint log和Cycle log箱数差异
    diff_box_count2: int          # Checkpoint log和Daily_report箱数差异
    diff_mileage: float          # Checkpoint log和Daily_report作业里程差异（km）

    # --- 扩展属性 ---
    # 存放非标字段或一致性校验的 Diff (e.g. {"mileage_diff": 0.5})
    extra_attributes: Dict[str, any]