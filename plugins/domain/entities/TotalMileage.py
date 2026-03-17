from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

@dataclass
class TotalMileage:
    """
    TotalMileage实体
    """
    vehicle_id: str         # 车辆号
    site_type: str          # 站点类型
    shift_date: datetime    # 批次日期
    mileage_distance: float          # 里程