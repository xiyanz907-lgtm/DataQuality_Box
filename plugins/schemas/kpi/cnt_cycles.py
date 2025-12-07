import pandera as pa
#from pandera import Column, Check, DataFrameSchema

# 定义 Schema
CntCyclesSchema = pa.DataFrameSchema(
    columns={
        # === 基础信息 ===
        "id": pa.Column(int, required=True),
        # "cycleId": pa.Column(int, nullable=True),
        
        # === 时间相关  ===
        "_time": pa.Column(str, nullable=True),
        "_time_begin": pa.Column(str, nullable=True),
        "_time_end": pa.Column(str, nullable=True),
        # "last_modified_timestamp": pa.Column(str, nullable=True),

        # # === 车辆/设备信息 ===
        # "vehicleId": pa.Column(str, nullable=True),
        # "vesselVisitID": pa.Column(str, nullable=True),
        # "vesselTosID": pa.Column(str, nullable=True),
        
        # # === 状态/标志位  ===
        # "haveDeliver": pa.Column(int, checks=pa.Check.isin([0, 1]), nullable=True),
        # "haveRevieve": pa.Column(int, checks=pa.Check.isin([0, 1]), nullable=True),
        # "matched": pa.Column(int, checks=pa.Check.isin([0, 1]), nullable=True),
        
        # # === 箱号 ===
        # "cnt01": pa.Column(str, nullable=True),
        # "cnt02": pa.Column(str, nullable=True),
        # "cnt03": pa.Column(str, nullable=True),
        
        # # === 数量指标 ===
        # "num20": pa.Column(int, nullable=True, coerce=True),
        # "num40": pa.Column(int, nullable=True, coerce=True),
        # "num45": pa.Column(int, nullable=True, coerce=True),
    },
    strict=False, # 允许表里有Schema未定义的列（设为True则报错）
    coerce=True   # 自动尝试类型转换
)