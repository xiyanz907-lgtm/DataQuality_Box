import pandera as pa
# from pandera import DataFrameSchema, Column, Check

CntNewCyclesSchema = pa.DataFrameSchema(
    columns={
        "id": pa.Column(int, required=True),        
        # ... (其他字段)
    },
    strict=False,
    coerce=True
)