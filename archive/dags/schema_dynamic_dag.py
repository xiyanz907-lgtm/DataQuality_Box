from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pendulum
import sys
import os

# 路径配置
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
plugins_dir = os.path.join(project_root, 'plugins')
if plugins_dir not in sys.path:
    sys.path.append(plugins_dir)

# ======================================================
# 1. 导入 Schema 
# ======================================================
from schemas.kpi.cnt_cycles_check import CntCyclesSchema
from schemas.kpi.cnt_newcycles import CntNewCyclesSchema
from services.data_validator import run_pandera_validation

# ======================================================
# 2. 配置区 
# ======================================================
TABLES_TO_CHECK = [
    {
        "table_name": "bidata.cnt_cycles_check",   
        "schema_obj": CntCyclesSchema,            
        "task_id": "cnt_cycles_check",                  
        "batch_size": int(os.getenv("BATCH_SIZE", 50000))
    },
    {
        "table_name": "bidata.cnt_newcycles",
        "schema_obj": CntNewCyclesSchema,
        "task_id": "new_cnt_cycles_check",
        "batch_size": int(os.getenv("BATCH_SIZE", 50000))
    }
]

default_args = {
    'owner': 'box_admin',
    'start_date': pendulum.today('UTC').add(days=-1),
}

# ======================================================
# 3. 通用执行函数 
# ======================================================
def _validate_wrapper(table_name, schema_obj, task_id, batch_size, **context):
    conn_id = 'cactus_mysql_conn'
    hook = MySqlHook(mysql_conn_id=conn_id)

    # schema_watermark
    watermark_var_name = f"schema_watermark_{task_id}"
    
    # 1. 获取上次处理到的 ID
    last_id = int(Variable.get(watermark_var_name, default_var=60258))
    
    # 2. 查询该表当前最大 ID
    max_id_sql = f"SELECT MAX(id) FROM {table_name}"
    current_max_id = hook.get_first(max_id_sql)[0]
    
    if current_max_id is None:
        print(f"表 {table_name} 为空，跳过。")
        return

    if current_max_id <= last_id:
        print(f"表 {table_name} 没有新数据 (Last: {last_id}, Max: {current_max_id})，跳过。")
        return

    # 3. 计算本批次目标 ID
    target_id = min(current_max_id, last_id + batch_size)
    print(f"[{task_id}] 本次校验范围: ID ({last_id}, {target_id}]")

    # 4. 执行校验
    result = run_pandera_validation(
        conn_id=conn_id,
        table_name=table_name,
        schema_obj=schema_obj,
        custom_where=f"id > {last_id} AND id <= {target_id}",
        sql_limit=None
    )
    
    # 5. 更新水位线

    Variable.set(watermark_var_name, target_id)
    print(f"🔄 [{task_id}] 水位线已更新为: {target_id}")

    if target_id < current_max_id:
        print(f"⚠️ 还有追赶数据，下次继续。")

    if result['status'] == 'FAILED':
        raise ValueError(f"[{table_name}] Validation Failed! Found {result['error_count']} errors.")

    elif result['status'] == 'SKIPPED':
        Variable.set(watermark_var_name, target_id)
        print(f"⚠️ [{task_id}] 无区间数据，已跳过。")

    else:
        print(f"✅ [{task_id}] Validation Succeeded for ID up to {target_id}.")

# ======================================================
# 4. 动态生成 DAG
# ======================================================
# with DAG(
#     'schema_quality_check_dynamic',
#     default_args=default_args,
#     schedule='*/30 * * * *',
#     catchup=False,
#     is_paused_upon_creation=True,
#     tags=['quality', 'schema', 'dynamic']
# ) as dag:

#     for config in TABLES_TO_CHECK:
#         PythonOperator(
#             task_id=f"check_{config['task_id']}", 
#             python_callable=_validate_wrapper,
#             op_kwargs={
#                 "table_name": config["table_name"],
#                 "schema_obj": config["schema_obj"],
#                 "task_id": config["task_id"],
#                 "batch_size": config.get("batch_size", int(os.getenv("BATCH_SIZE", 50000)))
#             }
#         )