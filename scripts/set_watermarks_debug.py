#!/usr/bin/env python3
"""
临时调试脚本：设置 scanner_universal_dag 的水位线变量
用于调试目的，设置后可以控制扫描器从指定位置开始扫描

使用方法：
1. 在 Airflow 容器内执行：
   docker exec -it <airflow_container> python /path/to/scripts/set_watermarks_debug.py

2. 或者在 Airflow Python 环境中直接运行此脚本
"""
import sys
import os

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

# 设置 Airflow 环境变量（如果需要）
os.environ.setdefault('AIRFLOW_HOME', '/opt/airflow')

try:
    from airflow.models import Variable
    from airflow import settings
    
    # 初始化 Airflow 会话
    session = settings.Session()
except Exception as e:
    print(f"⚠️  Airflow 初始化失败: {e}")
    print("请确保在 Airflow 环境中运行此脚本")
    sys.exit(1)

# 根据 scanner_universal_dag.py 的命名规范生成变量名
TASK_NAME = "cnt_cycles"

# 变量名
VAR_NGEN_ID = f"watermark_{TASK_NAME}_ngen_id"
VAR_CACTUS_TIME = f"watermark_{TASK_NAME}_cactus_time"

# 要设置的值
NGEN_ID_VALUE = 1000000
CACTUS_TIME_VALUE = "2025-11-01 15:00:09"

def set_watermarks():
    """设置水位线变量"""
    print(f"🔧 设置水位线变量用于调试...")
    print(f"\n设置 {VAR_NGEN_ID} = {NGEN_ID_VALUE}")
    Variable.set(VAR_NGEN_ID, NGEN_ID_VALUE)
    
    print(f"设置 {VAR_CACTUS_TIME} = {CACTUS_TIME_VALUE}")
    Variable.set(VAR_CACTUS_TIME, CACTUS_TIME_VALUE)
    
    # 验证设置
    print("\n✅ 水位线设置完成！验证结果：")
    print(f"  - {VAR_NGEN_ID}: {Variable.get(VAR_NGEN_ID)}")
    print(f"  - {VAR_CACTUS_TIME}: {Variable.get(VAR_CACTUS_TIME)}")
    
    session.close()

if __name__ == "__main__":
    set_watermarks()

