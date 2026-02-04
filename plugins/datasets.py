"""
Airflow Dataset 定义
用于跨 DAG 事件驱动

Author: Data Governance Team
Date: 2026-02-02
"""
from airflow.datasets import Dataset


# ============================================================
# Dataset 定义
# ============================================================

# 资产打包队列 Dataset
# DAG A 写入资产到 meta 表（单表方案）后，触发此 Dataset
# DAG B 监听此 Dataset，启动打包流程
GOVERNANCE_ASSET_DATASET = Dataset(
    'mysql://qa_mysql_conn/auto_test_case_catalog'
)


# ============================================================
# 使用说明
# ============================================================
# 
# 1. 在 DAG A 中声明 outlets（生产者）：
#    
#    save_assets = PythonOperator(
#        task_id='save_assets_to_queue',
#        python_callable=write_assets_to_db,
#        outlets=[GOVERNANCE_ASSET_DATASET],  # 声明输出
#    )
#
# 2. 在 DAG B 中监听 schedule（消费者）：
#    
#    with DAG(
#        dag_id='asset_packing_dag',
#        schedule=[GOVERNANCE_ASSET_DATASET],  # 监听触发
#        ...
#    ) as dag:
#        ...
#
# 3. Dataset 触发机制：
#    - DAG A 完成 save_assets 任务后，Airflow 自动触发 DAG B
#    - DAG B 不需要轮询，完全事件驱动
#    - 支持多个消费者（可以有多个 DAG 监听同一 Dataset）
#
# 4. 查看 Dataset 依赖关系：
#    Airflow UI -> Browse -> Datasets
#    可以看到哪些 DAG 生产/消费此 Dataset
#
