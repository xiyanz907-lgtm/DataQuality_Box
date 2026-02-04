"""
动态治理 DAG 注册入口
自动扫描 plugins/configs/sources/*.yaml 并生成 DAG

Airflow 会自动执行这个文件，将生成的 DAG 注册到全局命名空间
"""
import logging
from plugins.orchestration.dag_factory import register_all_dags

logger = logging.getLogger(__name__)

# ============= 注册所有动态生成的 DAG =============
try:
    logger.info("🚀 Starting dynamic DAG generation...")
    generated_dags = register_all_dags()
    
    # 将生成的 DAG 注册到全局命名空间
    for dag_id, dag_obj in generated_dags.items():
        globals()[dag_id] = dag_obj
    
    logger.info(f"✅ Successfully registered {len(generated_dags)} dynamic DAGs")
    
except Exception as e:
    logger.error(f"❌ Failed to generate dynamic DAGs: {e}", exc_info=True)
    # 不抛出异常，避免阻塞其他 DAG 文件的加载
