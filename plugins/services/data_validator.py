from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandera as pa
import pandas as pd
import logging

logger = logging.getLogger("airflow.task")

def run_pandera_validation(conn_id, table_name, schema_obj, custom_where=None, sql_limit=None):
    """
    通用校验函数
    """
    logger.info(f"🚀 开始校验表: {table_name}")
    
    hook = MySqlHook(mysql_conn_id=conn_id)
    
    # 构造 SQL
    where_clause = f"WHERE {custom_where}" if custom_where else ""
    limit_clause = f"LIMIT {sql_limit}" if sql_limit else ""
    
    # 拼装完整 SQL
    sql = f"SELECT * FROM {table_name} {where_clause} {limit_clause}"
    
    logger.info(f"执行 SQL: {sql}")
    
    # 读取数据
    df = hook.get_pandas_df(sql)
    
    if len(df) == 0:
        logger.info("⚠️ 没有读取到数据，跳过校验。")
        return {"status": "SKIPPED", "error_count": 0, "rows": 0}
        
    logger.info(f"📥 读取数据完成，共 {len(df)} 行，开始执行规则...")

    try:
        schema_obj.validate(df, lazy=True)
        logger.info(f"✅ {table_name} 校验通过！")
        return {"status": "SUCCESS", "error_count": 0, "rows": len(df)}

    except pa.errors.SchemaErrors as err:
        failure_cases = err.failure_cases
        error_count = len(failure_cases)
        logger.error(f"❌ {table_name} 校验失败！发现 {error_count} 个问题。")
        
        # 简单打印摘要
        logger.error(f"错误样本:\n{failure_cases.head(5)}")
        
        return {
            "status": "FAILED",
            "error_count": error_count,
            "report": failure_cases.to_json()
        }