"""
operators/loader.py
通用数据加载器

职责：
- 从多个数据源（MySQL/InfluxDB）抽取数据
- 渲染 SQL 模板（支持 Jinja）
- 写入 Raw Parquet
- 支持多表抽取
"""
import polars as pl
from typing import Dict, Any
from jinja2 import Template

from airflow.providers.mysql.hooks.mysql import MySqlHook
from plugins.infra.operators import BaseGovernanceOperator
from plugins.domian.context import GovernanceContext


class UniversalLoaderOperator(BaseGovernanceOperator):
    """
    通用数据加载器
    
    配置示例 (configs/sources/datalog_mysql.yaml):
    ```yaml
    source_meta:
      id: "mysql_datalog_raw"
      type: "mysql"
      connection_id: "datalog_mysql_conn"
    
    extractions:
      - id: "summary"
        output_key: "raw_summary"
        sql: "SELECT * FROM cycle_section_summary WHERE shift_date = '{{ ds }}'"
        alt_key: "Summary"
      
      - id: "subtarget"
        output_key: "raw_subtarget"
        sql: "SELECT * FROM subtarget_vehicle_cycle WHERE SHIFT_DATE = '{{ ds }}'"
        alt_key: "Subtarget"
    ```
    
    使用示例:
    ```python
    loader = UniversalLoaderOperator(
        task_id='load_raw_data',
        config_path='configs/sources/datalog_mysql.yaml',
        dag=dag
    )
    ```
    """
    
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        抽取逻辑
        
        流程：
        1. 遍历配置中的所有抽取任务
        2. 对每个任务：清理 → 抽取 → 写入
        3. 记录抽取统计
        """
        # 验证配置
        self._validate_config(['source_meta', 'extractions'])
        
        source_meta = self._config['source_meta']
        extractions = self._config.get('extractions', [])
        
        if not extractions:
            self.log.warning("No extractions configured, skipping...")
            return
        
        self.log.info(f"Starting extractions: {len(extractions)} tasks")
        
        # 执行每个抽取任务
        for task in extractions:
            self._execute_extraction_task(task, source_meta, ctx, context)
        
        self.log.info(f"✅ Completed {len(extractions)} extractions")
    
    def _execute_extraction_task(
        self, 
        task: Dict[str, Any], 
        source_meta: Dict[str, Any],
        ctx: GovernanceContext, 
        context: Dict[str, Any]
    ) -> None:
        """
        执行单个抽取任务
        
        Args:
            task: 抽取任务配置
            source_meta: 数据源元信息
            ctx: 治理上下文
            context: Airflow context
        """
        task_id = task.get('id', 'unknown')
        output_key = task['output_key']
        
        self.log.info(f"📥 Extracting [{task_id}] -> {output_key}")
        
        try:
            # 1. 清理旧数据
            self._clean_partition(ctx, stage="RAW", key=output_key)
            
            # 2. 抽取数据
            df = self._extract_from_source(task, source_meta, context)
            
            # 3. 数据质量检查（可选）
            if df.height == 0:
                self.log.warning(f"⚠️ [{task_id}] extracted 0 rows")
            
            # 4. 写入 Context
            ctx.put_dataframe(
                key=output_key,
                df=df,
                stage="RAW",
                alt_key=task.get('alt_key')
            )
            
            self.log.info(f"✅ [{task_id}] extracted {df.height} rows")
            
        except Exception as e:
            self.log.error(f"❌ [{task_id}] extraction failed: {e}")
            raise
    
    def _extract_from_source(
        self, 
        task: Dict[str, Any], 
        source_meta: Dict[str, Any],
        context: Dict[str, Any]
    ) -> pl.DataFrame:
        """
        从数据源抽取数据
        
        Args:
            task: 抽取任务配置
            source_meta: 数据源元信息
            context: Airflow context
        
        Returns:
            Polars DataFrame
        """
        # 1. 渲染 SQL（处理 Jinja 模板）
        # 支持 'query' 和 'sql' 字段（兼容不同配置格式）
        sql_template = task.get('query') or task.get('sql')
        if not sql_template:
            raise ValueError(f"Missing 'query' or 'sql' field in extraction config: {task.get('id')}")
        
        sql = self._render_sql(sql_template, context)
        
        self.log.info(f"Rendered SQL: {sql[:200]}...")  # 打印前200字符
        
        # 2. 根据数据源类型抽取
        source_type = task.get('source_type', 'mysql')
        
        if source_type == 'mysql':
            return self._extract_from_mysql(sql, task)
        elif source_type == 'postgresql':
            # 预留：PostgreSQL 抽取逻辑
            raise NotImplementedError("PostgreSQL support not implemented yet")
        elif source_type == 'influxdb':
            # 预留：InfluxDB 抽取逻辑
            raise NotImplementedError("InfluxDB support not implemented yet")
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _extract_from_mysql(
        self, 
        sql: str, 
        task: Dict[str, Any]
    ) -> pl.DataFrame:
        """
        从 MySQL 抽取数据（使用 Airflow Hook + Pandas 中转）
        
        策略：Hook → Pandas → Polars
        优点：无外部依赖（connectorx），稳定可靠
        
        Args:
            sql: 渲染后的 SQL
            task: 抽取任务配置（包含 conn_id）
        
        Returns:
            Polars DataFrame
        """
        # 支持 'conn_id' 和 'connection_id' 字段（兼容不同配置格式）
        connection_id = task.get('conn_id') or task.get('connection_id')
        if not connection_id:
            raise ValueError(f"Missing 'conn_id' or 'connection_id' in extraction task: {task.get('id')}")
        
        try:
            # 1. 使用 MySqlHook 获取 Pandas DataFrame
            hook = MySqlHook(mysql_conn_id=connection_id)
            self.log.info(f"🔌 Connecting to MySQL via Hook: {connection_id}")
            
            pandas_df = hook.get_pandas_df(sql=sql)
            self.log.info(f"✅ Fetched {len(pandas_df)} rows from MySQL")
            
            # 2. 处理空结果集
            if pandas_df.empty:
                self.log.warning("⚠️ Query returned empty result")
                return pl.from_pandas(pandas_df)
            
            # 3. 转换为 Polars DataFrame
            polars_df = pl.from_pandas(pandas_df)
            self.log.info(
                f"✅ Converted to Polars: {polars_df.height} rows × "
                f"{polars_df.width} columns"
            )
            
            return polars_df
            
        except Exception as e:
            self.log.error(f"❌ MySQL extraction failed: {str(e)}")
            self.log.error(f"SQL preview: {sql[:500]}...")
            raise
    
    def _render_sql(self, sql_template: str, context: Dict[str, Any]) -> str:
        """
        渲染 SQL 模板（支持 Jinja）
        
        Args:
            sql_template: SQL 模板字符串
            context: Airflow context（包含 ds, ts 等宏）
        
        Returns:
            渲染后的 SQL 字符串
        
        示例:
            模板: "SELECT * FROM table WHERE date = '{{ ds }}'"
            渲染: "SELECT * FROM table WHERE date = '2026-01-26'"
        """
        template = Template(sql_template)
        
        # 提取常用宏
        template_vars = {
            'ds': context.get('ds'),                    # 2026-01-26
            'ds_nodash': context.get('ds_nodash'),      # 20260126
            'ts': context.get('ts'),                    # 2026-01-26T12:00:00+00:00
            'ts_nodash': context.get('ts_nodash'),      # 20260126T120000
            'execution_date': context.get('execution_date'),
            'prev_ds': context.get('prev_ds'),
            'next_ds': context.get('next_ds'),
            'yesterday_ds': context.get('yesterday_ds'),
            'tomorrow_ds': context.get('tomorrow_ds'),
        }
        
        # 渲染
        rendered_sql = template.render(**template_vars)
        
        return rendered_sql
