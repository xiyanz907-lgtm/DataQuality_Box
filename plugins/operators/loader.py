"""
operators/loader.py
通用数据加载器 V2

职责：
- 从多个数据源（MySQL / InfluxDB / HTTP API）抽取数据
- 支持漏斗式提取（depends_on + input_ref）
- 按拓扑排序执行 extraction 链
- 支持 batch / per_row 两种迭代模式
- 渲染查询模板（Jinja: Airflow 宏 + 上游数据引用）
- 写入 Raw Parquet

V2 改进：
- 引入 Extractor 抽象层（MySQL / InfluxDB / HTTP）
- extraction 之间支持 depends_on 声明执行依赖
- input_ref 引用上游 DataFrame 用于查询参数化
- batch 模式: {{ ref.values('col') }} → SQL IN 子句
- per_row 模式: {{ row.col }} → 逐行迭代查询
"""
import polars as pl
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque
from jinja2 import Template

from plugins.infra.operators import BaseGovernanceOperator
from plugins.domain.context import GovernanceContext
from plugins.infra.extractors import get_extractor
from plugins.infra.extractors.base import UpstreamRef


class UniversalLoaderOperator(BaseGovernanceOperator):
    """
    通用数据加载器 V2

    支持扁平并行抽取（向后兼容）和漏斗式链式抽取。

    配置示例 (扁平模式 - 向后兼容):
    ```yaml
    extractions:
      - id: "raw_cycle"
        source_type: "mysql"
        conn_id: "datalog_mysql_conn"
        query: "SELECT * FROM cycle WHERE DATE(shift_date) = '{{ ds }}'"
        output_key: "raw_cycle"
    ```

    配置示例 (漏斗模式):
    ```yaml
    extractions:
      - id: "raw_vehicles"
        source_type: "mysql"
        conn_id: "datalog_mysql_conn"
        query: "SELECT DISTINCT vehicle_id FROM cycle WHERE DATE(shift_date) = '{{ ds }}'"
        output_key: "raw_vehicles"

      - id: "raw_telemetry"
        source_type: "influxdb"
        conn_id: "influxdb_conn"
        depends_on: ["raw_vehicles"]
        input_ref: "raw_vehicles"
        iteration_mode: "per_row"
        query: |
          from(bucket: "telemetry")
            |> range(start: {{ row.start_time }}, stop: {{ row.end_time }})
            |> filter(fn: (r) => r.vehicle_id == "{{ row.vehicle_id }}")
        output_key: "raw_telemetry"
    ```
    """

    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        抽取逻辑 V2

        流程：
        1. 对 extraction 列表做拓扑排序
        2. 按层级顺序执行（同层无依赖的可并行，当前串行执行）
        3. 有 input_ref 时，根据 iteration_mode 做模板渲染
        """
        self._validate_config(['source_meta', 'extractions'])

        extractions = self._config.get('extractions', [])
        if not extractions:
            self.log.warning("No extractions configured, skipping...")
            return

        self.log.info(f"Starting extractions: {len(extractions)} tasks")

        extraction_map = {e['id']: e for e in extractions}
        sorted_ids = self._topological_sort(extractions)

        self.log.info(f"📋 Execution order: {sorted_ids}")

        completed: Dict[str, pl.DataFrame] = {}

        for ext_id in sorted_ids:
            task = extraction_map[ext_id]
            output_key = task['output_key']

            upstream_df = self._resolve_upstream(task, completed, ctx)

            if upstream_df is not None and upstream_df.height == 0:
                self.log.warning(
                    f"⏭️ [{ext_id}] upstream '{task.get('input_ref')}' is empty, "
                    f"skipping extraction"
                )
                completed[output_key] = pl.DataFrame()
                ctx.put_dataframe(key=output_key, df=pl.DataFrame(), stage="RAW",
                                  alt_key=task.get('alt_key'))
                continue

            df = self._execute_extraction(task, ctx, context, upstream_df)
            completed[output_key] = df

        self.log.info(f"✅ Completed {len(extractions)} extractions")

    # ========================================================================
    # 拓扑排序
    # ========================================================================

    def _topological_sort(self, extractions: List[Dict]) -> List[str]:
        """
        对 extraction 列表做拓扑排序

        无 depends_on 的排在前面（Layer 0），有依赖的按依赖链排列。
        存在环路时抛出 ValueError。

        Returns:
            排序后的 extraction id 列表
        """
        id_set = {e['id'] for e in extractions}
        in_degree: Dict[str, int] = defaultdict(int)
        adjacency: Dict[str, List[str]] = defaultdict(list)

        for e in extractions:
            ext_id = e['id']
            in_degree.setdefault(ext_id, 0)
            deps = e.get('depends_on') or []
            for dep in deps:
                if dep not in id_set:
                    raise ValueError(
                        f"Extraction '{ext_id}' depends on '{dep}', "
                        f"which is not defined in extractions"
                    )
                adjacency[dep].append(ext_id)
                in_degree[ext_id] += 1

        queue = deque(eid for eid in id_set if in_degree[eid] == 0)
        result = []

        while queue:
            current = queue.popleft()
            result.append(current)
            for neighbor in adjacency[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(id_set):
            raise ValueError(
                f"Circular dependency detected in extractions! "
                f"Sorted: {result}, All: {id_set}"
            )

        return result

    # ========================================================================
    # 上游数据解析
    # ========================================================================

    def _resolve_upstream(
        self,
        task: Dict[str, Any],
        completed: Dict[str, pl.DataFrame],
        ctx: GovernanceContext
    ) -> Optional[pl.DataFrame]:
        """
        解析上游 DataFrame

        优先级：
        1. completed 字典（当前 Loader 内已完成的 extraction）
        2. GovernanceContext（其他 Operator 写入的数据，如 entity 表）

        Returns:
            上游 DataFrame，无 input_ref 时返回 None
        """
        input_ref = task.get('input_ref')
        if not input_ref:
            return None

        if input_ref in completed:
            df = completed[input_ref]
            self.log.info(
                f"📎 [{task['id']}] resolved input_ref='{input_ref}' "
                f"from completed ({df.height} rows)"
            )
            return df

        try:
            df = ctx.get_dataframe(key=input_ref, use_alt_key=True)
            self.log.info(
                f"📎 [{task['id']}] resolved input_ref='{input_ref}' "
                f"from context ({df.height} rows)"
            )
            return df
        except KeyError:
            raise ValueError(
                f"Extraction '{task['id']}' references input_ref='{input_ref}', "
                f"but it was not found in completed extractions or context. "
                f"Available: {list(completed.keys())}"
            )

    # ========================================================================
    # 执行单个 extraction
    # ========================================================================

    def _execute_extraction(
        self,
        task: Dict[str, Any],
        ctx: GovernanceContext,
        context: Dict[str, Any],
        upstream_df: Optional[pl.DataFrame]
    ) -> pl.DataFrame:
        """
        执行单个 extraction（支持 batch / per_row 模式）

        Args:
            task: extraction 任务配置
            ctx: 治理上下文
            context: Airflow context
            upstream_df: 上游 DataFrame（可为 None）

        Returns:
            抽取结果 DataFrame
        """
        task_id = task.get('id', 'unknown')
        output_key = task['output_key']
        source_type = task.get('source_type', 'mysql')
        conn_id = task.get('conn_id') or task.get('connection_id')
        iteration_mode = task.get('iteration_mode', 'batch')

        self.log.info(f"📥 Extracting [{task_id}] (type={source_type}, mode={iteration_mode})")

        try:
            self._clean_partition(ctx, stage="RAW", key=output_key)

            extractor = get_extractor(source_type, conn_id, self.log)
            query_template = task.get('query') or task.get('sql')
            if not query_template:
                raise ValueError(
                    f"Missing 'query' or 'sql' in extraction '{task_id}'"
                )

            airflow_vars = self._build_airflow_vars(context)

            if upstream_df is None:
                rendered = self._render_template(query_template, airflow_vars)
                df = extractor.extract_single(rendered, task)

            elif iteration_mode == 'batch':
                df = self._execute_batch(
                    extractor, query_template, airflow_vars, upstream_df, task
                )

            elif iteration_mode == 'per_row':
                df = self._execute_per_row(
                    extractor, query_template, airflow_vars, upstream_df, task
                )

            else:
                raise ValueError(f"Unknown iteration_mode: '{iteration_mode}'")

            if df.height == 0:
                self.log.warning(f"⚠️ [{task_id}] extracted 0 rows")

            ctx.put_dataframe(
                key=output_key, df=df, stage="RAW", alt_key=task.get('alt_key')
            )

            self.log.info(f"✅ [{task_id}] extracted {df.height} rows")
            return df

        except Exception as e:
            self.log.error(f"❌ [{task_id}] extraction failed: {e}")
            raise

    # ========================================================================
    # Batch 模式
    # ========================================================================

    def _execute_batch(
        self,
        extractor,
        query_template: str,
        airflow_vars: dict,
        upstream_df: pl.DataFrame,
        task: dict
    ) -> pl.DataFrame:
        """
        batch 模式：上游 DataFrame 整体作为 ref 传入模板

        模板变量：
            {{ ref.values('vehicle_id') }}  → 'V001','V002','V003'
            {{ ref.count }}                 → 42
            {{ ref.min('shift_date') }}     → '2026-01-01'
        """
        ref = UpstreamRef(upstream_df)
        rendered = self._render_template(query_template, airflow_vars, ref=ref)
        self.log.info(f"  Batch query rendered ({upstream_df.height} upstream rows)")
        return extractor.extract_single(rendered, task)

    # ========================================================================
    # Per-Row 模式
    # ========================================================================

    def _execute_per_row(
        self,
        extractor,
        query_template: str,
        airflow_vars: dict,
        upstream_df: pl.DataFrame,
        task: dict
    ) -> pl.DataFrame:
        """
        per_row 模式：逐行迭代上游 DataFrame，每行生成一条查询

        模板变量：
            {{ row.vehicle_id }}   → 'V001'
            {{ row.start_time }}   → '2026-01-01 08:00:00'

        所有行的结果拼接为一个 DataFrame。
        """
        total = upstream_df.height
        self.log.info(f"  Per-row iteration: {total} rows")

        results: List[pl.DataFrame] = []
        success_count = 0

        for idx, row in enumerate(upstream_df.iter_rows(named=True)):
            try:
                rendered = self._render_template(query_template, airflow_vars, row=row)
                row_df = extractor.extract_single(rendered, task)

                if row_df.height > 0:
                    results.append(row_df)
                    success_count += 1

                if (idx + 1) % 50 == 0:
                    self.log.info(
                        f"  Progress: {idx + 1}/{total} rows processed, "
                        f"{success_count} with results"
                    )

            except Exception as e:
                self.log.warning(f"  ⚠️ Row {idx} failed: {e}")
                continue

        self.log.info(
            f"  Per-row completed: {success_count}/{total} rows returned data"
        )

        if results:
            return pl.concat(results, how='diagonal_relaxed')
        return pl.DataFrame()

    # ========================================================================
    # 模板渲染
    # ========================================================================

    def _render_template(
        self,
        template_str: str,
        airflow_vars: dict,
        ref: Optional[UpstreamRef] = None,
        row: Optional[dict] = None
    ) -> str:
        """
        渲染 Jinja 查询模板

        变量优先级：
        1. Airflow 宏（ds, ts, ...）— 始终可用
        2. ref（UpstreamRef 对象）— batch 模式
        3. row（dict）— per_row 模式
        """
        template = Template(template_str)

        render_vars = dict(airflow_vars)
        if ref is not None:
            render_vars['ref'] = ref
        if row is not None:
            render_vars['row'] = row

        return template.render(**render_vars)

    def _build_airflow_vars(self, context: Dict[str, Any]) -> dict:
        """提取 Airflow 宏变量"""
        return {
            'ds': context.get('ds'),
            'ds_nodash': context.get('ds_nodash'),
            'ts': context.get('ts'),
            'ts_nodash': context.get('ts_nodash'),
            'execution_date': context.get('execution_date'),
            'prev_ds': context.get('prev_ds'),
            'next_ds': context.get('next_ds'),
            'yesterday_ds': context.get('yesterday_ds'),
            'tomorrow_ds': context.get('tomorrow_ds'),
        }
