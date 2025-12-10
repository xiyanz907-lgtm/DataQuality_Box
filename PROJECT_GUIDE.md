# PROJECT_GUIDE

## 项目简介
一个基于 Airflow 的数据质量检测框架：按水位扫描增量数据，拉取 MySQL（kpi、nGen）数据，用 Polars 规则与 Pandera Schema 进行校验，并通过邮件输出报告。

## 快速开始
1) 安装依赖
   - 确保使用与 Airflow 兼容的 Python 版本（建议 3.9）。
   - 在项目根目录执行：
     - `python -m venv venv && source venv/bin/activate`
     - `pip install -r requirements.txt`
2) 配置 Airflow & 连接
   - 设置 Airflow 变量/连接（至少）：
     - 连接：`cactus_mysql_conn`（指向 kpi 数据库），`ngen_mysql_conn`（指向 nGen），可选 `postgres`/`smtp` 等按现有部署。
     - 变量：`ngen_last_processed_id`，`logic_watermark_cnt_newcycles`，`schema_watermark_cnt_cycles`，`schema_watermark_new_cnt_cycles`（或按业务重命名）。
   - 环境变量（可选）：`SITE_TIMEZONE`（默认 UTC），`THRESHOLD_TIME_DIFF`（默认 300 秒），`ALERT_EMAIL_TO`（收件人）。
3) 初始化与首跑
   - 启动 Airflow：`airflow db init`，`airflow webserver`，`airflow scheduler`（或使用随附 docker-compose）。
   - 首次手动触发：
     - `scanner_ngen_updates_by_id`（扫描 nGen 新增并按日期触发 worker_cycle_check）。
     - 或直接触发 `worker_cycle_check` / `worker_newcycle_check`，在 `Conf` 里传 `{"date_filter": "2024-11-12"}` 等目标日期。
     - Schema 路径可触发 `schema_quality_check_dynamic` 做 Pandera 校验（按 ID 水位批次）。

## 代码导航
- `dags/`
  - `scanner_ngen_updates_by_id.py`：按 nGen id 水位扫描增量日期并触发跨库校验 DAG。
  - `scanner_internal_dag.py`：内部增量扫描（cnt_newcycles），另一个 `_scan_ngen` 待实现。
  - `worker_cycle_check.py`：跨库 KPI vs nGen 规则执行 + 邮件。
  - `worker_newcycle_check.py`：单表链路连续性校验 + 邮件（当前调用逻辑需修复，见 TODO）。
  - `schema_dynamic_dag.py`：Pandera Schema 校验，按 ID 水位分批。
- `plugins/services/logic_runner.py`
  - V2 执行器：按表名动态加载 `rules/kpi/{table}.py` 的 `CONFIG` 与 `get_logic_rules`，负责取数、清洗、调用规则、汇总报告。
- `plugins/rules/`
  - `kpi/cnt_cycles.py`：跨库规则示例（匹配率、时间差、3-sigma 时长）。
  - `single/cnt_newcycles.py`：单表车辆作业链连续性规则。
  - `external_cross/cnt_cycles.py`：早期跨库时间差实现。
- `plugins/dq_lib/`
  - 通用规则库：完整性、时间一致性、链路连续性、分布 3-sigma（Polars）。
  - `pandera_utils.py` 当前为空，预留 Pandera 辅助。
- `plugins/schemas/kpi/`
  - `cnt_cycles.py`，`cnt_newcycles.py`：Pandera Schema 定义。
- `plugins/cycle_checks.py`
  - 早期单函数版本的时间差校验（已被 LogicRunner V2 取代）。

## TODO 列表
- 补齐 `_scan_ngen` 实现或移除 `scanner_internal_ngen` DAG 避免运行失败。
- 修复 `worker_newcycle_check` 对 `LogicRunner` 的调用：应实例化后调用 `run_checks('cnt_newcycles', date_filter)`，并修正邮件 XCom 引用。
- 补充缺失的 `plugins/rules/kpi/cnt_newcycles.py` 以匹配 V2 动态加载路径（或调整 runner 逻辑）。
- 完善 `plugins/dq_lib/pandera_utils.py`（目前为空）；补充常用 Pandera 辅助检查。
- 校准各水位默认值（如 `ngen_last_processed_id=1000000`、`schema` 水位 60258），避免跳过历史数据。
- 对字段命名和返回字段进行对齐：`violations_count` vs `violation_count`，避免异常信息不一致。
- 邮件模板 XCom key 对齐：`worker_newcycle_check` 邮件当前引用了错误 task_id。

