# DEPLOY

## 环境要求
- Python：建议 3.9（需与 Airflow 依赖兼容）。官方 Airflow 镜像已包含运行时，可优先使用镜像方式。
- 系统库：用于编译/运行常见 Python 科学栈与 MySQL 客户端
  - `build-essential`（或 gcc 等编译工具链）
  - `libmysqlclient-dev`（MySQL 开发包，若使用二进制轮子可省略）
  - `libpq-dev`（若自行安装 psycopg2）
  - `openssl`、`libssl-dev`（TLS 依赖）
- 数据库：MySQL（业务库 kpi_data_db 与 hutchisonports/ngen），PostgreSQL（Airflow 元数据库）。
- 时区：建议设置 `SITE_TIMEZONE`（默认 UTC）。

## 配置说明
- 入口配置来源：`.env`（或 docker-compose 环境变量）、Airflow Connections、Airflow Variables。
- 关键环境变量（示例）
  - `AIRFLOW__CORE__SQL_ALCHEMY_CONN`：Airflow 元数据库连接串（PostgreSQL）。
  - `AIRFLOW_CONN_CACTUS_MYSQL_CONN`：业务库连接，格式 `mysql://user:pass@host:port/dbname`。
  - `AIRFLOW_CONN_NGEN_MYSQL_CONN`：nGen 参考库连接，格式同上。
  - `ALTER_EMAIL_TO`：告警/报告收件人邮箱。
  - `THRESHOLD_TIME_DIFF`：时间差阈值（秒），默认 300。
  - `CNT_CYCLES_SQL_LIMIT`：单日样本读取 LIMIT（可选，控制 3-sigma 内存占用）。
  - `SITE_TIMEZONE`：站点时区，默认 UTC。
  - SMTP 相关：`SMTP_HOST`、`SMTP_PORT`、`SMTP_USER`、`SMTP_PASSWORD`、`SMTP_MAIL_FROM`（用于 Airflow 邮件通知）。
- 集中表级配置：`plugins/services/config.py`
  - `cnt_cycles`：参考表、join key、日期列、时间差阈值、SQL 模板与 limit。
  - `cnt_newcycles`：单表日期列、链路容差（秒）。
- Airflow Variables（可选，按 DAG 需要）
  - `ngen_last_processed_id`、`logic_watermark_cnt_newcycles`、`schema_watermark_*`：用于扫描/Schema 校验的水位。

## 启动命令
### 使用 docker-compose（推荐）
```bash
docker-compose up -d
```
- 会启动 `postgres_airflow`（元数据库）与 `airflow` 容器，挂载 dags/plugins 等目录。

### 本地开发（不推荐生产）
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt  # 若使用官方镜像，可省略
export AIRFLOW_HOME=./airflow_home
airflow db init
airflow webserver & airflow scheduler
```

## 查看日志
- Web UI：Airflow Task Logs。
- 容器：`docker logs -f airflow`。
- 本地文件：`./logs/`（由 Airflow 生成）。

## 健康检查 / 部署验证
1) Airflow Web UI 可访问（默认 8081:8080）。  
2) 连接可用：在 Airflow “Admin -> Connections” 测试 `cactus_mysql_conn`、`ngen_mysql_conn`。  
3) 手动触发一次 DAG（示例）：  
   - `worker_cycle_check`，Conf: `{"date_filter": "2025-11-22"}`  
   - 日志中应看到规则执行结果，邮件收到报告。  
4) Schema 校验：可手动触发 `schema_quality_check_dynamic`，确认 Pandera 任务成功。  
5) （可选）检查版本/依赖：`pip show apache-airflow` 或 `airflow version` 在容器内执行。

