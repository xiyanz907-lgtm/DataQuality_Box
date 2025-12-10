# 配置项审计报告

## 扫描时间
2025-12-10

## 一、可废弃的环境变量

### 1. `BATCH_SIZE` 环境变量 ❌ **可废弃**
**位置：**
- `docker-compose.yaml:50`
- `docker-compose.dev.yaml:84`

**问题：**
- 环境变量 `BATCH_SIZE` 在 docker-compose 中定义，但代码中从未读取
- 实际使用位置均硬编码：
  - `dags/cycle_check_dag.py:17` - `BATCH_SIZE = 50000`
  - `dags/schema_dynamic_dag.py:31,37` - `"batch_size": 50000`

**建议：**
- 删除 docker-compose 中的 `BATCH_SIZE=${BATCH_SIZE}` 配置
- 如需支持环境变量，应在代码中使用 `os.getenv("BATCH_SIZE", 50000)`

---

## 二、配置项拼写错误/不一致

### 2. `ALERT_EMAIL_TO` vs `ALTER_EMAIL_TO` ⚠️ **需要统一**
**位置：**
- `docker-compose.yaml:48` - 使用 `ALERT_EMAIL_TO`
- `docker-compose.dev.yaml:82` - 使用 `ALTER_EMAIL_TO` (拼写错误)
- `dags/cycle_check_dag.py:150` - 使用 `ALTER_EMAIL_TO`
- `dags/new_cycles_check_dag.py:70` - 使用 `ALTER_EMAIL_TO`

**问题：**
- 生产环境配置使用 `ALERT_EMAIL_TO`
- 开发环境配置和代码使用 `ALTER_EMAIL_TO` (拼写错误)

**建议：**
- 统一使用 `ALERT_EMAIL_TO` (正确的拼写)
- 修改 `docker-compose.dev.yaml` 和两个 DAG 文件

---

### 3. `THRESHOLD_TIME_DIFF` vs `THREAD_TIME_DIFF` ⚠️ **拼写错误**
**位置：**
- `docker-compose.yaml:49` - 使用 `THRESHOLD_TIME_DIFF` ✅
- `docker-compose.dev.yaml:83` - 使用 `THREAD_TIME_DIFF` ❌ (拼写错误)

**问题：**
- 开发环境配置拼写错误，应该是 `THRESHOLD_TIME_DIFF`

**建议：**
- 修改 `docker-compose.dev.yaml:83` 为 `THRESHOLD_TIME_DIFF`

---

## 三、配置项缺失但被使用

### 4. `reference_id_col` 配置项 ⚠️ **建议补充**
**位置：**
- `plugins/services/logic_runner.py:53,115` - 使用 `config.get("reference_id_col", "id")`

**问题：**
- 代码中读取此配置项，但在 `plugins/services/config.py` 的 `TABLE_CONFIGS` 中未定义
- 目前依赖默认值 `"id"` 工作

**建议：**
- 在 `TABLE_CONFIGS["cnt_cycles"]` 中显式添加 `"reference_id_col": "id"`，提高可维护性

---

## 四、配置项使用情况汇总

### 已使用的配置项 ✅
| 配置项 | 定义位置 | 使用位置 | 状态 |
|--------|---------|---------|------|
| `THRESHOLD_TIME_DIFF` | config.py, docker-compose.yaml | logic_runner.py, cnt_cycles.py | ✅ 正常 |
| `CNT_CYCLES_SQL_LIMIT` | config.py | logic_runner.py | ✅ 正常 |
| `TIME_WINDOW_BUFFER_HOURS` | config.py | logic_runner.py | ✅ 正常 |
| `THRESHOLD_CHAIN_GAP` | config.py | cnt_newcycles.py | ✅ 正常 |
| `SITE_TIMEZONE` | docker-compose.yaml | logic_runner.py | ✅ 正常 |
| `need_reference` | config.py | logic_runner.py | ✅ 正常 |
| `reference_source` | config.py | logic_runner.py | ✅ 正常 |
| `reference_table` | config.py | logic_runner.py | ✅ 正常 |
| `join_key_target` | config.py | logic_runner.py | ✅ 正常 |
| `join_key_source` | config.py | logic_runner.py | ✅ 正常 |
| `date_filter_column` | config.py | logic_runner.py | ✅ 正常 |
| `select_sql_template` | config.py | logic_runner.py | ✅ 正常 |
| `gap_threshold_seconds` | config.py | cnt_newcycles.py | ✅ 正常 |

### 未使用的配置项 ❌
| 配置项 | 定义位置 | 状态 |
|--------|---------|------|
| `BATCH_SIZE` | docker-compose.yaml, docker-compose.dev.yaml | ❌ 未使用 |

### 拼写错误/不一致 ⚠️
| 配置项 | 正确拼写 | 错误位置 | 状态 |
|--------|---------|---------|------|
| `ALERT_EMAIL_TO` | `ALERT_EMAIL_TO` | docker-compose.dev.yaml, DAG 文件 | ⚠️ 需统一 |
| `THRESHOLD_TIME_DIFF` | `THRESHOLD_TIME_DIFF` | docker-compose.dev.yaml | ⚠️ 拼写错误 |

---

## 五、修复建议优先级

### 高优先级 🔴
1. **修复 `docker-compose.dev.yaml` 中的拼写错误**
   - `THREAD_TIME_DIFF` → `THRESHOLD_TIME_DIFF`
   - `ALTER_EMAIL_TO` → `ALERT_EMAIL_TO`

2. **统一邮件配置变量名**
   - 修改 DAG 文件中的 `ALTER_EMAIL_TO` → `ALERT_EMAIL_TO`

### 中优先级 🟡
3. **删除未使用的 `BATCH_SIZE` 环境变量**
   - 从 docker-compose 文件中删除
   - 或改为在代码中读取环境变量

4. **补充缺失的配置项**
   - 在 `TABLE_CONFIGS["cnt_cycles"]` 中添加 `reference_id_col`

---

## 六、SMTP 和数据库连接配置
这些配置项由 Airflow 框架使用，不在本次审计范围内：
- `AIRFLOW__SMTP__*` 系列配置
- `AIRFLOW_CONN_*` 系列连接配置
- `_AIRFLOW_WWW_USER_*` 系列管理员配置

