# 港口自动驾驶数据治理平台

## 📋 项目概述

**目标**：识别问题数据并发送告警，抓取边缘场景的数据。

**适用场景**：MySQL、InfluxDB、S3 等多元异构数据源。

**核心特性**：
- ✅ **配置驱动**：所有逻辑通过 YAML 定义
- ✅ **存算分离**：Parquet 作为中间存储
- ✅ **上下文总线**：标准化的对象传递
- ✅ **动态任务生成**：根据规则配置自动创建 Airflow 任务
- ✅ **防御性解析**：规则配置验证 + 循环依赖检测
- ✅ **降级处理**：告警模板渲染失败时自动发送纯文本 JSON

---

## 🏗️ 架构设计

### 治理等级定义

| 等级 | 含义 | 处理方式 |
|------|------|----------|
| **P0 (Blocker)** | 物理/逻辑错误 | 阻断、报警、修复 |
| **P1 (Asset)** | 高价值特征/非标作业 | 打标、豁免、打包 |
| **P2 (Violation)** | SLA 违规 | 结合 P1 结果进行判决、发工单 |

### 曳光弹场景

- **场景 1 (P0)**：时间倒挂校验 (`end_time < start_time`)
- **场景 2 (P1)**：双箱作业挖掘 (`is_twin_lift == 1`)
- **场景 3 (P2)**：作业超时监控 (`duration > 2h`)，且无双箱豁免

### 数据流转

```
Source DB -> RAW Parquet -> Domain Entity Parquet -> Rule Result Parquet -> Governance Context -> Action
```

### DAG 结构

```
Loader -> Adapter -> [Rule Tasks (并行)] -> Aggregator -> Dispatcher
                          ↓
                    (根据依赖关系排序)
```

---

## 📂 项目结构

```
cactus-box/
├── dags/
│   └── governance_main_dag.py          # 主 DAG
├── plugins/
│   ├── domian/
│   │   ├── context.py                  # GovernanceContext（核心数据总线）
│   │   └── entities/
│   │       └── cycle.py                # Cycle 实体定义
│   ├── infra/
│   │   ├── operators.py                # BaseGovernanceOperator
│   │   ├── io_strategy.py              # IOStrategy（Local/MinIO 抽象）
│   │   └── config.py                   # Config（环境变量管理）
│   ├── operators/
│   │   ├── loader.py                   # UniversalLoaderOperator
│   │   ├── adapter.py                  # DomainAdapterOperator
│   │   ├── rule_engine.py              # GenericRuleOperator
│   │   ├── aggregator.py               # ContextAggregatorOperator
│   │   └── dispatcher.py               # NotificationDispatcherOperator
│   ├── orchestration/
│   │   └── rule_scanner.py             # RuleScanner（规则扫描器）
│   ├── services/
│   │   └── notification.py             # NotificationService
│   ├── configs/
│   │   ├── sources.yaml                # 数据源配置
│   │   ├── adapters/
│   │   │   └── cycle_adapter.yaml      # 领域适配器配置
│   │   └── rules/
│   │       ├── p0_time_check.yaml      # P0 规则
│   │       ├── p1_twin_lift.yaml       # P1 规则
│   │       └── p2_timeout.yaml         # P2 规则
│   └── schemas/
│       └── XCom.json                    # XCom 序列化协议
├── database/
│   └── schemas/
│       └── schema_auto_test_case_catalog.sql  # 资产目录表
├── test_phase1.py                       # Phase 1 测试（Context + IOStrategy）
├── test_phase2.py                       # Phase 2 测试（BaseGovernanceOperator）
├── test_phase4.py                       # Phase 4 测试（RuleScanner + NotificationService）
└── deploy/
    ├── docker-compose.yml               # Docker 编排
    ├── env.template                     # 环境变量模板
    └── requirements.txt                 # Python 依赖
```

---

## 🚀 快速开始

### 1. 环境变量配置

复制 `deploy/env.template` 并修改配置：

```bash
cp deploy/env.template .env
```

关键配置项（所有配置都通过环境变量注入）：

```bash
# ============================================================
# 数据治理平台配置
# ============================================================

# 存储类型：local（本地文件系统）/ minio（MinIO 对象存储）
GOVERNANCE_STORAGE_TYPE=local

# MinIO 配置（当 GOVERNANCE_STORAGE_TYPE=minio 时生效）
MINIO_GOVERNANCE_ENDPOINT=http://minio:9000        # MinIO 服务地址
MINIO_GOVERNANCE_ACCESS_KEY=minioadmin             # 访问密钥
MINIO_GOVERNANCE_SECRET_KEY=minioadmin             # 密钥
MINIO_GOVERNANCE_BUCKET=governance-data            # Bucket 名称
MINIO_GOVERNANCE_REGION=us-east-1                  # 区域
MINIO_GOVERNANCE_SECURE=False                      # 是否使用 HTTPS

# Parquet 压缩配置（可选，默认为 zstd）
PARQUET_COMPRESSION=zstd                           # 全局默认压缩算法
PARQUET_COMPRESSION_RAW=zstd                       # RAW 阶段压缩
PARQUET_COMPRESSION_ENTITY=zstd                    # ENTITY 阶段压缩
PARQUET_COMPRESSION_RESULT=zstd                    # RESULT 阶段压缩

# ============================================================
# 邮件配置
# ============================================================
AIRFLOW__SMTP__SMTP_HOST=smtp.example.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=alerts@example.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_password
AIRFLOW__SMTP__SMTP_MAIL_FROM=alerts@example.com
ALERT_EMAIL_TO=admin@example.com
```

**配置说明**：
- 所有配置项在 `deploy/env.template` 中都有示例
- 修改 `.env` 文件后需要重启容器：`docker-compose restart airflow`
- MinIO 配置只在 `GOVERNANCE_STORAGE_TYPE=minio` 时生效
- Parquet 压缩算法支持：`zstd`（推荐）、`snappy`、`gzip`、`lz4`、`brotli`

### 2. 启动服务

```bash
cd deploy
docker-compose up -d
```

### 3. 创建 Airflow Connection

访问 Airflow UI（http://localhost:8080），创建以下连接：

- **mysql_default**：MySQL 数据源
- **influxdb_default**：InfluxDB 数据源（可选）

### 4. 触发 DAG

在 Airflow UI 中手动触发 `governance_main_dag`，可以传入参数：

```json
{
  "batch_id": "BATCH_20260123_001",
  "run_date": "2026-01-23"
}
```

### 5. 查看结果

- **XCom**：查看 `GovernanceContext` 的序列化结果
- **日志**：查看每个 Task 的执行日志
- **邮件**：P0 和 P2 告警会发送到配置的邮箱
- **数据库**：资产数据会写入 `auto_test_case_catalog` 表

---

## 🧪 测试

### 运行单元测试

```bash
# Phase 1 测试（Context + IOStrategy）
python3 test_phase1.py

# Phase 2 测试（BaseGovernanceOperator）
python3 test_phase2.py

# Phase 4 测试（RuleScanner + NotificationService）
python3 test_phase4.py
```

---

## 📖 配置说明

### 数据源配置（sources.yaml）

```yaml
sources:
  - id: mysql_summary
    type: mysql
    conn_id: mysql_default
    query: |
      SELECT * FROM operation_summary
      WHERE DATE(start_time) = '{{ ds }}'
    output:
      key: raw_mysql_summary
      stage: RAW
```

### 适配器配置（adapters/cycle_adapter.yaml）

```yaml
target_entity: Cycle
inputs:
  - key: raw_mysql_summary
    alias: summary
output:
  key: entity_cycle
  stage: ENTITY
field_mapping:
  cycle_id: cycle_id
  vehicle_id: vehicle_id
  # ... 其他字段映射
data_cleaning:
  drop_duplicates: [cycle_id]
  drop_nulls: [cycle_id, start_time, end_time]
  fill_nulls:
    is_twin_lift: 0
```

### 规则配置（rules/*.yaml）

```yaml
meta:
  rule_id: rule_p0_time_check
  severity: P0
  description: "时间倒挂校验"
target_entity: Cycle
input:
  key: entity_cycle
logic:
  filter_expr: "pl.col('end_time') < pl.col('start_time')"
output:
  key: rule_p0_time_check_hits
  stage: RESULT
alert:
  enabled: true
  template: "时间倒挂: {trigger_cycle_ids}"
  title: "P0 告警：时间倒挂检测"
depends_on: []
```

---

## 🔧 扩展开发

### 添加新规则

1. 在 `plugins/configs/rules/` 下创建新的 YAML 文件
2. 定义规则元信息、逻辑、输出、依赖关系
3. DAG 会自动扫描并生成新任务

### 添加新数据源

1. 在 `plugins/configs/sources.yaml` 中添加新的数据源配置
2. `UniversalLoaderOperator` 会自动提取数据
3. 如果需要新的数据源类型，扩展 `UniversalLoaderOperator._extract_from_source` 方法

### 自定义告警模板

修改 `plugins/services/notification.py` 中的 `_get_single_alert_template()` 和 `_get_batch_summary_template()` 方法。

---

## 📊 监控与运维

### 日志位置

- **Airflow 日志**：`$AIRFLOW_HOME/logs/`
- **Task 日志**：每个 Task 的日志在 Airflow UI 中查看

### 数据保留策略

- **Parquet 文件**：默认保留 7 天（在 T+1 批处理模式下）
- **XCom 数据**：由 Airflow 配置控制
- **数据库记录**：由业务需求决定

### 性能调优

- **Parquet 压缩**：调整 `PARQUET_COMPRESSION_*` 环境变量
- **并发度**：调整 Airflow 的 `parallelism` 和 `max_active_tasks_per_dag`
- **资源限制**：在 `docker-compose.yml` 中调整容器资源限制

---

## 🤝 联系方式

- **Owner**: data-governance@example.com
- **项目地址**: https://github.com/your-org/cactus-box

---

## 📝 版本历史

### v1.0.0 (2026-01-26)

- ✅ 完成核心框架（Context、IOStrategy、BaseOperator）
- ✅ 实现 5 大算子（Loader、Adapter、Rule、Aggregator、Dispatcher）
- ✅ 实现辅助模块（RuleScanner、NotificationService）
- ✅ 完成曳光弹场景（P0 时间倒挂、P1 双箱、P2 超时）
- ✅ 防御性解析 + 降级处理

---

**🎉 数据治理平台 v1.0 已就绪！**
