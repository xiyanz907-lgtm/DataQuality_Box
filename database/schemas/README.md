# Database Schemas

此目录存放 MySQL 数据库表的 DDL（Data Definition Language）文件。

## 📂 目录结构

```
database/
  └── schemas/
      ├── README.md                           (本文件)
      └── schema_qa_ground_truth_result.sql   (Ground Truth 验证结果表 - 已重命名为 datalog_logic_check_result)
```

## 🆚 与 `plugins/schemas/` 的区别

| 目录 | 用途 | 文件类型 | 示例 |
|------|------|----------|------|
| **`database/schemas/`** | SQL DDL（建表语句） | `.sql` 文件 | `CREATE TABLE ...` |
| **`plugins/schemas/`** | Pandera 验证规则 | `.py` 文件 | `pa.DataFrameSchema(...)` |

## 📋 表清单

### datalog_logic_check_result (原 qa_ground_truth_result)
- **用途**: 地面真相验证结果表
- **关联 DAG**: `dq_v1_worker_ground_truth` (Worker), `dq_v1_controller` (Controller)
- **文件**: `schema_qa_ground_truth_result.sql`

## 🚀 使用方法

### 创建表

```bash
# 方法 1：直接执行 SQL 文件
mysql -h <host> -u <user> -p dagster_pipelines < database/schemas/schema_qa_ground_truth_result.sql

# 方法 2：进入 MySQL 后执行
mysql -h <host> -u <user> -p
USE dagster_pipelines;
SOURCE database/schemas/schema_qa_ground_truth_result.sql;
```

### 验证表是否创建成功

```sql
-- 查看表结构
DESC dagster_pipelines.datalog_logic_check_result;

-- 查看索引
SHOW INDEX FROM dagster_pipelines.datalog_logic_check_result;
```

## 📝 添加新表的规范

1. **命名规范**: `schema_<表名>.sql`
2. **包含内容**:
   - 表注释（说明用途）
   - 完整的 DDL 语句（使用 `CREATE TABLE IF NOT EXISTS`）
   - 字段注释（说明每个字段的含义）
   - 索引定义
   - 使用说明（可选）

3. **示例模板**:

```sql
-- <表名> 表说明
-- 用途: ...
-- 关联 DAG: ...

CREATE TABLE IF NOT EXISTS <database>.<table_name> (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    -- ... 其他字段
    KEY idx_<column> (<column>) COMMENT '索引说明'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='表说明';

-- 使用说明:
-- 1. ...
-- 2. ...
```

## 🔍 相关文档

- Worker DAG: `dags/dag_worker_ground_truth.py`
- Controller DAG: `dags/dq_v1_controller_dag.py`
- 部署说明: `QUICK_START.md`

