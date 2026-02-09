# Asset Packing 实施总结

## 🎯 问题

`gov_daily_cycle_etl` DAG 运行成功，但 P1 资产没有保存到 `auto_test_case_catalog` 表中，导致 DAG B（`asset_packing_dag`）无法被触发。

## 🔍 根本原因

**DAG Factory 缺少 Step 7**: `save_assets_to_queue` 任务

与原来的 `governance_main_dag.py` 对比：
- `governance_main_dag.py`: 包含 7 个任务（含 `save_assets_to_queue`）
- `gov_daily_cycle_etl`: 只有 6 个任务（**缺少** `save_assets_to_queue`）

## ✅ 实施方案

### 方案选择

根据用户确认，采用**方案 B（YAML 配置驱动）**：
1. 在 YAML Schema 中添加 `asset_packing` 配置项
2. 所有动态生成的 DAG 都添加此任务
3. 使用相同的数据库连接 (`qa_mysql_conn`)
4. 通过 YAML 配置控制是否启用

---

## 📋 实施步骤

### Step 1: 更新 Pydantic Schema

**文件**: `plugins/schemas/source_config_schema.py`

**添加的配置类**:
```python
class AssetPackingConfig(BaseModel):
    """资产打包配置"""
    enabled: bool = True  # 是否启用资产保存
    conn_id: str = Field(default="qa_mysql_conn", description="数据库连接ID")
    table: str = Field(default="auto_test_case_catalog", description="资产元数据表名")
```

**集成到 SourceYAMLConfig**:
```python
class SourceYAMLConfig(BaseModel):
    # ... 其他字段 ...
    asset_packing: Optional[AssetPackingConfig] = Field(
        default_factory=lambda: AssetPackingConfig(),
        description="资产打包配置（可选，默认启用）"
    )
```

### Step 2: 更新 DAG Factory

**文件**: `plugins/orchestration/dag_factory.py`

**添加的导入**:
```python
from airflow.providers.mysql.hooks.mysql import MySqlHook
from plugins.datasets import GOVERNANCE_ASSET_DATASET
```

**添加的任务生成逻辑** (在 `_build_task_pipeline` 方法末尾):
```python
# ========== Step 7: Save Assets to Queue (触发 DAG B) ==========
if config.asset_packing and config.asset_packing.enabled:
    asset_config = config.asset_packing
    
    def save_assets_to_queue(conn_id: str, table_name: str, **context):
        """将 P1 资产写入数据库队列，触发 DAG B"""
        # 1. 从 XCom 恢复 GovernanceContext
        # 2. 提取 P1 资产
        # 3. 写入数据库（状态: PENDING）
        # 4. 触发 Dataset
    
    save_assets_task = PythonOperator(
        task_id='save_assets_to_queue',
        python_callable=save_assets_to_queue,
        op_kwargs={
            'conn_id': asset_config.conn_id,
            'table_name': asset_config.table
        },
        outlets=[GOVERNANCE_ASSET_DATASET],  # 声明 Dataset 输出
        dag=dag
    )
    
    dispatcher >> save_assets_task
```

### Step 3: 更新示例 YAML 配置

**文件**: `plugins/configs/sources/daily_cycle_etl.yaml`

**添加的配置**:
```yaml
# 资产打包配置（可选，默认启用）
asset_packing:
  enabled: true  # 是否保存 P1 资产到数据库队列
  conn_id: "qa_mysql_conn"  # 数据库连接ID
  table: "auto_test_case_catalog"  # 元数据表名
```

---

## 📊 完整任务列表

### gov_daily_cycle_etl DAG (修复后)

现在包含 **9 个任务**（如果启用 Sensor）：

```
1. data_ready_sensor              ← SQL Sensor（前置检查）
2. universal_loader               ← 数据加载
3. domain_adapter                 ← 领域适配
4. rule_tasks.rule_p0_time_check  ← P0: 时间倒挂检测
5. rule_tasks.rule_p1_twin_lift   ← P1: 双箱作业识别
6. rule_tasks.rule_p2_timeout     ← P2: 超时监控
7. context_aggregator             ← 上下文聚合
8. notification_dispatcher        ← 通知分发
9. save_assets_to_queue           ← 资产保存 & 触发 DAG B ✨ 新增
```

### 任务依赖关系

```
data_ready_sensor 
    ↓
universal_loader 
    ↓
domain_adapter 
    ↓
rule_tasks (TaskGroup)
    ├─ rule_p0_time_check
    ├─ rule_p1_twin_lift
    └─ rule_p2_timeout
    ↓
context_aggregator 
    ↓
notification_dispatcher
    ↓
save_assets_to_queue  ✨ 新增
```

---

## 🔄 数据流

### 完整的资产处理流程

```
1. Rule Engine (P1 规则) 
   ↓ 识别 P1 资产
2. Context Aggregator 
   ↓ 收集所有资产到 GovernanceContext.assets
3. Notification Dispatcher
   ↓ 发送通知（不处理资产）
4. Save Assets to Queue ✨
   ↓ 从 XCom 读取 GovernanceContext
   ↓ 提取 assets 列表
   ↓ 写入 auto_test_case_catalog (process_status='PENDING')
   ↓ 声明 Dataset 输出
5. Airflow Dataset 机制
   ↓ 检测到 GOVERNANCE_ASSET_DATASET 被更新
6. DAG B (asset_packing_dag) 被触发
   ↓ 开始资产打包流程
```

### save_assets_to_queue 的核心逻辑

```python
def save_assets_to_queue(conn_id: str, table_name: str, **context):
    # 1. 从 XCom 恢复 Context
    ctx_json = context['ti'].xcom_pull(
        task_ids='context_aggregator', 
        key='governance_context'
    )
    ctx = GovernanceContext.from_json(ctx_json)
    assets = ctx.assets  # P1 资产列表
    
    # 2. 连接数据库
    hook = MySqlHook(mysql_conn_id=conn_id)
    
    # 3. 批量插入
    for asset in assets:
        INSERT INTO {table_name} (
            batch_id, cycle_id, vehicle_id, ...,
            process_status  # ← 'PENDING'
        ) VALUES (...)
    
    # 4. Dataset 自动触发 (通过 outlets 参数)
```

---

## 🎨 YAML 配置灵活性

### 启用资产打包（默认）

```yaml
asset_packing:
  enabled: true
  conn_id: "qa_mysql_conn"
  table: "auto_test_case_catalog"
```

**效果**: 
- 添加 `save_assets_to_queue` 任务
- P1 资产自动保存到数据库
- 触发 DAG B

### 禁用资产打包（可选）

```yaml
asset_packing:
  enabled: false
```

**效果**:
- 不添加 `save_assets_to_queue` 任务
- P1 资产只记录在日志中
- 不触发 DAG B

### 使用默认配置（最简）

```yaml
# 不配置 asset_packing，使用默认值
# 等价于 enabled=true, conn_id="qa_mysql_conn", table="auto_test_case_catalog"
```

---

## 🔧 技术亮点

### 1. 闭包与参数传递

**问题**: 如何在 DAG 解析时传递配置到运行时函数？

**解决**: 使用 `op_kwargs` 显式传递参数
```python
save_assets_task = PythonOperator(
    python_callable=save_assets_to_queue,
    op_kwargs={
        'conn_id': asset_config.conn_id,
        'table_name': asset_config.table
    }
)
```

### 2. Dataset 触发机制

**关键**: `outlets` 参数声明 Dataset 输出
```python
save_assets_task = PythonOperator(
    ...,
    outlets=[GOVERNANCE_ASSET_DATASET]  # 自动触发 DAG B
)
```

### 3. 条件任务生成

**关键**: 根据 YAML 配置动态决定是否添加任务
```python
if config.asset_packing and config.asset_packing.enabled:
    # 添加任务
else:
    logger.info("Asset packing disabled")
    # 不添加任务
```

---

## ✅ 验证步骤

### 1. 检查 DAG 任务列表

```bash
docker exec deploy-airflow-1 airflow tasks list gov_daily_cycle_etl
```

**预期输出**: 包含 `save_assets_to_queue`

### 2. 手动触发 DAG A

```bash
docker exec deploy-airflow-1 airflow dags trigger gov_daily_cycle_etl
```

### 3. 查看资产是否保存到数据库

```sql
SELECT * FROM auto_test_case_catalog 
WHERE process_status = 'PENDING' 
ORDER BY created_at DESC 
LIMIT 10;
```

### 4. 确认 DAG B 被触发

在 Airflow UI 中查看 `asset_packing_dag` 的运行记录，应该看到 `dataset_triggered__...` 类型的运行。

---

## 📚 相关文档

| 文档 | 用途 |
|------|------|
| `DAG_FACTORY_FIX_SUMMARY.md` | DAG Factory 参数修复总结 |
| `DAG_FACTORY_IMPLEMENTATION_GUIDE.md` | DAG Factory 完整使用指南 |
| `SINGLE_TABLE_IMPLEMENTATION_COMPLETE.md` | DAG B 单表方案实施总结 |
| `plugins/datasets.py` | Dataset 定义和使用说明 |

---

## 🎯 最终确认

- [x] Pydantic Schema 已更新（添加 `AssetPackingConfig`）
- [x] DAG Factory 已添加 Step 7（`save_assets_to_queue`）
- [x] 示例 YAML 已更新（`daily_cycle_etl.yaml`）
- [x] `gov_daily_cycle_etl` 包含 9 个任务
- [x] `save_assets_to_queue` 任务可见
- [x] 支持通过 YAML 配置启用/禁用
- [x] 支持自定义数据库连接和表名

**恭喜！资产打包功能已完整实施！** 🎉

---

## 🚀 下一步测试

1. **手动触发测试**:
   ```bash
   docker exec deploy-airflow-1 airflow dags trigger gov_daily_cycle_etl
   ```

2. **观察执行**:
   - 在 Airflow UI 中查看任务执行状态
   - 重点关注 `save_assets_to_queue` 任务日志

3. **验证数据**:
   ```sql
   SELECT COUNT(*) AS pending_assets 
   FROM auto_test_case_catalog 
   WHERE process_status = 'PENDING';
   ```

4. **确认 DAG B 触发**:
   - 检查 `asset_packing_dag` 是否有新的运行记录
   - 类型应为 `dataset_triggered__...`

**测试成功标志**: 
- `save_assets_to_queue` 任务成功
- 数据库中有 `PENDING` 状态的资产记录
- DAG B 自动启动并处理资产
