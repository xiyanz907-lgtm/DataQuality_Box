# Context 恢复测试指南

## 问题描述
DomainAdapterOperator 无法从 UniversalLoaderOperator 恢复 Context，导致 `data_registry` 为空。

## 修复内容
在 `plugins/infra/operators.py` 的 `_restore_context()` 方法中添加了自动检测上游任务的逻辑。

## 验证步骤

### 1. 检查代码修改是否生效
```bash
docker exec deploy-airflow-1 grep -A 10 "Auto-detected upstream task" /opt/airflow/plugins/infra/operators.py
```

**预期输出**：应该能看到新增的代码逻辑。

### 2. 触发 DAG 测试
```bash
docker exec deploy-airflow-1 airflow dags trigger governance_main_dag \
  --conf '{"batch_id": "BATCH_CONTEXT_TEST"}'
```

### 3. 查看 domain_adapter 日志
等待 30 秒后，在 Airflow Web UI 查看 `domain_adapter` 任务日志，应该看到：

✅ **成功标志**：
```
🔍 Auto-detected upstream task: universal_loader
✅ Restored context from upstream: universal_loader
🔄 Adapting to entity: Cycle
```

❌ **失败标志**（修复前）：
```
📦 Creating new context (no valid upstream found)
❌ Task [domain_adapter] failed: "Data key 'raw_cycle_section' not found in registry. Available keys: []"
```

### 4. 检查生成的文件
```bash
# 查看是否生成了 ENTITY 阶段的文件
ls -R /home/ubuntu/cactus_box/data/governance/batch_id=BATCH_CONTEXT_TEST/

# 预期输出应该包含：
# - stage=RAW/key=raw_cycle_section/
# - stage=ENTITY/key=entity_cycle/  <-- 新增！
```

## 如果还有问题

### 方案 B：显式指定 upstream_task_id
如果自动检测失败，可以在 DAG 文件中显式指定：

```python
# dags/governance_main_dag.py
adapter_task = DomainAdapterOperator(
    task_id='domain_adapter',
    config_path='configs/adapters/cycle_adapter.yaml',
    upstream_task_id='universal_loader',  # 添加这一行
)
```

## 预期结果
修复后，`domain_adapter` 应该能够：
1. ✅ 自动检测上游任务（universal_loader）
2. ✅ 从 XCom 恢复 Context
3. ✅ 读取 `raw_cycle_section` 数据
4. ✅ 生成 `entity_cycle` 数据
5. ✅ 将 Entity 数据写入 Parquet
