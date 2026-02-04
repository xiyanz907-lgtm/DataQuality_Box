# DAG Factory - 下一步行动指南

## ✅ 已完成的工作总结

### 核心功能实现 (100%)

| 组件 | 文件 | 状态 |
|------|------|------|
| **Pydantic Schema** | `plugins/schemas/source_config_schema.py` | ✅ 完成 |
| **DAG Factory** | `plugins/orchestration/dag_factory.py` | ✅ 完成 |
| **DAG 注册入口** | `dags/dynamic_governance_dags.py` | ✅ 完成 |
| **示例配置 1** | `plugins/configs/sources/daily_cycle_etl.yaml` | ✅ 完成 |
| **示例配置 2** | `plugins/configs/sources/asset_driven_etl.yaml` | ✅ 完成 |
| **示例配置 3** | `plugins/configs/sources/manual_adhoc_analysis.yaml` | ✅ 完成 |
| **单元测试** | `tests/test_dag_factory.py` | ✅ 完成 |
| **验证脚本** | `tests/validate_dag_factory.sh` | ✅ 完成 |
| **实施指南** | `DAG_FACTORY_IMPLEMENTATION_GUIDE.md` | ✅ 完成 |
| **部署清单** | `DAG_FACTORY_DEPLOYMENT_CHECKLIST.md` | ✅ 完成 |
| **实施总结** | `DAG_FACTORY_SUMMARY.md` | ✅ 完成 |
| **依赖更新** | `deploy/requirements.txt` | ✅ 完成 |

---

## 🚀 下一步：立即执行的操作

### Step 1: 重新构建并启动 Airflow (必需)

```bash
cd /home/ubuntu/cactus_box/cactus-box/deploy

# 1. 停止现有容器
docker-compose down

# 2. 重新构建镜像（安装 pydantic 和 pytest）
docker-compose build --no-cache

# 3. 启动服务
docker-compose up -d

# 4. 等待 Airflow 完全启动（约 30 秒）
sleep 30
```

**预期结果**：
- 容器成功启动
- 新依赖 (`pydantic`, `pytest`) 已安装

---

### Step 2: 运行验证脚本 (推荐)

```bash
# 运行自动验证脚本
/home/ubuntu/cactus_box/cactus-box/tests/validate_dag_factory.sh
```

**预期输出**：
```
🚀 Starting DAG Factory Validation...
======================================
[Step 1] Checking Airflow container...
✅ Container is running

[Step 2] Checking source config files...
✅ Found 3 source config files

[Step 3] Validating Pydantic Schema...
✅ Pydantic Schema validation passed

[Step 4] Testing DAG Factory...
✅ Generated 3 DAGs:
   - gov_daily_cycle_etl: 7 tasks
   - gov_asset_driven_etl: 7 tasks
   - gov_manual_adhoc_analysis: 7 tasks

[Step 5] Validating example YAML files...
✅ daily_cycle_etl.yaml is valid
✅ asset_driven_etl.yaml is valid
✅ manual_adhoc_analysis.yaml is valid

✅ DAG Factory Validation Complete
```

**如果有错误**：
- 检查日志：`docker logs cactus_airflow_container --tail=100`
- 查看详细错误信息

---

### Step 3: 检查 Airflow UI (必需)

1. **访问 Airflow UI**:
   ```
   http://localhost:8080
   ```

2. **查找生成的 DAG**:
   - 搜索框输入 `gov_`
   - 应该能看到 3 个 DAG：
     - `gov_daily_cycle_etl`
     - `gov_asset_driven_etl`
     - `gov_manual_adhoc_analysis`

3. **检查 DAG 详情**:
   - 点击 DAG ID 进入详情页
   - 查看 **Graph View**，确认任务流：
     ```
     [Sensor] → universal_loader → domain_adapter 
         → rule_tasks.rule_p0_time_check
         → rule_tasks.rule_p1_twin_lift
         → rule_tasks.rule_p2_timeout
         → context_aggregator
         → notification_dispatcher
     ```

4. **检查标签**:
   - 每个 DAG 应该包含标签：`auto-generated`, `governance`

---

### Step 4: 运行单元测试 (可选，但推荐)

```bash
docker exec cactus_airflow_container pytest /opt/airflow/tests/test_dag_factory.py -v
```

**预期输出**：
```
tests/test_dag_factory.py::TestSourceConfigSchema::test_valid_cron_config PASSED
tests/test_dag_factory.py::TestSourceConfigSchema::test_invalid_cron_missing_expression PASSED
tests/test_dag_factory.py::TestSourceConfigSchema::test_valid_sql_sensor PASSED
...
tests/test_dag_factory.py::TestIntegrationWithExamples::test_load_daily_cycle_etl PASSED
tests/test_dag_factory.py::TestIntegrationWithExamples::test_load_asset_driven_etl PASSED

===================== 10 passed in 2.34s =====================
```

---

### Step 5: 手动触发测试 DAG (可选)

```bash
# 触发 MANUAL 模式的 DAG
docker exec cactus_airflow_container airflow dags trigger gov_manual_adhoc_analysis

# 查看运行状态
docker exec cactus_airflow_container airflow dags list-runs -d gov_manual_adhoc_analysis --state running

# 或在 UI 中查看：
# http://localhost:8080/dags/gov_manual_adhoc_analysis/grid
```

---

## ⚠️ 常见问题处理

### 问题 1: DAG 未出现在 UI 中

**排查步骤**：

1. **检查 Import Errors**:
   ```bash
   docker exec cactus_airflow_container airflow dags list-import-errors
   ```

2. **查看 Scheduler 日志**:
   ```bash
   docker logs cactus_airflow_container --tail=200 | grep -A 10 "dag_factory"
   ```

3. **手动测试加载**:
   ```bash
   docker exec -it cactus_airflow_container python /opt/airflow/dags/dynamic_governance_dags.py
   ```

---

### 问题 2: Pydantic 校验错误

**现象**：Import Error 显示 `ValidationError`

**解决**：

```bash
docker exec -it cactus_airflow_container python

>>> import yaml
>>> from plugins.schemas.source_config_schema import SourceYAMLConfig
>>> with open('/opt/airflow/plugins/configs/sources/daily_cycle_etl.yaml') as f:
...     config = yaml.safe_load(f)
>>> SourceYAMLConfig(**config)  # 会显示详细错误
```

---

### 问题 3: Adapter 或 Rules 未找到

**现象**：DAG Factory 跳过了某个 source YAML

**排查**：

```bash
# 检查 adapter 文件是否存在
ls -l /home/ubuntu/cactus_box/cactus-box/plugins/configs/adapters/cycle_adapter.yaml

# 检查 rules 的 target_entity
grep -r "target_entity" /home/ubuntu/cactus_box/cactus-box/plugins/configs/rules/

# 确保：
# - adapters/cycle_adapter.yaml 存在（小写）
# - rules/*.yaml 中至少有一个 target_entity: "Cycle"（首字母大写）
```

---

## 📋 验证清单

**请在执行完上述步骤后，逐项确认：**

### 部署验证
- [ ] Docker 容器成功重启
- [ ] Pydantic 和 pytest 已安装
- [ ] 验证脚本执行成功（无错误）
- [ ] 单元测试全部通过

### 功能验证
- [ ] Airflow UI 中可见 3 个 `gov_` 开头的 DAG
- [ ] 每个 DAG 有 7 个 Task
- [ ] DAG 的 Schedule 配置正确：
  - `gov_daily_cycle_etl`: `0 2 * * *`
  - `gov_asset_driven_etl`: Dataset 触发
  - `gov_manual_adhoc_analysis`: None (手动)
- [ ] Task 依赖关系正确（Graph View 查看）
- [ ] 手动触发 `gov_manual_adhoc_analysis` 能成功运行

### 配置验证
- [ ] 3 个示例 YAML 都通过 Pydantic 校验
- [ ] `cycle_adapter.yaml` 存在且格式正确
- [ ] Rules 文件的 `target_entity` 匹配
- [ ] 数据库连接 (`conn_id`) 配置正确（如果不正确，DAG 运行会失败，但不影响 DAG 生成）

---

## 🎯 可选：迁移现有 governance_main_dag.py

如果验证成功，你可以选择替代现有的 `governance_main_dag.py`：

### 方案 A: 完全替代（推荐）

```bash
# 1. 备份旧 DAG
cp /home/ubuntu/cactus_box/cactus-box/dags/governance_main_dag.py \
   /home/ubuntu/cactus_box/cactus-box/dags/governance_main_dag.py.bak

# 2. 删除旧 DAG
rm /home/ubuntu/cactus_box/cactus-box/dags/governance_main_dag.py

# 3. 重启 Scheduler
docker-compose restart airflow

# 4. 确认旧 DAG 消失，新 DAG 依然存在
```

### 方案 B: 暂时共存（过渡期）

```bash
# 保留 governance_main_dag.py
# 同时使用 DAG Factory 生成的 DAG
# 逐步迁移配置
```

---

## 📚 参考文档

| 文档 | 用途 |
|------|------|
| [DAG_FACTORY_IMPLEMENTATION_GUIDE.md](DAG_FACTORY_IMPLEMENTATION_GUIDE.md) | 完整的实施和使用指南 |
| [DAG_FACTORY_DEPLOYMENT_CHECKLIST.md](DAG_FACTORY_DEPLOYMENT_CHECKLIST.md) | 部署前检查清单 |
| [DAG_FACTORY_SUMMARY.md](DAG_FACTORY_SUMMARY.md) | 技术实施总结 |

---

## 📞 支持

如有问题或需要协助：
- **技术负责人**: box_admin
- **邮件**: xiyan.zhou@westwell-lab.com
- **文档**: 本目录下的相关 Markdown 文件

---

## ✅ 完成标志

当你完成以上所有步骤并确认所有验证清单后，即可认为 **DAG Factory 实施完成**。

**恭喜！你现在可以：**
1. 通过添加 YAML 文件快速创建新的数据治理 DAG
2. 无需编写 Python 代码即可配置复杂的调度策略
3. 享受 Pydantic 类型安全带来的配置可靠性
4. 基于 `target_entity` 自动关联 Adapter 和 Rules

**下一步优化方向**：
- 添加更多 Sensor 类型（HttpSensor, S3KeySensor）
- 实现 Web UI 可视化配置界面
- 添加配置文件版本控制和审计日志
