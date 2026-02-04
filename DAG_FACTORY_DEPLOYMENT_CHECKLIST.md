# DAG Factory 部署确认清单

## 📋 已完成的工作

### 1. 核心代码实现 ✅

| 文件 | 状态 | 说明 |
|------|------|------|
| `plugins/schemas/source_config_schema.py` | ✅ 已创建 | Pydantic Schema，严格校验YAML配置 |
| `plugins/orchestration/dag_factory.py` | ✅ 已创建 | DAG Factory核心逻辑，动态生成DAG |
| `dags/dynamic_governance_dags.py` | ✅ 已创建 | DAG注册入口，Airflow自动加载 |

### 2. 配置文件和示例 ✅

| 文件 | 状态 | 说明 |
|------|------|------|
| `plugins/configs/sources/` | ✅ 目录已创建 | 数据源配置目录 |
| `plugins/configs/sources/daily_cycle_etl.yaml` | ✅ 已创建 | 示例1: CRON定时触发 |
| `plugins/configs/sources/asset_driven_etl.yaml` | ✅ 已创建 | 示例2: DATASET事件驱动 |
| `plugins/configs/sources/manual_adhoc_analysis.yaml` | ✅ 已创建 | 示例3: MANUAL手动触发 |

### 3. 测试和文档 ✅

| 文件 | 状态 | 说明 |
|------|------|------|
| `tests/test_dag_factory.py` | ✅ 已创建 | 单元测试（Schema校验、DAG生成） |
| `tests/validate_dag_factory.sh` | ✅ 已创建 | 快速验证脚本 |
| `DAG_FACTORY_IMPLEMENTATION_GUIDE.md` | ✅ 已创建 | 完整实施指南 |
| `DAG_FACTORY_DEPLOYMENT_CHECKLIST.md` | ✅ 已创建 | 本文件 |

### 4. 依赖更新 ✅

| 依赖 | 状态 | 版本 |
|------|------|------|
| `pydantic` | ✅ 已添加 | >=2.0.0 |
| `pytest` | ✅ 已添加 | >=7.0.0 |

### 5. 代码质量 ✅

- ✅ **Linter检查**: 无错误
- ✅ **类型安全**: Pydantic严格校验
- ✅ **错误处理**: 跳过错误配置，不阻塞其他DAG
- ✅ **日志记录**: 完整的logger记录

---

## 🔍 部署前确认事项

### A. 环境检查

```bash
# 1. 检查Docker容器是否运行
docker ps | grep airflow

# 2. 检查目录结构
ls -R /home/ubuntu/cactus_box/cactus-box/plugins/configs/

# 3. 检查依赖是否安装（部署后）
docker exec cactus_airflow_container pip list | grep pydantic
docker exec cactus_airflow_container pip list | grep pytest
```

### B. 配置文件确认

**请确认以下配置文件是否符合预期：**

1. **数据库连接** (`datalog_mysql_conn`, `qa_mysql_conn`)
   - [ ] Airflow Connection 是否已配置？
   - [ ] 示例YAML中的 `conn_id` 是否正确？

2. **Adapter 文件** (`cycle_adapter.yaml`)
   - [ ] 是否存在于 `plugins/configs/adapters/` 目录？
   - [ ] 配置是否完整（inputs, joins, field_mapping）？

3. **Rules 文件** (p0/p1/p2 规则)
   - [ ] 是否存在于 `plugins/configs/rules/` 目录？
   - [ ] `target_entity` 字段是否正确设置为 `"Cycle"`？

### C. 迁移策略确认

**你选择的方案是：替代现有 `governance_main_dag.py`**

需要执行的操作：
```bash
# 1. 备份现有 DAG（可选）
cp /home/ubuntu/cactus_box/cactus-box/dags/governance_main_dag.py \
   /home/ubuntu/cactus_box/cactus-box/dags/governance_main_dag.py.bak

# 2. 删除旧 DAG（如果确认迁移）
rm /home/ubuntu/cactus_box/cactus-box/dags/governance_main_dag.py

# 3. 迁移原有 sources.yaml（如果需要）
# 注意：需要手动调整为新的Schema格式，特别是添加 scheduling 字段
```

**是否需要保留 `governance_main_dag.py`？**
- [ ] 是 - 暂时共存，逐步迁移
- [ ] 否 - 完全替代（推荐）

---

## 🚀 部署步骤

### Step 1: 安装依赖

```bash
cd /home/ubuntu/cactus_box/cactus-box/deploy
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

**预期结果**：
- 容器成功启动
- `pydantic` 和 `pytest` 已安装

### Step 2: 验证配置加载

```bash
# 运行验证脚本
/home/ubuntu/cactus_box/cactus-box/tests/validate_dag_factory.sh
```

**预期输出**：
```
✅ Container is running
✅ Found 3 source config files
✅ Pydantic Schema validation passed
✅ DAG Factory test passed
   - gov_daily_cycle_etl: 7 tasks
   - gov_asset_driven_etl: 7 tasks
   - gov_manual_adhoc_analysis: 7 tasks
```

### Step 3: 检查 Airflow UI

1. 访问 http://localhost:8080
2. 查找 DAG ID 以 `gov_` 开头的 DAG
3. 检查以下信息：
   - [ ] DAG 数量正确（3个示例 + 可能的自定义配置）
   - [ ] DAG 标签包含 `auto-generated`, `governance`
   - [ ] Schedule 配置正确（CRON/Dataset/None）
   - [ ] Task 依赖关系正确（Sensor → Loader → Adapter → Rules → Aggregator → Dispatcher）

### Step 4: 运行单元测试

```bash
docker exec cactus_airflow_container pytest /opt/airflow/tests/test_dag_factory.py -v
```

**预期结果**：
- 所有测试通过（约 10+ 个测试用例）
- 无 Schema 校验错误
- 无 DAG 生成错误

### Step 5: 触发测试运行

```bash
# 手动触发一个 MANUAL 模式的 DAG
docker exec cactus_airflow_container airflow dags trigger gov_manual_adhoc_analysis

# 查看运行日志
docker exec cactus_airflow_container airflow dags list-runs -d gov_manual_adhoc_analysis --state running
```

---

## ⚠️ 已知问题和注意事项

### 1. Adapter 文件缺失

**现象**：DAG Factory 扫描到 source YAML，但没有生成 DAG

**原因**：`plugins/configs/adapters/{target_entity}_adapter.yaml` 不存在

**解决**：
```bash
# 检查 adapter 文件
ls /home/ubuntu/cactus_box/cactus-box/plugins/configs/adapters/

# 如果缺少，创建对应的 adapter 文件
# 文件名必须与 source YAML 中的 target_entity 小写匹配
# 例如：target_entity: "Cycle" → cycle_adapter.yaml
```

### 2. Rules 未被挂载

**现象**：DAG 生成成功，但没有 rule_tasks TaskGroup

**原因**：`rules/*.yaml` 中的 `target_entity` 与 source 不匹配

**解决**：
```bash
# 检查所有规则的 target_entity
grep -r "target_entity" /home/ubuntu/cactus_box/cactus-box/plugins/configs/rules/

# 确保至少有一个规则的 target_entity 与 source 一致
```

### 3. Pydantic 校验失败

**现象**：Airflow UI 显示 Import Error

**解决**：
```bash
# 查看详细错误
docker exec cactus_airflow_container airflow dags list-import-errors

# 使用 Python 单独校验 YAML
docker exec -it cactus_airflow_container python
>>> import yaml
>>> from plugins.schemas.source_config_schema import SourceYAMLConfig
>>> with open('/opt/airflow/plugins/configs/sources/YOUR_FILE.yaml') as f:
...     config = yaml.safe_load(f)
>>> SourceYAMLConfig(**config)  # 会显示详细错误
```

### 4. 示例配置需要调整

**需要修改的地方**：
- `conn_id`: 确保与 Airflow Connections 中的 ID 一致
- `query`: 根据实际表结构调整 SQL
- `email`: 修改为实际的邮件地址
- `cron_expression`: 根据实际调度需求调整

---

## 📊 验证清单

### 部署验证

- [ ] Docker 容器成功启动
- [ ] 依赖 `pydantic` 和 `pytest` 已安装
- [ ] 验证脚本执行成功
- [ ] 单元测试全部通过

### 功能验证

- [ ] Airflow UI 中可见生成的 DAG
- [ ] DAG 数量符合预期
- [ ] DAG 的 Schedule 配置正确
- [ ] Task 依赖关系正确
- [ ] 手动触发 DAG 能成功运行

### 配置验证

- [ ] 所有示例 YAML 通过 Pydantic 校验
- [ ] Adapter 文件存在且格式正确
- [ ] Rules 文件的 `target_entity` 匹配
- [ ] 数据库连接 (`conn_id`) 配置正确

### 错误处理验证

- [ ] 故意创建一个错误的 YAML，确认被跳过
- [ ] 检查 log 中有 `⚠️ Skip invalid YAML` 记录
- [ ] 其他正常的 DAG 不受影响

---

## 📞 问题排查

### 日志查看

```bash
# Airflow Scheduler 日志
docker logs cactus_airflow_container --tail=200 | grep -A 10 "dag_factory"

# 特定 DAG 的日志
docker exec cactus_airflow_container airflow dags show gov_daily_cycle_etl

# Import Errors
docker exec cactus_airflow_container airflow dags list-import-errors
```

### 手动测试 DAG Factory

```bash
docker exec -it cactus_airflow_container python

>>> import sys
>>> sys.path.insert(0, '/opt/airflow')
>>> from plugins.orchestration.dag_factory import DAGFactory
>>> factory = DAGFactory()
>>> dags = factory.scan_and_generate_dags()
>>> print(f"Generated {len(dags)} DAGs")
>>> for dag_id in dags.keys():
...     print(f"  - {dag_id}")
```

---

## 🎯 后续优化建议

### 短期（1-2周）

1. **迁移现有数据源**
   - 将 `sources.yaml` 拆分为多个独立文件
   - 调整为新的 Schema 格式
   - 逐个验证功能等价性

2. **完善监控**
   - 添加 DAG 生成数量的监控指标
   - 配置错误的告警（Slack/Email）

3. **文档完善**
   - 为团队编写快速入门指南
   - 录制演示视频

### 中期（1-2月）

1. **扩展 Sensor 类型**
   - 支持 `HttpSensor`（API 就绪检查）
   - 支持 `S3KeySensor`（对象存储文件检查）

2. **增强配置能力**
   - 支持 DAG 级别的 SLA 配置
   - 支持自定义 Task 超时时间

3. **性能优化**
   - 大量 YAML 时的加载性能优化
   - 缓存机制（避免重复解析）

### 长期（3-6月）

1. **可视化配置界面**
   - Web UI 编辑 YAML 配置
   - 实时预览生成的 DAG 结构

2. **多租户支持**
   - 按业务线隔离配置
   - 权限管理

3. **智能推荐**
   - 根据历史数据推荐调度时间
   - 自动优化规则执行顺序

---

## ✅ 最终确认

**请在完成所有验证后，勾选以下确认项：**

- [ ] 所有代码已提交到版本控制
- [ ] 文档已更新并归档
- [ ] 团队已知晓新的配置方式
- [ ] 监控和告警已配置
- [ ] 回滚方案已准备（备份旧 DAG）

**部署负责人签字**：_________________  
**部署日期**：_________________  
**审核人签字**：_________________

---

## 📚 相关文档

- [DAG Factory 实施指南](DAG_FACTORY_IMPLEMENTATION_GUIDE.md)
- [Source Config Schema](plugins/schemas/source_config_schema.py)
- [示例配置文件](plugins/configs/sources/)
- [单元测试](tests/test_dag_factory.py)

**如有问题，请联系**: xiyan.zhou@westwell-lab.com
