# Deploy 文件夹环境变量配置说明

## 📋 配置 InfluxDB 和 Map API

### 步骤 1：创建 .env 文件

```bash
cd /home/ubuntu/cactus_box/cactus-box/deploy

# 如果已有 .env 文件，直接编辑
nano .env

# 如果没有 .env 文件，从模板创建
cp env.template .env
nano .env
```

### 步骤 2：添加/修改 InfluxDB 配置

在 `.env` 文件中添加或修改以下内容：

```bash
# ============================================================
# InfluxDB 配置（Ground Truth Validation）
# ============================================================
INFLUX_URL=http://10.105.66.20:8086
INFLUX_TOKEN=你的实际token
INFLUX_ORG=你的组织名
INFLUX_BUCKET=vehicle_telemetry
INFLUX_TIMEOUT=30000

# ============================================================
# Map API 配置（Ground Truth Validation）
# ============================================================
MAP_API_URL=http://10.105.66.20:1234/api/v1/annotate/batch
MAP_PORT=AQCTMap_20251121V1.0
MAP_API_TIMEOUT=30
MAP_API_USE_CACHE=true

# ============================================================
# 验证规则配置（可选，有默认值）
# ============================================================
MAX_SPEED_FOR_STATIONARY=0.5
DQ_VEHICLE_SHARD_SIZE=8
```

### 步骤 3：重启 Airflow 容器

```bash
cd /home/ubuntu/cactus_box/cactus-box/deploy

# 重启 Airflow（让新的环境变量生效）
docker-compose restart airflow

# 或者完全重启所有服务
docker-compose down
docker-compose up -d
```

### 步骤 4：验证配置

```bash
# 进入容器检查环境变量
docker exec -it airflow bash
echo $INFLUX_URL
echo $INFLUX_TOKEN
echo $MAP_API_URL
exit
```

## 🔍 工作原理

### docker-compose.yml 环境变量注入

已更新 `docker-compose.yml`，新增了以下环境变量注入：

```yaml
environment:
  # InfluxDB 配置
  - INFLUX_URL=${INFLUX_URL}
  - INFLUX_TOKEN=${INFLUX_TOKEN}
  - INFLUX_ORG=${INFLUX_ORG}
  - INFLUX_BUCKET=${INFLUX_BUCKET}
  
  # Map API 配置
  - MAP_API_URL=${MAP_API_URL}
  - MAP_PORT=${MAP_PORT}
```

### Worker DAG 读取方式

Worker DAG 通过 `os.getenv()` 读取这些环境变量：

```python
# dags/dag_worker_ground_truth.py 第 64-72 行
INFLUX_URL = os.getenv("INFLUX_URL", "http://10.105.66.20:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "your_token_here")
INFLUX_ORG = os.getenv("INFLUX_ORG", "your_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "vehicle_telemetry")
```

## ✅ 优势

1. **集中管理**：所有配置都在 `deploy/.env` 文件中
2. **安全性**：.env 文件不会提交到 Git（已在 .gitignore）
3. **统一风格**：和现有的 MySQL 配置方式一致
4. **Docker 友好**：docker-compose 自动读取 .env 文件

## 📝 注意事项

1. `.env` 文件包含敏感信息（Token、密码），**不要提交到 Git**
2. 修改 `.env` 后需要重启容器才能生效
3. 可以使用 `env.template` 作为模板，分享给团队成员
4. 如果某个环境变量没设置，Worker DAG 会使用默认值

## 🔧 故障排查

### 问题 1：环境变量没生效

```bash
# 检查容器内的环境变量
docker exec airflow printenv | grep INFLUX
```

### 问题 2：Token 不正确

```bash
# 测试 InfluxDB 连接
curl -H "Authorization: Token 你的token" \
  http://10.105.66.20:8086/health
```

### 问题 3：容器无法访问 InfluxDB

```bash
# 进入容器测试网络
docker exec airflow curl -I http://10.105.66.20:8086/health
```

## 📂 相关文件

- **deploy/env.template** - 环境变量模板（可分享）
- **deploy/.env** - 实际环境变量（不提交 Git）
- **deploy/docker-compose.yml** - Docker Compose 配置（已更新）
- **dags/dag_worker_ground_truth.py** - Worker DAG（读取环境变量）
- **plugins/dq_lib/ground_truth_utils.py** - 工具类（使用配置）

