#!/bin/bash
# ============================================================
# DAG Factory 验证脚本
# 用于快速验证 DAG Factory 的功能
# ============================================================

set -e

echo "🚀 Starting DAG Factory Validation..."
echo "======================================"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

CONTAINER_NAME="deploy-airflow-1"

# Step 1: 检查容器是否运行
echo -e "\n${YELLOW}[Step 1]${NC} Checking Airflow container..."
if docker ps | grep -q "$CONTAINER_NAME"; then
    echo -e "${GREEN}✅ Container is running${NC}"
else
    echo -e "${RED}❌ Container is not running. Please start it first.${NC}"
    exit 1
fi

# Step 2: 检查配置文件是否存在
echo -e "\n${YELLOW}[Step 2]${NC} Checking source config files..."
CONFIG_COUNT=$(docker exec $CONTAINER_NAME find /opt/airflow/plugins/configs/sources -name "*.yaml" 2>/dev/null | wc -l)
if [ "$CONFIG_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ Found $CONFIG_COUNT source config files${NC}"
    docker exec $CONTAINER_NAME ls -lh /opt/airflow/plugins/configs/sources/
else
    echo -e "${RED}❌ No source config files found${NC}"
    exit 1
fi

# Step 3: 验证 Pydantic Schema
echo -e "\n${YELLOW}[Step 3]${NC} Validating Pydantic Schema..."
docker exec $CONTAINER_NAME python3 << 'PYTHON_SCRIPT'
import sys
sys.path.insert(0, '/opt/airflow')

try:
    from plugins.schemas.source_config_schema import (
        SourceYAMLConfig, 
        SourceMetaConfig, 
        SchedulingConfig,
        SensorConfig
    )
    print("✅ Pydantic Schema imported successfully")
except Exception as e:
    print(f"❌ Failed to import Schema: {e}")
    sys.exit(1)

# 测试一个简单的配置
test_config = {
    'source_meta': {
        'id': 'test',
        'name': 'Test',
        'target_entity': 'Cycle'
    },
    'scheduling': {
        'trigger_mode': 'MANUAL'
    },
    'extractions': [{
        'id': 'test_extract',
        'source_type': 'mysql',
        'conn_id': 'test_conn',
        'query': 'SELECT 1',
        'output_key': 'test_key'
    }]
}

try:
    config = SourceYAMLConfig(**test_config)
    print("✅ Schema validation passed")
except Exception as e:
    print(f"❌ Schema validation failed: {e}")
    sys.exit(1)
PYTHON_SCRIPT

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Pydantic Schema validation passed${NC}"
else
    echo -e "${RED}❌ Pydantic Schema validation failed${NC}"
    exit 1
fi

# Step 4: 测试 DAG Factory 加载
echo -e "\n${YELLOW}[Step 4]${NC} Testing DAG Factory..."
docker exec $CONTAINER_NAME python3 << 'PYTHON_SCRIPT'
import sys
sys.path.insert(0, '/opt/airflow')

try:
    from plugins.orchestration.dag_factory import DAGFactory
    print("✅ DAG Factory imported successfully")
    
    factory = DAGFactory()
    dags = factory.scan_and_generate_dags()
    
    print(f"✅ Generated {len(dags)} DAGs:")
    for dag_id, dag in dags.items():
        print(f"   - {dag_id}: {len(dag.tasks)} tasks")
        print(f"     Schedule: {dag.schedule_interval}")
        print(f"     Tags: {dag.tags}")
    
    if len(dags) == 0:
        print("⚠️  Warning: No DAGs generated (check adapter and rules files)")
    
except Exception as e:
    print(f"❌ DAG Factory test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYTHON_SCRIPT

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ DAG Factory test passed${NC}"
else
    echo -e "${RED}❌ DAG Factory test failed${NC}"
    exit 1
fi

# Step 5: 验证 YAML 配置
echo -e "\n${YELLOW}[Step 5]${NC} Validating example YAML files..."
for yaml_file in daily_cycle_etl.yaml asset_driven_etl.yaml manual_adhoc_analysis.yaml; do
    echo -e "  Checking ${yaml_file}..."
    docker exec $CONTAINER_NAME python3 << PYTHON_SCRIPT
import sys
import yaml
sys.path.insert(0, '/opt/airflow')

from plugins.schemas.source_config_schema import SourceYAMLConfig

try:
    with open('/opt/airflow/plugins/configs/sources/${yaml_file}', 'r') as f:
        raw_config = yaml.safe_load(f)
    config = SourceYAMLConfig(**raw_config)
    print(f"    ✅ ${yaml_file} is valid")
except FileNotFoundError:
    print(f"    ⚠️  ${yaml_file} not found (skipped)")
except Exception as e:
    print(f"    ❌ ${yaml_file} validation failed: {e}")
    sys.exit(1)
PYTHON_SCRIPT
done

# Step 6: 检查 Airflow Import Errors
echo -e "\n${YELLOW}[Step 6]${NC} Checking Airflow import errors..."
sleep 2  # 等待 Airflow 扫描
docker exec $CONTAINER_NAME airflow dags list-import-errors 2>/dev/null || echo "  No import errors (or command not available)"

# Step 7: 列出生成的 DAG
echo -e "\n${YELLOW}[Step 7]${NC} Listing generated DAGs..."
docker exec $CONTAINER_NAME airflow dags list 2>/dev/null | grep "gov_" || echo "  No gov_ DAGs found yet (Scheduler may need time)"

# 总结
echo -e "\n${GREEN}======================================"
echo "✅ DAG Factory Validation Complete"
echo -e "======================================${NC}"

echo -e "\n${YELLOW}Next Steps:${NC}"
echo "1. Check Airflow UI: http://localhost:8080"
echo "2. Look for DAGs with prefix 'gov_'"
echo "3. Run unit tests: docker exec $CONTAINER_NAME pytest /opt/airflow/tests/test_dag_factory.py -v"
echo "4. Check logs: docker logs $CONTAINER_NAME --tail=100 | grep dag_factory"
