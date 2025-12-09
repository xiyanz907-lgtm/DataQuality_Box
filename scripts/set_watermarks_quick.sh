#!/bin/bash
# 快速设置水位线的 Shell 脚本
# 在 Airflow 容器内执行

# 变量名（根据 scanner_universal_dag.py 的命名规范）
VAR_NGEN_ID="watermark_cnt_cycles_ngen_id"
VAR_CACTUS_TIME="watermark_cnt_cycles_cactus_time"

# 要设置的值
NGEN_ID_VALUE=1000000
CACTUS_TIME_VALUE="2025-11-01 15:00:09"

echo "🔧 设置水位线变量用于调试..."
echo ""

# 设置 nGen ID 水位线
airflow variables set "${VAR_NGEN_ID}" "${NGEN_ID_VALUE}"
echo "✅ 设置 ${VAR_NGEN_ID} = ${NGEN_ID_VALUE}"

# 设置 Cactus 时间戳水位线
airflow variables set "${VAR_CACTUS_TIME}" "${CACTUS_TIME_VALUE}"
echo "✅ 设置 ${VAR_CACTUS_TIME} = ${CACTUS_TIME_VALUE}"

echo ""
echo "📋 验证设置结果："
airflow variables get "${VAR_NGEN_ID}"
airflow variables get "${VAR_CACTUS_TIME}"

