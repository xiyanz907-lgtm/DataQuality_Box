#!/bin/bash
# ============================================================
#  DAG 重跑 / 补跑脚本
#  用法：修改下方参数，然后执行 bash rerun.sh
# ============================================================

# ============ ↓↓↓ 修改这里：日期参数 ↓↓↓ ============

DAG_ID="gov_daily_cycle_etl"       # DAG ID
START_DATE="2026-03-11"            # 起始日期（包含）
END_DATE="2026-03-12"              # 结束日期（不包含），单天重跑 = 起始日期 + 1 天

# ============ ↓↓↓ 修改这里：环境配置 ↓↓↓ ============

# Docker Compose 命令：V2 用 "docker compose"，V1 用 "docker-compose"
COMPOSE_CMD="docker compose"

# Docker Compose 文件路径（留空则自动使用脚本所在目录的 docker-compose.yml）
# 生产环境如果 compose 文件不在同一目录，请填写绝对路径
# 例如：COMPOSE_FILE="/opt/cactus-box/deploy/docker-compose.yml"
COMPOSE_FILE=""

# Airflow 容器的 service 名称（docker-compose.yml 中定义的 service name）
AIRFLOW_SERVICE="airflow"

# Web UI 地址（仅用于最后输出提示）
WEB_UI_URL="http://localhost:8081"

# ============ ↑↑↑ 修改完成 ↑↑↑ ============

# 重跑时只清除以下任务（跳过 sensor），用于 tasks clear 场景
TASK_REGEX="^(universal_loader|domain_adapter|rule_tasks|context_aggregator|notification_dispatcher|save_assets)"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 构建完整的 compose 命令
if [ -n "$COMPOSE_FILE" ]; then
    COMPOSE="$COMPOSE_CMD -f $COMPOSE_FILE"
else
    cd "$SCRIPT_DIR"
    COMPOSE="$COMPOSE_CMD"
fi

echo ""
echo "=========================================="
echo "   DAG 重跑 / 补跑工具"
echo "=========================================="
echo ""
echo -e "  DAG ID:       ${CYAN}${DAG_ID}${NC}"
echo -e "  起始日期:     ${CYAN}${START_DATE}${NC}（包含）"
echo -e "  结束日期:     ${CYAN}${END_DATE}${NC}（不包含）"
echo -e "  跳过 Sensor:  ${CYAN}是（自动跳过 data_ready_sensor）${NC}"
echo -e "  Compose 命令: ${CYAN}${COMPOSE}${NC}"
echo -e "  服务名:       ${CYAN}${AIRFLOW_SERVICE}${NC}"
if [ -n "$COMPOSE_FILE" ]; then
    echo -e "  Compose 文件: ${CYAN}${COMPOSE_FILE}${NC}"
else
    echo -e "  Compose 文件: ${CYAN}${SCRIPT_DIR}/docker-compose.yml${NC}"
fi
echo ""

# -------- 参数校验 --------
if ! date -d "$START_DATE" &>/dev/null; then
    echo -e "${RED}❌ 起始日期格式错误: ${START_DATE}，请使用 YYYY-MM-DD${NC}"
    exit 1
fi

if ! date -d "$END_DATE" &>/dev/null; then
    echo -e "${RED}❌ 结束日期格式错误: ${END_DATE}，请使用 YYYY-MM-DD${NC}"
    exit 1
fi

if [[ "$START_DATE" > "$END_DATE" || "$START_DATE" == "$END_DATE" ]]; then
    echo -e "${RED}❌ 结束日期必须大于起始日期${NC}"
    exit 1
fi

# -------- 检查 compose 文件是否存在 --------
if [ -n "$COMPOSE_FILE" ]; then
    if [ ! -f "$COMPOSE_FILE" ]; then
        echo -e "${RED}❌ Compose 文件不存在: ${COMPOSE_FILE}${NC}"
        exit 1
    fi
else
    if [ ! -f "${SCRIPT_DIR}/docker-compose.yml" ]; then
        echo -e "${RED}❌ 当前目录未找到 docker-compose.yml: ${SCRIPT_DIR}/${NC}"
        echo -e "${YELLOW}提示：如果生产环境的 compose 文件不在脚本同目录，请设置 COMPOSE_FILE 变量${NC}"
        exit 1
    fi
fi

# -------- 检查 docker compose 命令是否可用 --------
echo -e "${YELLOW}[1/4]${NC} 检查 Docker Compose 命令..."
if ! $COMPOSE version &>/dev/null; then
    echo -e "${RED}❌ '$COMPOSE_CMD' 命令不可用${NC}"
    echo -e "${YELLOW}提示：如果是旧版 Docker，请将 COMPOSE_CMD 改为 \"docker-compose\"${NC}"
    exit 1
fi
COMPOSE_VERSION=$($COMPOSE version --short 2>/dev/null || $COMPOSE version 2>/dev/null | head -1)
echo -e "${GREEN}✓ Docker Compose 可用 (${COMPOSE_VERSION})${NC}"

# -------- 检查容器是否在运行 --------
echo -e "${YELLOW}[2/4]${NC} 检查 Airflow 容器状态..."
RUNNING_CONTAINERS=$($COMPOSE ps 2>&1)
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ docker compose ps 失败：${NC}"
    echo "$RUNNING_CONTAINERS"
    exit 1
fi

if ! echo "$RUNNING_CONTAINERS" | grep -q "$AIRFLOW_SERVICE"; then
    echo -e "${RED}❌ 未找到服务 '${AIRFLOW_SERVICE}'${NC}"
    echo ""
    echo "当前 compose 中的服务："
    echo "$RUNNING_CONTAINERS"
    echo ""
    echo -e "${YELLOW}提示：请检查 AIRFLOW_SERVICE 变量是否与 docker-compose.yml 中的 service 名称一致${NC}"
    exit 1
fi
echo -e "${GREEN}✓ 服务 '${AIRFLOW_SERVICE}' 运行中${NC}"

# -------- 检查容器内 airflow 命令是否可用 --------
echo -e "${YELLOW}[3/4]${NC} 检查 DAG 是否已注册..."
DAG_LIST_OUTPUT=$($COMPOSE exec -T "$AIRFLOW_SERVICE" airflow dags list 2>&1)
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ 容器内 airflow 命令执行失败：${NC}"
    echo "$DAG_LIST_OUTPUT" | head -20
    exit 1
fi

if ! echo "$DAG_LIST_OUTPUT" | grep -q "$DAG_ID"; then
    echo -e "${RED}❌ DAG '${DAG_ID}' 未找到${NC}"
    echo ""
    echo "当前已注册的 governance DAG："
    echo "$DAG_LIST_OUTPUT" | grep "gov_" || echo "  （无 gov_ 开头的 DAG）"
    echo ""
    echo "所有 DAG："
    echo "$DAG_LIST_OUTPUT" | head -20
    exit 1
fi
echo -e "${GREEN}✓ DAG '${DAG_ID}' 已注册${NC}"

# -------- 数据预检查：确认主表有数据 --------
echo -e "${YELLOW}[4/5]${NC} 检查目标日期范围内的源数据是否就绪..."

DATA_CHECK_FAILED=0
CURRENT="$START_DATE"
while [[ "$CURRENT" < "$END_DATE" ]]; do
    ROW_COUNT=$($COMPOSE exec -T "$AIRFLOW_SERVICE" python3 -c "
from airflow.providers.mysql.hooks.mysql import MySqlHook
try:
    hook = MySqlHook(mysql_conn_id='datalog_mysql_conn')
    result = hook.get_first(\"SELECT COUNT(*) FROM cycle_section_summary WHERE DATE(shift_date) = '${CURRENT}'\")
    print(result[0] if result else 0)
except Exception as e:
    print(f'ERROR:{e}')
" 2>/dev/null)

    ROW_COUNT=$(echo "$ROW_COUNT" | tr -d '[:space:]')

    if echo "$ROW_COUNT" | grep -q "^ERROR:"; then
        echo -e "${RED}  ❌ ${CURRENT}: 数据库查询失败 - ${ROW_COUNT#ERROR:}${NC}"
        DATA_CHECK_FAILED=1
    elif [[ "$ROW_COUNT" == "0" ]]; then
        echo -e "${RED}  ❌ ${CURRENT}: cycle_section_summary 无数据（0 行）${NC}"
        DATA_CHECK_FAILED=1
    else
        echo -e "${GREEN}  ✓ ${CURRENT}: ${ROW_COUNT} 行数据就绪${NC}"
    fi

    CURRENT=$(date -d "$CURRENT + 1 day" +%Y-%m-%d)
done

if [ $DATA_CHECK_FAILED -ne 0 ]; then
    echo ""
    echo -e "${RED}❌ 部分日期无源数据，终止重跑。${NC}"
    echo -e "${YELLOW}提示：主表无数据时强行重跑会导致类型推断错误。请确认数据已入库后再执行。${NC}"
    exit 1
fi
echo ""

# -------- 检查是否存在已有的 DagRun --------
echo -e "${YELLOW}[5/5]${NC} 检查目标日期的 DagRun 状态..."
echo ""

LIST_RUNS_OUTPUT=$($COMPOSE exec -T "$AIRFLOW_SERVICE" \
    airflow dags list-runs -d "$DAG_ID" -o json 2>&1)

EXISTING_RUNS=$(echo "$LIST_RUNS_OUTPUT" | python3 -c "
import sys, json
try:
    runs = json.load(sys.stdin)
    for r in runs:
        ed = r.get('execution_date','')[:10]
        if ed >= '${START_DATE}' and ed < '${END_DATE}':
            print(f\"  {ed}  状态: {r.get('state','unknown')}\")
except:
    pass
" 2>/dev/null)

if [ -n "$EXISTING_RUNS" ]; then
    echo -e "${YELLOW}⚠ 目标日期范围内已存在以下 DagRun：${NC}"
    echo "$EXISTING_RUNS"
    echo ""
    echo -e "将使用 ${CYAN}airflow tasks clear${NC} 清除后重跑（跳过 Sensor）。"
    echo ""
    read -p "确认执行？(y/N): " CONFIRM
    if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
        echo "已取消。"
        exit 0
    fi

    echo ""
    echo -e "${CYAN}>>> 执行 tasks clear（跳过 sensor）...${NC}"
    CLEAR_OUTPUT=$($COMPOSE exec -T "$AIRFLOW_SERVICE" \
        airflow tasks clear "$DAG_ID" \
        -s "$START_DATE" -e "$END_DATE" \
        -t "$TASK_REGEX" \
        -y 2>&1)
    RESULT=$?

    echo "$CLEAR_OUTPUT"

    if [ $RESULT -ne 0 ]; then
        echo ""
        echo -e "${RED}⚠ tasks clear 返回非零退出码: ${RESULT}${NC}"
    fi

    # 检查是否真的清除了任务
    if echo "$CLEAR_OUTPUT" | grep -qi "no task"; then
        echo ""
        echo -e "${YELLOW}⚠ 似乎没有匹配到任何任务。请检查：${NC}"
        echo "  1. 日期范围内是否有对应的 DagRun"
        echo "  2. TASK_REGEX 是否能匹配到 DAG 中的 task"
        echo ""
        echo "DAG 中的所有 task："
        $COMPOSE exec -T "$AIRFLOW_SERVICE" \
            airflow tasks list "$DAG_ID" 2>&1
    fi

else
    echo "目标日期范围内无已有 DagRun，将使用 backfill 补跑（自动跳过 Sensor）。"
    echo ""
    read -p "确认执行？(y/N): " CONFIRM
    if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
        echo "已取消。"
        exit 0
    fi

    echo ""
    echo -e "${CYAN}>>> 执行 backfill（后台轮询跳过 sensor）...${NC}"

    # 在容器内用 Python 执行：后台启动 backfill + 轮询将 sensor 强制标记为 SUCCESS
    # 解决 reschedule 模式与 backfill 引擎的兼容性问题
    $COMPOSE exec -T "$AIRFLOW_SERVICE" python3 -u -c "
import subprocess, time, threading, sys

DAG_ID = '${DAG_ID}'
START_DATE = '${START_DATE}'
END_DATE = '${END_DATE}'
SENSOR_TASK_ID = 'data_ready_sensor'

# 1. 后台启动 backfill
proc = subprocess.Popen(
    ['airflow', 'dags', 'backfill', DAG_ID,
     '-s', START_DATE, '-e', END_DATE,
     '--reset-dagruns', '-y'],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
)

# 2. 守护线程：每 5 秒轮询，将 sensor 强制标记为 SUCCESS
def force_sensor_success():
    time.sleep(10)  # 等待 DagRun 创建
    from airflow.models import TaskInstance
    from airflow.utils.state import TaskInstanceState
    from airflow.utils.session import create_session

    while proc.poll() is None:
        try:
            with create_session() as session:
                stuck = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == DAG_ID,
                    TaskInstance.task_id == SENSOR_TASK_ID,
                    TaskInstance.state.in_([
                        TaskInstanceState.UP_FOR_RESCHEDULE,
                        TaskInstanceState.RUNNING,
                        TaskInstanceState.QUEUED,
                    ]),
                ).all()
                for ti in stuck:
                    ti.state = TaskInstanceState.SUCCESS
                    print(f'✅ Sensor 已跳过: {ti.run_id}', flush=True)
                if stuck:
                    session.commit()
        except Exception as e:
            print(f'⚠️ Sensor 跳过失败: {e}', flush=True)
        time.sleep(5)

t = threading.Thread(target=force_sensor_success, daemon=True)
t.start()

# 3. 实时输出 backfill 日志（只显示关键信息）
for line in proc.stdout:
    line = line.strip()
    if any(kw in line for kw in ['finished run', 'ERROR', 'Deadlock', 'WARNING', 'Sensor 已跳过']):
        print(line, flush=True)

proc.wait()
sys.exit(proc.returncode)
"

    RESULT=$?
fi

# -------- 结果输出 --------
echo ""
echo "=========================================="
if [ $RESULT -eq 0 ]; then
    echo -e "${GREEN}✅ 操作完成！${NC}"
else
    echo -e "${RED}❌ 操作异常，退出码: ${RESULT}${NC}"
    echo "请检查日志: $COMPOSE exec $AIRFLOW_SERVICE airflow dags list-runs -d ${DAG_ID}"
fi
echo "=========================================="
echo ""
echo "📊 查看运行状态:"
echo "  - Web UI:  ${WEB_UI_URL}"
echo "  - CLI:     $COMPOSE exec $AIRFLOW_SERVICE airflow dags list-runs -d ${DAG_ID} --limit 10"
echo ""
