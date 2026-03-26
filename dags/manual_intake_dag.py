"""
人工反馈 Corner Case 录入 DAG
通过 Airflow UI 表单手动录入现场反馈的异常场景

使用方式：
  Airflow UI → DAGs → manual_corner_case_intake → Trigger DAG w/ config
  填写表单后提交即可，系统会自动：
  1. 校验输入参数
  2. 尝试根据 vehicle_id + 时间范围反查 cycle_id
  3. 写入 auto_test_case_catalog 表（process_status = PENDING）
  4. 触发 asset_packing_dag 自动打包原始数据

Author: Data Governance Team
Date: 2026-03-11
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import json
import logging
import os

from plugins.datasets import GOVERNANCE_ASSET_DATASET


# ============================================================
# 配置
# ============================================================
MYSQL_CONN_ID = 'qa_mysql_conn'
DATALOG_CONN_ID = 'datalog_mysql_conn'
META_TABLE = 'auto_test_case_catalog'

ALERT_EMAIL = os.getenv('ALERT_EMAIL_TO', 'xiyan.zhou@westwell-lab.com')


# ============================================================
# Task 1: 校验输入参数
# ============================================================
def validate_input(**context):
    """
    校验用户输入的表单参数，确保关键字段合法。
    校验通过后将标准化的参数推入 XCom 供后续 task 使用。
    """
    logger = logging.getLogger(__name__)
    params = context['params']

    vehicle_id = params.get('vehicle_id', '').strip()
    shift_date = params.get('shift_date', '').strip()
    time_start = params.get('time_window_start', '').strip()
    time_end = params.get('time_window_end', '').strip()
    description = params.get('description', '').strip()

    errors = []
    if not vehicle_id:
        errors.append("vehicle_id 不能为空")
    if not shift_date:
        errors.append("shift_date 不能为空")
    if not time_start or not time_end:
        errors.append("time_window_start 和 time_window_end 不能为空")
    if not description:
        errors.append("description 不能为空")

    if time_start and time_end:
        try:
            ts = datetime.fromisoformat(time_start)
            te = datetime.fromisoformat(time_end)
            if te <= ts:
                errors.append("time_window_end 必须晚于 time_window_start")
        except ValueError as e:
            errors.append(f"时间格式错误（需要 ISO 格式 YYYY-MM-DDTHH:MM:SS）: {e}")

    if errors:
        msg = "参数校验失败:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(msg)
        raise ValueError(msg)

    validated = {
        'vehicle_id': vehicle_id,
        'shift_date': shift_date,
        'time_window_start': time_start,
        'time_window_end': time_end,
        'description': description,
        'cycle_id': params.get('cycle_id', '').strip() or None,
        'severity': params.get('severity', 'P1'),
        'reporter': params.get('reporter', '').strip() or 'anonymous',
        'site': params.get('site', '').strip() or 'unknown',
        'tags': [t.strip() for t in params.get('tags', '').split(',') if t.strip()],
    }

    logger.info(f"✅ 参数校验通过: vehicle={vehicle_id}, date={shift_date}, "
                f"time={time_start}~{time_end}")
    context['ti'].xcom_push(key='validated_params', value=validated)


# ============================================================
# Task 2: 反查 cycle_id（可选增强）
# ============================================================
def resolve_cycle_id(**context):
    """
    如果用户未填写 cycle_id，尝试根据 vehicle_id + 时间范围
    从 datalog 数据库反查最匹配的 cycle_id。
    查不到则使用时间戳生成占位 ID。
    """
    logger = logging.getLogger(__name__)
    params = context['ti'].xcom_pull(key='validated_params')

    if params['cycle_id']:
        logger.info(f"✅ 用户已提供 cycle_id: {params['cycle_id']}")
        context['ti'].xcom_push(key='resolved_params', value=params)
        return

    vehicle_id = params['vehicle_id']
    time_start = params['time_window_start']
    time_end = params['time_window_end']

    try:
        hook = MySqlHook(mysql_conn_id=DATALOG_CONN_ID)
        lookup_sql = """
            SELECT cycle_id
            FROM cycle_section_summary
            WHERE vehicle_id = %s
              AND cycle_start_time <= %s
              AND cycle_end_time >= %s
            ORDER BY ABS(TIMESTAMPDIFF(SECOND, cycle_start_time, %s)) ASC
            LIMIT 1
        """
        result = hook.get_first(lookup_sql, parameters=(
            vehicle_id, time_end, time_start, time_start
        ))

        if result and result[0]:
            params['cycle_id'] = str(result[0])
            logger.info(f"✅ 反查到 cycle_id: {params['cycle_id']}")
        else:
            params['cycle_id'] = _generate_placeholder_id(vehicle_id, time_start)
            logger.info(f"⚠️ 未查到匹配 cycle，使用占位 ID: {params['cycle_id']}")
    except Exception as e:
        params['cycle_id'] = _generate_placeholder_id(vehicle_id, time_start)
        logger.warning(f"⚠️ 反查 cycle_id 异常，使用占位 ID: {params['cycle_id']} ({e})")

    context['ti'].xcom_push(key='resolved_params', value=params)


def _generate_placeholder_id(vehicle_id: str, time_start: str) -> str:
    ts = time_start.replace('-', '').replace('T', '').replace(':', '')[:14]
    return f"MANUAL_{vehicle_id}_{ts}"


# ============================================================
# Task 3: 写入 auto_test_case_catalog
# ============================================================
def write_to_catalog(**context):
    """
    将人工反馈写入 auto_test_case_catalog 表。
    字段对齐现有自动路径，通过 triggered_rule_id='manual_report'
    和 category='ManualCornerCase' 区分来源。
    """
    logger = logging.getLogger(__name__)
    params = context['ti'].xcom_pull(key='resolved_params')

    batch_id = f"MANUAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    cycle_id = params['cycle_id']
    vehicle_id = params['vehicle_id']
    shift_date = params['shift_date']
    time_start = datetime.fromisoformat(params['time_window_start'])
    time_end = datetime.fromisoformat(params['time_window_end'])

    all_tags = ['MANUAL'] + params.get('tags', [])

    manual_context = {
        'reporter': params['reporter'],
        'site': params['site'],
        'description': params['description'],
        'source': 'airflow_manual_form',
        'intake_time': datetime.now().isoformat(),
    }

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    insert_sql = f"""
        INSERT INTO {META_TABLE} (
            batch_id, cycle_id, vehicle_id, shift_date, rule_version,
            category, case_tags, severity, confidence,
            trigger_timestamp, time_window_start, time_window_end,
            triggered_rule_id, pack_base_path, process_status,
            metric_snapshot
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, 'PENDING',
            %s
        )
        ON DUPLICATE KEY UPDATE
            process_status = 'PENDING',
            case_tags = VALUES(case_tags),
            metric_snapshot = VALUES(metric_snapshot),
            updated_at = NOW()
    """

    base_path = f"/data/assets/manual_report/{vehicle_id}/{shift_date}/"

    try:
        hook.run(insert_sql, parameters=(
            batch_id,
            cycle_id,
            vehicle_id,
            shift_date,
            'manual_v1',
            'ManualCornerCase',
            json.dumps(all_tags, ensure_ascii=False),
            params['severity'],
            1.0,
            time_start,
            time_start,
            time_end,
            'manual_report',
            base_path,
            json.dumps(manual_context, ensure_ascii=False),
        ))

        logger.info(
            f"✅ 已写入 {META_TABLE}: cycle_id={cycle_id}, "
            f"vehicle={vehicle_id}, date={shift_date}, "
            f"reporter={params['reporter']}, site={params['site']}"
        )
        logger.info(f"   描述: {params['description']}")

        context['ti'].xcom_push(key='intake_result', value={
            'batch_id': batch_id,
            'cycle_id': cycle_id,
            'vehicle_id': vehicle_id,
            'status': 'SUCCESS',
        })

    except Exception as e:
        logger.error(f"❌ 写入失败: {e}")
        raise


# ============================================================
# DAG 定义
# ============================================================
default_args = {
    'owner': 'data-governance',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': [ALERT_EMAIL],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='manual_corner_case_intake',
    default_args=default_args,
    description='人工录入 Corner Case（通过 Airflow UI 表单提交）',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=['governance', 'manual-intake', 'corner-case'],
    render_template_as_native_obj=True,
    params={
        'vehicle_id': Param(
            default='',
            type='string',
            title='车辆号 *',
            description='例如 AT01, AT03, AT12',
        ),
        'shift_date': Param(
            default='',
            type='string',
            title='作业日期 *',
            description='格式: YYYY-MM-DD，例如 2026-03-11',
        ),
        'time_window_start': Param(
            default='',
            type='string',
            title='异常开始时间 *',
            description='格式: YYYY-MM-DDTHH:MM:SS，例如 2026-03-11T08:30:00',
        ),
        'time_window_end': Param(
            default='',
            type='string',
            title='异常结束时间 *',
            description='格式: YYYY-MM-DDTHH:MM:SS，例如 2026-03-11T09:15:00',
        ),
        'description': Param(
            default='',
            type='string',
            title='异常描述 *',
            description='简要说明异常现象，例如: AT03在6-7号桥吊间异常停车约3分钟',
        ),
        'cycle_id': Param(
            default='',
            type=['null', 'string'],
            title='作业圈 ID',
            description='（选填）如果知道具体 cycle_id 可以填写，不填则系统自动反查',
        ),
        'severity': Param(
            default='P1',
            type='string',
            title='严重等级',
            enum=['P0', 'P1', 'P2'],
            description='P0-致命 / P1-严重 / P2-一般',
        ),
        'tags': Param(
            default='',
            type='string',
            title='场景标签',
            description='（选填）逗号分隔，例如: 异常停车,桥吊区域',
        ),
        'reporter': Param(
            default='',
            type='string',
            title='上报人',
            description='（选填）例如: 张工',
        ),
        'site': Param(
            default=os.getenv('SITE_NAME', ''),
            type='string',
            title='现场名称',
            description='（选填）例如: AQCT, NPCT, SPA',
        ),
    },
) as dag:

    validate = PythonOperator(
        task_id='validate_input',
        python_callable=validate_input,
    )

    resolve = PythonOperator(
        task_id='resolve_cycle_id',
        python_callable=resolve_cycle_id,
    )

    write = PythonOperator(
        task_id='write_to_catalog',
        python_callable=write_to_catalog,
        outlets=[GOVERNANCE_ASSET_DATASET],
    )

    validate >> resolve >> write
