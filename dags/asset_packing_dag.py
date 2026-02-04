"""
资产打包 DAG (DAG B)
事件驱动 + 数据库队列 + 僵尸任务处理

触发方式：由 DAG A 通过 Dataset 触发
工作流程：
1. 清理僵尸任务（PROCESSING/POLLING 超时 2 小时）
2. 从队列获取待处理资产（PENDING，批量 50 条）
3. 调用打包服务（异步接口）
4. 轮询打包状态（最多 60 次）
5. 更新元数据表（process_status = PACKAGED）
6. 发送失败汇总邮件（如有失败任务）

Author: Data Governance Team
Date: 2026-02-02
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from typing import List, Dict, Any
import logging
import os

from plugins.datasets import GOVERNANCE_ASSET_DATASET
from plugins.services.packing_service import create_packing_client


# ============================================================
# 配置参数
# ============================================================
MYSQL_CONN_ID = 'qa_mysql_conn'
META_TABLE = 'auto_test_case_catalog'  # 单表方案：只使用 meta 表

BATCH_SIZE = 50  # 每次处理的资产数量
ZOMBIE_TIMEOUT_HOURS = 2  # 僵尸任务超时时间
MAX_RETRY_COUNT = 3  # 最大重试次数
BACKLOG_ALERT_THRESHOLD = 500  # 队列积压告警阈值


# ============================================================
# Task 1: 清理僵尸任务
# ============================================================
def cleanup_zombie_tasks(**context):
    """
    清理僵尸任务（PROCESSING/POLLING 状态超时 2 小时）
    
    逻辑：
    1. 检测 status IN ('PROCESSING', 'POLLING') 且 updated_at < NOW() - INTERVAL 2 HOUR
    2. 如果 retry_count < 3: 重置为 PENDING
    3. 如果 retry_count >= 3: 标记为 ABANDONED
    """
    logger = logging.getLogger(__name__)
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    # SQL: 重置可重试的僵尸任务（单表方案）
    reset_sql = f"""
        UPDATE {META_TABLE}
        SET process_status = 'PENDING',
            pack_error_message = CONCAT(IFNULL(pack_error_message, ''), ' [Zombie Reset]'),
            pack_retry_count = pack_retry_count + 1,
            updated_at = NOW()
        WHERE process_status IN ('PROCESSING', 'POLLING')
          AND updated_at < NOW() - INTERVAL {ZOMBIE_TIMEOUT_HOURS} HOUR
          AND pack_retry_count < {MAX_RETRY_COUNT}
    """
    
    # SQL: 放弃超过重试次数的僵尸任务（单表方案）
    abandon_sql = f"""
        UPDATE {META_TABLE}
        SET process_status = 'ABANDONED',
            pack_error_message = CONCAT(IFNULL(pack_error_message, ''), ' [Max Retries Exceeded]'),
            updated_at = NOW()
        WHERE process_status IN ('PROCESSING', 'POLLING')
          AND updated_at < NOW() - INTERVAL {ZOMBIE_TIMEOUT_HOURS} HOUR
          AND pack_retry_count >= {MAX_RETRY_COUNT}
    """
    
    try:
        # 执行重置
        reset_count = hook.run(reset_sql, handler=lambda cursor: cursor.rowcount)
        logger.info(f"♻️ Reset {reset_count} zombie tasks to PENDING")
        
        # 执行放弃
        abandon_count = hook.run(abandon_sql, handler=lambda cursor: cursor.rowcount)
        logger.info(f"🗑️ Abandoned {abandon_count} zombie tasks (max retries exceeded)")
        
        # 推送 XCom
        context['ti'].xcom_push(key='zombie_reset_count', value=reset_count)
        context['ti'].xcom_push(key='zombie_abandon_count', value=abandon_count)
        
        # 告警检查
        if reset_count > 10 or abandon_count > 5:
            logger.warning(f"⚠️ High zombie task count! Reset: {reset_count}, Abandoned: {abandon_count}")
        
    except Exception as e:
        logger.error(f"❌ Cleanup zombie tasks failed: {str(e)}")
        raise


# ============================================================
# Task 2: 获取待处理资产
# ============================================================
def get_pending_assets(**context):
    """
    从队列获取待处理资产（批量 50 条，行锁防止并发冲突）
    
    返回：List[Dict] 包含所有必要字段
    """
    logger = logging.getLogger(__name__)
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    # SQL: 获取待处理任务（使用行锁，单表方案）
    select_sql = f"""
        SELECT id, batch_id, cycle_id as asset_id, triggered_rule_id as rule_id,
               vehicle_id, time_window_start as start_time, time_window_end as end_time, 
               pack_base_path as base_path, pack_retry_count as retry_count
        FROM {META_TABLE}
        WHERE process_status = 'PENDING'
        ORDER BY created_at ASC
        LIMIT {BATCH_SIZE}
        FOR UPDATE SKIP LOCKED
    """
    
    # SQL: 更新为 PROCESSING（单表方案）
    update_sql = f"""
        UPDATE {META_TABLE}
        SET process_status = 'PROCESSING',
            pack_started_at = NOW(),
            updated_at = NOW()
        WHERE id IN ({{}})
    """
    
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # 开启事务
        conn.autocommit = False
        
        # 查询待处理任务
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        
        if not rows:
            logger.info("✅ No pending assets, skipping packing")
            conn.rollback()
            context['ti'].xcom_push(key='pending_assets', value=[])
            return []
        
        # 构造资产列表
        assets = []
        asset_ids = []
        
        for row in rows:
            asset = {
                'queue_id': row[0],
                'batch_id': row[1],
                'asset_id': row[2],
                'rule_id': row[3],
                'vehicle_id': row[4],
                'start_time': row[5],
                'end_time': row[6],
                'base_path': row[7],
                'retry_count': row[8],
            }
            assets.append(asset)
            asset_ids.append(row[0])
        
        # 批量更新状态为 PROCESSING
        ids_str = ','.join(map(str, asset_ids))
        cursor.execute(update_sql.format(ids_str))
        
        # 提交事务
        conn.commit()
        
        logger.info(f"📦 Fetched {len(assets)} pending assets")
        logger.info(f"   Asset IDs: {asset_ids}")
        
        # 推送 XCom
        context['ti'].xcom_push(key='pending_assets', value=assets)
        
        # 队列积压告警（单表方案）
        cursor.execute(f"SELECT COUNT(*) FROM {META_TABLE} WHERE process_status = 'PENDING'")
        pending_count = cursor.fetchone()[0]
        if pending_count > BACKLOG_ALERT_THRESHOLD:
            logger.warning(f"⚠️ Queue backlog alert! {pending_count} assets pending")
        
        cursor.close()
        conn.close()
        
        return assets
        
    except Exception as e:
        logger.error(f"❌ Get pending assets failed: {str(e)}")
        if conn:
            conn.rollback()
        raise


# ============================================================
# Task 3: 分支判断
# ============================================================
def check_has_assets(**context):
    """
    检查是否有待处理资产
    
    返回：'pack_assets' 或 'skip_packing'
    """
    assets = context['ti'].xcom_pull(task_ids='get_pending_assets', key='pending_assets')
    
    if assets:
        return 'pack_assets'
    else:
        return 'skip_packing'


# ============================================================
# Task 4: 执行打包
# ============================================================
def pack_assets(**context):
    """
    批量调用打包服务（异步接口 + 轮询）
    
    流程：
    1. 遍历资产列表
    2. 调用 start_packing() 获取 pack_key
    3. 更新状态为 POLLING
    4. 调用 wait_for_completion() 等待完成
    5. 更新状态为 SUCCESS/FAILED
    """
    logger = logging.getLogger(__name__)
    assets = context['ti'].xcom_pull(task_ids='get_pending_assets', key='pending_assets')
    
    if not assets:
        logger.info("✅ No assets to pack")
        return
    
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    packing_client = create_packing_client(logger=logger)
    
    success_count = 0
    failed_count = 0
    failed_assets = []
    
    for asset in assets:
        queue_id = asset['queue_id']
        asset_id = asset['asset_id']
        vehicle_id = asset['vehicle_id']
        start_time = asset['start_time']
        end_time = asset['end_time']
        base_path = asset['base_path']
        retry_count = asset['retry_count']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📦 Processing asset: {asset_id}")
        logger.info(f"   Vehicle: {vehicle_id}, Time: {start_time} ~ {end_time}")
        logger.info(f"   Retry count: {retry_count}/{MAX_RETRY_COUNT}")
        
        try:
            # 1. 启动打包任务
            success, pack_key, error = packing_client.start_packing(
                vehicle_id=vehicle_id,
                start_time=start_time,
                end_time=end_time,
                base_path=base_path
            )
            
            if not success:
                # 打包启动失败
                logger.error(f"❌ Start packing failed: {error}")
                _update_queue_status(hook, queue_id, 'FAILED', error, retry_count)
                failed_count += 1
                failed_assets.append({'asset_id': asset_id, 'error': error})
                continue
            
            # 2. 更新状态为 POLLING（单表方案）
            update_sql = f"""
                UPDATE {META_TABLE}
                SET process_status = 'POLLING',
                    pack_key = %s,
                    pack_poll_count = 0,
                    updated_at = NOW()
                WHERE id = %s
            """
            hook.run(update_sql, parameters=(pack_key, queue_id))
            logger.info(f"🔍 Polling started, pack_key: {pack_key}")
            
            # 3. 等待打包完成
            complete, error = packing_client.wait_for_completion(pack_key)
            
            if complete:
                # 打包成功
                logger.info(f"✅ Packing completed for asset: {asset_id}")
                _update_queue_status(hook, queue_id, 'SUCCESS', None, retry_count, pack_key)
                success_count += 1
            else:
                # 打包失败（超时或查询失败）
                logger.error(f"❌ Packing failed: {error}")
                _update_queue_status(hook, queue_id, 'FAILED', error, retry_count)
                failed_count += 1
                failed_assets.append({'asset_id': asset_id, 'error': error})
                
        except Exception as e:
            # 异常捕获
            logger.error(f"💥 Unexpected error for asset {asset_id}: {str(e)}")
            _update_queue_status(hook, queue_id, 'FAILED', str(e), retry_count)
            failed_count += 1
            failed_assets.append({'asset_id': asset_id, 'error': str(e)})
    
    # 统计结果
    logger.info(f"\n{'='*60}")
    logger.info(f"📊 Packing Summary:")
    logger.info(f"   ✅ Success: {success_count}")
    logger.info(f"   ❌ Failed: {failed_count}")
    logger.info(f"   📦 Total: {len(assets)}")
    
    # 推送 XCom
    context['ti'].xcom_push(key='success_count', value=success_count)
    context['ti'].xcom_push(key='failed_count', value=failed_count)
    context['ti'].xcom_push(key='failed_assets', value=failed_assets)


def _update_queue_status(hook, queue_id, status, error_msg, retry_count, pack_key=None):
    """
    更新队列状态（单表方案）
    
    Args:
        hook: MySqlHook
        queue_id: 队列记录ID（meta 表的 id）
        status: 新状态
        error_msg: 错误信息
        retry_count: 当前重试次数
        pack_key: 打包任务Key
    """
    # 如果失败且超过重试次数，标记为 ABANDONED
    if status == 'FAILED' and retry_count >= MAX_RETRY_COUNT:
        status = 'ABANDONED'
        error_msg = f"{error_msg} [Max Retries Exceeded]"
    
    # 如果失败但还可以重试，重置为 PENDING
    if status == 'FAILED' and retry_count < MAX_RETRY_COUNT:
        status = 'PENDING'
        retry_count += 1
    
    # 单表方案：更新 process_status
    update_sql = f"""
        UPDATE {META_TABLE}
        SET process_status = %s,
            pack_error_message = %s,
            pack_retry_count = %s,
            pack_poll_count = pack_poll_count + 1,
            updated_at = NOW(),
            pack_completed_at = CASE WHEN %s IN ('PACKAGED', 'ABANDONED') THEN NOW() ELSE NULL END
        WHERE id = %s
    """
    
    # 注意：SUCCESS 状态映射为 PACKAGED
    final_status = 'PACKAGED' if status == 'SUCCESS' else status
    hook.run(update_sql, parameters=(final_status, error_msg, retry_count, final_status, queue_id))


# ============================================================
# Task 5: 验证打包结果（单表方案：不需要同步表）
# ============================================================
def validate_packing_results(**context):
    """
    验证打包结果（单表方案：状态已在 pack_assets 中更新）
    
    逻辑：
    1. 统计本次运行的打包结果
    2. 记录日志
    """
    logger = logging.getLogger(__name__)
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    
    # 查询本次运行的结果统计（最近 1 小时）
    stats_sql = f"""
        SELECT 
            process_status,
            COUNT(*) as count
        FROM {META_TABLE}
        WHERE pack_completed_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
        GROUP BY process_status
    """
    
    try:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(stats_sql)
        rows = cursor.fetchall()
        
        if not rows:
            logger.info("✅ No assets processed in the last hour")
            return
        
        # 记录统计结果
        logger.info("📊 Packing Results Summary (last 1 hour):")
        for row in rows:
            status, count = row
            logger.info(f"   {status}: {count} assets")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {str(e)}")
        # 不抛出异常，允许 DAG 继续执行
        pass


# ============================================================
# Task 6: 发送失败汇总邮件
# ============================================================
def send_failure_summary(**context):
    """
    发送失败汇总邮件（如有失败任务）
    
    内容：
    - 失败数量
    - 失败资产列表
    - 错误信息
    """
    logger = logging.getLogger(__name__)
    
    failed_count = context['ti'].xcom_pull(task_ids='pack_assets', key='failed_count') or 0
    failed_assets = context['ti'].xcom_pull(task_ids='pack_assets', key='failed_assets') or []
    
    if failed_count == 0:
        logger.info("✅ No failures, skipping email")
        return
    
    # 构造邮件内容
    subject = f"[Data Governance] Asset Packing Failures - {failed_count} Assets"
    
    body = f"""
<h2>资产打包失败汇总</h2>
<p><strong>失败数量：</strong> {failed_count}</p>
<p><strong>DAG 运行时间：</strong> {context['execution_date']}</p>

<h3>失败资产列表：</h3>
<table border="1" cellpadding="5" cellspacing="0">
    <tr>
        <th>Asset ID</th>
        <th>错误信息</th>
    </tr>
"""
    
    for asset in failed_assets:
        body += f"""
    <tr>
        <td>{asset['asset_id']}</td>
        <td>{asset['error']}</td>
    </tr>
"""
    
    body += """
</table>

<p><strong>建议操作：</strong></p>
<ul>
    <li>检查打包服务是否正常</li>
    <li>查看队列表中 FAILED/ABANDONED 记录</li>
    <li>如需重试，手动将 status 改回 PENDING</li>
</ul>
"""
    
    try:
        send_email(
            to=[os.getenv('ALERT_EMAIL_TO', 'xiyan.zhou@westwell-lab.com')],
            subject=subject,
            html_content=body
        )
        logger.info(f"📧 Failure summary email sent ({failed_count} assets)")
        
    except Exception as e:
        logger.error(f"❌ Send email failed: {str(e)}")


# ============================================================
# Task 7: 跳过打包（无任务时执行）
# ============================================================
def skip_packing(**context):
    """无任务时的占位任务"""
    logger = logging.getLogger(__name__)
    logger.info("✅ No assets to pack, skipping")


# ============================================================
# DAG 定义
# ============================================================
default_args = {
    'owner': 'data-governance',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['xiyan.zhou@westwell-lab.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='asset_packing_dag',
    default_args=default_args,
    description='Asset Packing DAG (Event-Driven by Dataset)',
    schedule=[GOVERNANCE_ASSET_DATASET],  # Dataset 驱动
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # 防止并发
    tags=['governance', 'asset-packing', 'dag-b'],
) as dag:
    
    # Task 1: 清理僵尸任务
    cleanup_zombies = PythonOperator(
        task_id='cleanup_zombie_tasks',
        python_callable=cleanup_zombie_tasks,
    )
    
    # Task 2: 获取待处理资产
    get_assets = PythonOperator(
        task_id='get_pending_assets',
        python_callable=get_pending_assets,
    )
    
    # Task 3: 分支判断
    branch = BranchPythonOperator(
        task_id='check_has_assets',
        python_callable=check_has_assets,
    )
    
    # Task 4: 执行打包
    pack = PythonOperator(
        task_id='pack_assets',
        python_callable=pack_assets,
    )
    
    # Task 5: 跳过打包
    skip = PythonOperator(
        task_id='skip_packing',
        python_callable=skip_packing,
    )
    
    # Task 6: 验证打包结果
    validate_results = PythonOperator(
        task_id='validate_results',
        python_callable=validate_packing_results,
        trigger_rule='none_failed',
    )
    
    # Task 7: 发送失败汇总邮件
    send_summary = PythonOperator(
        task_id='send_failure_summary',
        python_callable=send_failure_summary,
        trigger_rule='none_failed',
    )
    
    # 定义任务依赖
    cleanup_zombies >> get_assets >> branch
    branch >> [pack, skip]
    [pack, skip] >> validate_results >> send_summary
