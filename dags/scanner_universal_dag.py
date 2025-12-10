from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import pendulum
import os

# 监控配置：便于后续扩展更多表
MONITOR_CONFIG = [
    {
        "name": "cnt_cycles",
        "worker_dag_id": "worker_cycle_check",
        "source_a": {
            "conn_id": "ngen_mysql_conn",
            "schema": "hutchisonports",
            "table": "ngen",
            "id_col": "id",
            "biz_date_col": "On_Chasis_Datetime",  # DD/MM/YYYY
            "date_fmt": "%d/%m/%Y",
            "source_type": "ngen",  # 用于动态生成变量名
        },
        "source_b": {
            "conn_id": "cactus_mysql_conn",
            "schema": "kpi_data_db",
            "table": "cnt_cycles",
            "ts_col": "last_modified_timestamp",
            "biz_date_col": "_time_begin",
            "source_type": "cactus",  # 用于动态生成变量名
        },
    }
]

default_args = {
    "owner": "box_admin",
    "start_date": pendulum.today("UTC").add(days=-1),
}


def _get_watermark_var_name(task_name, source_type, metric_type):
    """动态生成水位线变量名：watermark_{task_name}_{source_type}_{metric_type}"""
    return f"watermark_{task_name}_{source_type}_{metric_type}"


def _collect_ids_source_a(cfg, task_name):
    """
    扫描 Source A (nGen ID 水位)
    首次运行时：查询当前 MAX(id)，写入变量，跳过触发
    返回: (start_id, end_id) tuple, current_max_id, skipped
    """
    try:
        hook = MySqlHook(mysql_conn_id=cfg["conn_id"])
        watermark_var = _get_watermark_var_name(task_name, cfg["source_type"], "id")
        
        # 读取水位线
        last_id_raw = Variable.get(watermark_var, default_var=None)
        last_id = int(last_id_raw) if last_id_raw is not None else None

        # 查询当前最大 ID
        max_id_sql = f"SELECT MAX({cfg['id_col']}) FROM {cfg['schema']}.{cfg['table']}"
        current_max_id = hook.get_first(max_id_sql)[0]
        
        if current_max_id is None:
            print(f"[source_a:{cfg.get('table')}] 表为空，跳过扫描")
            return None, None, False

        # 首次运行：初始化水位线，跳过触发
        if last_id is None:
            Variable.set(watermark_var, current_max_id)
            print(f"First run detected. Initializing watermark to {current_max_id}. Skipping trigger.")
            return None, current_max_id, True

        # 有增量数据：返回 ID 范围
        if current_max_id > last_id:
            print(f"[source_a:{cfg.get('table')}] 发现增量 ID: {last_id} -> {current_max_id}")
            return (last_id + 1, current_max_id), current_max_id, False

        return None, current_max_id, False
    except Exception as exc:
        print(f"[source_a:{cfg.get('table')}] scan failed: {exc}")
        import traceback
        traceback.print_exc()
        return None, None, False


def _collect_dates_source_b(cfg, task_name):
    """
    扫描 Source B (Cactus Timestamp 水位)
    首次运行时：查询当前 MAX(timestamp)，写入变量，跳过触发
    """
    try:
        hook = MySqlHook(mysql_conn_id=cfg["conn_id"])
        watermark_var = _get_watermark_var_name(task_name, cfg["source_type"], "time")
        
        # 读取水位线
        last_ts = Variable.get(watermark_var, default_var=None)

        # 查询当前最大时间戳
        max_ts_sql = f"SELECT MAX({cfg['ts_col']}) FROM {cfg['schema']}.{cfg['table']}"
        current_max_ts = hook.get_first(max_ts_sql)[0]
        
        if current_max_ts is None:
            print(f"[source_b:{cfg.get('table')}] 表为空，跳过扫描")
            return set(), None, False

        # 首次运行：初始化水位线，跳过触发
        if last_ts is None:
            Variable.set(watermark_var, str(current_max_ts))
            print(f"First run detected. Initializing watermark to {current_max_ts}. Skipping trigger.")
            return set(), current_max_ts, True

        # 有增量数据：查询增量日期
        date_list = []
        if current_max_ts > last_ts:
            incremental_sql = f"""
                SELECT DISTINCT DATE({cfg['biz_date_col']}) AS dt
                FROM {cfg['schema']}.{cfg['table']}
                WHERE {cfg['ts_col']} > '{last_ts}'
                  AND {cfg['ts_col']} <= '{current_max_ts}'
            """
            df_increment = hook.get_pandas_df(incremental_sql)
            date_list = df_increment["dt"].astype(str).tolist()
            print(f"[source_b:{cfg.get('table')}] 发现增量时间戳: {last_ts} -> {current_max_ts}, 涉及日期: {len(date_list)} 个")

        cleaned = {d for d in date_list if d and d != "None" and str(d).strip() != "None"}
        return cleaned, current_max_ts, False
    except Exception as exc:
        print(f"[source_b:{cfg.get('table')}] scan failed: {exc}")
        import traceback
        traceback.print_exc()
        return set(), None, False


def _universal_scan(**context):
    """
    通用双向水位扫描主函数
    遍历配置 -> 双向水位扫描 -> 触发 worker (ID分片 或 日期模式) -> 更新水位
    """
    for item in MONITOR_CONFIG:
        task_name = item["name"]
        print(f"开始扫描配置: {task_name}")

        # 扫描 Source A (nGen ID)
        id_range_a, new_id, skipped_a = _collect_ids_source_a(item["source_a"], task_name)
        
        # 扫描 Source B (Cactus Timestamp)
        dates_b, new_ts, skipped_b = _collect_dates_source_b(item["source_b"], task_name)

        # 1. 处理 Source A 触发 (ID Range Mode)
        if id_range_a and not skipped_a:
            start, end = id_range_a
            print(f"[{task_name}] Source A 触发: ID Range {start} - {end}")
            try:
                TriggerDagRunOperator(
                    task_id=f"trigger_{task_name}_id_{start}_{end}",
                    trigger_dag_id=item["worker_dag_id"],
                    conf={
                        "mode": "id_range",
                        "start_id": start,
                        "end_id": end,
                        "date_filter": f"Batch_{start}_{end}" # 用于日志展示
                    },
                    wait_for_completion=False,
                ).execute(context)
            except Exception as exc:
                print(f"[{task_name}] Source A 触发失败: {exc}")
                import traceback
                traceback.print_exc()

        # 2. 处理 Source B 触发 (Date Mode)
        if dates_b and not skipped_b:
            sorted_dates = sorted(dates_b)
            print(f"[{task_name}] Source B 触发日期: {sorted_dates}")
            for date_str in sorted_dates:
                try:
                    TriggerDagRunOperator(
                        task_id=f"trigger_{task_name}_{date_str.replace('-', '_')}",
                        trigger_dag_id=item["worker_dag_id"],
                        conf={"date_filter": date_str},
                        wait_for_completion=False,
                    ).execute(context)
                except Exception as exc:
                    print(f"[{task_name}] Source B 触发失败 (日期: {date_str}): {exc}")
                    import traceback
                    traceback.print_exc()

        if not id_range_a and not dates_b:
             print(f"[{task_name}] 未发现需要处理的增量")

        # 更新水位线
        if new_id is not None:
            watermark_var_a = _get_watermark_var_name(task_name, item["source_a"]["source_type"], "id")
            Variable.set(watermark_var_a, new_id)
            print(f"[{task_name}] Source A 水位线已更新为: {new_id}")
        
        if new_ts is not None:
            watermark_var_b = _get_watermark_var_name(task_name, item["source_b"]["source_type"], "time")
            Variable.set(watermark_var_b, str(new_ts))
            print(f"[{task_name}] Source B 水位线已更新为: {new_ts}")


with DAG(
    "scanner_universal",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    tags=["scanner", "universal"],
) as dag:
    scan_task = PythonOperator(
        task_id="scan_bidirectional_watermarks",
        python_callable=_universal_scan,
    )

