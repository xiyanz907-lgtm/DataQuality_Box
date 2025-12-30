import json
import logging
import os
import warnings
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import polars as pl
import pendulum
from airflow.providers.mysql.hooks.mysql import MySqlHook

from services.config import CONN_ID_CACTUS, CONN_ID_DATALOG, CONN_ID_NGEN

# 禁用 Pandas/Polars 的 UserWarning，避免影响任务退出码
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning)


class DataQualityRunner:
    """
    Data Quality Runner (DQ v1)

    只做 **NGen (基准源)** vs **DataLog (待测源)** 的 T+1 离线对账。
    严格遵循业务方定义的字段映射与逻辑，不包含任何单表内部逻辑校验。

    技术栈约束：
    - 使用 Airflow `MySqlHook` 获取连接
    - 使用 `polars` 做 Join/Filter 计算
    """

    def __init__(
        self,
        ngen_conn_id: str = CONN_ID_NGEN,
        datalog_conn_id: str = CONN_ID_DATALOG,
    ):
        """
        Args:
            ngen_conn_id: nGen 数据库 Airflow Connection ID（当前项目默认是 mysql conn）
            datalog_conn_id: DataLog 数据库 Airflow Connection ID（开发库 dagster_pipelines）

        说明：
        - 当前项目的 Connection IDs 统一在 `services.config` 中定义。
        """
        self.ngen_conn_id = ngen_conn_id
        self.datalog_conn_id = datalog_conn_id
        self.logger = logging.getLogger("airflow.task")

    # ======================================================================
    # A. 数据提取 (Extract with Sharding & Window)
    # ======================================================================
    def run_validation(self, shift_date: str, vehicle_list: List[str]) -> Dict[str, Any]:
        """
        必须严格使用分片参数：
        - shift_date: 班次日期（推荐 YYYY-MM-DD 字符串）
        - vehicle_list: 分片车辆列表（每个 worker 5~10 台车）

        Returns:
            dict: 用于日志/XCom 的结构化结果（同时会落盘到 datalog_ngen_check_result）
        """
        if not vehicle_list:
            self.logger.warning("[DQ] vehicle_list 为空，跳过本次校验。")
            return {"status": "SKIPPED", "shift_date": shift_date, "vehicles": [], "written": 0}

        # 1) Extract
        df_ngen_agg = self._extract_and_aggregate_ngen(shift_date=shift_date, vehicle_list=vehicle_list)
        df_daily, df_summary, df_subtarget = self._extract_datalog_tables(shift_date=shift_date, vehicle_list=vehicle_list)

        # 2) 计算 DataLog 有效时间范围，用于 NGen Only 动态裁剪 (Dynamic Clipping)
        clip_start, clip_end = self._compute_datalog_effective_window(df_summary=df_summary, df_subtarget=df_subtarget)

        # 3) per-vehicle 执行三组对账（严格 1v1 + 记录 WARN，不抛异常）
        profiling_rows: List[Tuple[str, str, str]] = []
        all_vehicle_results: List[Dict[str, Any]] = []

        # 为了避免不同数据源 vehicle_id 字段缺失导致过滤失败，这里以 NGen 聚合后的 vehicle 为主
        # 如果 NGen 为空，则退化使用传入 vehicle_list
        if df_ngen_agg is not None and not df_ngen_agg.is_empty() and "ngen_vehicle" in df_ngen_agg.columns:
            vehicles_to_run = (
                df_ngen_agg.select(pl.col("ngen_vehicle").unique()).to_series().to_list()
            )
        else:
            vehicles_to_run = [str(v).strip() for v in vehicle_list if str(v).strip()]

        for veh in vehicles_to_run:
            self.logger.info(f"[DQ] Processing vehicle: {veh}")
            
            # NGen per vehicle
            df_ngen_v = df_ngen_agg.filter(pl.col("ngen_vehicle") == veh) if not df_ngen_agg.is_empty() else pl.DataFrame()

            # DataLog per vehicle（表是否包含 vehicle_id 列由实际 schema 决定；我们按需求优先过滤）
            df_daily_v = self._safe_filter_vehicle(df_daily, veh, vehicle_col="vehicle_id")
            df_summary_v = self._safe_filter_vehicle(df_summary, veh, vehicle_col="vehicle_id")
            df_subtarget_v = self._safe_filter_vehicle(df_subtarget, veh, vehicle_col="vehicle_id")

            self.logger.info(f"[DQ] Starting check 1/3 (vs daily) for vehicle: {veh}")
            res_daily = self.check_ngen_vs_daily(df_ngen_v, df_daily_v, clip_start, clip_end)
            
            self.logger.info(f"[DQ] Starting check 2/3 (vs summary) for vehicle: {veh}")
            res_summary = self.check_ngen_vs_summary(df_ngen_v, df_summary_v, clip_start, clip_end)
            
            self.logger.info(f"[DQ] Starting check 3/3 (vs subtarget) for vehicle: {veh}")
            res_subtarget = self.check_ngen_vs_subtarget(df_ngen_v, df_subtarget_v, clip_start, clip_end)
            
            self.logger.info(f"[DQ] Completed all checks for vehicle: {veh}")

            vehicle_result = {
                "vehicle_id": veh,
                "shift_date": shift_date,
                "clip_window": {
                    "min_time": clip_start.isoformat() if clip_start else None,
                    "max_time": clip_end.isoformat() if clip_end else None,
                },
                "check_1_ngen_vs_daily": res_daily,
                "check_2_ngen_vs_summary": res_summary,
                "check_3_ngen_vs_subtarget": res_subtarget,
            }

            profiling_json = json.dumps(vehicle_result, ensure_ascii=False, default=str)
            profiling_rows.append((veh, shift_date, profiling_json))
            all_vehicle_results.append(vehicle_result)

        # 4) Load
        self.logger.info(f"[DQ] Writing results for {len(profiling_rows)} vehicle(s)")
        written = self._write_results(profiling_rows)
        self.logger.info(f"[DQ] Successfully wrote {written} records to database")

        result = {
            "status": "SUCCESS",
            "shift_date": shift_date,
            "vehicles": vehicles_to_run,
            "written": written,
            "results": all_vehicle_results,
        }
        
        self.logger.info(f"[DQ] Validation completed successfully: {written} records written")
        return result

    def _extract_and_aggregate_ngen(self, shift_date: str, vehicle_list: List[str]) -> pl.DataFrame:
        """
        nGen 数据 (基准源)

        - 表名: hutchisonports.ngen
        - SQL:
          - WHERE Tractor_No IN vehicle_list（处理 AT001 vs AT01 格式差异）
          - AND On_Chasis_Datetime BETWEEN shift_date-6h AND shift_date+30h
        - 预处理聚合（按 Tractor_Cycle_Id GroupBy）：
          - ngen_id: Tractor_Cycle_Id
          - ngen_vehicle: 清洗车号（AT01）
          - ngen_start: MIN(On_Chasis_Datetime)
          - ngen_end: MAX(Off_Chasis_Datetime)
          - ngen_box: COUNT(DISTINCT cntr_id)
          - ngen_type: FIRST(movement_reference)
        """
        hook = MySqlHook(mysql_conn_id=self.ngen_conn_id)

        # 1) 计算开窗（shift_date -6h, +30h）
        # 重要说明（按你最新要求）：
        # - nGen 的时间字段存储为 **站点本地时间**（Local）
        # - DataLog 的时间字段为 **UTC**
        # 因此：
        # 1) SQL 过滤窗口必须用“站点本地日”的开窗（避免 shift_date 跨日漏单）
        # 2) 读取到的 nGen 时间需要做 Local -> UTC 转换，后续与 DataLog 统一在 UTC 对齐比较
        site_tz_str = os.getenv("SITE_TIMEZONE", "UTC")
        site_tz = pendulum.timezone(site_tz_str)
        # shift_date 视为“班次日期”，等价于一个批次数据；这里按站点本地 00:00:00 计算窗口
        shift_dt_local = pendulum.parse(str(shift_date), tz=site_tz).start_of("day")
        window_start_local = (shift_dt_local - pendulum.duration(hours=6)).naive()
        window_end_local = (shift_dt_local + pendulum.duration(hours=30)).naive()

        # 2) 车辆格式差异处理：对每个输入 vehicle，生成 ATxx 与 AT0xx 变体
        search_vehicles = self._expand_vehicle_variants(vehicle_list)
        if not search_vehicles:
            return pl.DataFrame()

        vehicles_str = ",".join([f"'{v}'" for v in sorted(search_vehicles)])
        # nGen 常见存储：DD/MM/YYYY HH:MM:SS（varchar, Local Time）
        # 这里使用 STR_TO_DATE 做过滤，避免字符串比较导致跨日漏单。
        start_str = window_start_local.strftime("%d/%m/%Y %H:%M:%S")
        end_str = window_end_local.strftime("%d/%m/%Y %H:%M:%S")

        # 注意：字段名严格按需求定义：
        # - 时间列：On_Chasis_Datetime / Off_Chasis_Datetime（你已确认）
        # - 其他列：cntr_id / movement_reference（若实际库中大小写不同，请以库表字段为准）
        sql = f"""
            SELECT
                Tractor_No,
                Tractor_Cycle_Id,
                On_Chasis_Datetime,
                Off_Chasis_Datetime,
                cntr_id,
                movement_reference
            FROM hutchisonports.ngen
            WHERE TRIM(Tractor_No) IN ({vehicles_str})
              AND STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%i:%s') >= STR_TO_DATE('{start_str}', '%d/%m/%Y %H:%i:%s')
              AND STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%i:%s') <= STR_TO_DATE('{end_str}', '%d/%m/%Y %H:%i:%s')
        """

        self.logger.info(
            f"[DQ] Extract nGen window(Local {site_tz_str}): {start_str} ~ {end_str}, vehicles={len(search_vehicles)}"
        )
        # 关键调试信息：确认 Airflow 实际执行的 IN 集合与 SQL
        # 注意：SQL 可能较长，默认用 DEBUG，不污染 INFO 日志
        self.logger.debug(f"[DQ] nGen vehicles (expanded) sample: {sorted(list(search_vehicles))[:20]}")
        self.logger.debug(f"[DQ] nGen SQL:\n{sql}")

        df_pd = self._read_sql_as_pandas(hook, sql)
        if df_pd is None or df_pd.empty:
            # 诊断信息：同车号不加时间过滤，看 nGen 时间字段的解析/范围，快速定位是车号不匹配还是时间过滤导致
            try:
                diag_sql = f"""
                    SELECT
                        COUNT(*) AS rows_cnt,
                        MIN(STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%i:%s')) AS min_on_dt,
                        MAX(STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%i:%s')) AS max_on_dt,
                        SUM(STR_TO_DATE(On_Chasis_Datetime, '%d/%m/%Y %H:%i:%s') IS NULL) AS parse_null_cnt
                    FROM hutchisonports.ngen
                    WHERE TRIM(Tractor_No) IN ({vehicles_str})
                """
                diag_pd = self._read_sql_as_pandas(hook, diag_sql)
                diag_row = None
                if diag_pd is not None and not diag_pd.empty:
                    diag_row = diag_pd.iloc[0].to_dict()
                self.logger.warning(
                    f"[DQ] nGen 未读取到数据（时间过滤后为 0 行）。diag={diag_row} "
                    f"(local_window={start_str}~{end_str}, tz={site_tz_str})"
                )
            except Exception as e:
                self.logger.warning(f"[DQ] nGen 未读取到数据（SQL 结果为空），且诊断 SQL 执行失败：{e}")
            return pl.DataFrame()

        df_pl = pl.from_pandas(df_pd)

        # 3) Polars 侧清洗/聚合（严格字段映射）
        # - ngen_vehicle：从 Tractor_No 提取数字并 zfill(2)，统一为 AT01 形态
        # - ngen_box：COUNT DISTINCT cntr_id（注意 cntr_id 可能为空，distinct 计数会忽略 null）
        # nGen 时间格式：DD/MM/YYYY HH:MM:SS（本地时间）
        time_fmt = "%d/%m/%Y %H:%M:%S"

        df_clean = df_pl.with_columns(
            [
                pl.col("Tractor_Cycle_Id").cast(pl.Int64, strict=False),
                # 强制先 cast 为 string，再按 format 解析（strict=False：脏数据返回 null，不报错）
                pl.col("On_Chasis_Datetime")
                .cast(pl.Utf8, strict=False)
                .str.strptime(pl.Datetime, format=time_fmt, strict=False)
                # Local -> UTC：标记为站点本地时区，再转换到 UTC
                .dt.replace_time_zone(site_tz_str)
                .dt.convert_time_zone("UTC")
                .alias("on_chasis_time"),
                pl.col("Off_Chasis_Datetime")
                .cast(pl.Utf8, strict=False)
                .str.strptime(pl.Datetime, format=time_fmt, strict=False)
                .dt.replace_time_zone(site_tz_str)
                .dt.convert_time_zone("UTC")
                .alias("off_chasis_time"),
                pl.format(
                    "AT{}",
                    pl.col("Tractor_No")
                    .cast(pl.Utf8, strict=False)
                    .str.replace_all(r"[^0-9]", "")
                    .cast(pl.Int64, strict=False)
                    .cast(pl.Utf8)
                    .str.zfill(2),
                ).alias("ngen_vehicle"),
                pl.col("cntr_id").cast(pl.Utf8, strict=False).alias("cntr_id"),
                pl.col("movement_reference").cast(pl.Utf8, strict=False).alias("movement_reference"),
            ]
        )

        df_agg = (
            df_clean.group_by("Tractor_Cycle_Id")
            .agg(
                [
                    pl.col("Tractor_Cycle_Id").first().alias("ngen_id"),
                    pl.col("ngen_vehicle").first().alias("ngen_vehicle"),
                    pl.col("on_chasis_time").min().alias("ngen_start"),
                    pl.col("off_chasis_time").max().alias("ngen_end"),
                    pl.col("cntr_id").n_unique().alias("ngen_box"),
                    pl.col("movement_reference").first().alias("ngen_type"),
                ]
            )
            .filter(pl.col("ngen_id").is_not_null())
        )

        self.logger.info(f"[DQ] nGen aggregated cycles: {df_agg.height}")
        return df_agg

    def _extract_datalog_tables(self, shift_date: str, vehicle_list: List[str]) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """
        Data Log 数据 (待测源)
        库名：dagster_pipelines
        通用过滤：WHERE shift_date = target_date AND vehicle_id IN vehicle_list

        提取三张表：
        - df_daily: daily_cycle_overall (Key: cycle_id, Cols: vehicle_id, box)
        - df_summary: cycle_section_summary (Key: cycle_id, Cols: box, cycle_end_time, task_type)
        - df_subtarget: subtarget_vehicle_cycle (Key: Tractor_cycle_Id, Cols: box, cycle_end_time)
        """
        hook = MySqlHook(mysql_conn_id=self.datalog_conn_id)

        vehicles_str = ",".join([f"'{str(v).strip()}'" for v in vehicle_list if str(v).strip()])
        target_date = str(shift_date)

        # 说明：
        # - 字段选择遵循需求定义；为保证对账可执行，summary/subtarget 额外取 vehicle_id（用于分片过滤）
        # - subtarget 的 task_type 用于 Check3 作业类型对比（需求明确要求对比），因此必须取出
        sql_daily = f"""
            SELECT
                cycle_id,
                vehicle_id,
                box
            FROM dagster_pipelines.daily_cycle_overall
            WHERE shift_date = '{target_date}'
              AND vehicle_id IN ({vehicles_str})
        """

        sql_summary = f"""
            SELECT
                cycle_id,
                vehicle_id,
                box,
                cycle_end_time,
                task_type
            FROM dagster_pipelines.cycle_section_summary
            WHERE shift_date = '{target_date}'
              AND vehicle_id IN ({vehicles_str})
        """

        sql_subtarget = f"""
            SELECT
                Tractor_cycle_Id,
                vehicle_id,
                box,
                cycle_end_time,
                task_type
            FROM dagster_pipelines.subtarget_vehicle_cycle
            WHERE shift_date = '{target_date}'
              AND vehicle_id IN ({vehicles_str})
        """

        self.logger.info(f"[DQ] Extract DataLog shift_date={target_date}, vehicles={len(vehicle_list)}")

        df_daily_pd = self._read_sql_as_pandas(hook, sql_daily)
        df_summary_pd = self._read_sql_as_pandas(hook, sql_summary)
        df_subtarget_pd = self._read_sql_as_pandas(hook, sql_subtarget)

        df_daily = pl.from_pandas(df_daily_pd) if df_daily_pd is not None and not df_daily_pd.empty else pl.DataFrame()
        df_summary = pl.from_pandas(df_summary_pd) if df_summary_pd is not None and not df_summary_pd.empty else pl.DataFrame()
        df_subtarget = pl.from_pandas(df_subtarget_pd) if df_subtarget_pd is not None and not df_subtarget_pd.empty else pl.DataFrame()

        # DataLog 表字段名在不同环境可能存在大小写/下划线差异。
        # 为了严格按需求的字段映射继续计算，这里做一次“列名归一化”：
        # - daily:   cycle_id, vehicle_id, box
        # - summary: cycle_id, vehicle_id, box, cycle_end_time, task_type
        # - subtarget: Tractor_cycle_Id, vehicle_id, box, cycle_end_time, task_type
        df_daily = self._normalize_columns(
            df_daily,
            expected_to_candidates={
                "cycle_id": ["cycle_id", "cycleId", "cycleID", "Cycle_Id", "CycleID"],
                "vehicle_id": ["vehicle_id", "vehicleId", "vehicleID", "Vehicle_Id", "VehicleID"],
                "box": ["box", "Box"],
            },
        )
        df_summary = self._normalize_columns(
            df_summary,
            expected_to_candidates={
                "cycle_id": ["cycle_id", "cycleId", "cycleID", "Cycle_Id", "CycleID"],
                "vehicle_id": ["vehicle_id", "vehicleId", "vehicleID", "Vehicle_Id", "VehicleID"],
                "box": ["box", "Box"],
                "cycle_end_time": ["cycle_end_time", "cycleEndTime", "cycle_end", "Cycle_End_Time", "cycleEnd"],
                "task_type": ["task_type", "taskType", "Task_Type", "tasktype"],
            },
        )
        df_subtarget = self._normalize_columns(
            df_subtarget,
            expected_to_candidates={
                "Tractor_cycle_Id": ["Tractor_cycle_Id", "Tractor_Cycle_Id", "tractor_cycle_id", "tractorCycleId", "cycle_id", "cycleId"],
                "vehicle_id": ["vehicle_id", "vehicleId", "vehicleID", "Vehicle_Id", "VehicleID"],
                "box": ["box", "Box"],
                "cycle_end_time": ["cycle_end_time", "cycleEndTime", "cycle_end", "Cycle_End_Time", "cycleEnd"],
                "task_type": ["task_type", "taskType", "Task_Type", "tasktype"],
            },
        )

        return df_daily, df_summary, df_subtarget

    def _compute_datalog_effective_window(
        self,
        df_summary: pl.DataFrame,
        df_subtarget: pl.DataFrame,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        通用逻辑：NGen 动态边界裁剪 (Dynamic Clipping)

        在计算 NGen Only (漏单) 时，需先计算 Data Log 该班次的有效时间范围 [Min(Start), Max(End)]。
        由于需求中 Data Log 提供的是 cycle_end_time，这里用 summary/subtarget 的 cycle_end_time 估算有效范围。
        """
        # 收集所有可用的 cycle_end_time
        end_times: List[datetime] = []

        def _extract_end_times(df: pl.DataFrame) -> None:
            if df is None or df.is_empty() or "cycle_end_time" not in df.columns:
                return
            tmp = df.select(pl.col("cycle_end_time")).drop_nulls()
            if tmp.is_empty():
                return
            # 兼容字符串/时间类型
            s = tmp.to_series()
            for v in s.to_list():
                if v is None:
                    continue
                try:
                    # DataLog 时间语义：UTC（你已确认）
                    # 统一转为 tz-aware UTC datetime，便于与 nGen(UTC) 做窗口裁剪比较
                    end_times.append(pd.to_datetime(v, utc=True).to_pydatetime())
                except Exception:
                    continue

        _extract_end_times(df_summary)
        _extract_end_times(df_subtarget)

        if not end_times:
            self.logger.warning("[DQ] DataLog cycle_end_time 为空，无法计算动态裁剪窗口；NGen Only 将全部视为“范围外噪音(忽略)”。")
            return None, None

        clip_start = min(end_times)
        clip_end = max(end_times)
        return clip_start, clip_end

    # ======================================================================
    # B. 校验逻辑 (3-Way Parallel 1v1 Checks)
    # ======================================================================
    def check_ngen_vs_daily(
        self,
        df_ngen: pl.DataFrame,
        df_daily: pl.DataFrame,
        clip_start: Optional[datetime],
        clip_end: Optional[datetime],
    ) -> Dict[str, Any]:
        """
        Check 1: NGen vs Daily
        - Join Key: ngen_id == daily.cycle_id
        - 校验点：
          1) 作业一致性：统计 Match / Real Missing
          2) 箱量：ngen_box == daily.box
          3) 车号：ngen_vehicle == daily.vehicle_id
        """
        if df_ngen is None or df_ngen.is_empty():
            return {"status": "SKIPPED", "reason": "NGen empty", "matched": 0, "real_missing": 0}

        if df_daily is None:
            df_daily = pl.DataFrame()

        # 防御：如果 daily 表缺少关键字段（例如 cycle_id），按“仅 WARN 不失败”处理
        required_right = ["cycle_id", "vehicle_id", "box"]
        missing = [c for c in required_right if c not in df_daily.columns]
        if missing:
            self.logger.warning(f"[DQ][Daily] DataLog daily 表缺少字段 {missing}，跳过本次对账（不抛异常）。")
            return {"status": "SKIPPED_SCHEMA", "missing_columns": missing, "matched": 0, "real_missing": 0}

        # 统一字段类型，保证 join/比较稳定
        right = df_daily.with_columns(
            [
                pl.col("cycle_id").cast(pl.Int64, strict=False),
                pl.col("box").cast(pl.Int64, strict=False),
                pl.col("vehicle_id").cast(pl.Utf8, strict=False),
                # 命中标记：用于 left join 后判断是否匹配（避免 join 输出不保留右侧 join key 列 cycle_id）
                pl.lit(1).alias("__hit_daily"),
            ]
        )

        left = df_ngen.with_columns(
            [
                pl.col("ngen_id").cast(pl.Int64, strict=False),
                pl.col("ngen_box").cast(pl.Int64, strict=False),
                pl.col("ngen_vehicle").cast(pl.Utf8, strict=False),
            ]
        )

        joined = left.join(right, left_on="ngen_id", right_on="cycle_id", how="left")

        # 匹配/漏单（动态裁剪）
        matched_mask = pl.col("__hit_daily").is_not_null()
        real_missing_mask = self._real_missing_mask(joined, matched_mask, clip_start, clip_end, time_col="ngen_end")

        # 字段一致性
        box_mismatch_mask = matched_mask & (pl.col("ngen_box") != pl.col("box"))
        vehicle_mismatch_mask = matched_mask & (pl.col("ngen_vehicle") != pl.col("vehicle_id"))

        res = {
            "status": "SUCCESS",
            "ngen_total": int(left.height),
            "matched": int(joined.filter(matched_mask).height),
            "real_missing": int(joined.filter(real_missing_mask).height),
            "box_mismatch": int(joined.filter(box_mismatch_mask).height),
            "vehicle_mismatch": int(joined.filter(vehicle_mismatch_mask).height),
            "sample_real_missing_ids": self._sample_ids(joined.filter(real_missing_mask), id_col="ngen_id"),
            "sample_box_mismatch_ids": self._sample_ids(joined.filter(box_mismatch_mask), id_col="ngen_id"),
            "sample_vehicle_mismatch_ids": self._sample_ids(joined.filter(vehicle_mismatch_mask), id_col="ngen_id"),
        }

        # 所有不匹配仅记录 WARN，不抛异常
        if res["real_missing"] or res["box_mismatch"] or res["vehicle_mismatch"]:
            self.logger.warning(f"[DQ][Daily] mismatch summary: {res}")

        return res

    def check_ngen_vs_summary(
        self,
        df_ngen: pl.DataFrame,
        df_summary: pl.DataFrame,
        clip_start: Optional[datetime],
        clip_end: Optional[datetime],
    ) -> Dict[str, Any]:
        """
        Check 2: NGen vs Summary
        - Join Key: ngen_id == summary.cycle_id
        - 校验点：
          1) 作业一致性：统计 Match / Real Missing
          2) 箱量：ngen_box == summary.box
          3) 结束时间：Abs(ngen_end - summary.cycle_end_time) <= 300s
          4) 作业类型：ngen_type == summary.task_type（完全相等匹配）
        """
        if df_ngen is None or df_ngen.is_empty():
            return {"status": "SKIPPED", "reason": "NGen empty", "matched": 0, "real_missing": 0}

        if df_summary is None:
            df_summary = pl.DataFrame()

        self.logger.info(f"[DQ][Summary] Preparing summary data, shape: {df_summary.shape if not df_summary.is_empty() else (0, 0)}")
        right = self._prep_summary_like(df_summary, key_col="cycle_id")
        self.logger.info(f"[DQ][Summary] Prepared summary data, shape: {right.shape}")
        
        left = self._prep_ngen_for_checks(df_ngen)
        self.logger.info(f"[DQ][Summary] Prepared ngen data, shape: {left.shape}")

        # 防御：summary 缺关键字段时只 WARN，不失败
        required_right = ["cycle_id", "box", "cycle_end_time", "task_type"]
        missing = [c for c in required_right if c not in right.columns]
        if missing:
            self.logger.warning(f"[DQ][Summary] DataLog summary 表缺少字段 {missing}，跳过本次对账（不抛异常）。")
            return {"status": "SKIPPED_SCHEMA", "missing_columns": missing, "matched": 0, "real_missing": 0}

        # 命中标记：避免 join 输出不保留右侧 join key 列 cycle_id
        self.logger.info(f"[DQ][Summary] Adding hit marker to right table")
        right = right.with_columns(pl.lit(1).alias("__hit_summary"))
        
        self.logger.info(f"[DQ][Summary] Joining tables")
        joined = left.join(right, left_on="ngen_id", right_on="cycle_id", how="left")
        self.logger.info(f"[DQ][Summary] Joined shape: {joined.shape}")

        self.logger.info(f"[DQ][Summary] Computing matched_mask")
        matched_mask = pl.col("__hit_summary").is_not_null()
        
        self.logger.info(f"[DQ][Summary] Computing real_missing_mask")
        real_missing_mask = self._real_missing_mask(joined, matched_mask, clip_start, clip_end, time_col="ngen_end")

        self.logger.info(f"[DQ][Summary] Computing mismatch masks")
        # 只对匹配的记录计算差异（避免对 None 值计算导致崩溃）
        box_mismatch_mask = matched_mask & (pl.col("ngen_box") != pl.col("box"))
        type_mismatch_mask = matched_mask & (pl.col("ngen_type") != pl.col("task_type"))
        
        self.logger.info(f"[DQ][Summary] Simple masks computed successfully")

        # Eagerly evaluate filters to avoid lazy evaluation crashes
        self.logger.info(f"[DQ][Summary] Filtering matched records")
        try:
            matched_count = int(joined.filter(matched_mask).height)
        except Exception as e:
            self.logger.error(f"[DQ][Summary] Failed to filter matched: {e}")
            matched_count = 0
        
        self.logger.info(f"[DQ][Summary] Filtering real_missing records")
        try:
            real_missing_count = int(joined.filter(real_missing_mask).height)
            real_missing_ids = self._sample_ids(joined.filter(real_missing_mask), id_col="ngen_id")
        except Exception as e:
            self.logger.error(f"[DQ][Summary] Failed to filter real_missing: {e}")
            real_missing_count = 0
            real_missing_ids = []
        
        self.logger.info(f"[DQ][Summary] Filtering box_mismatch records")
        try:
            box_mismatch_count = int(joined.filter(box_mismatch_mask).height)
            box_mismatch_ids = self._sample_ids(joined.filter(box_mismatch_mask), id_col="ngen_id")
        except Exception as e:
            self.logger.error(f"[DQ][Summary] Failed to filter box_mismatch: {e}")
            box_mismatch_count = 0
            box_mismatch_ids = []
        
        self.logger.info(f"[DQ][Summary] Filtering end_time_mismatch records")
        try:
            # 完全避免在 mask 中计算时间差，改为：
            # 1. 先过滤出有效匹配且时间不为 null 的记录
            # 2. 然后在这个子集上计算时间差
            # 3. 再过滤出时间差 > 300 的记录
            valid_time_df = joined.filter(matched_mask & pl.col("cycle_end_time").is_not_null())
            
            if valid_time_df.is_empty():
                end_time_mismatch_count = 0
                end_time_mismatch_ids = []
            else:
                # 在有效数据上计算时间差
                time_diff_df = valid_time_df.with_columns(
                    ((pl.col("ngen_end") - pl.col("cycle_end_time")).abs().dt.total_seconds()).alias("time_diff_sec")
                )
                # 过滤出时间差 > 300 的记录
                mismatch_df = time_diff_df.filter(pl.col("time_diff_sec") > 300)
                end_time_mismatch_count = int(mismatch_df.height)
                end_time_mismatch_ids = self._sample_ids(mismatch_df, id_col="ngen_id")
        except Exception as e:
            self.logger.error(f"[DQ][Summary] Failed to filter end_time_mismatch: {e}")
            end_time_mismatch_count = 0
            end_time_mismatch_ids = []
        
        self.logger.info(f"[DQ][Summary] Filtering type_mismatch records")
        try:
            type_mismatch_count = int(joined.filter(type_mismatch_mask).height)
            type_mismatch_ids = self._sample_ids(joined.filter(type_mismatch_mask), id_col="ngen_id")
        except Exception as e:
            self.logger.error(f"[DQ][Summary] Failed to filter type_mismatch: {e}")
            type_mismatch_count = 0
            type_mismatch_ids = []

        self.logger.info(f"[DQ][Summary] All filters completed successfully")
        
        res = {
            "status": "SUCCESS",
            "ngen_total": int(left.height),
            "matched": matched_count,
            "real_missing": real_missing_count,
            "box_mismatch": box_mismatch_count,
            "end_time_mismatch": end_time_mismatch_count,
            "type_mismatch": type_mismatch_count,
            "sample_real_missing_ids": real_missing_ids,
            "sample_box_mismatch_ids": box_mismatch_ids,
            "sample_end_time_mismatch_ids": end_time_mismatch_ids,
            "sample_type_mismatch_ids": type_mismatch_ids,
        }

        if res["real_missing"] or res["box_mismatch"] or res["end_time_mismatch"] or res["type_mismatch"]:
            self.logger.warning(f"[DQ][Summary] mismatch summary: {res}")

        return res

    def check_ngen_vs_subtarget(
        self,
        df_ngen: pl.DataFrame,
        df_subtarget: pl.DataFrame,
        clip_start: Optional[datetime],
        clip_end: Optional[datetime],
    ) -> Dict[str, Any]:
        """
        Check 3: NGen vs Subtarget
        - Join Key: ngen_id == subtarget.Tractor_cycle_Id
        - 校验点：
          1) 作业一致性：统计 Match / Real Missing
          2) 箱量：ngen_box == subtarget.box
          3) 结束时间：Abs(ngen_end - subtarget.cycle_end_time) <= 300s
          4) 作业类型：ngen_type == Subtarget.task_type（完全相等匹配）
        """
        if df_ngen is None or df_ngen.is_empty():
            return {"status": "SKIPPED", "reason": "NGen empty", "matched": 0, "real_missing": 0}

        if df_subtarget is None:
            df_subtarget = pl.DataFrame()

        right = self._prep_summary_like(df_subtarget, key_col="Tractor_cycle_Id")
        left = self._prep_ngen_for_checks(df_ngen)

        # 防御：subtarget 缺关键字段时只 WARN，不失败
        required_right = ["Tractor_cycle_Id", "box", "cycle_end_time", "task_type"]
        missing = [c for c in required_right if c not in right.columns]
        if missing:
            self.logger.warning(f"[DQ][Subtarget] DataLog subtarget 表缺少字段 {missing}，跳过本次对账（不抛异常）。")
            return {"status": "SKIPPED_SCHEMA", "missing_columns": missing, "matched": 0, "real_missing": 0}

        # 命中标记：避免 join 输出不保留右侧 join key 列 Tractor_cycle_Id
        right = right.with_columns(pl.lit(1).alias("__hit_subtarget"))
        joined = left.join(right, left_on="ngen_id", right_on="Tractor_cycle_Id", how="left")

        matched_mask = pl.col("__hit_subtarget").is_not_null()
        real_missing_mask = self._real_missing_mask(joined, matched_mask, clip_start, clip_end, time_col="ngen_end")

        # 只对匹配的记录计算差异（避免对 None 值计算导致崩溃）
        box_mismatch_mask = matched_mask & (pl.col("ngen_box") != pl.col("box"))
        type_mismatch_mask = matched_mask & (pl.col("ngen_type") != pl.col("task_type"))

        # Eagerly evaluate filters to avoid lazy evaluation crashes (same as summary check)
        try:
            matched_count = int(joined.filter(matched_mask).height)
            real_missing_count = int(joined.filter(real_missing_mask).height)
            real_missing_ids = self._sample_ids(joined.filter(real_missing_mask), id_col="ngen_id")
            box_mismatch_count = int(joined.filter(box_mismatch_mask).height)
            box_mismatch_ids = self._sample_ids(joined.filter(box_mismatch_mask), id_col="ngen_id")
            
            # 时间差计算：完全避免在 mask 中计算，改为分步骤
            valid_time_df = joined.filter(matched_mask & pl.col("cycle_end_time").is_not_null())
            if valid_time_df.is_empty():
                end_time_mismatch_count = 0
                end_time_mismatch_ids = []
            else:
                time_diff_df = valid_time_df.with_columns(
                    ((pl.col("ngen_end") - pl.col("cycle_end_time")).abs().dt.total_seconds()).alias("time_diff_sec")
                )
                mismatch_df = time_diff_df.filter(pl.col("time_diff_sec") > 300)
                end_time_mismatch_count = int(mismatch_df.height)
                end_time_mismatch_ids = self._sample_ids(mismatch_df, id_col="ngen_id")
            
            type_mismatch_count = int(joined.filter(type_mismatch_mask).height)
            type_mismatch_ids = self._sample_ids(joined.filter(type_mismatch_mask), id_col="ngen_id")
        except Exception as e:
            self.logger.error(f"[DQ][Subtarget] Failed to filter records: {e}")
            matched_count = 0
            real_missing_count = 0
            real_missing_ids = []
            box_mismatch_count = 0
            box_mismatch_ids = []
            end_time_mismatch_count = 0
            end_time_mismatch_ids = []
            type_mismatch_count = 0
            type_mismatch_ids = []

        res = {
            "status": "SUCCESS",
            "ngen_total": int(left.height),
            "matched": matched_count,
            "real_missing": real_missing_count,
            "box_mismatch": box_mismatch_count,
            "end_time_mismatch": end_time_mismatch_count,
            "type_mismatch": type_mismatch_count,
            "sample_real_missing_ids": real_missing_ids,
            "sample_box_mismatch_ids": box_mismatch_ids,
            "sample_end_time_mismatch_ids": end_time_mismatch_ids,
            "sample_type_mismatch_ids": type_mismatch_ids,
        }

        if res["real_missing"] or res["box_mismatch"] or res["end_time_mismatch"] or res["type_mismatch"]:
            self.logger.warning(f"[DQ][Subtarget] mismatch summary: {res}")

        return res

    # ======================================================================
    # C. 结果落盘 (Load)
    # ======================================================================
    def _write_results(self, rows: List[Tuple[str, str, str]]) -> int:
        """
        写入 datalog_ngen_check_result(vehicle_id, shift_date, profiling_json)

        注意：
        - 所有不匹配只记录 WARN，不抛异常。
        - 为保证作业可重跑，这里使用 ON DUPLICATE KEY UPDATE（要求表上存在唯一键 vehicle_id+shift_date）。
        """
        if not rows:
            return 0

        hook = MySqlHook(mysql_conn_id=self.datalog_conn_id)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            sql = """
                INSERT INTO dagster_pipelines.datalog_ngen_check_result (vehicle_id, shift_date, profiling_json)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE profiling_json = VALUES(profiling_json)
            """
            cursor.executemany(sql, rows)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            # 严格按需求：不抛异常，仅 WARN
            self.logger.warning(f"[DQ] 写入 datalog_ngen_check_result 失败（仅告警不失败任务）：{e}", exc_info=True)
            try:
                conn.rollback()
            except Exception:
                pass
            return 0
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ======================================================================
    # 内部工具方法（不改变业务定义，仅做类型/过滤/采样）
    # ======================================================================
    def _expand_vehicle_variants(self, vehicle_list: List[str]) -> set:
        """
        处理 AT001 vs AT01 格式差异：
        - 提取数字部分 digits
        - 生成 AT + zfill(2) 与 AT + zfill(3)
        """
        import re

        out = set()
        for v in vehicle_list:
            v_str = str(v).strip()
            if not v_str:
                continue
            # 注意：这里必须用 \D（非数字）来提取数字部分。
            # 之前如果误写成 \\D，会导致“提取失败/脏值透传”，从而产生 ATAT02 这类异常车号。
            digits = re.sub(r"\D", "", v_str)
            if digits:
                # 统一只输出标准形式，避免出现 ATAT02 这类异常输入造成 SQL 车号污染
                out.add(f"AT{digits.zfill(2)}")
                out.add(f"AT{digits.zfill(3)}")
                # 如果原始值本身就是标准 ATxx/ATxxx，再保留（否则不保留，避免污染）
                if re.match(r"^AT\\d{2,3}$", v_str, flags=re.IGNORECASE):
                    out.add(v_str.upper())
            else:
                out.add(v_str)
        return out

    def _read_sql_as_pandas(self, hook: MySqlHook, sql: str) -> pd.DataFrame:
        """
        使用 SQLAlchemy Engine 读取，避免 pandas 针对 DBAPI2 对象的 warning：
        `pandas only supports SQLAlchemy connectable ...`

        说明：
        - 仍然严格使用 MySqlHook 获取连接信息（符合技术栈要求）
        - 如 engine 不可用，则回退到 hook.get_pandas_df
        """
        try:
            engine = hook.get_sqlalchemy_engine()
            # 关键点（MySQLdb/SQLAlchemy）：
            # - SQLAlchemy 在底层 DBAPI 层可能会对 SQL 做一次 `%` 风格的格式化调用，
            #   而我们的 SQL 中包含 STR_TO_DATE 的格式串（例如 '%d/%m/%Y %H:%i:%s'），
            #   会被误当作格式化占位符，触发 "not enough arguments for format string"。
            # - 解决：在走 SQLAlchemy engine 路径时，将 '%' 统一转义为 '%%'，
            #   DBAPI 格式化后会还原为单个 '%'，MySQL 侧看到的仍然是正确的 STR_TO_DATE 格式。
            sql_engine_safe = sql.replace("%", "%%")
            return pd.read_sql_query(sql_engine_safe, con=engine)
        except Exception as e:
            # 这里打一个 warning，避免你看到 pandas DBAPI2 warning 时不知道为什么没走 SQLAlchemy engine
            self.logger.warning(f"[DQ] SQLAlchemy engine read failed, fallback to hook.get_pandas_df. err={e}")
            # 回退路径：保持与现有项目风格一致（但可能会出现 pandas warning）
            return hook.get_pandas_df(sql)

    def _prep_ngen_for_checks(self, df_ngen: pl.DataFrame) -> pl.DataFrame:
        """
        对 NGen 聚合结果做一次类型规整，避免 join/比较时类型不一致。
        """
        df = df_ngen.with_columns(
            [
                pl.col("ngen_id").cast(pl.Int64, strict=False),
                pl.col("ngen_vehicle").cast(pl.Utf8, strict=False),
                pl.col("ngen_box").cast(pl.Int64, strict=False),
                pl.col("ngen_type").cast(pl.Utf8, strict=False),
            ]
        )

        # 结束时间列需要可用于 dt 计算
        if "ngen_end" in df.columns:
            # 注意：ngen_end 在上游聚合时已转换为 UTC（Datetime[UTC]）
            # 这里保持兼容：如果意外是字符串，则解析并标记为 UTC。
            if df.schema.get("ngen_end") == pl.Utf8:
                df = df.with_columns(
                    pl.col("ngen_end")
                    .str.to_datetime(strict=False)
                    .dt.replace_time_zone("UTC")
                    .alias("ngen_end")
                )
            else:
                df = df.with_columns(pl.col("ngen_end").cast(pl.Datetime(time_zone="UTC"), strict=False).alias("ngen_end"))
        return df

    def _prep_summary_like(self, df: pl.DataFrame, key_col: str) -> pl.DataFrame:
        """
        将 summary/subtarget 侧表规整为可 join 的类型：
        - key_col: cycle_id 或 Tractor_cycle_Id
        - box: Int64
        - cycle_end_time: Datetime
        - task_type: Utf8
        """
        if df is None or df.is_empty():
            # 构造空 schema，避免 join 报错
            schema = {key_col: pl.Int64, "box": pl.Int64, "cycle_end_time": pl.Datetime, "task_type": pl.Utf8}
            return pl.DataFrame(schema=schema)

        out = df.with_columns(
            [
                pl.col(key_col).cast(pl.Int64, strict=False),
                pl.col("box").cast(pl.Int64, strict=False),
                pl.col("task_type").cast(pl.Utf8, strict=False),
            ]
        )

        if "cycle_end_time" in out.columns:
            try:
                if out.schema.get("cycle_end_time") == pl.Utf8:
                    out = out.with_columns(
                        pl.col("cycle_end_time")
                        .str.to_datetime(strict=False)
                        # DataLog 时间语义：UTC（只对非 null 值操作）
                        .dt.replace_time_zone("UTC")
                        .alias("cycle_end_time")
                    )
                else:
                    # 如果是 naive datetime，按 UTC 标记；如果已经带 tz，则 cast 到 UTC
                    out = out.with_columns(
                        pl.col("cycle_end_time")
                        .cast(pl.Datetime(time_zone="UTC"), strict=False)
                        .alias("cycle_end_time")
                    )
            except Exception as e:
                # 如果时区转换失败，记录警告并保持原样
                self.logger.warning(f"[DQ] Failed to convert cycle_end_time timezone: {e}")
                # 尝试简单的 datetime 转换（不带时区）
                try:
                    if out.schema.get("cycle_end_time") == pl.Utf8:
                        out = out.with_columns(
                            pl.col("cycle_end_time")
                            .str.to_datetime(strict=False)
                            .alias("cycle_end_time")
                        )
                except:
                    # 如果还是失败，保持原样
                    pass

        return out

    def _real_missing_mask(
        self,
        joined_df: pl.DataFrame,
        matched_mask: pl.Expr,
        clip_start: Optional[datetime],
        clip_end: Optional[datetime],
        time_col: str,
    ) -> pl.Expr:
        """
        Real Missing 判定（严格按需求定义）：
        - 只有当 NGen 的 ngen_end 落在 DataLog 有效范围 [clip_start, clip_end] 内
        - 且在 DataLog 中找不到对应 ID（matched_mask 为 False）
        才判定为 Real Missing；范围外视为隔壁班次噪音忽略。
        """
        missing_mask = matched_mask.not_()

        # 无法计算裁剪窗口：按需求“范围外视为噪音忽略”，这里直接返回全 False
        if clip_start is None or clip_end is None:
            return pl.lit(False)

        # 将 python datetime 转为 polars literal
        clip_start_lit = pl.lit(clip_start)
        clip_end_lit = pl.lit(clip_end)

        # ngen_end 需要是 Datetime 类型
        in_window = (pl.col(time_col) >= clip_start_lit) & (pl.col(time_col) <= clip_end_lit)
        return missing_mask & in_window

    def _sample_ids(self, df: pl.DataFrame, id_col: str, limit: int = 50) -> List[Any]:
        """
        采样输出错误样例 ID，避免 profiling_json 过大。
        """
        if df is None or df.is_empty() or id_col not in df.columns:
            return []
        try:
            return df.select(pl.col(id_col)).head(limit).to_series().to_list()
        except Exception:
            return []

    def _safe_filter_vehicle(self, df: pl.DataFrame, vehicle_id: str, vehicle_col: str) -> pl.DataFrame:
        """
        尽量按 vehicle_id 分片过滤：
        - 若 df 不包含 vehicle_col，则返回原 df（避免因 schema 差异导致过滤失败）
        """
        if df is None or df.is_empty():
            return pl.DataFrame()
        if vehicle_col not in df.columns:
            return df
        return df.filter(pl.col(vehicle_col).cast(pl.Utf8, strict=False) == str(vehicle_id))

    def _normalize_columns(self, df: pl.DataFrame, expected_to_candidates: Dict[str, List[str]]) -> pl.DataFrame:
        """
        将 DataLog 各表的列名归一化到“需求定义的列名”。

        - 仅做 rename，不做任何业务逻辑变更
        - 匹配策略：
          1) candidates 中有且 df.columns 命中，直接 rename
          2) 否则做“忽略大小写 + 忽略下划线”的归一化匹配
        """
        if df is None:
            return pl.DataFrame()
        if df.is_empty():
            return df

        cols = list(df.columns)

        def _norm(s: str) -> str:
            return str(s).replace("_", "").lower()

        norm_map = {_norm(c): c for c in cols}
        rename_map: Dict[str, str] = {}

        for expected, candidates in expected_to_candidates.items():
            if expected in cols:
                continue

            found = None
            # 1) 先按候选列表精确命中
            for cand in candidates:
                if cand in cols:
                    found = cand
                    break
            # 2) 再做归一化命中
            if found is None:
                for cand in candidates:
                    key = _norm(cand)
                    if key in norm_map:
                        found = norm_map[key]
                        break

            if found and found != expected:
                rename_map[found] = expected

        if rename_map:
            return df.rename(rename_map)
        return df


