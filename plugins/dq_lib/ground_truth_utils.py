"""
Ground Truth Validation Utilities

此模块提供三层真相检查的客户端实现：
1. InfluxClient: 查询 InfluxDB 获取物理层真相（实际车辆位置/速度）
2. MapClient: 批量查询地图服务获取语义层真相（道路类型）
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import requests
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.exceptions import InfluxDBError


class InfluxClient:
    """
    InfluxDB 客户端（用于查询车辆物理层真相）
    
    功能：
    - 根据 Unix 时间戳查询指定时间窗口的车辆位置和速度
    - 自动聚合（MEAN）以降噪
    """
    
    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str,
        timeout: int = 30000,
    ):
        """
        初始化 InfluxDB 客户端
        
        Args:
            url: InfluxDB URL (e.g., "http://10.105.66.20:8086")
            token: API Token
            org: Organization name
            bucket: Bucket name
            timeout: Query timeout in milliseconds
        """
        self.client = InfluxDBClient(url=url, token=token, org=org, timeout=timeout)
        self.query_api: QueryApi = self.client.query_api()
        self.bucket = bucket
        self.org = org
        self.logger = logging.getLogger("airflow.task.InfluxClient")
    
    def query_position_at_timestamp(
        self,
        vehicle_id: str,
        unix_timestamp: int,
        window_seconds: int = 1,
    ) -> Optional[Dict[str, float]]:
        """
        查询指定 Unix 时间戳前后窗口内的车辆位置和速度（聚合）
        
        Args:
            vehicle_id: 车辆 ID
            unix_timestamp: Unix 时间戳（秒）
            window_seconds: 时间窗口（前后各 N 秒），默认 1 秒
        
        Returns:
            dict: {"actual_x": float, "actual_y": float, "actual_speed": float}
            或 None（如果查询失败或无数据）
        """
        try:
            # 将 Unix 秒级时间戳转换为 InfluxDB RFC3339 时间格式
            # InfluxDB 需要格式: '2025-12-21T00:00:00.000Z' (带毫秒)
            center_time = datetime.utcfromtimestamp(unix_timestamp)
            start_time = center_time - timedelta(seconds=window_seconds)
            end_time = center_time + timedelta(seconds=window_seconds)
            
            # 格式化为 RFC3339 格式（带毫秒）
            # 例如: 2025-12-21T09:32:01.000Z
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            # 构建 Flux 查询
            # 先 pivot（将 x, y, speed 从行转为列），然后在 Python 中聚合
            # _time 是 InfluxDB 的标准时间字段
            query = f'''
from(bucket: "{self.bucket}")
  |> range(start: {start_time_str}, stop: {end_time_str})
  |> filter(fn: (r) => r["_measurement"] == "vehicledata")
  |> filter(fn: (r) => r["vehicleId"] == "{vehicle_id}")
  |> filter(fn: (r) => r["_field"] == "x" or r["_field"] == "y" or r["_field"] == "speed")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
            
            # 🔍 调试日志：打印实际的查询语句
            self.logger.info(f"[DEBUG] Flux Query for vehicle={vehicle_id}, timestamp={unix_timestamp}:\n{query}")
            
            tables = self.query_api.query(query, org=self.org)
            
            # 🔍 调试日志：打印查询结果的数量
            self.logger.info(f"[DEBUG] Query returned {len(tables) if tables else 0} tables")
            
            # 解析结果
            if not tables or len(tables) == 0:
                self.logger.warning(
                    f"[InfluxClient] No data for vehicle={vehicle_id}, timestamp={unix_timestamp}"
                )
                return None
            
            # 收集所有记录（可能有多行），然后计算平均值
            x_values = []
            y_values = []
            speed_values = []
            
            for table in tables:
                for record in table.records:
                    values = record.values
                    x = values.get("x")
                    y = values.get("y")
                    speed = values.get("speed")
                    
                    if x is not None:
                        x_values.append(float(x))
                    if y is not None:
                        y_values.append(float(y))
                    if speed is not None:
                        speed_values.append(float(speed))
            
            # 如果没有有效数据，返回 None
            if not x_values and not y_values and not speed_values:
                return None
            
            # 计算平均值（如果有多行数据）
            result = {
                "actual_x": sum(x_values) / len(x_values) if x_values else None,
                "actual_y": sum(y_values) / len(y_values) if y_values else None,
                "actual_speed": sum(speed_values) / len(speed_values) if speed_values else None,
            }
            
            # 🔍 调试日志：记录成功查询的数据（处理 None 值）
            x_val = f"{result['actual_x']:.2f}" if result['actual_x'] is not None else "None"
            y_val = f"{result['actual_y']:.2f}" if result['actual_y'] is not None else "None"
            speed_val = f"{result['actual_speed']:.2f}" if result['actual_speed'] is not None else "None"
            
            self.logger.info(
                f"[InfluxClient] ✅ Found data for vehicle={vehicle_id}, "
                f"timestamp={unix_timestamp}, "
                f"x={x_val}, y={y_val}, speed={speed_val}"
            )
            
            return result
            
        except InfluxDBError as e:
            self.logger.error(f"[InfluxClient] Query error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"[InfluxClient] Unexpected error: {e}")
            return None
    
    def query_batch(
        self,
        queries: List[Tuple[str, int]],
        window_seconds: int = 1,
    ) -> List[Optional[Dict[str, float]]]:
        """
        批量查询（逐个查询，未来可优化为单次查询）
        
        Args:
            queries: [(vehicle_id, unix_timestamp), ...]
            window_seconds: 时间窗口
        
        Returns:
            list[dict|None]: 与 queries 顺序一致的结果列表
        """
        results = []
        for vehicle_id, unix_timestamp in queries:
            result = self.query_position_at_timestamp(
                vehicle_id=vehicle_id,
                unix_timestamp=unix_timestamp,
                window_seconds=window_seconds,
            )
            results.append(result)
        return results
    
    def close(self):
        """关闭客户端连接"""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MapClient:
    """
    地图服务客户端（用于查询语义层真相：道路类型）
    
    功能：
    - 批量查询指定车辆在给定时间戳和坐标下的道路类型
    - 支持 vehicle_id 级别的批量优化（单次请求携带多个点）
    """
    
    def __init__(
        self,
        base_url: str = "http://10.105.66.20:1234/api/v1/annotate/batch",
        port: str = "AQCTMap_20251121V1.0",
        timeout: int = 30,
        use_cache: bool = True,
    ):
        """
        初始化地图服务客户端
        
        Args:
            base_url: 地图服务 API URL
            port: 地图端口/版本标识
            timeout: 请求超时（秒）
            use_cache: 是否启用服务端缓存
        """
        self.base_url = base_url
        self.port = port
        self.timeout = timeout
        self.use_cache = use_cache
        self.logger = logging.getLogger("airflow.task.MapClient")
    
    def annotate_batch(
        self,
        vehicle_id: str,
        points: List[Dict[str, float]],
    ) -> List[Optional[str]]:
        """
        批量查询单个车辆的多个点的道路类型
        
        Args:
            vehicle_id: 车辆 ID
            points: [{"x": float, "y": float, "timestamp": int}, ...]
                    注意：timestamp 必须是 Unix 时间戳（秒，Int）
        
        Returns:
            list[str|None]: 道路类型列表，顺序与 points 一致
                            返回示例: ["QC", "Road", None]
        """
        if not points:
            return []
        
        try:
            payload = {
                "port": self.port,
                "format": "json",
                "vehicle_id": vehicle_id,
                "points": points,
                "use_cache": self.use_cache,
            }
            
            response = requests.post(
                self.base_url,
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            data = response.json()
            
            # 解析结果（根据实际 API 响应格式调整）
            # 假设返回: {"results": [{"attributes": {"road_type": {"road_type": "QC"}}}, ...]}
            results = data.get("results", [])
            road_types = []
            
            for result in results:
                try:
                    road_type = (
                        result.get("attributes", {})
                        .get("road_type", {})
                        .get("road_type")
                    )
                    road_types.append(road_type)
                except (AttributeError, TypeError):
                    road_types.append(None)
            
            # 确保返回的长度与 points 一致
            while len(road_types) < len(points):
                road_types.append(None)
            
            return road_types[:len(points)]
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"[MapClient] Request error for vehicle={vehicle_id}: {e}")
            return [None] * len(points)
        except Exception as e:
            self.logger.error(f"[MapClient] Unexpected error: {e}")
            return [None] * len(points)
    
    def annotate_multiple_vehicles(
        self,
        vehicle_points: Dict[str, List[Dict[str, float]]],
    ) -> Dict[str, List[Optional[str]]]:
        """
        批量查询多个车辆的点
        
        Args:
            vehicle_points: {
                "AT01": [{"x": 548, "y": 594, "timestamp": 1703064552}, ...],
                "AT02": [{"x": 550, "y": 600, "timestamp": 1703064560}, ...],
            }
        
        Returns:
            dict: {vehicle_id: [road_type, ...]}
        """
        results = {}
        for vehicle_id, points in vehicle_points.items():
            results[vehicle_id] = self.annotate_batch(vehicle_id, points)
        return results

