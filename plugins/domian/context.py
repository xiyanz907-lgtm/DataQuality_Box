"""
domain/context.py
核心治理上下文定义 - LLD Version 1.0 (Frozen)
"""
import os
import json
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
from enum import Enum
from datetime import datetime

# ==========================================
# 1. 枚举定义 (Enums) - 统一语言
# ==========================================

class Severity(str, Enum):
    P0 = "P0"  # 阻断/系统级错误
    P1 = "P1"  # 核心资产/业务特征
    P2 = "P2"  # 业务违规/SLA破坏
    INFO = "INFO"

class DataStage(str, Enum):
    RAW = "RAW"
    ENTITY = "ENTITY"
    RESULT = "RESULT"

# ==========================================
# 2. 组件对象 (Component Objects)
# ==========================================

@dataclass
class DataRef:
    """数据指针：指向 MinIO/本地 的 Parquet 文件"""
    key: str                # 标准名 (e.g. 'entity_cycle')
    uri: str                # 物理路径 (e.g. 's3://bucket/b1/cycle.parquet')
    stage: str              # 阶段
    row_count: int          # 行数
    alt_key: Optional[str] = None  # 原始名/别名
    schema_hash: str = ""   # 预留：Schema 校验指纹

@dataclass
class AlertItem:
    """告警信号：用于分发通知"""
    rule_id: str            # 规则ID
    severity: str           # P0/P2
    title: str              # 邮件标题
    content: str            # 邮件正文 (已渲染)
    
    # [扩展] 业务标签，用于未来高级路由
    labels: Dict[str, str] = field(default_factory=dict) 
    
    # [扩展] 原始上下文，用于调试
    trigger_data: Dict[str, Any] = field(default_factory=dict)

@dataclass
class AssetItem:
    """资产信号：用于触发打包"""
    asset_id: str           # 业务主键 (Cycle ID)
    asset_type: str         # 类型 (e.g. 'TWIN_LIFT')
    
    # [核心] 打包所需参数
    vehicle_id: str         
    time_window: Dict[str, str]  # {'start': 'ISO...', 'end': 'ISO...'}
    target_storage_path: str     # DAG A 预生成的存储路径 (s3://.../batch/id/)
    
    # 辅助信息
    tags: List[str] = field(default_factory=list)
    raw_refs_snapshot: Dict[str, str] = field(default_factory=dict) # 溯源

# ==========================================
# 3. 核心上下文容器 (The Bus)
# ==========================================

@dataclass
class GovernanceContext:
    # --- Identity ---
    batch_id: str
    run_date: str
    
    # --- Registry (Data) ---
    # 主索引: { "primary_key": DataRef }
    data_registry: Dict[str, DataRef] = field(default_factory=dict)
    # 辅索引: { "alt_key": "primary_key" }
    _alt_key_index: Dict[str, str] = field(default_factory=dict)
    
    # --- Signals (Result) ---
    alerts: List[AlertItem] = field(default_factory=list)
    assets: List[AssetItem] = field(default_factory=list)
    
    # --- Audit ---
    audit_logs: List[str] = field(default_factory=list)

    # -------------------------------------------------------
    # Logic Methods (只展示核心逻辑)
    # -------------------------------------------------------
    
    def register_data(self, key: str, uri: str, stage: str, count: int, alt_key: str = None):
        """注册数据指针"""
        ref = DataRef(key, uri, stage, count, alt_key)
        self.data_registry[key] = ref
        if alt_key:
            self._alt_key_index[alt_key] = key
        self.audit_logs.append(f"Registered {key} ({count} rows)")

    def get_data_uri(self, key: str, use_alt: bool = False) -> str:
        """获取数据地址"""
        target = self._alt_key_index.get(key, key) if use_alt else key
        if target in self.data_registry:
            return self.data_registry[target].uri
        raise KeyError(f"Data key '{key}' not found.")

    def add_asset(self, asset_id, asset_type, vehicle_id, start_ts, end_ts, tags, target_path):
        """注册资产 (封装了参数构建逻辑)"""
        item = AssetItem(
            asset_id=asset_id,
            asset_type=asset_type,
            vehicle_id=vehicle_id,
            time_window={"start": start_ts, "end": end_ts},
            target_storage_path=target_path,
            tags=tags
        )
        self.assets.append(item)

    # -------------------------------------------------------
    # Serialization (XCom Adapter)
    # -------------------------------------------------------
    
    def to_json(self) -> str:
        """序列化为 JSON 字符串 (存 XCom)"""
        return json.dumps(asdict(self), default=str)

    @classmethod
    def from_json(cls, json_str: str):
        """从 JSON 字符串反序列化"""
        data = json.loads(json_str)
        # 需手动重建 DataRef, AlertItem, AssetItem 对象列表
        # (此处省略具体的重建代码，但在 coding 阶段需实现)
        obj = cls(batch_id=data['batch_id'], run_date=data['run_date'])
        # ... Rebuild logic ...
        return obj
