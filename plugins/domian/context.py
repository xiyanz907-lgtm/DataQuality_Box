"""
domain/context.py
核心治理上下文定义 - LLD Version 2.0
升级内容：
- 新增 RuleOutputRef（规则结果指针）
- 完善 DataRef（支持 storage_type）
- 实现完整的 DataFrame 读写接口
- 完善序列化/反序列化逻辑
"""
import json
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
from enum import Enum
from datetime import datetime

# ==========================================
# 1. 枚举定义 (Enums) - 统一语言
# ==========================================

class Severity(str, Enum):
    """治理等级"""
    P0 = "P0"       # 阻断/系统级错误
    P1 = "P1"       # 核心资产/业务特征
    P2 = "P2"       # 业务违规/SLA破坏
    INFO = "INFO"   # 信息级

class DataStage(str, Enum):
    """数据阶段"""
    RAW = "RAW"         # 原始快照
    ENTITY = "ENTITY"   # 标准实体
    RESULT = "RESULT"   # 规则结果

class StorageType(str, Enum):
    """存储类型"""
    LOCAL = "local"     # 本地文件系统
    MINIO = "minio"     # MinIO 对象存储

# ==========================================
# 2. 组件对象 (Component Objects)
# ==========================================

@dataclass
class DataRef:
    """数据指针：指向 Parquet 分区目录"""
    key: str                        # 标准键名 (e.g. 'entity_cycle')
    uri: str                        # 分区目录路径 (e.g. 'file://.../stage=ENTITY/key=entity_cycle/')
    stage: str                      # 数据阶段 ("RAW" / "ENTITY" / "RESULT")
    row_count: int                  # 总行数
    storage_type: str               # 存储类型 ("local" / "minio")
    alt_key: Optional[str] = None   # 备用键名 (e.g. 'Cycle')
    schema_hash: str = ""           # 预留：Schema 校验指纹

@dataclass
class RuleOutputRef:
    """规则执行结果的元数据（不存储实际数据）"""
    rule_id: str                    # 规则ID (e.g. 'rule_p0_time_inversion')
    status: str                     # 执行状态 ("SUCCESS" / "FAILED" / "SKIPPED")
    output_uri: str = ""            # 结果文件路径（如有命中）
    hit_count: int = 0              # 命中行数
    execution_time_sec: float = 0.0 # 执行耗时（秒）
    error_message: str = ""         # 失败原因（如果失败）
    timestamp: str = ""             # 执行时间戳

@dataclass
class AlertItem:
    """告警信号：用于分发通知"""
    rule_id: str                    # 触发规则
    severity: str                   # P0 / P2
    title: str                      # 告警标题
    content: str                    # 告警内容（已渲染）
    
    # 扩展字段
    trigger_cycle_ids: List[str] = field(default_factory=list)  # 触发的 cycle_id 列表（用于追溯）
    labels: Dict[str, str] = field(default_factory=dict)        # 业务标签（用于路由）
    created_at: str = ""            # 生成时间

@dataclass
class AssetItem:
    """资产信号：用于触发打包"""
    asset_id: str                   # 业务主键 (Cycle ID)
    asset_type: str                 # 类型 (e.g. 'TWIN_LIFT')
    
    # 核心打包参数
    vehicle_id: str                 # 车辆号
    time_window: Dict[str, str]     # 时间窗口 {'start': 'ISO...', 'end': 'ISO...'}
    target_storage_path: str        # 打包目标路径
    
    # 辅助信息
    tags: List[str] = field(default_factory=list)                   # 标签（用于白名单豁免）
    raw_refs_snapshot: Dict[str, str] = field(default_factory=dict) # 数据溯源快照

# ==========================================
# 3. 核心上下文容器 (The Bus)
# ==========================================

@dataclass
class GovernanceContext:
    """
    治理上下文总线
    
    职责：
    - 管理数据指针（DataRef）
    - 管理规则结果指针（RuleOutputRef）
    - 收集告警和资产信号
    - 提供 DataFrame 读写接口
    - 支持序列化传递（XCom）
    """
    
    # ============ 身份标识 ============
    batch_id: str                   # 批次ID (e.g. 'BATCH_20260123_001')
    run_date: str                   # 运行日期 (e.g. '2026-01-23')
    storage_type: str = "local"     # 全局存储类型（初始化时设置）
    
    # ============ 数据注册表 ============
    data_registry: Dict[str, DataRef] = field(default_factory=dict)     # 主索引: key -> DataRef
    _alt_key_index: Dict[str, str] = field(default_factory=dict)        # 辅索引: alt_key -> key
    
    # ============ 规则结果注册表 ============
    rule_outputs: Dict[str, RuleOutputRef] = field(default_factory=dict)  # rule_id -> RuleOutputRef
    
    # ============ 信号对象 ============
    alerts: List[AlertItem] = field(default_factory=list)   # 告警列表
    assets: List[AssetItem] = field(default_factory=list)   # 资产列表
    
    # ============ 审计日志 ============
    audit_logs: List[str] = field(default_factory=list)

    # ========================================================================
    # 数据管理方法
    # ========================================================================
    
    def register_data(self, key: str, uri: str, stage: str, count: int, 
                     storage_type: str, alt_key: Optional[str] = None) -> None:
        """
        注册数据指针
        
        Args:
            key: 标准键名
            uri: 分区目录路径
            stage: 数据阶段
            count: 总行数
            storage_type: 存储类型
            alt_key: 备用键名（可选）
        """
        ref = DataRef(
            key=key,
            uri=uri,
            stage=stage,
            row_count=count,
            storage_type=storage_type,
            alt_key=alt_key
        )
        self.data_registry[key] = ref
        
        # 注册别名映射
        if alt_key:
            self._alt_key_index[alt_key] = key
        
        self.log(f"Registered data: {key} ({count} rows, stage={stage})")

    def put_dataframe(self, key: str, df, stage: str, 
                     alt_key: Optional[str] = None) -> None:
        """
        写入 DataFrame 到 Parquet 并自动注册
        
        Args:
            key: 标准键名
            df: Polars DataFrame
            stage: 数据阶段
            alt_key: 备用键名（可选）
        
        流程：
            1. 构建分区路径
            2. 调用 IOStrategy 写入
            3. 注册到 data_registry
        """
        # 延迟导入（避免循环依赖）
        from plugins.infra.io_strategy import IOStrategy
        
        # 1. 构建分区路径
        uri = self._build_partition_path(stage, key)
        
        # 2. 写入 Parquet
        IOStrategy.write_parquet(df, uri, self.storage_type)
        
        # 3. 注册
        self.register_data(key, uri, stage, df.height, self.storage_type, alt_key)
    
    def get_dataframe(self, key: str, use_alt_key: bool = False):
        """
        读取 DataFrame（支持别名）
        
        Args:
            key: 键名（标准键或别名）
            use_alt_key: 是否启用别名查找
        
        Returns:
            Polars DataFrame
        """
        # 延迟导入
        from plugins.infra.io_strategy import IOStrategy
        
        # 1. 解析键名
        resolved_key = self._resolve_key(key, use_alt_key)
        
        # 2. 获取引用
        if resolved_key not in self.data_registry:
            raise KeyError(f"Data key '{key}' not found in registry. Available keys: {list(self.data_registry.keys())}")
        
        ref = self.data_registry[resolved_key]
        
        # 3. 读取
        return IOStrategy.read_parquet(ref.uri, ref.storage_type)
    
    def get_data_uri(self, key: str, use_alt_key: bool = False) -> str:
        """
        获取数据 URI
        
        Args:
            key: 键名
            use_alt_key: 是否启用别名查找
        
        Returns:
            URI 字符串
        """
        resolved_key = self._resolve_key(key, use_alt_key)
        if resolved_key not in self.data_registry:
            raise KeyError(f"Data key '{key}' not found.")
        return self.data_registry[resolved_key].uri
    
    # ========================================================================
    # 规则结果管理方法
    # ========================================================================
    
    def register_rule_output(self, rule_id: str, status: str, output_uri: str = "",
                            hit_count: int = 0, execution_time: float = 0.0,
                            error_message: str = "") -> None:
        """
        注册规则执行结果
        
        Args:
            rule_id: 规则ID
            status: 执行状态 ("SUCCESS" / "FAILED" / "SKIPPED")
            output_uri: 结果文件路径
            hit_count: 命中行数
            execution_time: 执行耗时（秒）
            error_message: 失败原因
        """
        ref = RuleOutputRef(
            rule_id=rule_id,
            status=status,
            output_uri=output_uri,
            hit_count=hit_count,
            execution_time_sec=execution_time,
            error_message=error_message,
            timestamp=datetime.utcnow().isoformat()
        )
        self.rule_outputs[rule_id] = ref
        
        self.log(f"Rule {rule_id}: {status} (hits={hit_count}, time={execution_time:.2f}s)")
    
    def get_rule_output_uri(self, rule_id: str) -> str:
        """
        获取规则结果的 URI
        
        Args:
            rule_id: 规则ID
        
        Returns:
            结果文件 URI
        """
        if rule_id not in self.rule_outputs:
            raise KeyError(f"Rule '{rule_id}' output not found.")
        return self.rule_outputs[rule_id].output_uri
    
    # ========================================================================
    # 信号管理方法
    # ========================================================================
    
    def add_alert(self, rule_id: str, severity: str, title: str, content: str,
                 trigger_cycle_ids: Optional[List[str]] = None, **kwargs) -> None:
        """
        添加告警
        
        Args:
            rule_id: 触发规则
            severity: 严重等级
            title: 告警标题
            content: 告警内容
            trigger_cycle_ids: 触发的 cycle_id 列表
            **kwargs: 其他扩展字段
        """
        alert = AlertItem(
            rule_id=rule_id,
            severity=severity,
            title=title,
            content=content,
            trigger_cycle_ids=trigger_cycle_ids or [],
            labels=kwargs.get('labels', {}),
            created_at=datetime.utcnow().isoformat()
        )
        self.alerts.append(alert)
        self.log(f"Added alert: {rule_id} ({severity})")
    
    def add_asset(self, asset_id: str, asset_type: str, vehicle_id: str,
                 start_ts: str, end_ts: str, tags: List[str], 
                 target_path: str) -> None:
        """
        添加资产
        
        Args:
            asset_id: 资产ID（通常是 cycle_id）
            asset_type: 资产类型
            vehicle_id: 车辆号
            start_ts: 开始时间
            end_ts: 结束时间
            tags: 标签列表
            target_path: 打包目标路径
        """
        asset = AssetItem(
            asset_id=asset_id,
            asset_type=asset_type,
            vehicle_id=vehicle_id,
            time_window={"start": start_ts, "end": end_ts},
            target_storage_path=target_path,
            tags=tags
        )
        self.assets.append(asset)
        self.log(f"Added asset: {asset_id} ({asset_type})")

    # ========================================================================
    # 审计方法
    # ========================================================================
    
    def log(self, message: str) -> None:
        """
        记录审计日志
        
        Args:
            message: 日志消息
        """
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.audit_logs.append(f"[{timestamp}] {message}")
    
    # ========================================================================
    # 序列化方法
    # ========================================================================
    
    def to_json(self) -> str:
        """
        序列化为 JSON 字符串（用于 XCom 传递）
        
        Returns:
            JSON 字符串
        """
        return json.dumps(asdict(self), default=str, ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> 'GovernanceContext':
        """
        从 JSON 字符串反序列化
        
        Args:
            json_str: JSON 字符串
        
        Returns:
            GovernanceContext 对象
        """
        data = json.loads(json_str)
        
        # 重建基础对象
        ctx = cls(
            batch_id=data['batch_id'],
            run_date=data['run_date'],
            storage_type=data.get('storage_type', 'local')
        )
        
        # 重建 DataRef 对象
        for key, ref_dict in data.get('data_registry', {}).items():
            ctx.data_registry[key] = DataRef(**ref_dict)
        
        # 重建 alt_key 索引
        ctx._alt_key_index = data.get('_alt_key_index', {})
        
        # 重建 RuleOutputRef 对象
        for rule_id, ref_dict in data.get('rule_outputs', {}).items():
            ctx.rule_outputs[rule_id] = RuleOutputRef(**ref_dict)
        
        # 重建 AlertItem 对象
        for alert_dict in data.get('alerts', []):
            ctx.alerts.append(AlertItem(**alert_dict))
        
        # 重建 AssetItem 对象
        for asset_dict in data.get('assets', []):
            ctx.assets.append(AssetItem(**asset_dict))
        
        # 审计日志
        ctx.audit_logs = data.get('audit_logs', [])
        
        return ctx
    
    # ========================================================================
    # 内部工具方法
    # ========================================================================
    
    def _resolve_key(self, key: str, use_alt_key: bool) -> str:
        """
        解析键名（支持别名查找）
        
        Args:
            key: 输入键名
            use_alt_key: 是否启用别名查找
        
        Returns:
            标准键名
        """
        if use_alt_key and key in self._alt_key_index:
            return self._alt_key_index[key]
        return key
    
    def _get_base_path(self) -> str:
        """
        获取基础路径（根据 storage_type）
        
        Returns:
            基础路径字符串
        """
        if self.storage_type == "minio":
            # 从配置读取 bucket 名称
            try:
                from plugins.infra.config import Config
                bucket = Config.get('MINIO_GOVERNANCE_BUCKET', 'governance-bucket')
            except ImportError:
                # 如果 Config 还未实现，使用默认值
                bucket = 'governance-bucket'
            return f"s3://{bucket}"
        else:
            # 使用已挂载的 /opt/airflow/data 目录（对应宿主机 ../data）
            return "file:///opt/airflow/data/governance"
    
    def _build_partition_path(self, stage: str, key: str) -> str:
        """
        构建 Hive 风格分区路径
        
        Args:
            stage: 数据阶段
            key: 键名
        
        Returns:
            完整分区路径
            
        示例：
            - local: file:///data/governance/batch_id=BATCH_20260123_001/stage=ENTITY/key=entity_cycle/
            - minio: s3://governance-bucket/batch_id=BATCH_20260123_001/stage=ENTITY/key=entity_cycle/
        """
        base = self._get_base_path()
        return f"{base}/batch_id={self.batch_id}/stage={stage}/key={key}/"
    
    # ========================================================================
    # 便捷方法
    # ========================================================================
    
    def summary(self) -> Dict[str, Any]:
        """
        生成上下文摘要（用于日志和调试）
        
        Returns:
            摘要字典
        """
        return {
            "batch_id": self.batch_id,
            "run_date": self.run_date,
            "storage_type": self.storage_type,
            "data_count": len(self.data_registry),
            "rule_count": len(self.rule_outputs),
            "alert_count": len(self.alerts),
            "asset_count": len(self.assets),
            "total_rows": sum(ref.row_count for ref in self.data_registry.values()),
        }
