"""
infra/config.py
全局配置管理器

职责：
- 统一管理环境变量和配置
- 提供类型安全的配置访问接口
- 支持默认值和配置验证
"""
import os
from typing import Any, Optional, Dict


class Config:
    """
    全局配置管理器
    
    设计原则：
    - 所有配置优先从环境变量读取
    - 支持默认值回退
    - 提供类型转换（str, int, bool）
    - 缓存配置（避免重复读取）
    """
    
    # 配置缓存
    _cache: Dict[str, Any] = {}
    
    # ============================================================
    # 核心方法
    # ============================================================
    
    @staticmethod
    def get(key: str, default: Any = None) -> Any:
        """
        获取配置值（优先从环境变量，支持缓存）
        
        Args:
            key: 配置键名
            default: 默认值
        
        Returns:
            配置值
        
        示例:
            Config.get('MINIO_GOVERNANCE_ENDPOINT', 'http://minio:9000')
        """
        # 检查缓存
        if key in Config._cache:
            return Config._cache[key]
        
        # 从环境变量读取
        value = os.getenv(key, default)
        
        # 缓存
        Config._cache[key] = value
        
        return value
    
    @staticmethod
    def get_int(key: str, default: int = 0) -> int:
        """
        获取整数配置
        
        Args:
            key: 配置键名
            default: 默认值
        
        Returns:
            整数值
        """
        value = Config.get(key, str(default))
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def get_bool(key: str, default: bool = False) -> bool:
        """
        获取布尔配置
        
        支持的真值: "true", "1", "yes", "on"
        支持的假值: "false", "0", "no", "off"
        
        Args:
            key: 配置键名
            default: 默认值
        
        Returns:
            布尔值
        """
        value = Config.get(key, str(default))
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        
        return default
    
    @staticmethod
    def get_float(key: str, default: float = 0.0) -> float:
        """
        获取浮点数配置
        
        Args:
            key: 配置键名
            default: 默认值
        
        Returns:
            浮点数值
        """
        value = Config.get(key, str(default))
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def set(key: str, value: Any) -> None:
        """
        设置配置值（运行时修改，仅影响缓存）
        
        Args:
            key: 配置键名
            value: 配置值
        
        注意: 该方法仅修改缓存，不修改环境变量
        """
        Config._cache[key] = value
    
    @staticmethod
    def clear_cache() -> None:
        """清空配置缓存"""
        Config._cache.clear()
    
    @staticmethod
    def reload(key: str) -> Any:
        """
        重新加载配置（清除缓存后读取）
        
        Args:
            key: 配置键名
        
        Returns:
            最新配置值
        """
        if key in Config._cache:
            del Config._cache[key]
        return Config.get(key)
    
    # ============================================================
    # 预定义配置键（提供类型提示和文档）
    # ============================================================
    
    @staticmethod
    def get_storage_type() -> str:
        """
        获取全局存储类型
        
        Returns:
            "local" 或 "minio"
        """
        return Config.get('GOVERNANCE_STORAGE_TYPE', 'local')
    
    @staticmethod
    def get_minio_config() -> Dict[str, str]:
        """
        获取 MinIO 配置（返回 Polars S3 storage_options 格式）
        
        Returns:
            配置字典，可直接用于 Polars 的 storage_options
        """
        return {
            'endpoint_url': Config.get('MINIO_GOVERNANCE_ENDPOINT', 'http://minio:9000'),
            'aws_access_key_id': Config.get('MINIO_GOVERNANCE_ACCESS_KEY', 'minioadmin'),
            'aws_secret_access_key': Config.get('MINIO_GOVERNANCE_SECRET_KEY', 'minioadmin'),
            'region': Config.get('MINIO_GOVERNANCE_REGION', 'us-east-1'),
        }
    
    @staticmethod
    def get_minio_bucket() -> str:
        """
        获取 MinIO Bucket 名称
        
        Returns:
            Bucket 名称
        """
        return Config.get('MINIO_GOVERNANCE_BUCKET', 'governance-data')
    
    @staticmethod
    def get_compression_config() -> Dict[str, str]:
        """
        获取压缩配置
        
        Returns:
            分阶段的压缩算法配置
        """
        return {
            'global': Config.get('PARQUET_COMPRESSION', 'zstd'),
            'raw': Config.get('PARQUET_COMPRESSION_RAW', None),
            'entity': Config.get('PARQUET_COMPRESSION_ENTITY', None),
            'result': Config.get('PARQUET_COMPRESSION_RESULT', None),
        }
    
    # ============================================================
    # 配置验证
    # ============================================================
    
    @staticmethod
    def validate_required(keys: list) -> None:
        """
        验证必需的配置是否存在
        
        Args:
            keys: 必需的配置键列表
        
        Raises:
            ValueError: 如果配置缺失
        """
        missing = []
        for key in keys:
            if Config.get(key) is None:
                missing.append(key)
        
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
    
    @staticmethod
    def get_all() -> Dict[str, Any]:
        """
        获取所有已加载的配置（用于调试）
        
        Returns:
            配置字典
        """
        return Config._cache.copy()
    
    @staticmethod
    def summary() -> str:
        """
        生成配置摘要（用于日志）
        
        Returns:
            配置摘要字符串
        """
        lines = ["Configuration Summary:"]
        lines.append(f"  Storage Type: {Config.get_storage_type()}")
        
        if Config.get_storage_type() == 'minio':
            minio_cfg = Config.get_minio_config()
            lines.append(f"  MinIO Endpoint: {minio_cfg['endpoint']}")
            lines.append(f"  MinIO Bucket: {minio_cfg['bucket']}")
        
        compression_cfg = Config.get_compression_config()
        lines.append(f"  Compression (Global): {compression_cfg['global']}")
        
        return "\n".join(lines)


# ============================================================
# 便捷函数（向后兼容）
# ============================================================

def get_config(key: str, default: Any = None) -> Any:
    """便捷函数：获取配置"""
    return Config.get(key, default)


def get_storage_type() -> str:
    """便捷函数：获取存储类型"""
    return Config.get_storage_type()


def get_minio_config() -> Dict[str, str]:
    """便捷函数：获取 MinIO 配置"""
    return Config.get_minio_config()
