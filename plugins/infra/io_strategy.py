"""
infra/io_strategy.py
存储抽象层 - 统一本地和 MinIO 的 Parquet 读写接口

设计原则：
- 支持本地文件系统和 MinIO 对象存储
- 自动生成 part-{thread_id}-{uuid}.parquet 文件名
- 支持可配置的压缩算法（默认 zstd）
- 支持目录级清空（重跑时使用）
"""
import os
import uuid
import shutil
from typing import Dict, Any, Optional
import polars as pl


class IOStrategy:
    """
    存储抽象层
    
    职责：
    - 提供统一的 Parquet 读写接口
    - 屏蔽本地和 MinIO 的差异
    - 管理文件命名和压缩策略
    """
    
    # ============================================================
    # 压缩策略配置
    # ============================================================
    
    # 默认压缩算法（分阶段优化）
    DEFAULT_COMPRESSION = {
        "RAW": "zstd",      # 原始数据：平衡速度和压缩率
        "ENTITY": "zstd",   # 实体数据：同上
        "RESULT": "zstd",   # 结果数据：可在环境变量中覆盖为 zstd:5
    }
    
    # ============================================================
    # 核心方法
    # ============================================================
    
    @staticmethod
    def write_parquet(df: pl.DataFrame, uri: str, storage_type: str, 
                     stage: str = "RAW", thread_id: int = 0) -> str:
        """
        写入 Parquet 文件
        
        Args:
            df: Polars DataFrame
            uri: 分区目录路径（以 / 结尾）
            storage_type: "local" 或 "minio"
            stage: 数据阶段（"RAW" / "ENTITY" / "RESULT"）
            thread_id: 线程ID（多线程写入时使用）
        
        Returns:
            完整文件路径
        
        示例：
            write_parquet(df, "file:///data/.../stage=RAW/key=mysql_summary/", "local")
            -> 写入: /data/.../stage=RAW/key=mysql_summary/part-00000-abc123.parquet
        """
        # 1. 获取压缩算法
        compression = IOStrategy._get_compression(stage)
        
        # 2. 生成文件名
        filename = IOStrategy._generate_filename(thread_id)
        
        # 3. 根据存储类型写入
        if storage_type == "local":
            full_path = IOStrategy._write_local(df, uri, filename, compression)
        elif storage_type == "minio":
            full_path = IOStrategy._write_minio(df, uri, filename, compression)
        else:
            raise ValueError(f"Unsupported storage_type: {storage_type}")
        
        return full_path
    
    @staticmethod
    def read_parquet(uri: str, storage_type: str) -> pl.DataFrame:
        """
        读取 Parquet 文件（自动扫描目录下所有文件）
        
        Args:
            uri: 分区目录路径
            storage_type: "local" 或 "minio"
        
        Returns:
            Polars DataFrame
        
        示例：
            read_parquet("file:///data/.../stage=ENTITY/key=entity_cycle/", "local")
            -> 自动读取该目录下所有 .parquet 文件并合并
        """
        if storage_type == "local":
            return IOStrategy._read_local(uri)
        elif storage_type == "minio":
            return IOStrategy._read_minio(uri)
        else:
            raise ValueError(f"Unsupported storage_type: {storage_type}")
    
    @staticmethod
    def clean_directory(uri: str, storage_type: str) -> None:
        """
        清空分区目录（重跑时调用）
        
        Args:
            uri: 分区目录路径
            storage_type: "local" 或 "minio"
        
        注意：
            - 该操作不可逆，请谨慎使用
            - MVP 阶段直接删除，不做备份
        """
        if storage_type == "local":
            IOStrategy._clean_local(uri)
        elif storage_type == "minio":
            IOStrategy._clean_minio(uri)
        else:
            raise ValueError(f"Unsupported storage_type: {storage_type}")
    
    # ============================================================
    # 本地文件系统实现
    # ============================================================
    
    @staticmethod
    def _write_local(df: pl.DataFrame, uri: str, filename: str, 
                    compression: str) -> str:
        """写入本地文件系统"""
        # 移除 file:// 前缀
        local_dir = uri.replace("file://", "")
        
        # 创建目录（如果不存在）
        os.makedirs(local_dir, exist_ok=True)
        
        # 完整文件路径
        full_path = os.path.join(local_dir, filename)
        
        # 写入
        df.write_parquet(full_path, compression=compression)
        
        return full_path
    
    @staticmethod
    def _read_local(uri: str) -> pl.DataFrame:
        """读取本地文件系统"""
        import glob as glob_module
        
        local_dir = uri.replace("file://", "")
        
        # 检查目录是否存在
        if not os.path.exists(local_dir):
            raise FileNotFoundError(f"Directory not found: {local_dir}")
        
        # ⚠️ 关键修复：不使用 Polars 的 glob 模式（会触发 Hive 分区推断）
        # 而是手动列出所有文件，逐个读取后合并
        pattern = os.path.join(local_dir, "*.parquet")
        parquet_files = sorted(glob_module.glob(pattern))
        
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in: {local_dir}")
        
        # 读取所有文件并合并
        if len(parquet_files) == 1:
            # 单文件：直接读取
            df = pl.read_parquet(parquet_files[0])
        else:
            # 多文件：逐个读取后拼接
            dfs = [pl.read_parquet(f) for f in parquet_files]
            df = pl.concat(dfs, how="vertical")
        
        # ✅ 此时数据中不包含 Hive 分区字段（因为我们读取的是单个文件，不是目录）
        # 但为了保险起见，仍然检查并删除（防止文件内部包含这些字段）
        partition_columns = ['batch_id', 'stage', 'key']
        columns_to_drop = [col for col in partition_columns if col in df.columns]
        
        if columns_to_drop:
            df = df.drop(columns_to_drop)
        
        return df
    
    @staticmethod
    def _clean_local(uri: str) -> None:
        """清空本地目录"""
        local_dir = uri.replace("file://", "")
        
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
            # 注意：不重新创建目录，由 write 时创建
    
    # ============================================================
    # MinIO (S3) 实现
    # ============================================================
    
    @staticmethod
    def _write_minio(df: pl.DataFrame, uri: str, filename: str, 
                    compression: str) -> str:
        """写入 MinIO"""
        # 获取 S3 配置
        storage_options = IOStrategy._get_s3_config()
        
        # 完整 S3 路径
        full_path = uri + filename
        
        # Polars 原生支持 S3 写入
        df.write_parquet(
            full_path,
            compression=compression,
            storage_options=storage_options
        )
        
        return full_path
    
    @staticmethod
    def _read_minio(uri: str) -> pl.DataFrame:
        """读取 MinIO"""
        storage_options = IOStrategy._get_s3_config()
        
        # Polars 支持 S3 glob 模式
        pattern = uri + "*.parquet"
        
        # 读取数据
        df = pl.read_parquet(pattern, storage_options=storage_options)
        
        # ⚠️ 移除 Hive 分区字段（与 _read_local 保持一致）
        partition_columns = ['batch_id', 'stage', 'key']
        columns_to_drop = [col for col in partition_columns if col in df.columns]
        
        if columns_to_drop:
            df = df.drop(columns_to_drop)
        
        return df
    
    @staticmethod
    def _clean_minio(uri: str) -> None:
        """清空 MinIO 目录"""
        # 需要使用 boto3 删除对象
        try:
            import boto3
            from botocore.exceptions import ClientError
        except ImportError:
            raise ImportError("boto3 is required for MinIO operations. Install: pip install boto3")
        
        # 解析 S3 URI
        bucket, prefix = IOStrategy._parse_s3_uri(uri)
        
        # 获取配置
        s3_config = IOStrategy._get_s3_config()
        
        # 创建 S3 客户端
        s3_client = boto3.client(
            's3',
            endpoint_url=s3_config.get('endpoint_url'),
            aws_access_key_id=s3_config.get('aws_access_key_id'),
            aws_secret_access_key=s3_config.get('aws_secret_access_key'),
            region_name=s3_config.get('region', 'us-east-1')
        )
        
        # 列出对象
        try:
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' in response:
                # 批量删除
                objects = [{'Key': obj['Key']} for obj in response['Contents']]
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects}
                )
        except ClientError as e:
            # 目录不存在时忽略错误
            if e.response['Error']['Code'] != 'NoSuchKey':
                raise
    
    # ============================================================
    # 工具方法
    # ============================================================
    
    @staticmethod
    def _generate_filename(thread_id: int = 0) -> str:
        """
        生成文件名
        
        格式: part-{thread_id:05d}-{uuid}.parquet
        
        Args:
            thread_id: 线程ID（多线程写入时使用）
        
        Returns:
            文件名字符串
        
        示例:
            _generate_filename(0)    -> "part-00000-a1b2c3d4.parquet"
            _generate_filename(123)  -> "part-00123-e5f6g7h8.parquet"
        """
        # 生成短 UUID（取前8位）
        short_uuid = str(uuid.uuid4()).split('-')[0]
        return f"part-{thread_id:05d}-{short_uuid}.parquet"
    
    @staticmethod
    def _get_compression(stage: str) -> str:
        """
        获取压缩算法（支持环境变量覆盖）
        
        优先级：
        1. 特定阶段的环境变量 (PARQUET_COMPRESSION_RAW)
        2. 全局环境变量 (PARQUET_COMPRESSION)
        3. 默认策略 (DEFAULT_COMPRESSION)
        
        Args:
            stage: 数据阶段
        
        Returns:
            压缩算法名称（如 "zstd", "zstd:5", "snappy"）
        """
        compression = None
        
        # 尝试从 Config 读取（如果已实现）
        try:
            from plugins.infra.config import Config
            
            # 优先级1: 特定阶段
            compression = Config.get(f'PARQUET_COMPRESSION_{stage}', None)
            
            # 优先级2: 全局
            if not compression:
                compression = Config.get('PARQUET_COMPRESSION', None)
        except ImportError:
            # Config 未实现时使用环境变量
            compression = os.getenv(f'PARQUET_COMPRESSION_{stage}')
            if not compression:
                compression = os.getenv('PARQUET_COMPRESSION')
        
        # 优先级3: 默认
        if not compression:
            compression = IOStrategy.DEFAULT_COMPRESSION.get(stage, "zstd")
        
        return compression
    
    @staticmethod
    def _get_s3_config() -> Dict[str, str]:
        """
        获取 S3 配置（统一使用 Config 类）
        
        Returns:
            storage_options 字典，可直接用于 Polars
        """
        try:
            from plugins.infra.config import Config
            return Config.get_minio_config()
        except ImportError:
            # 回退到环境变量（与 Config 保持一致）
            return {
                'endpoint_url': os.getenv('MINIO_GOVERNANCE_ENDPOINT', 'http://minio:9000'),
                'aws_access_key_id': os.getenv('MINIO_GOVERNANCE_ACCESS_KEY', 'minioadmin'),
                'aws_secret_access_key': os.getenv('MINIO_GOVERNANCE_SECRET_KEY', 'minioadmin'),
                'region': os.getenv('MINIO_GOVERNANCE_REGION', 'us-east-1'),
            }
    
    @staticmethod
    def _parse_s3_uri(uri: str) -> tuple:
        """
        解析 S3 URI
        
        Args:
            uri: S3 URI (e.g. "s3://bucket/prefix/path/")
        
        Returns:
            (bucket, prefix) 元组
        
        示例:
            _parse_s3_uri("s3://governance-bucket/batch_id=XXX/stage=RAW/")
            -> ("governance-bucket", "batch_id=XXX/stage=RAW/")
        """
        # 移除 s3:// 前缀
        path = uri.replace("s3://", "")
        
        # 分割 bucket 和 prefix
        parts = path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        
        return bucket, prefix
    
    # ============================================================
    # 便捷方法
    # ============================================================
    
    @staticmethod
    def exists(uri: str, storage_type: str) -> bool:
        """
        检查目录或文件是否存在
        
        Args:
            uri: 路径
            storage_type: 存储类型
        
        Returns:
            是否存在
        """
        if storage_type == "local":
            local_path = uri.replace("file://", "")
            return os.path.exists(local_path)
        elif storage_type == "minio":
            try:
                import boto3
                bucket, prefix = IOStrategy._parse_s3_uri(uri)
                s3_config = IOStrategy._get_s3_config()
                
                s3_client = boto3.client(
                    's3',
                    endpoint_url=s3_config.get('endpoint_url'),
                    aws_access_key_id=s3_config.get('aws_access_key_id'),
                    aws_secret_access_key=s3_config.get('aws_secret_access_key'),
                )
                
                response = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=1
                )
                
                return 'Contents' in response and len(response['Contents']) > 0
            except Exception:
                return False
        else:
            return False
    
    @staticmethod
    def get_file_count(uri: str, storage_type: str) -> int:
        """
        获取目录下的文件数量
        
        Args:
            uri: 目录路径
            storage_type: 存储类型
        
        Returns:
            文件数量
        """
        if storage_type == "local":
            local_dir = uri.replace("file://", "")
            if not os.path.exists(local_dir):
                return 0
            return len([f for f in os.listdir(local_dir) if f.endswith('.parquet')])
        elif storage_type == "minio":
            try:
                import boto3
                bucket, prefix = IOStrategy._parse_s3_uri(uri)
                s3_config = IOStrategy._get_s3_config()
                
                s3_client = boto3.client(
                    's3',
                    endpoint_url=s3_config.get('endpoint_url'),
                    aws_access_key_id=s3_config.get('aws_access_key_id'),
                    aws_secret_access_key=s3_config.get('aws_secret_access_key'),
                )
                
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
                
                if 'Contents' in response:
                    return len([obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')])
                return 0
            except Exception:
                return 0
        else:
            return 0
