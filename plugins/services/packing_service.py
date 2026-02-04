"""
资产打包服务客户端
支持异步接口调用 + 轮询机制

Author: Data Governance Team
Date: 2026-02-02
"""
import requests
import time
import os
from typing import Dict, Optional, Tuple
from datetime import datetime


class PackingServiceClient:
    """
    打包服务客户端（异步接口）
    
    工作流程：
    1. start_packing() 提交打包任务，获取 pack_key
    2. query_packing_status() 轮询查询打包状态
    3. wait_for_completion() 阻塞式等待打包完成
    """
    
    # ============================================================
    # 接口配置（从环境变量读取）
    # ============================================================
    BASE_URL = os.getenv('PACKING_SERVICE_BASE_URL', 'https://mock.apipost.net/mock/34a21a')
    PACK_ENDPOINT = os.getenv('PACKING_SERVICE_PACK_ENDPOINT', '/api/launcher/queryInfluxData')
    QUERY_ENDPOINT = os.getenv('PACKING_SERVICE_QUERY_ENDPOINT', '/api/launcher/querySyncCacheResult')
    AUTH_TOKEN = os.getenv('PACKING_SERVICE_AUTH_TOKEN', 
                          'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MSwidXNlcm5hbWUiOiJ3YW5nZGVmYSIsImV4cCI6MTc2MjUwODE5Nywic3ViIjoiQUNDRVNTIn0.W0b7YmmokSPw1GYb1hQb2AxdHjtKFPsIDaQeUOxPg2w')
    
    # ============================================================
    # 重试配置
    # ============================================================
    MAX_RETRIES = int(os.getenv('PACKING_SERVICE_MAX_RETRIES', '3'))
    RETRY_INTERVAL = int(os.getenv('PACKING_SERVICE_RETRY_INTERVAL', '10'))  # seconds
    REQUEST_TIMEOUT = int(os.getenv('PACKING_SERVICE_TIMEOUT', '300'))  # seconds
    
    # ============================================================
    # 轮询配置
    # ============================================================
    MAX_POLL_COUNT = int(os.getenv('PACKING_SERVICE_MAX_POLL_COUNT', '60'))  # 最多轮询60次
    POLL_INTERVAL = int(os.getenv('PACKING_SERVICE_POLL_INTERVAL', '10'))  # 每10秒轮询一次
    
    def __init__(self, logger=None):
        """
        初始化打包服务客户端
        
        Args:
            logger: Airflow Logger 实例
        """
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': self.AUTH_TOKEN,
            'Content-Type': 'application/json'
        })
    
    def start_packing(
        self, 
        vehicle_id: str, 
        start_time: datetime, 
        end_time: datetime, 
        base_path: str
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        启动打包任务（异步）
        
        Args:
            vehicle_id: 车辆ID
            start_time: 开始时间（datetime 对象）
            end_time: 结束时间（datetime 对象）
            base_path: 存储路径前缀
        
        Returns:
            Tuple[成功标志, pack_key, 错误信息]
            
        Example:
            success, key, error = client.start_packing(
                vehicle_id='V001',
                start_time=datetime(2026, 1, 1, 10, 0, 0),
                end_time=datetime(2026, 1, 1, 12, 0, 0),
                base_path='/data/assets/twin_lift/'
            )
        """
        url = f"{self.BASE_URL}{self.PACK_ENDPOINT}"
        
        # 格式化时间为 ISO 8601
        payload = {
            "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "vehicleId": vehicle_id,
            "basePath": base_path
        }
        
        # 重试逻辑
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                if self.logger:
                    self.logger.info(f"📦 Calling packing service (attempt {attempt}/{self.MAX_RETRIES})")
                    self.logger.info(f"   Payload: {payload}")
                
                response = self.session.post(
                    url,
                    json=payload,
                    timeout=self.REQUEST_TIMEOUT
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if result.get('code') == 0:
                        pack_key = result.get('data')
                        if self.logger:
                            self.logger.info(f"✅ Packing started, key: {pack_key}")
                        return True, pack_key, None
                    else:
                        error_msg = result.get('msg', 'Unknown error')
                        if self.logger:
                            self.logger.error(f"❌ Packing service error: {error_msg}")
                        return False, None, error_msg
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    if self.logger:
                        self.logger.warning(f"⚠️ Attempt {attempt} failed: {error_msg}")
                    
                    # 非最后一次尝试，等待后重试
                    if attempt < self.MAX_RETRIES:
                        time.sleep(self.RETRY_INTERVAL)
                        continue
                    
                    return False, None, error_msg
                    
            except requests.Timeout:
                error_msg = f"Request timeout after {self.REQUEST_TIMEOUT}s"
                if self.logger:
                    self.logger.error(f"⏱️ {error_msg}")
                
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_INTERVAL)
                    continue
                
                return False, None, error_msg
                
            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                if self.logger:
                    self.logger.error(f"💥 {error_msg}")
                
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_INTERVAL)
                    continue
                
                return False, None, error_msg
        
        return False, None, "Max retries exceeded"
    
    def query_packing_status(self, pack_key: str) -> Tuple[bool, bool, Optional[str]]:
        """
        查询打包状态（轮询）
        
        Args:
            pack_key: 打包任务Key
        
        Returns:
            Tuple[查询成功, 打包完成, 错误信息]
            
        Example:
            query_ok, is_done, error = client.query_packing_status('abc-123')
            if query_ok and is_done:
                print("打包完成！")
        """
        url = f"{self.BASE_URL}{self.QUERY_ENDPOINT}"
        
        try:
            response = self.session.get(
                url,
                params={'key': pack_key},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                
                if result.get('code') == 0:
                    is_complete = result.get('data', False)
                    return True, is_complete, None
                else:
                    error_msg = result.get('msg', 'Unknown error')
                    return False, False, error_msg
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                return False, False, error_msg
                
        except Exception as e:
            error_msg = f"Query error: {str(e)}"
            if self.logger:
                self.logger.error(f"❌ {error_msg}")
            return False, False, error_msg
    
    def wait_for_completion(
        self, 
        pack_key: str, 
        max_polls: int = None
    ) -> Tuple[bool, Optional[str]]:
        """
        等待打包完成（阻塞式轮询）
        
        Args:
            pack_key: 打包任务Key
            max_polls: 最大轮询次数（None=使用默认值60）
        
        Returns:
            Tuple[是否完成, 错误信息]
            
        Example:
            success, error = client.wait_for_completion('abc-123')
            if not success:
                print(f"打包失败: {error}")
        """
        max_polls = max_polls or self.MAX_POLL_COUNT
        
        for poll_num in range(1, max_polls + 1):
            if self.logger:
                self.logger.info(f"🔍 Polling packing status ({poll_num}/{max_polls})")
            
            query_success, is_complete, error = self.query_packing_status(pack_key)
            
            if not query_success:
                if self.logger:
                    self.logger.error(f"❌ Query failed: {error}")
                return False, error
            
            if is_complete:
                if self.logger:
                    self.logger.info(f"✅ Packing completed after {poll_num} polls")
                return True, None
            
            # 未完成，等待后继续轮询
            if poll_num < max_polls:
                time.sleep(self.POLL_INTERVAL)
        
        # 超过最大轮询次数
        error_msg = f"Packing timeout: exceeded {max_polls} polls"
        if self.logger:
            self.logger.error(f"⏱️ {error_msg}")
        return False, error_msg


# ============================================================
# 工厂函数（方便外部调用）
# ============================================================
def create_packing_client(logger=None) -> PackingServiceClient:
    """
    创建打包服务客户端实例
    
    Args:
        logger: Airflow Logger
    
    Returns:
        PackingServiceClient 实例
    """
    return PackingServiceClient(logger=logger)
