-- ============================================================
-- 资产打包队列表
-- 用于 DAG A 和 DAG B 之间的异步解耦
-- 支持异步打包接口 + 僵尸任务处理
-- ============================================================

CREATE TABLE IF NOT EXISTS governance_asset_packing_queue (
    -- ============================================================
    -- 主键与业务标识
    -- ============================================================
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    batch_id VARCHAR(100) NOT NULL COMMENT 'DAG A 批次ID',
    asset_id VARCHAR(100) NOT NULL COMMENT '资产ID（对应 meta 表的 id）',
    rule_id VARCHAR(100) NOT NULL COMMENT '规则ID（如 rule_p1_twin_lift）',
    
    -- ============================================================
    -- 打包所需信息
    -- ============================================================
    vehicle_id VARCHAR(50) NOT NULL COMMENT '车辆ID',
    start_time DATETIME NOT NULL COMMENT '时间窗口起始（ISO 8601格式）',
    end_time DATETIME NOT NULL COMMENT '时间窗口结束',
    base_path VARCHAR(500) NOT NULL COMMENT '自定义存储路径前缀',
    
    -- ============================================================
    -- 异步打包状态管理 ⭐
    -- ============================================================
    status ENUM('PENDING', 'PROCESSING', 'POLLING', 'SUCCESS', 'FAILED', 'ABANDONED') 
        DEFAULT 'PENDING' 
        COMMENT '状态：
            PENDING-待处理
            PROCESSING-打包中（已调用打包接口）
            POLLING-轮询中（等待打包完成）
            SUCCESS-成功
            FAILED-失败（可重试）
            ABANDONED-已放弃（超过重试次数）',
    
    -- ============================================================
    -- 异步任务跟踪 ⭐
    -- ============================================================
    pack_key VARCHAR(200) COMMENT '打包任务Key（异步接口返回的唯一标识）',
    pack_url VARCHAR(500) COMMENT '打包文件URL（打包完成后的下载地址）',
    poll_count INT DEFAULT 0 COMMENT '轮询次数（防止无限轮询）',
    
    -- ============================================================
    -- 错误处理 ⭐
    -- ============================================================
    error_message TEXT COMMENT '错误信息',
    retry_count INT DEFAULT 0 COMMENT '重试次数（失败后自动重试，超过3次则ABANDONED）',
    
    -- ============================================================
    -- 时间戳（用于僵尸任务检测）⭐
    -- ============================================================
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP 
        COMMENT '更新时间（僵尸任务检测依据）',
    pack_started_at DATETIME COMMENT '打包开始时间',
    processed_at DATETIME COMMENT '处理完成时间',
    
    -- ============================================================
    -- 索引设计
    -- ============================================================
    UNIQUE KEY uk_asset (batch_id, asset_id) COMMENT '防止重复写入',
    INDEX idx_status_created (status, created_at) COMMENT '获取待处理任务',
    INDEX idx_status_updated (status, updated_at) COMMENT '僵尸任务检测',
    INDEX idx_status_polling (status, poll_count) COMMENT '轮询任务查询',
    INDEX idx_batch_id (batch_id) COMMENT '按批次查询'
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
COMMENT='资产打包队列（支持异步接口 + 僵尸任务处理）';

-- ============================================================
-- 使用说明
-- ============================================================
-- 1. 状态机流转：
--    PENDING → PROCESSING → POLLING → SUCCESS
--                         ↓          ↓
--                       FAILED ← ABANDONED
--
-- 2. 僵尸任务检测：
--    如果 status IN ('PROCESSING', 'POLLING') 且 updated_at < NOW() - INTERVAL 2 HOUR
--    则视为僵尸任务，重置为 PENDING
--
-- 3. 重试策略：
--    retry_count < 3: 重置为 PENDING 继续尝试
--    retry_count >= 3: 标记为 ABANDONED，不再重试
--
-- 4. 轮询配置：
--    poll_count 用于跟踪轮询次数，超过 60 次标记为 FAILED
