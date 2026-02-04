-- ============================================================
-- Auto Test Case Catalog V3 升级
-- 单表方案：合并打包队列功能到元数据表
-- 升级日期：2026-02-02
-- ============================================================

-- ============================================================
-- Step 1: 新增打包队列管理字段
-- ============================================================

-- 添加 rule_id 字段（记录触发的规则ID）
ALTER TABLE auto_test_case_catalog 
ADD COLUMN triggered_rule_id VARCHAR(100) DEFAULT NULL 
COMMENT '触发规则ID（如 rule_p1_twin_lift，用于追溯资产来源）' 
AFTER replay_status;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN pack_key VARCHAR(200) DEFAULT NULL 
COMMENT '异步打包任务Key（打包服务返回的唯一标识）' 
AFTER triggered_rule_id;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN pack_url VARCHAR(500) DEFAULT NULL 
COMMENT '打包文件URL（打包完成后的下载地址）' 
AFTER pack_key;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN pack_base_path VARCHAR(500) DEFAULT NULL 
COMMENT '打包存储路径前缀' 
AFTER pack_url;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN pack_poll_count INT DEFAULT 0 
COMMENT '轮询次数（防止无限轮询）' 
AFTER pack_base_path;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN pack_retry_count INT DEFAULT 0 
COMMENT '打包重试次数（超过3次则ABANDONED）' 
AFTER pack_poll_count;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN pack_error_message TEXT DEFAULT NULL 
COMMENT '打包错误信息' 
AFTER pack_retry_count;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN pack_started_at DATETIME DEFAULT NULL 
COMMENT '打包开始时间' 
AFTER pack_error_message;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN pack_completed_at DATETIME DEFAULT NULL 
COMMENT '打包完成时间' 
AFTER pack_started_at;

ALTER TABLE auto_test_case_catalog 
ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP 
COMMENT '更新时间（用于僵尸任务检测）' 
AFTER pack_completed_at;

-- ============================================================
-- Step 2: 扩展 process_status 字段枚举值
-- ============================================================

-- 注意：MySQL 的 ENUM 类型不支持直接 ALTER，需要使用 MODIFY
ALTER TABLE auto_test_case_catalog 
MODIFY COLUMN process_status VARCHAR(20) NOT NULL DEFAULT 'IDENTIFIED' 
COMMENT '处理状态：
    IDENTIFIED - 已识别（DAG A 写入初始状态）
    PENDING - 待打包（等待 DAG B 处理）
    PROCESSING - 打包中（DAG B 正在处理）
    POLLING - 轮询中（等待异步打包完成）
    PACKAGED - 已打包（打包完成，原有状态）
    BENCHMARKED - 已基准测试（后续流程，原有状态）
    ABANDONED - 已放弃（打包重试次数超限）';

-- ============================================================
-- Step 3: 新增索引（打包队列查询优化）
-- ============================================================

-- 打包队列主索引：获取待处理任务 + 僵尸任务检测
ALTER TABLE auto_test_case_catalog 
ADD INDEX idx_pack_queue (process_status, updated_at, pack_retry_count) 
COMMENT '打包队列查询（PENDING任务 + 僵尸任务检测）';

-- 打包状态轮询索引
ALTER TABLE auto_test_case_catalog 
ADD INDEX idx_pack_polling (process_status, pack_poll_count) 
COMMENT '轮询任务查询（POLLING状态任务）';

-- ============================================================
-- Step 4: 数据迁移（如果存在旧队列表数据）
-- ============================================================

-- 如果之前已经在 governance_asset_packing_queue 表中有数据，需要迁移
-- 注意：这是可选步骤，仅在需要保留队列表历史数据时执行

-- 示例迁移脚本（根据实际情况调整）:
-- UPDATE auto_test_case_catalog atc
-- INNER JOIN governance_asset_packing_queue gapq 
--     ON atc.id = gapq.asset_id
-- SET 
--     atc.process_status = CASE 
--         WHEN gapq.status = 'SUCCESS' THEN 'PACKAGED'
--         WHEN gapq.status = 'ABANDONED' THEN 'ABANDONED'
--         WHEN gapq.status = 'PENDING' THEN 'PENDING'
--         WHEN gapq.status = 'PROCESSING' THEN 'PROCESSING'
--         WHEN gapq.status = 'POLLING' THEN 'POLLING'
--         ELSE atc.process_status
--     END,
--     atc.pack_key = gapq.pack_key,
--     atc.pack_url = gapq.pack_url,
--     atc.pack_base_path = gapq.base_path,
--     atc.pack_poll_count = gapq.poll_count,
--     atc.pack_retry_count = gapq.retry_count,
--     atc.pack_error_message = gapq.error_message,
--     atc.pack_started_at = gapq.pack_started_at,
--     atc.pack_completed_at = gapq.processed_at,
--     atc.updated_at = gapq.updated_at;

-- ============================================================
-- Step 5: 验证升级结果
-- ============================================================

-- 查看表结构
DESC auto_test_case_catalog;

-- 查看索引
SHOW INDEX FROM auto_test_case_catalog;

-- 统计各状态的记录数
SELECT 
    process_status,
    COUNT(*) AS count
FROM auto_test_case_catalog
GROUP BY process_status;

-- ============================================================
-- 升级说明
-- ============================================================

-- 1. 向后兼容：
--    - 所有新增字段都允许 NULL 或有默认值
--    - 原有业务逻辑不受影响
--    - 已有记录的 process_status 保持不变（IDENTIFIED/PACKAGED/BENCHMARKED）

-- 2. 状态迁移：
--    - IDENTIFIED 记录：保持不变，等待 DAG A 标记为 PENDING
--    - PACKAGED 记录：保持不变，打包已完成
--    - BENCHMARKED 记录：保持不变，已完成所有流程

-- 3. 新数据流程：
--    - DAG A 识别 P1 资产 → 写入 IDENTIFIED → 更新为 PENDING
--    - DAG B 处理队列 → PENDING → PROCESSING → POLLING → PACKAGED
--    - 后续流程 → PACKAGED → BENCHMARKED

-- 4. 性能影响：
--    - 新增字段：+9 个字段（约 1-2KB per row）
--    - 新增索引：+2 个索引（轻微影响写入性能）
--    - 查询优化：单表查询，性能提升

-- 5. 回滚方案（如需要）：
--    ALTER TABLE auto_test_case_catalog DROP COLUMN pack_key;
--    ALTER TABLE auto_test_case_catalog DROP COLUMN pack_url;
--    ALTER TABLE auto_test_case_catalog DROP COLUMN pack_base_path;
--    ALTER TABLE auto_test_case_catalog DROP COLUMN pack_poll_count;
--    ALTER TABLE auto_test_case_catalog DROP COLUMN pack_retry_count;
--    ALTER TABLE auto_test_case_catalog DROP COLUMN pack_error_message;
--    ALTER TABLE auto_test_case_catalog DROP COLUMN pack_started_at;
--    ALTER TABLE auto_test_case_catalog DROP COLUMN pack_completed_at;
--    ALTER TABLE auto_test_case_catalog DROP COLUMN updated_at;
--    ALTER TABLE auto_test_case_catalog DROP INDEX idx_pack_queue;
--    ALTER TABLE auto_test_case_catalog DROP INDEX idx_pack_polling;

-- ============================================================
-- 升级清单
-- ============================================================

-- [✅] 新增 9 个打包管理字段
-- [✅] 扩展 process_status 枚举值
-- [✅] 新增 2 个打包队列索引
-- [✅] 保留所有原有字段和索引
-- [✅] 提供数据迁移脚本（可选）
-- [✅] 提供回滚方案

-- ============================================================
-- 下一步
-- ============================================================

-- 1. 执行本升级脚本
-- 2. 修改 DAG A/B 代码（使用单表）
-- 3. 测试验证
-- 4. 删除旧队列表（可选）：DROP TABLE governance_asset_packing_queue;
