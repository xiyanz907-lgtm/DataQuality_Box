-- ============================================================
-- Auto Test Case Catalog 表结构升级
-- 版本：v2 - 支持打包重试计数和毒丸数据防护
-- ============================================================

-- 1. 添加 retry_count 字段
ALTER TABLE auto_test_case_catalog 
ADD COLUMN retry_count INT DEFAULT 0 
COMMENT '打包重试次数（超过3次则永久放弃，防止毒丸数据）' 
AFTER process_status;

-- 2. 修改 process_status 字段，添加 ABANDONED 状态
ALTER TABLE auto_test_case_catalog 
MODIFY COLUMN process_status 
VARCHAR(20) NOT NULL DEFAULT 'IDENTIFIED' 
COMMENT '处理状态：
    IDENTIFIED - 已识别（DAG A 写入初始状态）
    PACKAGED - 已打包（DAG B 完成打包）
    BENCHMARKED - 已基准测试（后续流程）
    ABANDONED - 已放弃（打包失败超过3次，永久放弃）';

-- 3. 添加索引优化
ALTER TABLE auto_test_case_catalog 
ADD INDEX idx_process_retry (process_status, retry_count) 
COMMENT '打包状态查询优化（查询可重试的失败任务）';

-- ============================================================
-- 升级说明
-- ============================================================
-- 1. 新增字段说明：
--    - retry_count: 记录打包失败重试次数
--      * 每次失败 +1
--      * retry_count >= 3 时，不再重试，标记为 ABANDONED
--      * 防止毒丸数据（Poison Pill）无限消耗资源
--
-- 2. 状态机扩展：
--    原状态：IDENTIFIED → PACKAGED → BENCHMARKED
--    新状态：IDENTIFIED → PACKAGED → BENCHMARKED
--                      ↓
--                   ABANDONED (重试失败)
--
-- 3. 回滚方案（如需要）：
--    ALTER TABLE auto_test_case_catalog DROP COLUMN retry_count;
--    ALTER TABLE auto_test_case_catalog DROP INDEX idx_process_retry;
