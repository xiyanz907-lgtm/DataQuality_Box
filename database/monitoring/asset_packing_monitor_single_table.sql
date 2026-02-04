-- ============================================================
-- 资产打包监控 SQL（单表方案）
-- 用于监控 auto_test_case_catalog 表中的打包状态
-- ============================================================

-- 1. 队列积压监控（告警阈值 500）
-- 每分钟执行，如超过阈值则告警
SELECT 
    COUNT(*) AS pending_count,
    CASE 
        WHEN COUNT(*) > 500 THEN '🚨 CRITICAL: Queue backlog!'
        WHEN COUNT(*) > 300 THEN '⚠️ WARNING: Queue growing'
        ELSE '✅ OK'
    END AS status
FROM auto_test_case_catalog
WHERE process_status = 'PENDING';

-- 2. 僵尸任务检测（超时 2 小时）
-- 每 10 分钟执行
SELECT 
    id,
    batch_id,
    cycle_id as asset_id,
    process_status,
    pack_retry_count as retry_count,
    TIMESTAMPDIFF(MINUTE, updated_at, NOW()) AS stuck_minutes,
    pack_error_message as error_message
FROM auto_test_case_catalog
WHERE process_status IN ('PROCESSING', 'POLLING')
  AND updated_at < NOW() - INTERVAL 2 HOUR
ORDER BY updated_at ASC;

-- 3. 打包成功率监控（最近 1 小时）
-- 每 15 分钟执行
SELECT 
    COUNT(*) AS total_processed,
    SUM(CASE WHEN process_status = 'PACKAGED' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN process_status = 'PENDING' AND pack_retry_count > 0 THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN process_status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned_count,
    ROUND(
        SUM(CASE WHEN process_status = 'PACKAGED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS success_rate_pct,
    CASE 
        WHEN ROUND(SUM(CASE WHEN process_status = 'PACKAGED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 80 
        THEN '🚨 CRITICAL: Low success rate!'
        WHEN ROUND(SUM(CASE WHEN process_status = 'PACKAGED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 95 
        THEN '⚠️ WARNING: Success rate degraded'
        ELSE '✅ OK'
    END AS status
FROM auto_test_case_catalog
WHERE pack_completed_at >= NOW() - INTERVAL 1 HOUR;

-- 4. 处理耗时监控（P95）
-- 每 30 分钟执行
SELECT 
    ROUND(AVG(TIMESTAMPDIFF(SECOND, pack_started_at, pack_completed_at)), 2) AS avg_duration_sec,
    MAX(TIMESTAMPDIFF(SECOND, pack_started_at, pack_completed_at)) AS max_duration_sec,
    CASE 
        WHEN MAX(TIMESTAMPDIFF(SECOND, pack_started_at, pack_completed_at)) > 600 
        THEN '⚠️ WARNING: Some packs took > 10 min'
        ELSE '✅ OK'
    END AS status
FROM auto_test_case_catalog
WHERE process_status = 'PACKAGED'
  AND pack_completed_at >= NOW() - INTERVAL 1 HOUR;

-- 5. 失败任务详情（最近 1 小时）
-- 用于故障排查
SELECT 
    id,
    batch_id,
    cycle_id as asset_id,
    vehicle_id,
    process_status,
    pack_retry_count as retry_count,
    pack_error_message as error_message,
    created_at,
    updated_at
FROM auto_test_case_catalog
WHERE process_status IN ('PENDING', 'ABANDONED')
  AND pack_retry_count > 0
  AND updated_at >= NOW() - INTERVAL 1 HOUR
ORDER BY updated_at DESC
LIMIT 100;

-- 6. 重试次数分布
-- 用于评估系统稳定性
SELECT 
    pack_retry_count as retry_count,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM auto_test_case_catalog
WHERE created_at >= NOW() - INTERVAL 24 HOUR
  AND pack_retry_count IS NOT NULL
GROUP BY pack_retry_count
ORDER BY pack_retry_count;

-- 7. 按批次统计（最近 10 个批次）
-- 用于批次级别的监控
SELECT 
    batch_id,
    COUNT(*) AS total,
    SUM(CASE WHEN process_status = 'PACKAGED' THEN 1 ELSE 0 END) AS packaged,
    SUM(CASE WHEN process_status = 'PENDING' AND pack_retry_count > 0 THEN 1 ELSE 0 END) AS failed_retrying,
    SUM(CASE WHEN process_status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned,
    SUM(CASE WHEN process_status IN ('PENDING', 'PROCESSING', 'POLLING') AND pack_retry_count = 0 THEN 1 ELSE 0 END) AS in_progress,
    MAX(created_at) AS batch_created_at
FROM auto_test_case_catalog
WHERE pack_retry_count IS NOT NULL  -- 只统计经过打包的记录
GROUP BY batch_id
ORDER BY batch_created_at DESC
LIMIT 10;

-- 8. 异常错误信息汇总（最近 24 小时）
-- 用于识别常见错误模式
SELECT 
    LEFT(pack_error_message, 100) AS error_prefix,
    COUNT(*) AS occurrence_count,
    MIN(updated_at) AS first_seen,
    MAX(updated_at) AS last_seen
FROM auto_test_case_catalog
WHERE process_status IN ('PENDING', 'ABANDONED')
  AND pack_error_message IS NOT NULL
  AND updated_at >= NOW() - INTERVAL 24 HOUR
GROUP BY LEFT(pack_error_message, 100)
ORDER BY occurrence_count DESC
LIMIT 20;

-- 9. 状态分布概览
-- 用于快速了解整体情况
SELECT 
    process_status,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM auto_test_case_catalog
WHERE pack_retry_count IS NOT NULL  -- 只统计经过打包的记录
GROUP BY process_status
ORDER BY count DESC;

-- 10. 打包队列健康度检查
-- 综合评估队列状态
SELECT 
    'Queue Health Check' AS metric_type,
    (SELECT COUNT(*) FROM auto_test_case_catalog WHERE process_status = 'PENDING') AS pending_count,
    (SELECT COUNT(*) FROM auto_test_case_catalog 
     WHERE process_status IN ('PROCESSING', 'POLLING') 
     AND updated_at < NOW() - INTERVAL 2 HOUR) AS zombie_count,
    (SELECT ROUND(AVG(pack_retry_count), 2) 
     FROM auto_test_case_catalog 
     WHERE pack_retry_count > 0) AS avg_retry_count,
    (SELECT COUNT(*) FROM auto_test_case_catalog 
     WHERE process_status = 'ABANDONED') AS abandoned_total,
    CASE 
        WHEN (SELECT COUNT(*) FROM auto_test_case_catalog WHERE process_status = 'PENDING') > 500 THEN '❌ UNHEALTHY'
        WHEN (SELECT COUNT(*) FROM auto_test_case_catalog 
              WHERE process_status IN ('PROCESSING', 'POLLING') 
              AND updated_at < NOW() - INTERVAL 2 HOUR) > 0 THEN '⚠️ WARNING'
        ELSE '✅ HEALTHY'
    END AS overall_status;

-- ============================================================
-- 使用说明（单表方案）
-- ============================================================
-- 1. 将以上查询集成到 Grafana/Prometheus 监控系统
-- 2. 设置告警规则：
--    - pending_count > 500 -> 发送告警
--    - zombie_count > 0 -> 发送告警
--    - success_rate < 80% -> 发送告警
-- 3. 定期查看失败任务详情，优化系统稳定性
--
-- 4. 单表方案的优势：
--    - 查询更简单（不需要 JOIN）
--    - 数据一致性更强（单表事务）
--    - 维护成本更低（只需维护一张表）
