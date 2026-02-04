-- ============================================================
-- 资产打包监控 SQL
-- 用于监控打包队列状态、僵尸任务、失败任务等
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
FROM governance_asset_packing_queue
WHERE status = 'PENDING';

-- 2. 僵尸任务检测（超时 2 小时）
-- 每 10 分钟执行
SELECT 
    id,
    batch_id,
    asset_id,
    status,
    retry_count,
    TIMESTAMPDIFF(MINUTE, updated_at, NOW()) AS stuck_minutes,
    error_message
FROM governance_asset_packing_queue
WHERE status IN ('PROCESSING', 'POLLING')
  AND updated_at < NOW() - INTERVAL 2 HOUR
ORDER BY updated_at ASC;

-- 3. 打包成功率监控（最近 1 小时）
-- 每 15 分钟执行
SELECT 
    COUNT(*) AS total_processed,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned_count,
    ROUND(
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) AS success_rate_pct,
    CASE 
        WHEN ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 80 
        THEN '🚨 CRITICAL: Low success rate!'
        WHEN ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) < 95 
        THEN '⚠️ WARNING: Success rate degraded'
        ELSE '✅ OK'
    END AS status
FROM governance_asset_packing_queue
WHERE processed_at >= NOW() - INTERVAL 1 HOUR;

-- 4. 处理耗时监控（P95）
-- 每 30 分钟执行
SELECT 
    ROUND(AVG(TIMESTAMPDIFF(SECOND, pack_started_at, processed_at)), 2) AS avg_duration_sec,
    MAX(TIMESTAMPDIFF(SECOND, pack_started_at, processed_at)) AS max_duration_sec,
    ROUND(
        (SELECT TIMESTAMPDIFF(SECOND, pack_started_at, processed_at)
         FROM governance_asset_packing_queue
         WHERE status = 'SUCCESS'
           AND processed_at >= NOW() - INTERVAL 1 HOUR
         ORDER BY TIMESTAMPDIFF(SECOND, pack_started_at, processed_at) DESC
         LIMIT 1 OFFSET (SELECT FLOOR(COUNT(*) * 0.05) 
                        FROM governance_asset_packing_queue
                        WHERE status = 'SUCCESS'
                          AND processed_at >= NOW() - INTERVAL 1 HOUR)),
        2
    ) AS p95_duration_sec,
    CASE 
        WHEN MAX(TIMESTAMPDIFF(SECOND, pack_started_at, processed_at)) > 600 
        THEN '⚠️ WARNING: Some packs took > 10 min'
        ELSE '✅ OK'
    END AS status
FROM governance_asset_packing_queue
WHERE status = 'SUCCESS'
  AND processed_at >= NOW() - INTERVAL 1 HOUR;

-- 5. 失败任务详情（最近 1 小时）
-- 用于故障排查
SELECT 
    id,
    batch_id,
    asset_id,
    rule_id,
    vehicle_id,
    retry_count,
    error_message,
    created_at,
    updated_at
FROM governance_asset_packing_queue
WHERE status IN ('FAILED', 'ABANDONED')
  AND updated_at >= NOW() - INTERVAL 1 HOUR
ORDER BY updated_at DESC
LIMIT 100;

-- 6. 重试次数分布
-- 用于评估系统稳定性
SELECT 
    retry_count,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM governance_asset_packing_queue
WHERE created_at >= NOW() - INTERVAL 24 HOUR
GROUP BY retry_count
ORDER BY retry_count;

-- 7. 按批次统计（最近 10 个批次）
-- 用于批次级别的监控
SELECT 
    batch_id,
    COUNT(*) AS total,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned,
    SUM(CASE WHEN status IN ('PENDING', 'PROCESSING', 'POLLING') THEN 1 ELSE 0 END) AS in_progress,
    MAX(created_at) AS batch_created_at
FROM governance_asset_packing_queue
GROUP BY batch_id
ORDER BY batch_created_at DESC
LIMIT 10;

-- 8. 异常错误信息汇总（最近 24 小时）
-- 用于识别常见错误模式
SELECT 
    LEFT(error_message, 100) AS error_prefix,
    COUNT(*) AS occurrence_count,
    MIN(updated_at) AS first_seen,
    MAX(updated_at) AS last_seen
FROM governance_asset_packing_queue
WHERE status IN ('FAILED', 'ABANDONED')
  AND error_message IS NOT NULL
  AND updated_at >= NOW() - INTERVAL 24 HOUR
GROUP BY LEFT(error_message, 100)
ORDER BY occurrence_count DESC
LIMIT 20;

-- ============================================================
-- 使用建议
-- ============================================================
-- 1. 将以上查询集成到 Grafana/Prometheus 监控系统
-- 2. 设置告警规则：
--    - pending_count > 500 -> 发送告警
--    - zombie_tasks > 0 -> 发送告警
--    - success_rate < 80% -> 发送告警
-- 3. 定期查看失败任务详情，优化系统稳定性
