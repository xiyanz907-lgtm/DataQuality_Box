-- ================================================================
-- 数据表重命名 SQL 脚本
-- ================================================================
-- 用途：将旧表名重命名为新的统一命名规范
-- 执行时间：根据实际需求在数据库中执行
-- 注意：执行前请确保没有正在运行的 DAG 任务
-- ================================================================

USE dagster_pipelines;

-- ----------------------------------------------------------------
-- 1. 重命名 NGen 检查结果表
-- ----------------------------------------------------------------
-- 旧表名: qa_daily_vehicle_result
-- 新表名: datalog_ngen_check_result
-- 说明：存储 NGen vs DataLog 对账结果

RENAME TABLE 
    dagster_pipelines.qa_daily_vehicle_result 
TO 
    dagster_pipelines.datalog_ngen_check_result;

-- 验证
SHOW TABLES LIKE 'datalog_ngen_check_result';


-- ----------------------------------------------------------------
-- 2. 重命名 Logic 检查结果表（汇总）
-- ----------------------------------------------------------------
-- 旧表名: qa_ground_truth_result
-- 新表名: datalog_logic_check_result
-- 说明：存储 Ground Truth 验证汇总结果（per vehicle）

RENAME TABLE 
    dagster_pipelines.qa_ground_truth_result 
TO 
    dagster_pipelines.datalog_logic_check_result;

-- 验证
SHOW TABLES LIKE 'datalog_logic_check_result';


-- ----------------------------------------------------------------
-- 3. 重命名 Logic 检查失败详情表
-- ----------------------------------------------------------------
-- 旧表名: qa_ground_truth_failure_detail
-- 新表名: datalog_logic_check_failed_detail
-- 说明：存储 Ground Truth 验证失败记录的详细信息

RENAME TABLE 
    dagster_pipelines.qa_ground_truth_failure_detail 
TO 
    dagster_pipelines.datalog_logic_check_failed_detail;

-- 验证
SHOW TABLES LIKE 'datalog_logic_check_failed_detail';


-- ================================================================
-- 执行完成后验证
-- ================================================================

-- 查看所有相关表
SHOW TABLES LIKE 'datalog_%';

-- 查看表结构（确认数据完整性）
DESC dagster_pipelines.datalog_ngen_check_result;
DESC dagster_pipelines.datalog_logic_check_result;
DESC dagster_pipelines.datalog_logic_check_failed_detail;

-- 查看索引（确认索引正常）
SHOW INDEX FROM dagster_pipelines.datalog_ngen_check_result;
SHOW INDEX FROM dagster_pipelines.datalog_logic_check_result;
SHOW INDEX FROM dagster_pipelines.datalog_logic_check_failed_detail;

-- 查看表记录数（确认数据未丢失）
SELECT 'datalog_ngen_check_result' AS table_name, COUNT(*) AS record_count 
FROM dagster_pipelines.datalog_ngen_check_result
UNION ALL
SELECT 'datalog_logic_check_result', COUNT(*) 
FROM dagster_pipelines.datalog_logic_check_result
UNION ALL
SELECT 'datalog_logic_check_failed_detail', COUNT(*) 
FROM dagster_pipelines.datalog_logic_check_failed_detail;

-- ================================================================
-- 【重要提醒】
-- ================================================================
-- 1. 执行前请停止所有相关 DAG（DatalogNgenCheck, DatalogLogicCheck）
-- 2. 执行后请重启 Airflow 容器，确保代码更新生效
-- 3. 建议在非高峰时段执行
-- 4. 如果需要回滚，可以将 RENAME TABLE 语句反向执行
-- ================================================================

