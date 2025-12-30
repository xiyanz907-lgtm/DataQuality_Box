-- ================================================================
-- 创建数据质量检查相关表（新表名）
-- ================================================================
-- 用途：如果表不存在则创建，如果已存在（通过 RENAME）则跳过
-- 执行：可以安全地重复执行
-- ================================================================

USE dagster_pipelines;

-- ----------------------------------------------------------------
-- 1. NGen 检查结果表
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dagster_pipelines.datalog_ngen_check_result (
    id BIGINT NOT NULL AUTO_INCREMENT,
    shift_date DATE NOT NULL,
    vehicle_id VARCHAR(50) NOT NULL,
    is_blocked TINYINT(1) DEFAULT 0,
    profiling_json LONGTEXT,
    execution_time DATETIME DEFAULT NULL,
    created_at DATETIME DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_shift_vehicle (shift_date, vehicle_id),
    KEY idx_shift_date (shift_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='NGen vs DataLog 对账结果表';

SELECT '✓ Table datalog_ngen_check_result checked/created' AS status;


-- ----------------------------------------------------------------
-- 2. Logic 检查结果表（汇总）
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dagster_pipelines.datalog_logic_check_result (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    shift_date DATE NOT NULL COMMENT '班次日期（用于 Controller 追踪进度）',
    vehicle_id VARCHAR(50) NOT NULL COMMENT '车辆 ID',
    total_records INT NOT NULL DEFAULT 0 COMMENT '总记录数',
    passed_records INT NOT NULL DEFAULT 0 COMMENT '通过验证的记录数',
    failed_records INT NOT NULL DEFAULT 0 COMMENT '验证失败的记录数',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_shift_vehicle (shift_date, vehicle_id) COMMENT '唯一约束：每个班次每辆车只有一条记录',
    KEY idx_shift_date (shift_date) COMMENT '班次日期索引（Controller 查询用）',
    KEY idx_vehicle_id (vehicle_id) COMMENT '车辆 ID 索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='圈分段逻辑验证结果表';

SELECT '✓ Table datalog_logic_check_result checked/created' AS status;


-- ----------------------------------------------------------------
-- 3. Logic 检查失败详情表
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dagster_pipelines.datalog_logic_check_failed_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    shift_date DATE NOT NULL COMMENT '班次日期',
    vehicle_id VARCHAR(50) NOT NULL COMMENT '车辆 ID',
    cycle_id VARCHAR(100) NOT NULL COMMENT '圈号',
    unix_timestamp BIGINT NOT NULL COMMENT '时间戳（秒级）',
    failure_type VARCHAR(50) NOT NULL COMMENT '失败类型',
    expected_condition VARCHAR(200) NOT NULL COMMENT '期望条件',
    actual_value VARCHAR(200) NULL COMMENT '实际值',
    actual_x DOUBLE NULL COMMENT 'X坐标',
    actual_y DOUBLE NULL COMMENT 'Y坐标',
    actual_speed DOUBLE NULL COMMENT '速度',
    map_road_type VARCHAR(100) NULL COMMENT '路段类型',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    KEY idx_shift_vehicle (shift_date, vehicle_id),
    KEY idx_cycle_id (cycle_id),
    KEY idx_failure_type (failure_type),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='验证失败明细表';

SELECT '✓ Table datalog_logic_check_failed_detail checked/created' AS status;


-- ================================================================
-- 验证所有表
-- ================================================================
SELECT '================================================' AS '';
SELECT '所有表创建/检查完成' AS 'Status';
SELECT '================================================' AS '';

SHOW TABLES LIKE 'datalog_%';

SELECT '================================================' AS '';
SELECT '表记录统计' AS '';
SELECT '================================================' AS '';

SELECT 
    'datalog_ngen_check_result' AS table_name, 
    COUNT(*) AS record_count 
FROM dagster_pipelines.datalog_ngen_check_result
UNION ALL
SELECT 
    'datalog_logic_check_result', 
    COUNT(*) 
FROM dagster_pipelines.datalog_logic_check_result
UNION ALL
SELECT 
    'datalog_logic_check_failed_detail', 
    COUNT(*) 
FROM dagster_pipelines.datalog_logic_check_failed_detail;

