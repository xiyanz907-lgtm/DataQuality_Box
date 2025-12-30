-- =====================================================
-- Ground Truth Validation - Failure Detail Table
-- =====================================================
-- 用途：记录圈分段逻辑验证失败的明细记录
-- 关联：datalog_logic_check_result (汇总表)
-- =====================================================

CREATE TABLE IF NOT EXISTS dagster_pipelines.datalog_logic_check_failed_detail (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
    shift_date DATE NOT NULL COMMENT '班次日期',
    vehicle_id VARCHAR(50) NOT NULL COMMENT '车辆 ID',
    cycle_id VARCHAR(100) NOT NULL COMMENT '圈号（TRACTOR_CYCLE_ID）',
    unix_timestamp BIGINT NOT NULL COMMENT '时间戳（秒级）',
    failure_type VARCHAR(50) NOT NULL COMMENT '失败类型：road_type_mismatch | speed_violation',
    expected_condition VARCHAR(200) NOT NULL COMMENT '期望条件（例如："map_road_type 应包含 QC"）',
    actual_value VARCHAR(200) NULL COMMENT '实际值（例如："service"）',
    actual_x DOUBLE NULL COMMENT 'InfluxDB 查询到的 X 坐标',
    actual_y DOUBLE NULL COMMENT 'InfluxDB 查询到的 Y 坐标',
    actual_speed DOUBLE NULL COMMENT 'InfluxDB 查询到的速度',
    map_road_type VARCHAR(100) NULL COMMENT 'Map API 返回的 road_type',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    KEY idx_shift_vehicle (shift_date, vehicle_id) COMMENT '按班次+车辆查询',
    KEY idx_cycle_id (cycle_id) COMMENT '按圈号查询',
    KEY idx_failure_type (failure_type) COMMENT '按失败类型查询',
    KEY idx_created_at (created_at) COMMENT '按创建时间查询'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='逻辑点位验证失败明细表';

