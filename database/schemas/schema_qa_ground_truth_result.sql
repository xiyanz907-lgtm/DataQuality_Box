-- Ground Truth Validation Result Table Schema
-- 用于存储圈分段逻辑验证结果（Controller 通过此表追踪完成进度）

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

-- 说明：
-- 1. Controller DAG 通过此表的 COUNT(DISTINCT vehicle_id) 判断是否所有车辆都已完成验证
-- 2. ON DUPLICATE KEY UPDATE 确保重复触发时不会产生重复记录
-- 3. created_at/updated_at 用于调试和追踪

