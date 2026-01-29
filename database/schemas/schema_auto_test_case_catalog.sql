-- Auto Test Case Catalog Schema
-- 用于存储从 Airflow 中挖掘出的 Corner Case（边缘场景）资产
-- 服务于港口自动驾驶数据治理系统的测试用例管理

CREATE TABLE IF NOT EXISTS auto_test_case_catalog (
    -- ============================================================
    -- 基础索引信息
    -- ============================================================
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键，自增 ID',
    batch_id VARCHAR(50) NOT NULL COMMENT '批次 ID（Airflow DAG Run ID，用于追溯数据来源）',
    cycle_id VARCHAR(50) NOT NULL COMMENT '业务主键（作业圈 ID，全局唯一标识一次完整作业）',
    vehicle_id VARCHAR(50) NOT NULL COMMENT '车辆 ID（如 AT01, AT02）',
    shift_date DATE NOT NULL COMMENT '作业班次日期（用于按日期分片查询）',
    rule_version VARCHAR(20) NOT NULL COMMENT '挖掘规则版本号（如 v1.0, v1.1，用于对比不同规则版本的效果）',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间（记录首次写入数据库的时刻）',
    
    -- ============================================================
    -- 业务分类信息
    -- ============================================================
    category VARCHAR(50) NOT NULL COMMENT '大类（Standard-标准场景 / CornerCase-边缘场景 / Unknown-未分类）',
    case_tags JSON DEFAULT NULL COMMENT '场景标签列表（如 ["TWIN_LIFT", "RISKY_OVERTAKE"]，支持多标签）',
    severity VARCHAR(20) NOT NULL DEFAULT 'INFO' COMMENT '严重等级（P0-致命 / P1-严重 / P2-一般 / INFO-信息）',
    confidence FLOAT NOT NULL DEFAULT 0.0 COMMENT '置信度（0.0-1.0，表示规则识别的可信程度）',
    
    -- ============================================================
    -- 时空上下文信息
    -- ============================================================
    trigger_timestamp DATETIME NOT NULL COMMENT '异常触发的精确时刻（用于时序分析和回放定位）',
    time_window_start DATETIME NOT NULL COMMENT '原始数据切片开始时间（数据包的起始边界）',
    time_window_end DATETIME NOT NULL COMMENT '原始数据切片结束时间（数据包的结束边界）',
    trigger_section VARCHAR(50) DEFAULT NULL COMMENT '触发路段（如 "ckp_6to7"，用于空间聚类分析）',
    geo_context JSON DEFAULT NULL COMMENT '地理快照（精简轨迹数据，用于前端快速预览，如 {"points": [[x1,y1], [x2,y2]]}）',
    
    -- ============================================================
    -- 资产与真值信息
    -- ============================================================
    raw_data_uri JSON DEFAULT NULL COMMENT '原始数据包地址列表（MinIO/S3 的 URI，支持多个文件，如 ["s3://bucket/path1.bag", "s3://bucket/path2.bag"]）',
    ground_truth_json JSON DEFAULT NULL COMMENT '理论真值（预留字段，用于后续人工标注或仿真回填，如 {"expected_speed": 5.0, "expected_trajectory": [...]}）',
    metric_snapshot JSON DEFAULT NULL COMMENT '关键指标快照（如 {"duration": 120, "kwh_per_km": 2.5, "max_speed": 15.0}）',
    process_status VARCHAR(20) NOT NULL DEFAULT 'IDENTIFIED' COMMENT '处理状态（IDENTIFIED-已识别 / PACKAGED-已打包 / BENCHMARKED-已基准测试）',
    replay_status VARCHAR(20) NOT NULL DEFAULT 'PENDING' COMMENT '回放状态（PENDING-待回放 / IN_PROGRESS-回放中 / SUCCESS-成功 / FAILED-失败）',
    
    -- ============================================================
    -- 索引设计（支持高效查询和数据治理）
    -- ============================================================
    UNIQUE KEY uk_batch_cycle (batch_id, cycle_id) COMMENT '唯一约束：同一批次同一作业圈只有一条记录（防止重复写入）',
    KEY idx_vehicle_shift (vehicle_id, shift_date) COMMENT '车辆+班次联合索引（支持按车按日查询）',
    KEY idx_category_severity (category, severity) COMMENT '分类+严重度索引（支持按场景类型和优先级筛选）',
    KEY idx_trigger_time (trigger_timestamp) COMMENT '触发时间索引（支持时序分析）',
    KEY idx_process_status (process_status) COMMENT '处理状态索引（支持状态机查询）',
    KEY idx_replay_status (replay_status) COMMENT '回放状态索引（支持回放任务调度）',
    KEY idx_rule_version (rule_version) COMMENT '规则版本索引（支持版本对比分析）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='自动驾驶测试用例目录表-用于存储从生产数据中挖掘的边缘场景资产';

-- ============================================================
-- 说明与使用指南
-- ============================================================
-- 1. 业务主键设计：
--    - batch_id + cycle_id 作为唯一键，确保可重跑（ON DUPLICATE KEY UPDATE）
--    - cycle_id 本身应该是全局唯一的，但加上 batch_id 可以追溯数据来源
--
-- 2. JSON 字段使用建议：
--    - case_tags: 数组格式 ["tag1", "tag2"]，便于前端展示和过滤
--    - geo_context: 轻量级轨迹数据，不要存储完整高精度轨迹
--    - raw_data_uri: 数组格式，支持一个 case 对应多个数据文件
--    - ground_truth_json: 结构化真值数据，便于后续自动化测试
--    - metric_snapshot: KPI 快照，避免每次查询都重新计算
--
-- 3. 状态机设计：
--    - process_status: IDENTIFIED -> PACKAGED -> BENCHMARKED
--    - replay_status: PENDING -> IN_PROGRESS -> SUCCESS/FAILED
--
-- 4. 查询优化建议：
--    - 按车辆+日期查询：使用 idx_vehicle_shift
--    - 按严重度筛选 P0/P1：使用 idx_category_severity
--    - 按状态查询待处理任务：使用 idx_process_status / idx_replay_status
--    - 按规则版本对比：使用 idx_rule_version
--
-- 5. 数据治理建议：
--    - 定期清理过期数据（如 shift_date < NOW() - INTERVAL 90 DAY）
--    - 监控 JSON 字段大小，避免单条记录过大影响性能
--    - raw_data_uri 中的文件应有独立的生命周期管理策略
