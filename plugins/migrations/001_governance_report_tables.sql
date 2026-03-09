-- ============================================================
-- 治理结果持久化表
-- 数据库: qa_mysql_conn 对应的数据库
-- 用途: 存储每次治理 DAG 运行的汇总和规则级结果
-- ============================================================

-- 1. 批次级运行汇总
CREATE TABLE IF NOT EXISTS governance_run_summary (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id        VARCHAR(128) NOT NULL,
    dag_id          VARCHAR(128) NOT NULL,
    run_date        DATE NOT NULL,
    entity_name     VARCHAR(128),
    total_rows      INT DEFAULT 0,
    total_rules     INT DEFAULT 0,
    success_rules   INT DEFAULT 0,
    failed_rules    INT DEFAULT 0,
    skipped_rules   INT DEFAULT 0,
    total_hits      INT DEFAULT 0,
    alert_count     INT DEFAULT 0,
    asset_count     INT DEFAULT 0,
    started_at      DATETIME,
    completed_at    DATETIME,
    duration_sec    FLOAT DEFAULT 0,
    status          VARCHAR(16) DEFAULT 'SUCCESS',
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_batch (batch_id),
    INDEX idx_run_date (run_date),
    INDEX idx_dag_id (dag_id, run_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- 2. 规则级执行结果
CREATE TABLE IF NOT EXISTS governance_rule_results (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id        VARCHAR(128) NOT NULL,
    run_date        DATE NOT NULL,
    rule_id         VARCHAR(128) NOT NULL,
    severity        VARCHAR(8) NOT NULL,
    logic_type      VARCHAR(32) DEFAULT 'filter',
    description     VARCHAR(512),
    status          VARCHAR(16) NOT NULL,
    hit_count       INT DEFAULT 0,
    execution_time_sec FLOAT DEFAULT 0,
    error_message   TEXT,
    alert_title     VARCHAR(256),
    trigger_ids     JSON,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_batch_rule (batch_id, rule_id),
    INDEX idx_run_date (run_date),
    INDEX idx_rule_id (rule_id, run_date),
    INDEX idx_severity (severity, run_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
