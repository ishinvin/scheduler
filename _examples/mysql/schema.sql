-- MySQL schema for Go Scheduler
-- Run this manually or via a migration tool (Liquibase, Flyway, etc.)
-- when using initialize-schema: never (the default).
--
-- Requires MySQL 8.0+ for FOR UPDATE NOWAIT support.
-- For MySQL 5.7, remove NOWAIT from the lock query in dialect.go
-- and rely on innodb_lock_wait_timeout instead.

CREATE TABLE IF NOT EXISTS scheduler_jobs (
    job_id         VARCHAR(255) PRIMARY KEY,
    name           VARCHAR(512) NOT NULL,
    trigger_type   VARCHAR(32)  NOT NULL,
    trigger_value  VARCHAR(512) NOT NULL,
    metadata       JSON,
    next_fire_time DATETIME(6),
    state          VARCHAR(32)  NOT NULL DEFAULT 'WAITING',
    instance_id    VARCHAR(255),
    acquired_at    DATETIME(6),
    enabled        TINYINT(1)   NOT NULL DEFAULT 1,
    created_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_sched_jobs_fire (next_fire_time, state, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scheduler_locks (
    lock_name VARCHAR(64) PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO scheduler_locks (lock_name) VALUES ('TRIGGER_ACCESS');
INSERT IGNORE INTO scheduler_locks (lock_name) VALUES ('STATE_ACCESS');

CREATE TABLE IF NOT EXISTS scheduler_executions (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id      VARCHAR(255) NOT NULL,
    instance_id VARCHAR(255),
    started_at  DATETIME(6)  NOT NULL,
    finished_at DATETIME(6)  NOT NULL,
    error       TEXT,
    INDEX idx_sched_exec_job (job_id, started_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
