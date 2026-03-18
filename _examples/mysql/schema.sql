-- MySQL schema for Go Scheduler
-- Run this manually or via a migration tool (Liquibase, Flyway, etc.)
-- when using initialize-schema: never (the default).

CREATE TABLE IF NOT EXISTS scheduler_jobs (
    job_id         VARCHAR(255) PRIMARY KEY,
    name           VARCHAR(512) NOT NULL,
    trigger_type   VARCHAR(32)  NOT NULL,
    trigger_value  VARCHAR(512) NOT NULL,
    timeout_secs   INT          NOT NULL DEFAULT 0,
    next_fire_time DATETIME(6),
    state          VARCHAR(32)  NOT NULL DEFAULT 'WAITING',
    instance_id    VARCHAR(255),
    acquired_at    DATETIME(6),
    enabled        TINYINT(1)   NOT NULL DEFAULT 1,
    created_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_sched_jobs_fire (next_fire_time, state, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
