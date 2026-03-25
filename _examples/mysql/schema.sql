-- MySQL schema for Go Scheduler
-- Run this manually or via a migration tool
-- when using initialize-schema: never (the default).
--
-- To use a table prefix, replace "scheduler_" with e.g. "myapp_scheduler_"
-- and update the index names accordingly.

CREATE TABLE IF NOT EXISTS scheduler_jobs (
    job_id         VARCHAR(255) PRIMARY KEY,
    name           VARCHAR(255) NOT NULL,
    trigger_type   VARCHAR(32)  NOT NULL,
    trigger_value  VARCHAR(255) NOT NULL,
    timeout_secs   INTEGER      NOT NULL DEFAULT 0,
    next_fire_time DATETIME(6),
    state          VARCHAR(32)  NOT NULL DEFAULT 'WAITING',
    instance_id    VARCHAR(255),
    acquired_at    DATETIME(6),
    created_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_sched_jobs_fire (next_fire_time, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;