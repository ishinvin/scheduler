-- PostgreSQL schema for Go Scheduler
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
    next_fire_time TIMESTAMPTZ,
    state          VARCHAR(32)  NOT NULL DEFAULT 'WAITING',
    instance_id    VARCHAR(255),
    acquired_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sched_jobs_fire
    ON scheduler_jobs (next_fire_time, state);