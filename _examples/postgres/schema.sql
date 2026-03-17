-- PostgreSQL schema for Go Scheduler
-- Run this manually or via a migration tool (Liquibase, Flyway, etc.)
-- when using initialize-schema: never (the default).
--
-- To use a table prefix, replace "scheduler_" with e.g. "myapp_scheduler_"
-- and update the index names accordingly.

CREATE TABLE IF NOT EXISTS scheduler_jobs (
    job_id         TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    trigger_type   TEXT NOT NULL,
    trigger_value  TEXT NOT NULL,
    timeout_secs   INTEGER NOT NULL DEFAULT 0,
    next_fire_time TIMESTAMPTZ,
    state          TEXT NOT NULL DEFAULT 'WAITING',
    instance_id    TEXT,
    acquired_at    TIMESTAMPTZ,
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sched_jobs_fire
    ON scheduler_jobs (next_fire_time)
    WHERE state = 'WAITING' AND enabled = TRUE;

CREATE TABLE IF NOT EXISTS scheduler_executions (
    id          BIGSERIAL PRIMARY KEY,
    job_id      TEXT NOT NULL,
    instance_id TEXT,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    error       TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_sched_exec_job
    ON scheduler_executions (job_id, started_at DESC);
