package store

import "fmt"

// Postgres implements dialect for PostgreSQL.
type Postgres struct{}

func (Postgres) Placeholder(index int) string { return fmt.Sprintf("$%d", index) }
func (Postgres) BooleanTrue() string          { return "TRUE" }

func (Postgres) DateAddSQL(col, secondsExpr string) string {
	return fmt.Sprintf("%s + %s * INTERVAL '1 second'", col, secondsExpr)
}

func (Postgres) SchemaSQL(prefix string) string {
	p := prefix
	return `
CREATE TABLE IF NOT EXISTS ` + p + `scheduler_jobs (
    job_id         TEXT        PRIMARY KEY,
    name           TEXT        NOT NULL,
    trigger_type   TEXT        NOT NULL,
    trigger_value  TEXT        NOT NULL,
    timeout_secs   INTEGER     NOT NULL DEFAULT 0,
    next_fire_time TIMESTAMPTZ,
    state          TEXT        NOT NULL DEFAULT 'WAITING',
    instance_id    TEXT,
    acquired_at    TIMESTAMPTZ,
    enabled        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_` + p + `sched_jobs_fire
    ON ` + p + `scheduler_jobs (next_fire_time)
    WHERE state = 'WAITING' AND enabled = TRUE;
`
}
