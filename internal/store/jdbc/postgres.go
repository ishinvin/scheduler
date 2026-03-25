package jdbc

import "fmt"

// Postgres implements dialect.Dialect for PostgreSQL.
type Postgres struct{}

func (Postgres) Placeholder(index int) string { return fmt.Sprintf("$%d", index) }
func (Postgres) DateAddSQL(col, secondsExpr string) string {
	return fmt.Sprintf("%s + %s * INTERVAL '1 second'", col, secondsExpr)
}

func (Postgres) SchemaSQL(prefix string) string {
	p := prefix
	return `
CREATE TABLE IF NOT EXISTS ` + p + `scheduler_jobs (
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

CREATE INDEX IF NOT EXISTS idx_` + p + `sched_jobs_fire
    ON ` + p + `scheduler_jobs (next_fire_time, state);
`
}
