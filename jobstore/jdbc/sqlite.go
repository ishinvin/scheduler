package jdbc

import (
	"fmt"
	"strings"
)

// sqliteColumns is the column list for SQLite SELECT queries.
const sqliteColumns = `job_id, name, trigger_type, trigger_value,
		        next_fire_time, state, instance_id, acquired_at, enabled, created_at, updated_at`

// SQLite implements Dialect for SQLite.
// Ideal for single-instance deployments that need persistence without an external database.
//
// Locking note: SQLite does not support SELECT ... FOR UPDATE NOWAIT.
// Instead, LockRowSQL uses a simple SELECT and relies on SQLite's database-level
// write lock (BEGIN IMMEDIATE in the transaction) for mutual exclusion.
// This is sufficient for single-instance use.
type SQLite struct{}

func (SQLite) Placeholder(_ int) string { return "?" }
func (SQLite) Columns() string          { return sqliteColumns }
func (SQLite) BooleanTrue() string      { return "1" }
func (SQLite) Col(name string) string   { return name }

func (SQLite) SchemaSQL(prefix string) string {
	return `
CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_jobs (
    job_id         TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    trigger_type   TEXT NOT NULL,
    trigger_value  TEXT NOT NULL,
    next_fire_time TEXT,
    state          TEXT NOT NULL DEFAULT 'WAITING',
    instance_id    TEXT,
    acquired_at    TEXT,
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_` + prefix + `sched_jobs_fire
    ON ` + prefix + `scheduler_jobs (next_fire_time)
    WHERE state = 'WAITING' AND enabled = 1;

CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_locks (
    lock_name TEXT PRIMARY KEY
);

INSERT OR IGNORE INTO ` + prefix + `scheduler_locks (lock_name) VALUES ('TRIGGER_ACCESS');

CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_executions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      TEXT NOT NULL,
    instance_id TEXT,
    started_at  TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    error       TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_` + prefix + `sched_exec_job
    ON ` + prefix + `scheduler_executions (job_id, started_at DESC);
`
}

func (SQLite) UpsertJobSQL(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (job_id, name, trigger_type, trigger_value,
		                next_fire_time, state, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (job_id) DO UPDATE SET
			name = excluded.name,
			trigger_type = excluded.trigger_type,
			trigger_value = excluded.trigger_value,
			next_fire_time = excluded.next_fire_time,
			enabled = excluded.enabled,
			updated_at = excluded.updated_at
	`, table)
}

// LockRowSQL returns a simple SELECT for the lock row.
// SQLite relies on database-level write locking (BEGIN IMMEDIATE) rather than
// row-level FOR UPDATE NOWAIT.
func (SQLite) LockRowSQL(table string) string {
	return fmt.Sprintf("SELECT lock_name FROM %s WHERE lock_name = ?", table)
}

// IsLockNotAvailable checks for SQLite busy/locked errors.
func (SQLite) IsLockNotAvailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "SQLITE_BUSY")
}
