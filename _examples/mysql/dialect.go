package main

import (
	"fmt"
)

// MySQL implements jdbc.Dialect for MySQL / MariaDB.
// This demonstrates how to create a custom dialect for any database.
type MySQL struct{}

func (MySQL) Placeholder(_ int) string { return "?" }
func (MySQL) BooleanTrue() string      { return "1" }

func (MySQL) SchemaSQL(prefix string) string {
	return `
CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_jobs (
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
    INDEX idx_` + prefix + `sched_jobs_fire (next_fire_time, state, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
}

func (MySQL) UpsertJobSQL(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (job_id, name, trigger_type, trigger_value, timeout_secs,
		                next_fire_time, state, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			name = VALUES(name),
			trigger_type = VALUES(trigger_type),
			trigger_value = VALUES(trigger_value),
			timeout_secs = VALUES(timeout_secs),
			next_fire_time = VALUES(next_fire_time),
			enabled = VALUES(enabled),
			updated_at = VALUES(updated_at)
	`, table)
}

func (MySQL) DateAddSQL(col, secondsExpr string) string {
	return fmt.Sprintf("TIMESTAMPADD(SECOND, %s, %s)", secondsExpr, col)
}
