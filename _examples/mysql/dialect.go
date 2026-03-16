package main

import (
	"fmt"
	"strings"
)

// mysqlColumns is the column list for MySQL SELECT queries.
const mysqlColumns = `job_id, name, trigger_type, trigger_value,
		        next_fire_time, state, instance_id, acquired_at, enabled, created_at, updated_at`

// MySQL implements jdbc.Dialect for MySQL / MariaDB.
// This demonstrates how to create a custom dialect for any database.
type MySQL struct{}

func (MySQL) Placeholder(_ int) string { return "?" }
func (MySQL) Columns() string          { return mysqlColumns }
func (MySQL) BooleanTrue() string      { return "1" }
func (MySQL) Col(name string) string   { return name }

func (MySQL) SchemaSQL(prefix string) string {
	return `
CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_jobs (
    job_id         VARCHAR(255) PRIMARY KEY,
    name           VARCHAR(512) NOT NULL,
    trigger_type   VARCHAR(32)  NOT NULL,
    trigger_value  VARCHAR(512) NOT NULL,
    next_fire_time DATETIME(6),
    state          VARCHAR(32)  NOT NULL DEFAULT 'WAITING',
    instance_id    VARCHAR(255),
    acquired_at    DATETIME(6),
    enabled        TINYINT(1)   NOT NULL DEFAULT 1,
    created_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_` + prefix + `sched_jobs_fire (next_fire_time, state, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_locks (
    lock_name VARCHAR(64) PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO ` + prefix + `scheduler_locks (lock_name) VALUES ('TRIGGER_ACCESS');

CREATE TABLE IF NOT EXISTS ` + prefix + `scheduler_executions (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id      VARCHAR(255) NOT NULL,
    instance_id VARCHAR(255),
    started_at  DATETIME(6)  NOT NULL,
    finished_at DATETIME(6)  NOT NULL,
    error       TEXT,
    INDEX idx_` + prefix + `sched_exec_job (job_id, started_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
}

func (MySQL) UpsertJobSQL(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (job_id, name, trigger_type, trigger_value,
		                next_fire_time, state, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			name = VALUES(name),
			trigger_type = VALUES(trigger_type),
			trigger_value = VALUES(trigger_value),
			next_fire_time = VALUES(next_fire_time),
			enabled = VALUES(enabled),
			updated_at = VALUES(updated_at)
	`, table)
}

// LockRowSQL uses FOR UPDATE with NOWAIT (MySQL 8.0+).
// For MySQL 5.7, remove NOWAIT and use innodb_lock_wait_timeout instead.
func (MySQL) LockRowSQL(table string) string {
	return fmt.Sprintf("SELECT lock_name FROM %s WHERE lock_name = ? FOR UPDATE NOWAIT", table)
}

// IsLockNotAvailable checks for MySQL lock wait timeout / NOWAIT errors.
// Error 3572: "Statement aborted because lock(s) could not be acquired immediately
//
//	and NOWAIT is set."
//
// Error 1205: "Lock wait timeout exceeded; try restarting transaction"
func (MySQL) IsLockNotAvailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Error 3572") ||
		strings.Contains(msg, "Error 1205")
}
