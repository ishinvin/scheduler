package jdbc

import (
	"fmt"
	"strings"
)

// Dialect abstracts SQL differences between databases.
// Implement only the truly database-specific methods — the store generates
// standard CRUD queries from Placeholder, Columns, BooleanTrue, and Col.
//
// Users can implement this interface to support additional databases (e.g., MySQL).
type Dialect interface {
	// Placeholder returns the SQL placeholder for the given 1-based parameter index.
	// e.g., "?" for MySQL/SQLite, "$1" for Postgres, ":1" for Oracle.
	Placeholder(index int) string

	// Columns returns the comma-separated column list for SELECT queries.
	// Must match the scan order in store.scanJob.
	Columns() string

	// BooleanTrue returns the SQL literal for true ("TRUE" for Postgres, "1" for SQLite/Oracle/MySQL).
	BooleanTrue() string

	// Col returns the column name in the dialect's preferred casing.
	// e.g., "job_id" for Postgres/SQLite/MySQL, "JOB_ID" for Oracle.
	Col(name string) string

	// SchemaSQL returns the full DDL string for the given table prefix.
	SchemaSQL(prefix string) string

	// UpsertJobSQL returns an upsert statement for the jobs table.
	UpsertJobSQL(table string) string

	// LockRowSQL returns a SELECT ... FOR UPDATE NOWAIT for the locks table.
	LockRowSQL(table string) string

	// IsLockNotAvailable returns true if the error indicates a NOWAIT lock failure.
	IsLockNotAvailable(err error) bool
}

// --- Query generators (used by store.go) ---

func deleteJobSQL(d Dialect, table string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
		table, d.Col("job_id"), d.Placeholder(1))
}

func getJobSQL(d Dialect, table string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s",
		d.Columns(), table, d.Col("job_id"), d.Placeholder(1))
}

func listJobsSQL(d Dialect, table string) string {
	return fmt.Sprintf("SELECT %s FROM %s", d.Columns(), table)
}

func listDueJobsSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = %s AND %s = %s AND %s <= %s ORDER BY %s",
		d.Columns(), table,
		d.Col("state"), d.Placeholder(1),
		d.Col("enabled"), d.BooleanTrue(),
		d.Col("next_fire_time"), d.Placeholder(2),
		d.Col("next_fire_time"))
}

func acquireJobSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = %s, %s = %s, %s = %s WHERE %s = %s AND %s = %s",
		table,
		d.Col("state"), d.Placeholder(1),
		d.Col("instance_id"), d.Placeholder(2),
		d.Col("acquired_at"), d.Placeholder(3),
		d.Col("updated_at"), d.Placeholder(4), //nolint:mnd // placeholder index
		d.Col("job_id"), d.Placeholder(5), //nolint:mnd // placeholder index
		d.Col("state"), d.Placeholder(6)) //nolint:mnd // placeholder index
}

func releaseJobSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = NULL, %s = NULL, %s = %s, %s = %s WHERE %s = %s",
		table,
		d.Col("state"), d.Placeholder(1),
		d.Col("instance_id"),
		d.Col("acquired_at"),
		d.Col("next_fire_time"), d.Placeholder(2),
		d.Col("updated_at"), d.Placeholder(3),
		d.Col("job_id"), d.Placeholder(4)) //nolint:mnd // placeholder index
}

func insertExecutionSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s)",
		table,
		d.Col("job_id"), d.Col("instance_id"), d.Col("started_at"), d.Col("finished_at"), d.Col("error"),
		d.Placeholder(1), d.Placeholder(2), d.Placeholder(3), d.Placeholder(4), d.Placeholder(5)) //nolint:mnd // placeholder indices
}

func recoverStaleJobsSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = NULL, %s = NULL, %s = %s WHERE %s = %s AND %s < %s",
		table,
		d.Col("state"), d.Placeholder(1),
		d.Col("instance_id"),
		d.Col("acquired_at"),
		d.Col("updated_at"), d.Placeholder(2),
		d.Col("state"), d.Placeholder(3),
		d.Col("acquired_at"), d.Placeholder(4)) //nolint:mnd // placeholder index
}

func purgeExecutionsSQL(d Dialect, table string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s < %s",
		table, d.Col("finished_at"), d.Placeholder(1))
}

// containsString checks if an error message contains a substring.
func containsString(err error, substr string) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), substr)
}
