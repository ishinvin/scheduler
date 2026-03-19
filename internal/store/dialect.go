package store

import (
	"fmt"
	"strings"
)

const jdbcColumns = `job_id, name, trigger_type, trigger_value, timeout_secs, next_fire_time, state, instance_id, acquired_at, enabled, created_at, updated_at` //nolint:lll // column list

// dialect matches the public scheduler.Dialect interface.
type dialect interface {
	Placeholder(index int) string
	BooleanTrue() string
	SchemaSQL(prefix string) string
	DateAddSQL(col, secondsExpr string) string
}

// dialectCol returns a column name, uppercased if the dialect requires it.
func dialectCol(d dialect, name string) string {
	type uppercaser interface{ UppercaseIdentifiers() bool }
	if u, ok := d.(uppercaser); ok && u.UppercaseIdentifiers() {
		return strings.ToUpper(name)
	}
	return name
}

// dialectCols returns all columns, used in SELECT and INSERT.
func dialectCols(d dialect) string {
	return dialectCol(d, jdbcColumns)
}

// insertJobSQL builds INSERT for CreateJob.
func insertJobSQL(d dialect, table string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
		table, dialectCols(d),
		d.Placeholder(1), d.Placeholder(2), d.Placeholder(3),
		d.Placeholder(4), d.Placeholder(5), d.Placeholder(6), //nolint:mnd // placeholder index
		d.Placeholder(7), d.Placeholder(8), d.Placeholder(9), //nolint:mnd // placeholder index
		d.Placeholder(10), d.Placeholder(11), d.Placeholder(12), //nolint:mnd // placeholder index
	)
}

// updateJobSQL builds UPDATE for UpdateJob.
func updateJobSQL(d dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s WHERE %s = %s",
		table,
		dialectCol(d, "name"), d.Placeholder(1),
		dialectCol(d, "trigger_type"), d.Placeholder(2),
		dialectCol(d, "trigger_value"), d.Placeholder(3),
		dialectCol(d, "timeout_secs"), d.Placeholder(4), //nolint:mnd // placeholder index
		dialectCol(d, "next_fire_time"), d.Placeholder(5), //nolint:mnd // placeholder index
		dialectCol(d, "enabled"), d.Placeholder(6), //nolint:mnd // placeholder index
		dialectCol(d, "updated_at"), d.Placeholder(7), //nolint:mnd // placeholder index
		dialectCol(d, "job_id"), d.Placeholder(8), //nolint:mnd // placeholder index
	)
}

// deleteJobSQL builds DELETE for DeleteJob.
func deleteJobSQL(d dialect, table string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = %s", table, dialectCol(d, "job_id"), d.Placeholder(1))
}

// getJobSQL builds SELECT for GetJob.
func getJobSQL(d dialect, table string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s", dialectCols(d), table, dialectCol(d, "job_id"), d.Placeholder(1))
}

// listDueJobsSQL builds SELECT with FOR UPDATE SKIP LOCKED for AcquireNextJobs.
func listDueJobsSQL(d dialect, table string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = %s AND %s = %s AND %s <= %s ORDER BY %s FOR UPDATE SKIP LOCKED",
		dialectCols(d), table,
		dialectCol(d, "state"), d.Placeholder(1),
		dialectCol(d, "enabled"), d.BooleanTrue(),
		dialectCol(d, "next_fire_time"), d.Placeholder(2),
		dialectCol(d, "next_fire_time"),
	)
}

// acquireJobSQL builds UPDATE to claim a job (WAITING -> ACQUIRED).
func acquireJobSQL(d dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = %s, %s = %s, %s = %s WHERE %s = %s AND %s = %s",
		table,
		dialectCol(d, "state"), d.Placeholder(1),
		dialectCol(d, "instance_id"), d.Placeholder(2),
		dialectCol(d, "acquired_at"), d.Placeholder(3),
		dialectCol(d, "updated_at"), d.Placeholder(4), //nolint:mnd // placeholder index
		dialectCol(d, "job_id"), d.Placeholder(5), //nolint:mnd // placeholder index
		dialectCol(d, "state"), d.Placeholder(6), //nolint:mnd // placeholder index
	)
}

// releaseJobSQL builds UPDATE to release a job (ACQUIRED -> WAITING/COMPLETE).
func releaseJobSQL(d dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = NULL, %s = NULL, %s = %s, %s = %s WHERE %s = %s",
		table,
		dialectCol(d, "state"), d.Placeholder(1),
		dialectCol(d, "instance_id"),
		dialectCol(d, "acquired_at"),
		dialectCol(d, "next_fire_time"), d.Placeholder(2),
		dialectCol(d, "updated_at"), d.Placeholder(3),
		dialectCol(d, "job_id"), d.Placeholder(4), //nolint:mnd // placeholder index
	)
}

// nextFireTimeSQL builds SELECT MIN(next_fire_time) for NextFireTime.
func nextFireTimeSQL(d dialect, table string) string {
	return fmt.Sprintf(
		"SELECT MIN(%s) FROM %s WHERE %s = %s AND %s = %s",
		dialectCol(d, "next_fire_time"), table,
		dialectCol(d, "state"), d.Placeholder(1),
		dialectCol(d, "enabled"), d.BooleanTrue(),
	)
}

// recoverStaleJobsSQL builds UPDATE to reset jobs stuck in ACQUIRED past the threshold.
func recoverStaleJobsSQL(d dialect, table string) string {
	staleExpr := d.DateAddSQL(
		dialectCol(d, "acquired_at"),
		fmt.Sprintf("GREATEST(%s, COALESCE(%s, 0))", d.Placeholder(4), dialectCol(d, "timeout_secs")), //nolint:mnd // placeholder index
	)
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = NULL, %s = NULL, %s = %s WHERE %s = %s AND %s < %s",
		table,
		dialectCol(d, "state"), d.Placeholder(1),
		dialectCol(d, "instance_id"),
		dialectCol(d, "acquired_at"),
		dialectCol(d, "updated_at"), d.Placeholder(2),
		dialectCol(d, "state"), d.Placeholder(3),
		staleExpr, d.Placeholder(5), //nolint:mnd // placeholder index
	)
}
