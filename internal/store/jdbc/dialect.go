package jdbc

import (
	"fmt"
	"strings"

	"github.com/ishinvin/scheduler/dialect"
)

const jdbcColumns = `job_id, name, trigger_type, trigger_value, timeout_secs, next_fire_time, state, instance_id, acquired_at, created_at, updated_at` //nolint:lll // column list

// col returns a column name, uppercased for Oracle.
func col(d dialect.Dialect, name string) string {
	if _, ok := d.(Oracle); ok {
		return strings.ToUpper(name)
	}
	return name
}

// cols returns all columns, used in SELECT and INSERT.
func cols(d dialect.Dialect) string {
	return col(d, jdbcColumns)
}

// insertJobSQL builds INSERT for CreateJob.
func insertJobSQL(d dialect.Dialect, table string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
		table, cols(d),
		d.Placeholder(1), d.Placeholder(2), d.Placeholder(3),
		d.Placeholder(4), d.Placeholder(5), d.Placeholder(6), //nolint:mnd // placeholder index
		d.Placeholder(7), d.Placeholder(8), d.Placeholder(9), //nolint:mnd // placeholder index
		d.Placeholder(10), d.Placeholder(11), //nolint:mnd // placeholder index
	)
}

// updateJobSQL builds UPDATE for UpdateJob.
func updateJobSQL(d dialect.Dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s WHERE %s = %s",
		table,
		col(d, "name"), d.Placeholder(1),
		col(d, "trigger_type"), d.Placeholder(2),
		col(d, "trigger_value"), d.Placeholder(3),
		col(d, "timeout_secs"), d.Placeholder(4), //nolint:mnd // placeholder index
		col(d, "next_fire_time"), d.Placeholder(5), //nolint:mnd // placeholder index
		col(d, "updated_at"), d.Placeholder(6), //nolint:mnd // placeholder index
		col(d, "job_id"), d.Placeholder(7), //nolint:mnd // placeholder index
	)
}

// deleteJobSQL builds DELETE for DeleteJob.
func deleteJobSQL(d dialect.Dialect, table string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = %s", table, col(d, "job_id"), d.Placeholder(1))
}

// getJobSQL builds SELECT for GetJob.
func getJobSQL(d dialect.Dialect, table string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s", cols(d), table, col(d, "job_id"), d.Placeholder(1))
}

// listDueJobsSQL builds SELECT with FOR UPDATE SKIP LOCKED for AcquireNextJobs.
func listDueJobsSQL(d dialect.Dialect, table string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = %s AND %s <= %s ORDER BY %s FOR UPDATE SKIP LOCKED",
		cols(d), table,
		col(d, "state"), d.Placeholder(1),
		col(d, "next_fire_time"), d.Placeholder(2),
		col(d, "next_fire_time"),
	)
}

// acquireJobSQL builds UPDATE to claim a job (WAITING -> ACQUIRED).
func acquireJobSQL(d dialect.Dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = %s, %s = %s, %s = %s WHERE %s = %s AND %s = %s",
		table,
		col(d, "state"), d.Placeholder(1),
		col(d, "instance_id"), d.Placeholder(2),
		col(d, "acquired_at"), d.Placeholder(3),
		col(d, "updated_at"), d.Placeholder(4), //nolint:mnd // placeholder index
		col(d, "job_id"), d.Placeholder(5), //nolint:mnd // placeholder index
		col(d, "state"), d.Placeholder(6), //nolint:mnd // placeholder index
	)
}

// releaseJobSQL builds UPDATE to release a job (ACQUIRED -> WAITING/COMPLETE).
func releaseJobSQL(d dialect.Dialect, table string) string {
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = NULL, %s = NULL, %s = %s, %s = %s WHERE %s = %s",
		table,
		col(d, "state"), d.Placeholder(1),
		col(d, "instance_id"),
		col(d, "acquired_at"),
		col(d, "next_fire_time"), d.Placeholder(2),
		col(d, "updated_at"), d.Placeholder(3),
		col(d, "job_id"), d.Placeholder(4), //nolint:mnd // placeholder index
	)
}

// nextFireTimeSQL builds SELECT MIN(next_fire_time) for NextFireTime.
func nextFireTimeSQL(d dialect.Dialect, table string) string {
	return fmt.Sprintf(
		"SELECT MIN(%s) FROM %s WHERE %s = %s",
		col(d, "next_fire_time"), table,
		col(d, "state"), d.Placeholder(1),
	)
}

// recoverStaleJobsSQL builds UPDATE to reset jobs stuck in ACQUIRED past the threshold.
func recoverStaleJobsSQL(d dialect.Dialect, table string) string {
	staleExpr := d.DateAddSQL(
		col(d, "acquired_at"),
		fmt.Sprintf("GREATEST(%s, COALESCE(%s, 0))", d.Placeholder(4), col(d, "timeout_secs")), //nolint:mnd // placeholder index
	)
	return fmt.Sprintf(
		"UPDATE %s SET %s = %s, %s = NULL, %s = NULL, %s = %s WHERE %s = %s AND %s < %s",
		table,
		col(d, "state"), d.Placeholder(1),
		col(d, "instance_id"),
		col(d, "acquired_at"),
		col(d, "updated_at"), d.Placeholder(2),
		col(d, "state"), d.Placeholder(3),
		staleExpr, d.Placeholder(5), //nolint:mnd // placeholder index
	)
}
