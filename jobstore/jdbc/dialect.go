package jdbc

import (
	"fmt"
	"strings"
)

const columns = `job_id, name, trigger_type, trigger_value, timeout_secs,
                 next_fire_time, state, instance_id, acquired_at, enabled, created_at, updated_at`

// Dialect abstracts SQL differences between databases.
type Dialect interface {
	// Placeholder returns the bind parameter for the given 1-based index.
	Placeholder(index int) string
	// BooleanTrue returns the SQL literal for true.
	BooleanTrue() string
	// SchemaSQL returns DDL for the given table prefix.
	SchemaSQL(prefix string) string
	// UpsertJobSQL returns an upsert statement.
	UpsertJobSQL(table string) string
	// DateAddSQL returns "timestamp + N seconds" expression.
	DateAddSQL(col, secondsExpr string) string
}

func col(d Dialect, name string) string {
	if _, ok := d.(Oracle); ok {
		return strings.ToUpper(name)
	}
	return name
}

func cols(d Dialect) string {
	return col(d, columns)
}

func deleteJobSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s = %s",
		table, col(d, "job_id"), d.Placeholder(1),
	)
}

func getJobSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = %s",
		cols(d), table, col(d, "job_id"), d.Placeholder(1),
	)
}

func listDueJobsSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = %s AND %s = %s AND %s <= %s ORDER BY %s FOR UPDATE SKIP LOCKED",
		cols(d), table,
		col(d, "state"), d.Placeholder(1),
		col(d, "enabled"), d.BooleanTrue(),
		col(d, "next_fire_time"), d.Placeholder(2),
		col(d, "next_fire_time"),
	)
}

func acquireJobSQL(d Dialect, table string) string {
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

func releaseJobSQL(d Dialect, table string) string {
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

func nextFireTimeSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"SELECT MIN(%s) FROM %s WHERE %s = %s AND %s = %s",
		col(d, "next_fire_time"), table,
		col(d, "state"), d.Placeholder(1),
		col(d, "enabled"), d.BooleanTrue(),
	)
}

func recoverStaleJobsSQL(d Dialect, table string) string {
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
