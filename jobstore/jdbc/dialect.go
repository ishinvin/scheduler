package jdbc

import (
	"fmt"
	"strings"
)

// columns is the column list for SELECT queries on the jobs table.
// All databases treat unquoted identifiers as case-insensitive,
// so lowercase works everywhere (Postgres, Oracle, MySQL, etc.).
const columns = `job_id, name, trigger_type, trigger_value, timeout_secs,
		        next_fire_time, state, instance_id, acquired_at, enabled, created_at, updated_at`

// Dialect abstracts SQL differences between databases.
// Implement only the truly database-specific methods — the store generates
// standard CRUD queries from Placeholder and BooleanTrue.
//
// Users can implement this interface to support additional databases (e.g., MySQL).
type Dialect interface {
	// Placeholder returns the SQL placeholder for the given 1-based parameter index.
	// e.g., "?" for MySQL, "$1" for Postgres, ":1" for Oracle.
	Placeholder(index int) string

	// BooleanTrue returns the SQL literal for true ("TRUE" for Postgres, "1" for Oracle/MySQL).
	BooleanTrue() string

	// SchemaSQL returns the full DDL string for the given table prefix.
	SchemaSQL(prefix string) string

	// UpsertJobSQL returns an upsert statement for the jobs table.
	UpsertJobSQL(table string) string

	// DateAddSQL returns a SQL expression for "timestamp + N seconds".
	// e.g., Postgres: "col + secs * INTERVAL '1 second'"
	//       Oracle:   "col + NUMTODSINTERVAL(secs, 'SECOND')"
	//       MySQL:    "TIMESTAMPADD(SECOND, secs, col)"
	DateAddSQL(col, secondsExpr string) string
}

// col returns the identifier in the dialect's preferred casing.
// Oracle uses uppercase; all others use lowercase as-is.
func col(d Dialect, name string) string {
	if _, ok := d.(Oracle); ok {
		return strings.ToUpper(name)
	}
	return name
}

// cols returns the column list in the dialect's preferred casing.
func cols(d Dialect) string {
	return col(d, columns)
}

// --- Query generators (used by store.go) ---

func deleteJobSQL(d Dialect, table string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
		table, col(d, "job_id"), d.Placeholder(1))
}

func getJobSQL(d Dialect, table string) string {
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s",
		cols(d), table, col(d, "job_id"), d.Placeholder(1))
}

func listDueJobsSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s = %s AND %s = %s AND %s <= %s ORDER BY %s FOR UPDATE SKIP LOCKED",
		cols(d), table,
		col(d, "state"), d.Placeholder(1),
		col(d, "enabled"), d.BooleanTrue(),
		col(d, "next_fire_time"), d.Placeholder(2),
		col(d, "next_fire_time"))
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
		col(d, "state"), d.Placeholder(6)) //nolint:mnd // placeholder index
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
		col(d, "job_id"), d.Placeholder(4)) //nolint:mnd // placeholder index
}

func insertExecutionSQL(d Dialect, table string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s)",
		table,
		col(d, "job_id"), col(d, "instance_id"), col(d, "started_at"), col(d, "finished_at"), col(d, "error"),
		d.Placeholder(1), d.Placeholder(2), d.Placeholder(3), d.Placeholder(4), d.Placeholder(5)) //nolint:mnd // placeholder indices
}

func nextFireTimeSQL(d Dialect, table string) string {
	return fmt.Sprintf("SELECT MIN(%s) FROM %s WHERE %s = %s AND %s = %s",
		col(d, "next_fire_time"), table,
		col(d, "state"), d.Placeholder(1),
		col(d, "enabled"), d.BooleanTrue())
}

func purgeExecutionsSQL(d Dialect, table string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s < %s",
		table, col(d, "finished_at"), d.Placeholder(1))
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
		staleExpr, d.Placeholder(5)) //nolint:mnd // placeholder index
}
