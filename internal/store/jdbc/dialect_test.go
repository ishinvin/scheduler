package jdbc

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Postgres
// ---------------------------------------------------------------------------

func TestPostgresPlaceholder(t *testing.T) {
	p := Postgres{}
	if got := p.Placeholder(1); got != "$1" {
		t.Errorf("Placeholder(1) = %q, want $1", got)
	}
	if got := p.Placeholder(5); got != "$5" {
		t.Errorf("Placeholder(5) = %q, want $5", got)
	}
}

func TestPostgresDateAddSQL(t *testing.T) {
	p := Postgres{}
	got := p.DateAddSQL("acquired_at", "60")
	if !strings.Contains(got, "acquired_at") || !strings.Contains(got, "60") || !strings.Contains(got, "INTERVAL") {
		t.Errorf("DateAddSQL = %q, expected interval expression", got)
	}
}

func TestPostgresSchemaSQL(t *testing.T) {
	p := Postgres{}
	ddl := p.SchemaSQL("")
	if !strings.Contains(ddl, "scheduler_jobs") {
		t.Error("SchemaSQL should contain table name")
	}
	if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS") {
		t.Error("SchemaSQL should use IF NOT EXISTS")
	}

	ddl = p.SchemaSQL("app_")
	if !strings.Contains(ddl, "app_scheduler_jobs") {
		t.Error("SchemaSQL should apply prefix")
	}
}

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

func TestOraclePlaceholder(t *testing.T) {
	o := Oracle{}
	if got := o.Placeholder(1); got != ":1" {
		t.Errorf("Placeholder(1) = %q, want :1", got)
	}
	if got := o.Placeholder(3); got != ":3" {
		t.Errorf("Placeholder(3) = %q, want :3", got)
	}
}

func TestOracleDateAddSQL(t *testing.T) {
	o := Oracle{}
	got := o.DateAddSQL("ACQUIRED_AT", "60")
	if !strings.Contains(got, "ACQUIRED_AT") || !strings.Contains(got, "60") || !strings.Contains(got, "NUMTODSINTERVAL") {
		t.Errorf("DateAddSQL = %q, expected NUMTODSINTERVAL expression", got)
	}
}

func TestOracleSchemaSQL(t *testing.T) {
	o := Oracle{}
	ddl := o.SchemaSQL("")
	if !strings.Contains(ddl, "SCHEDULER_JOBS") {
		t.Error("SchemaSQL should contain uppercased table name")
	}

	ddl = o.SchemaSQL("app_")
	if !strings.Contains(ddl, "APP_SCHEDULER_JOBS") {
		t.Error("SchemaSQL should uppercase prefix")
	}
}

// ---------------------------------------------------------------------------
// MySQL
// ---------------------------------------------------------------------------

func TestMySQLPlaceholder(t *testing.T) {
	m := MySQL{}
	if got := m.Placeholder(1); got != "?" {
		t.Errorf("Placeholder(1) = %q, want ?", got)
	}
	if got := m.Placeholder(5); got != "?" {
		t.Errorf("Placeholder(5) = %q, want ?", got)
	}
}

func TestMySQLDateAddSQL(t *testing.T) {
	m := MySQL{}
	got := m.DateAddSQL("acquired_at", "60")
	if !strings.Contains(got, "TIMESTAMPADD") || !strings.Contains(got, "acquired_at") || !strings.Contains(got, "60") {
		t.Errorf("DateAddSQL = %q, expected TIMESTAMPADD expression", got)
	}
}

func TestMySQLSchemaSQL(t *testing.T) {
	m := MySQL{}
	ddl := m.SchemaSQL("")
	if !strings.Contains(ddl, "scheduler_jobs") {
		t.Error("SchemaSQL should contain table name")
	}
	if !strings.Contains(ddl, "InnoDB") {
		t.Error("SchemaSQL should use InnoDB engine")
	}

	ddl = m.SchemaSQL("app_")
	if !strings.Contains(ddl, "app_scheduler_jobs") {
		t.Error("SchemaSQL should apply prefix")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func TestCol(t *testing.T) {
	if got := col(Postgres{}, "job_id"); got != "job_id" {
		t.Errorf("col(Postgres, job_id) = %q, want job_id", got)
	}
	if got := col(Oracle{}, "job_id"); got != "JOB_ID" {
		t.Errorf("col(Oracle, job_id) = %q, want JOB_ID", got)
	}
}

// ---------------------------------------------------------------------------
// SQL builders
// ---------------------------------------------------------------------------

func TestSQLBuilders(t *testing.T) {
	type builderCase struct {
		name string
		got  string
		want string
	}

	pgCols := "job_id, name, trigger_type, trigger_value, timeout_secs, next_fire_time, state, instance_id, acquired_at, created_at, updated_at"
	orCols := strings.ToUpper(pgCols)

	postgres := []builderCase{
		{
			name: "insert",
			got:  insertJobSQL(Postgres{}, "jobs"),
			want: "INSERT INTO jobs (" + pgCols + ") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		},
		{
			name: "update",
			got:  updateJobSQL(Postgres{}, "jobs"),
			want: "UPDATE jobs SET name = $1, trigger_type = $2, trigger_value = $3, timeout_secs = $4, next_fire_time = $5, updated_at = $6 WHERE job_id = $7",
		},
		{
			name: "delete",
			got:  deleteJobSQL(Postgres{}, "jobs"),
			want: "DELETE FROM jobs WHERE job_id = $1",
		},
		{
			name: "get",
			got:  getJobSQL(Postgres{}, "jobs"),
			want: "SELECT " + pgCols + " FROM jobs WHERE job_id = $1",
		},
		{
			name: "listDue",
			got:  listDueJobsSQL(Postgres{}, "jobs"),
			want: "SELECT " + pgCols + " FROM jobs WHERE state = $1 AND next_fire_time <= $2 ORDER BY next_fire_time FOR UPDATE SKIP LOCKED",
		},
		{
			name: "acquire",
			got:  acquireJobSQL(Postgres{}, "jobs"),
			want: "UPDATE jobs SET state = $1, instance_id = $2, acquired_at = $3, updated_at = $4 WHERE job_id = $5 AND state = $6",
		},
		{
			name: "release",
			got:  releaseJobSQL(Postgres{}, "jobs"),
			want: "UPDATE jobs SET state = $1, instance_id = NULL, acquired_at = NULL, next_fire_time = $2, updated_at = $3 WHERE job_id = $4",
		},
		{
			name: "nextFireTime",
			got:  nextFireTimeSQL(Postgres{}, "jobs"),
			want: "SELECT MIN(next_fire_time) FROM jobs WHERE state = $1",
		},
		{
			name: "recoverStale",
			got:  recoverStaleJobsSQL(Postgres{}, "jobs"),
			want: "UPDATE jobs SET state = $1, instance_id = NULL, acquired_at = NULL, updated_at = $2 WHERE state = $3 AND acquired_at + GREATEST($4, COALESCE(timeout_secs, 0)) * INTERVAL '1 second' < $5",
		},
	}

	oracle := []builderCase{
		{
			name: "insert",
			got:  insertJobSQL(Oracle{}, "JOBS"),
			want: "INSERT INTO JOBS (" + orCols + ") VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)",
		},
		{
			name: "update",
			got:  updateJobSQL(Oracle{}, "JOBS"),
			want: "UPDATE JOBS SET NAME = :1, TRIGGER_TYPE = :2, TRIGGER_VALUE = :3, TIMEOUT_SECS = :4, NEXT_FIRE_TIME = :5, UPDATED_AT = :6 WHERE JOB_ID = :7",
		},
		{
			name: "delete",
			got:  deleteJobSQL(Oracle{}, "JOBS"),
			want: "DELETE FROM JOBS WHERE JOB_ID = :1",
		},
		{
			name: "get",
			got:  getJobSQL(Oracle{}, "JOBS"),
			want: "SELECT " + orCols + " FROM JOBS WHERE JOB_ID = :1",
		},
		{
			name: "listDue",
			got:  listDueJobsSQL(Oracle{}, "JOBS"),
			want: "SELECT " + orCols + " FROM JOBS WHERE STATE = :1 AND NEXT_FIRE_TIME <= :2 ORDER BY NEXT_FIRE_TIME FOR UPDATE SKIP LOCKED",
		},
		{
			name: "acquire",
			got:  acquireJobSQL(Oracle{}, "JOBS"),
			want: "UPDATE JOBS SET STATE = :1, INSTANCE_ID = :2, ACQUIRED_AT = :3, UPDATED_AT = :4 WHERE JOB_ID = :5 AND STATE = :6",
		},
		{
			name: "release",
			got:  releaseJobSQL(Oracle{}, "JOBS"),
			want: "UPDATE JOBS SET STATE = :1, INSTANCE_ID = NULL, ACQUIRED_AT = NULL, NEXT_FIRE_TIME = :2, UPDATED_AT = :3 WHERE JOB_ID = :4",
		},
		{
			name: "nextFireTime",
			got:  nextFireTimeSQL(Oracle{}, "JOBS"),
			want: "SELECT MIN(NEXT_FIRE_TIME) FROM JOBS WHERE STATE = :1",
		},
		{
			name: "recoverStale",
			got:  recoverStaleJobsSQL(Oracle{}, "JOBS"),
			want: "UPDATE JOBS SET STATE = :1, INSTANCE_ID = NULL, ACQUIRED_AT = NULL, UPDATED_AT = :2 WHERE STATE = :3 AND ACQUIRED_AT + NUMTODSINTERVAL(GREATEST(:4, COALESCE(TIMEOUT_SECS, 0)), 'SECOND') < :5",
		},
	}

	mysql := []builderCase{
		{
			name: "insert",
			got:  insertJobSQL(MySQL{}, "jobs"),
			want: "INSERT INTO jobs (" + pgCols + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		},
		{
			name: "update",
			got:  updateJobSQL(MySQL{}, "jobs"),
			want: "UPDATE jobs SET name = ?, trigger_type = ?, trigger_value = ?, timeout_secs = ?, next_fire_time = ?, updated_at = ? WHERE job_id = ?",
		},
		{
			name: "delete",
			got:  deleteJobSQL(MySQL{}, "jobs"),
			want: "DELETE FROM jobs WHERE job_id = ?",
		},
		{
			name: "get",
			got:  getJobSQL(MySQL{}, "jobs"),
			want: "SELECT " + pgCols + " FROM jobs WHERE job_id = ?",
		},
		{
			name: "listDue",
			got:  listDueJobsSQL(MySQL{}, "jobs"),
			want: "SELECT " + pgCols + " FROM jobs WHERE state = ? AND next_fire_time <= ? ORDER BY next_fire_time FOR UPDATE SKIP LOCKED",
		},
		{
			name: "acquire",
			got:  acquireJobSQL(MySQL{}, "jobs"),
			want: "UPDATE jobs SET state = ?, instance_id = ?, acquired_at = ?, updated_at = ? WHERE job_id = ? AND state = ?",
		},
		{
			name: "release",
			got:  releaseJobSQL(MySQL{}, "jobs"),
			want: "UPDATE jobs SET state = ?, instance_id = NULL, acquired_at = NULL, next_fire_time = ?, updated_at = ? WHERE job_id = ?",
		},
		{
			name: "nextFireTime",
			got:  nextFireTimeSQL(MySQL{}, "jobs"),
			want: "SELECT MIN(next_fire_time) FROM jobs WHERE state = ?",
		},
		{
			name: "recoverStale",
			got:  recoverStaleJobsSQL(MySQL{}, "jobs"),
			want: "UPDATE jobs SET state = ?, instance_id = NULL, acquired_at = NULL, updated_at = ? WHERE state = ? AND TIMESTAMPADD(SECOND, GREATEST(?, COALESCE(timeout_secs, 0)), acquired_at) < ?",
		},
	}

	for _, group := range []struct {
		name  string
		cases []builderCase
	}{
		{"postgres", postgres},
		{"oracle", oracle},
		{"mysql", mysql},
	} {
		t.Run(group.name, func(t *testing.T) {
			for _, tc := range group.cases {
				t.Run(tc.name, func(t *testing.T) {
					if tc.got != tc.want {
						t.Errorf("\ngot:  %s\nwant: %s", tc.got, tc.want)
					}
				})
			}
		})
	}
}
