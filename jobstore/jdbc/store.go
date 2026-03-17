package jdbc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ishinvin/scheduler"
)

// InitializeSchema controls whether the JDBC store creates tables on startup.
type InitializeSchema string

const (
	// InitSchemaNever means the library never creates tables.
	// The user (or a migration tool like Liquibase/Flyway) manages the schema.
	InitSchemaNever InitializeSchema = "never"

	// InitSchemaAlways means the library creates tables on first use via CreateSchema().
	// Safe to call repeatedly — uses IF NOT EXISTS / MERGE idioms.
	InitSchemaAlways InitializeSchema = "always"
)

// Store implements scheduler.JobStore backed by a SQL database.
// Per-job optimistic locking (WHERE state = 'WAITING') ensures safe concurrent
// access across multiple scheduler instances without a separate lock table.
//
// Tables required:
//   - {prefix}scheduler_jobs       — job definitions with state and next_fire_time
//   - {prefix}scheduler_executions — execution audit log
type Store struct {
	db               *sql.DB
	dialect          Dialect
	instanceID       string
	tablePrefix      string
	initializeSchema InitializeSchema
}

// table returns a fully qualified table name with prefix in the dialect's preferred casing.
func (s *Store) table(name string) string {
	return col(s.dialect, s.tablePrefix+name)
}

func (s *Store) jobsTable() string  { return s.table("scheduler_jobs") }
func (s *Store) execsTable() string { return s.table("scheduler_executions") }

// Option configures a JDBC Store.
type Option func(*Store)

// WithTablePrefix sets a prefix for all table names.
// e.g., WithTablePrefix("myapp_") → myapp_scheduler_jobs.
func WithTablePrefix(prefix string) Option {
	return func(s *Store) { s.tablePrefix = prefix }
}

// WithInstanceID sets the instance identifier for distributed tracking.
func WithInstanceID(id string) Option {
	return func(s *Store) { s.instanceID = id }
}

// WithInitializeSchema controls whether the store creates tables on startup.
//
//   - InitSchemaNever (default) — the user manages the schema externally
//     (e.g., Liquibase, Flyway, or manual DDL). Use SchemaSQL() or
//     Postgres{}.SchemaSQL() / Oracle{}.SchemaSQL() to get the DDL.
//   - InitSchemaAlways — the store calls CreateSchema() automatically when
//     the scheduler starts. Safe to call repeatedly (uses IF NOT EXISTS).
func WithInitializeSchema(mode InitializeSchema) Option {
	return func(s *Store) { s.initializeSchema = mode }
}

// New creates a new JDBC-backed job store.
func New(db *sql.DB, dialect Dialect, opts ...Option) *Store {
	s := &Store{
		db:               db,
		dialect:          dialect,
		instanceID:       "default",
		initializeSchema: InitSchemaNever,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// CreateSchema executes the DDL statements to create all scheduler tables.
// Safe to call repeatedly — uses IF NOT EXISTS / MERGE.
func (s *Store) CreateSchema(ctx context.Context) error {
	ddl := s.SchemaSQL()
	for _, stmt := range splitStatements(ddl) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("jdbc store: create schema: %w", err)
		}
	}
	return nil
}

// SchemaSQL returns the full DDL string for this store's dialect and table prefix.
func (s *Store) SchemaSQL() string {
	return s.dialect.SchemaSQL(s.tablePrefix)
}

// splitStatements splits a multi-statement DDL string by semicolons.
func splitStatements(ddl string) []string {
	return strings.Split(ddl, ";")
}

// --- scheduler.JobStore implementation ---

// SaveJob persists or updates a job definition.
func (s *Store) SaveJob(ctx context.Context, job *scheduler.JobRecord) error {
	query := s.dialect.UpsertJobSQL(s.jobsTable())
	now := time.Now()

	timeoutSecs := int64(job.Timeout / time.Second)

	_, err := s.db.ExecContext(ctx, query,
		string(job.ID),
		job.Name,
		job.TriggerType,
		job.TriggerValue,
		timeoutSecs,
		job.NextFireTime,
		scheduler.StateWaiting,
		job.Enabled,
		now,
		now,
	)
	if err != nil {
		return fmt.Errorf("jdbc store: save job: %w", err)
	}
	return nil
}

// DeleteJob removes a job by ID.
func (s *Store) DeleteJob(ctx context.Context, id scheduler.JobID) error {
	query := deleteJobSQL(s.dialect, s.jobsTable())
	result, err := s.db.ExecContext(ctx, query, string(id))
	if err != nil {
		return fmt.Errorf("jdbc store: delete job: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return scheduler.ErrJobNotFound
	}
	return nil
}

// GetJob retrieves a single job record.
func (s *Store) GetJob(ctx context.Context, id scheduler.JobID) (*scheduler.JobRecord, error) {
	query := getJobSQL(s.dialect, s.jobsTable())
	row := s.db.QueryRowContext(ctx, query, string(id))
	rec, err := s.scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, scheduler.ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("jdbc store: get job: %w", err)
	}
	return rec, nil
}

// AcquireNextJobs finds due jobs and claims them (WAITING → ACQUIRED) in a single transaction.
// Uses FOR UPDATE SKIP LOCKED so concurrent instances each see only unlocked rows,
// distributing work fairly. The per-row UPDATE still includes WHERE state = 'WAITING'
// as a safety net against edge cases (e.g., recovery between SELECT and UPDATE).
//
// Flow:
//  1. BEGIN tx
//  2. SELECT due jobs ... FOR UPDATE SKIP LOCKED
//  3. UPDATE each job SET state = 'ACQUIRED' WHERE state = 'WAITING'
//  4. COMMIT
func (s *Store) AcquireNextJobs(ctx context.Context, now time.Time, instanceID string) ([]*scheduler.JobRecord, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("jdbc store: get conn: %w", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("jdbc store: begin tx: %w", err)
	}

	acquired, err := s.selectAndClaimDueJobs(ctx, tx, now, instanceID)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("jdbc store: commit acquire: %w", err)
	}
	return acquired, nil
}

// selectAndClaimDueJobs queries for due jobs and updates each to ACQUIRED within the given tx.
func (s *Store) selectAndClaimDueJobs(ctx context.Context, tx *sql.Tx, now time.Time, instanceID string) ([]*scheduler.JobRecord, error) {
	selectQuery := listDueJobsSQL(s.dialect, s.jobsTable())
	rows, err := tx.QueryContext(ctx, selectQuery, scheduler.StateWaiting, now)
	if err != nil {
		return nil, fmt.Errorf("jdbc store: list due jobs: %w", err)
	}

	var dueJobs []*scheduler.JobRecord
	for rows.Next() {
		rec, err := s.scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("jdbc store: scan due job: %w", err)
		}
		dueJobs = append(dueJobs, rec)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("jdbc store: iterate due jobs: %w", err)
	}

	if len(dueJobs) == 0 {
		return nil, nil
	}

	ts := time.Now()
	claimQuery := acquireJobSQL(s.dialect, s.jobsTable())
	var acquired []*scheduler.JobRecord
	for _, rec := range dueJobs {
		result, err := tx.ExecContext(ctx, claimQuery,
			scheduler.StateAcquired, instanceID, ts, ts, string(rec.ID), scheduler.StateWaiting,
		)
		if err != nil {
			return nil, fmt.Errorf("jdbc store: claim job %s: %w", rec.ID, err)
		}
		n, _ := result.RowsAffected()
		if n > 0 {
			rec.State = scheduler.StateAcquired
			rec.InstanceID = instanceID
			rec.AcquiredAt = ts
			acquired = append(acquired, rec)
		}
	}
	return acquired, nil
}

// ReleaseJob transitions a job from ACQUIRED back to WAITING with the new next fire time.
// If nextFireTime is zero (e.g., OnceTrigger completed), the job is set to COMPLETE.
func (s *Store) ReleaseJob(ctx context.Context, id scheduler.JobID, nextFireTime time.Time) error {
	state := scheduler.StateWaiting
	if nextFireTime.IsZero() {
		state = scheduler.StateComplete
	}
	query := releaseJobSQL(s.dialect, s.jobsTable())
	_, err := s.db.ExecContext(ctx, query,
		state, nextFireTime, time.Now(), string(id),
	)
	if err != nil {
		return fmt.Errorf("jdbc store: release job: %w", err)
	}
	return nil
}

// RecordExecution logs a completed execution for audit.
func (s *Store) RecordExecution(ctx context.Context, exec *scheduler.ExecutionRecord) error {
	query := insertExecutionSQL(s.dialect, s.execsTable())
	_, err := s.db.ExecContext(ctx, query,
		string(exec.JobID), exec.Instance, exec.StartedAt, exec.FinishedAt, exec.Err,
	)
	if err != nil {
		return fmt.Errorf("jdbc store: record execution: %w", err)
	}
	return nil
}

// RecoverStaleJobs resets jobs stuck in ACQUIRED state longer than the threshold.
// For jobs with a timeout_secs longer than the threshold, the timeout is used instead
// to avoid recovering jobs that are still legitimately running.
func (s *Store) RecoverStaleJobs(ctx context.Context, threshold time.Duration) (int, error) {
	now := time.Now()
	thresholdSecs := int64(threshold / time.Second)
	query := recoverStaleJobsSQL(s.dialect, s.jobsTable())
	result, err := s.db.ExecContext(ctx, query,
		scheduler.StateWaiting, now, scheduler.StateAcquired, thresholdSecs, now,
	)
	if err != nil {
		return 0, fmt.Errorf("jdbc store: recover stale jobs: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// NextFireTime returns the earliest next_fire_time among WAITING enabled jobs.
func (s *Store) NextFireTime(ctx context.Context) (time.Time, error) {
	query := nextFireTimeSQL(s.dialect, s.jobsTable())
	var t sql.NullTime
	if err := s.db.QueryRowContext(ctx, query, scheduler.StateWaiting).Scan(&t); err != nil {
		return time.Time{}, fmt.Errorf("jdbc store: next fire time: %w", err)
	}
	return t.Time, nil
}

// PurgeExecutions deletes execution records older than the given time.
func (s *Store) PurgeExecutions(ctx context.Context, before time.Time) (int, error) {
	query := purgeExecutionsSQL(s.dialect, s.execsTable())
	result, err := s.db.ExecContext(ctx, query, before)
	if err != nil {
		return 0, fmt.Errorf("jdbc store: purge executions: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// Close releases any resources held by the store.
func (*Store) Close() error {
	return nil
}

// Init implements scheduler.JobStoreInitializer.
// When InitializeSchema is set to InitSchemaAlways, it creates the schema.
func (s *Store) Init(ctx context.Context) error {
	if s.initializeSchema == InitSchemaAlways {
		return s.CreateSchema(ctx)
	}
	return nil
}

// --- scan helpers ---

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func (*Store) scanJob(sc scanner) (*scheduler.JobRecord, error) {
	var rec scheduler.JobRecord
	var id string
	var timeoutSecs int64
	var instanceID sql.NullString
	var acquiredAt sql.NullTime
	err := sc.Scan(&id, &rec.Name, &rec.TriggerType, &rec.TriggerValue, &timeoutSecs,
		&rec.NextFireTime, &rec.State, &instanceID, &acquiredAt, &rec.Enabled,
		&rec.CreatedAt, &rec.UpdatedAt)
	if err != nil {
		return nil, err
	}
	rec.ID = scheduler.JobID(id)
	rec.Timeout = time.Duration(timeoutSecs) * time.Second
	rec.InstanceID = instanceID.String
	rec.AcquiredAt = acquiredAt.Time
	return &rec, nil
}
