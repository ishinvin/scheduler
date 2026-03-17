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
// Controls whether the JDBC store creates tables on startup.
type InitializeSchema string

const (
	// InitSchemaNever means the library never creates tables.
	// The user (or a migration tool like Liquibase/Flyway) manages the schema.
	InitSchemaNever InitializeSchema = "never"

	// InitSchemaAlways means the library creates tables on first use via CreateSchema().
	// Safe to call repeatedly — uses IF NOT EXISTS / MERGE idioms.
	InitSchemaAlways InitializeSchema = "always"
)

// LockTriggerAccess is the named lock row used by AcquireNextJobs in clustered mode.
const LockTriggerAccess = "TRIGGER_ACCESS"

// Store implements scheduler.JobStore using a JDBC approach:
// named lock rows (TRIGGER_ACCESS) protect atomic state transitions on the jobs table.
//
// Tables required:
//   - {prefix}scheduler_jobs       — job definitions with state and next_fire_time
//   - {prefix}scheduler_locks      — named lock rows (TRIGGER_ACCESS)
//   - {prefix}scheduler_executions — execution audit log
type Store struct {
	db               *sql.DB
	dialect          Dialect
	instanceID       string
	tablePrefix      string
	initializeSchema InitializeSchema
	clustered        bool
}

// table returns a fully qualified table name with prefix, using the dialect's
// identifier casing (e.g., lowercase for Postgres, uppercase for Oracle).
func (s *Store) table(name string) string {
	return s.dialect.Col(s.tablePrefix + name)
}

func (s *Store) jobsTable() string  { return s.table("scheduler_jobs") }
func (s *Store) locksTable() string { return s.table("scheduler_locks") }
func (s *Store) execsTable() string { return s.table("scheduler_executions") }

// Option configures a JDBC Store.
type Option func(*Store)

// WithTablePrefix sets a prefix for all table names.
// The dialect's Col() method applies the correct identifier casing.
// e.g., WithTablePrefix("myapp_") → myapp_scheduler_jobs (Postgres) or MYAPP_SCHEDULER_JOBS (Oracle).
func WithTablePrefix(prefix string) Option {
	return func(s *Store) { s.tablePrefix = prefix }
}

// WithInstanceID sets the instance identifier for distributed tracking.
func WithInstanceID(id string) Option {
	return func(s *Store) { s.instanceID = id }
}

// WithClustered enables table-level locking in AcquireNextJobs for multi-instance
// deployments. Without this, the store skips the TRIGGER_ACCESS lock —
// faster for single-instance, but unsafe with concurrent schedulers.
func WithClustered() Option {
	return func(s *Store) { s.clustered = true }
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

// CreateSchema executes the DDL statements to create all scheduler tables
// and seed the lock rows. Safe to call repeatedly — uses IF NOT EXISTS / MERGE.
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

	var timeoutStr string
	if job.Timeout > 0 {
		timeoutStr = job.Timeout.String()
	}

	_, err := s.db.ExecContext(ctx, query,
		string(job.ID),
		job.Name,
		job.TriggerType,
		job.TriggerValue,
		timeoutStr,
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

// ListJobs returns all persisted jobs.
func (s *Store) ListJobs(ctx context.Context) ([]*scheduler.JobRecord, error) {
	query := listJobsSQL(s.dialect, s.jobsTable())
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("jdbc store: list jobs: %w", err)
	}
	defer rows.Close() // best-effort close

	var result []*scheduler.JobRecord
	for rows.Next() {
		rec, err := s.scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("jdbc store: scan job: %w", err)
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

// AcquireNextJobs finds due jobs and claims them (WAITING → ACQUIRED) in a single transaction.
//
// In clustered mode, it acquires the TRIGGER_ACCESS row lock first to serialize
// across instances. In single-instance mode, the lock table is skipped.
//
// Flow:
//  1. BEGIN tx
//  2. [clustered] SELECT ... FROM scheduler_locks FOR UPDATE NOWAIT
//  3. SELECT due jobs WHERE state = 'WAITING' AND enabled AND next_fire_time <= now
//  4. UPDATE each job SET state = 'ACQUIRED'
//  5. COMMIT
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

	// Clustered: acquire TRIGGER_ACCESS row lock to serialize across instances.
	if s.clustered {
		lockQuery := s.dialect.LockRowSQL(s.locksTable())
		var lockName string
		err = tx.QueryRowContext(ctx, lockQuery, LockTriggerAccess).Scan(&lockName)
		if err != nil {
			_ = tx.Rollback()
			if s.dialect.IsLockNotAvailable(err) {
				return nil, nil // another instance is working; not an error
			}
			return nil, fmt.Errorf("jdbc store: acquire trigger lock: %w", err)
		}
	}

	// Select due jobs and claim them.
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

// RecoverStaleJobs resets jobs stuck in ACQUIRED state longer than the threshold
// back to WAITING. Returns the number of recovered jobs.
func (s *Store) RecoverStaleJobs(ctx context.Context, threshold time.Duration) (int, error) {
	cutoff := time.Now().Add(-threshold)
	query := recoverStaleJobsSQL(s.dialect, s.jobsTable())
	result, err := s.db.ExecContext(ctx, query,
		scheduler.StateWaiting, time.Now(), scheduler.StateAcquired, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("jdbc store: recover stale jobs: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
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
	var timeoutStr string
	var instanceID sql.NullString
	var acquiredAt sql.NullTime
	err := sc.Scan(&id, &rec.Name, &rec.TriggerType, &rec.TriggerValue, &timeoutStr,
		&rec.NextFireTime, &rec.State, &instanceID, &acquiredAt, &rec.Enabled,
		&rec.CreatedAt, &rec.UpdatedAt)
	if err != nil {
		return nil, err
	}
	rec.ID = scheduler.JobID(id)
	if timeoutStr != "" {
		rec.Timeout, _ = time.ParseDuration(timeoutStr)
	}
	rec.InstanceID = instanceID.String
	rec.AcquiredAt = acquiredAt.Time
	return &rec, nil
}
