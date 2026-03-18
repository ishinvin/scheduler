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

// InitializeSchema controls whether the store creates tables on startup.
type InitializeSchema string

const (
	InitSchemaNever  InitializeSchema = "never"
	InitSchemaAlways InitializeSchema = "always"
)

// Store implements scheduler.JobStore backed by a SQL database.
type Store struct {
	db               *sql.DB
	dialect          Dialect
	instanceID       string
	tablePrefix      string
	initializeSchema InitializeSchema
}

func (s *Store) table(name string) string {
	return col(s.dialect, s.tablePrefix+name)
}

func (s *Store) jobsTable() string { return s.table("scheduler_jobs") }

type Option func(*Store)

func WithTablePrefix(prefix string) Option {
	return func(s *Store) { s.tablePrefix = prefix }
}

func WithInstanceID(id string) Option {
	return func(s *Store) { s.instanceID = id }
}

func WithInitializeSchema(mode InitializeSchema) Option {
	return func(s *Store) { s.initializeSchema = mode }
}

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

func (s *Store) CreateSchema(ctx context.Context) error {
	ddl := s.dialect.SchemaSQL(s.tablePrefix)
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

func splitStatements(ddl string) []string {
	return strings.Split(ddl, ";")
}

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

func (s *Store) selectAndClaimDueJobs(ctx context.Context, tx *sql.Tx, now time.Time, instanceID string) ([]*scheduler.JobRecord, error) {
	selectQuery := listDueJobsSQL(s.dialect, s.jobsTable())
	rows, err := tx.QueryContext(ctx, selectQuery, scheduler.StateWaiting, now)
	if err != nil {
		return nil, fmt.Errorf("jdbc store: list due jobs: %w", err)
	}
	defer rows.Close()

	var dueJobs []*scheduler.JobRecord
	for rows.Next() {
		rec, err := s.scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("jdbc store: scan due job: %w", err)
		}
		dueJobs = append(dueJobs, rec)
	}
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

func (s *Store) NextFireTime(ctx context.Context) (time.Time, error) {
	query := nextFireTimeSQL(s.dialect, s.jobsTable())
	var t sql.NullTime
	if err := s.db.QueryRowContext(ctx, query, scheduler.StateWaiting).Scan(&t); err != nil {
		return time.Time{}, fmt.Errorf("jdbc store: next fire time: %w", err)
	}
	return t.Time, nil
}

func (s *Store) Init(ctx context.Context) error {
	if s.initializeSchema == InitSchemaAlways {
		return s.CreateSchema(ctx)
	}
	return nil
}

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
