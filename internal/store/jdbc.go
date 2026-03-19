package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// jdbcQueries holds cached SQL strings, built once in NewJDBC.
type jdbcQueries struct {
	insert       string
	update       string
	delete       string
	get          string
	listDue      string
	acquire      string
	release      string
	nextFireTime string
	recoverStale string
}

// JDBCStore implements JobStore backed by a SQL database.
type JDBCStore struct {
	db               *sql.DB
	dialect          dialect
	tablePrefix      string
	initializeSchema InitializeSchema
	q                jdbcQueries
}

// NewJDBC creates a new JDBC-backed job store.
// The dialect parameter must implement the scheduler.Dialect interface.
func NewJDBC(db *sql.DB, d any, tablePrefix string, initSchema InitializeSchema) *JDBCStore {
	dial := d.(dialect)
	s := &JDBCStore{
		db:               db,
		dialect:          dial,
		tablePrefix:      tablePrefix,
		initializeSchema: initSchema,
	}
	t := dialectCol(dial, s.tablePrefix+"scheduler_jobs")
	s.q = jdbcQueries{
		insert:       insertJobSQL(dial, t),
		update:       updateJobSQL(dial, t),
		delete:       deleteJobSQL(dial, t),
		get:          getJobSQL(dial, t),
		listDue:      listDueJobsSQL(dial, t),
		acquire:      acquireJobSQL(dial, t),
		release:      releaseJobSQL(dial, t),
		nextFireTime: nextFireTimeSQL(dial, t),
		recoverStale: recoverStaleJobsSQL(dial, t),
	}
	return s
}

// SchemaSQL returns the DDL for the store's table.
func (s *JDBCStore) SchemaSQL() string {
	return s.dialect.SchemaSQL(s.tablePrefix)
}

func (s *JDBCStore) CreateSchema(ctx context.Context) error {
	ddl := s.dialect.SchemaSQL(s.tablePrefix)
	for _, stmt := range strings.Split(ddl, ";") {
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

func (s *JDBCStore) CreateJob(ctx context.Context, job *JobRecord) error {
	now := time.Now()
	timeoutSecs := int64(job.Timeout / time.Second)
	if _, err := s.db.ExecContext(ctx, s.q.insert, job.ID, job.Name, job.TriggerType, job.TriggerValue, timeoutSecs, job.NextFireTime, StateWaiting, "", nil, job.Enabled, now, now); err != nil { //nolint:lll // readable
		return fmt.Errorf("jdbc store: create job: %w", err)
	}
	return nil
}

func (s *JDBCStore) UpdateJob(ctx context.Context, job *JobRecord) error {
	timeoutSecs := int64(job.Timeout / time.Second)
	if _, err := s.db.ExecContext(ctx, s.q.update, job.Name, job.TriggerType, job.TriggerValue, timeoutSecs, job.NextFireTime, job.Enabled, time.Now(), job.ID); err != nil { //nolint:lll // readable
		return fmt.Errorf("jdbc store: update job: %w", err)
	}
	return nil
}

func (s *JDBCStore) DeleteJob(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, s.q.delete, id)
	if err != nil {
		return fmt.Errorf("jdbc store: delete job: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrJobNotFound
	}
	return nil
}

func (s *JDBCStore) GetJob(ctx context.Context, id string) (*JobRecord, error) {
	row := s.db.QueryRowContext(ctx, s.q.get, id)
	rec, err := s.scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("jdbc store: get job: %w", err)
	}
	return rec, nil
}

// AcquireNextJobs uses SELECT FOR UPDATE SKIP LOCKED + UPDATE within a single
// transaction to atomically find and claim due jobs.
func (s *JDBCStore) AcquireNextJobs(ctx context.Context, now time.Time, instanceID string) ([]*JobRecord, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("jdbc store: get conn: %w", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("jdbc store: begin tx: %w", err)
	}

	rows, err := tx.QueryContext(ctx, s.q.listDue, StateWaiting, now)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("jdbc store: list due jobs: %w", err)
	}
	defer rows.Close()

	var dueJobs []*JobRecord
	for rows.Next() {
		rec, err := s.scanJob(rows)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("jdbc store: scan due job: %w", err)
		}
		dueJobs = append(dueJobs, rec)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("jdbc store: iterate due jobs: %w", err)
	}

	if len(dueJobs) == 0 {
		_ = tx.Rollback()
		return nil, nil
	}

	ts := time.Now()
	var acquired []*JobRecord
	for _, rec := range dueJobs {
		result, err := tx.ExecContext(ctx, s.q.acquire, StateAcquired, instanceID, ts, ts, rec.ID, StateWaiting)
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("jdbc store: claim job %s: %w", rec.ID, err)
		}
		n, _ := result.RowsAffected()
		if n > 0 {
			rec.State = StateAcquired
			rec.InstanceID = instanceID
			rec.AcquiredAt = ts
			acquired = append(acquired, rec)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("jdbc store: commit acquire: %w", err)
	}
	return acquired, nil
}

func (s *JDBCStore) ReleaseJob(ctx context.Context, id string, nextFireTime time.Time) error {
	state := StateWaiting
	if nextFireTime.IsZero() {
		state = StateComplete
	}
	if _, err := s.db.ExecContext(ctx, s.q.release, state, nextFireTime, time.Now(), id); err != nil {
		return fmt.Errorf("jdbc store: release job: %w", err)
	}
	return nil
}

func (s *JDBCStore) RecoverStaleJobs(ctx context.Context, threshold time.Duration) (int, error) {
	now := time.Now()
	thresholdSecs := int64(threshold / time.Second)
	result, err := s.db.ExecContext(ctx, s.q.recoverStale, StateWaiting, now, StateAcquired, thresholdSecs, now)
	if err != nil {
		return 0, fmt.Errorf("jdbc store: recover stale jobs: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

func (s *JDBCStore) NextFireTime(ctx context.Context) (time.Time, error) {
	var t sql.NullTime
	if err := s.db.QueryRowContext(ctx, s.q.nextFireTime, StateWaiting).Scan(&t); err != nil {
		return time.Time{}, fmt.Errorf("jdbc store: next fire time: %w", err)
	}
	return t.Time, nil
}

func (s *JDBCStore) Init(ctx context.Context) error {
	if s.initializeSchema == InitSchemaAlways {
		return s.CreateSchema(ctx)
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func (*JDBCStore) scanJob(sc scanner) (*JobRecord, error) {
	var rec JobRecord
	var timeoutSecs int64
	var instanceID sql.NullString
	var acquiredAt sql.NullTime
	if err := sc.Scan(&rec.ID, &rec.Name, &rec.TriggerType, &rec.TriggerValue, &timeoutSecs, &rec.NextFireTime, &rec.State, &instanceID, &acquiredAt, &rec.Enabled, &rec.CreatedAt, &rec.UpdatedAt); err != nil { //nolint:lll // column list
		return nil, err
	}
	rec.Timeout = time.Duration(timeoutSecs) * time.Second
	rec.InstanceID = instanceID.String
	rec.AcquiredAt = acquiredAt.Time
	return &rec, nil
}
