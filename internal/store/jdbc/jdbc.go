package jdbc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ishinvin/scheduler/dialect"
	"github.com/ishinvin/scheduler/internal/store"
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

// jdbcStore implements JobStore backed by a SQL database.
type jdbcStore struct {
	db          *sql.DB
	dialect     dialect.Dialect
	tablePrefix string
	q           jdbcQueries
}

// NewJDBC creates a new JDBC-backed job store.
func NewJDBC(db *sql.DB, d dialect.Dialect, tablePrefix string) store.JobStore {
	s := &jdbcStore{
		db:          db,
		dialect:     d,
		tablePrefix: tablePrefix,
	}
	t := col(d, s.tablePrefix+"scheduler_jobs")
	s.q = jdbcQueries{
		insert:       insertJobSQL(d, t),
		update:       updateJobSQL(d, t),
		delete:       deleteJobSQL(d, t),
		get:          getJobSQL(d, t),
		listDue:      listDueJobsSQL(d, t),
		acquire:      acquireJobSQL(d, t),
		release:      releaseJobSQL(d, t),
		nextFireTime: nextFireTimeSQL(d, t),
		recoverStale: recoverStaleJobsSQL(d, t),
	}
	return s
}

func (s *jdbcStore) CreateSchema(ctx context.Context) error {
	ddl := s.dialect.SchemaSQL(s.tablePrefix)
	for _, stmt := range strings.Split(ddl, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			// Oracle lacks IF NOT EXISTS; ORA-00955 means object already exists.
			if _, ok := s.dialect.(Oracle); ok && strings.Contains(err.Error(), "ORA-00955") {
				continue
			}
			return fmt.Errorf("jdbc store: create schema: %w", err)
		}
	}
	return nil
}

func (s *jdbcStore) CreateJob(ctx context.Context, job *store.JobRecord) error {
	now := time.Now()
	timeoutSecs := int64(job.Timeout / time.Second)
	var nextFire any = job.NextFireTime
	if job.NextFireTime.IsZero() {
		nextFire = nil
	}
	if _, err := s.db.ExecContext(ctx, s.q.insert, job.ID, job.Name, job.TriggerType, job.TriggerValue, timeoutSecs, nextFire, string(store.StateWaiting), nil, nil, now, now); err != nil { //nolint:lll // readable
		return fmt.Errorf("jdbc store: create job: %w", err)
	}
	return nil
}

func (s *jdbcStore) UpdateJob(ctx context.Context, job *store.JobRecord) error {
	timeoutSecs := int64(job.Timeout / time.Second)
	var nextFire any = job.NextFireTime
	if job.NextFireTime.IsZero() {
		nextFire = nil
	}
	if _, err := s.db.ExecContext(ctx, s.q.update, job.Name, job.TriggerType, job.TriggerValue, timeoutSecs, nextFire, time.Now(), job.ID); err != nil { //nolint:lll // readable
		return fmt.Errorf("jdbc store: update job: %w", err)
	}
	return nil
}

func (s *jdbcStore) DeleteJob(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, s.q.delete, id)
	if err != nil {
		return fmt.Errorf("jdbc store: delete job: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("jdbc store: delete job rows affected: %w", err)
	}
	if n == 0 {
		return store.ErrJobNotFound
	}
	return nil
}

func (s *jdbcStore) GetJob(ctx context.Context, id string) (*store.JobRecord, error) {
	row := s.db.QueryRowContext(ctx, s.q.get, id)
	rec, err := s.scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("jdbc store: get job: %w", err)
	}
	return rec, nil
}

// AcquireNextJobs uses SELECT FOR UPDATE SKIP LOCKED + UPDATE within a single
// transaction to atomically find and claim due jobs.
func (s *jdbcStore) AcquireNextJobs(ctx context.Context, now time.Time, instanceID string) ([]*store.JobRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("jdbc store: begin tx: %w", err)
	}

	rows, err := tx.QueryContext(ctx, s.q.listDue, string(store.StateWaiting), now)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("jdbc store: list due jobs: %w", err)
	}
	defer rows.Close()

	var dueJobs []*store.JobRecord
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
	var acquired []*store.JobRecord
	for _, rec := range dueJobs {
		result, err := tx.ExecContext(ctx, s.q.acquire, string(store.StateAcquired), instanceID, ts, ts, rec.ID, string(store.StateWaiting))
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("jdbc store: claim job %s: %w", rec.ID, err)
		}
		n, _ := result.RowsAffected()
		if n > 0 {
			rec.State = store.StateAcquired
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

func (s *jdbcStore) ReleaseJob(ctx context.Context, id string, nextFireTime time.Time) error {
	state := string(store.StateWaiting)
	var nextFire any = nextFireTime
	if nextFireTime.IsZero() {
		state = string(store.StateComplete)
		nextFire = nil
	}
	if _, err := s.db.ExecContext(ctx, s.q.release, state, nextFire, time.Now(), id); err != nil {
		return fmt.Errorf("jdbc store: release job: %w", err)
	}
	return nil
}

func (s *jdbcStore) RecoverStaleJobs(ctx context.Context, threshold time.Duration) (int, error) {
	now := time.Now()
	thresholdSecs := int64(threshold / time.Second)
	result, err := s.db.ExecContext(ctx, s.q.recoverStale, string(store.StateWaiting), now, string(store.StateAcquired), thresholdSecs, now)
	if err != nil {
		return 0, fmt.Errorf("jdbc store: recover stale jobs: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

func (s *jdbcStore) NextFireTime(ctx context.Context) (time.Time, error) {
	var t sql.NullTime
	if err := s.db.QueryRowContext(ctx, s.q.nextFireTime, string(store.StateWaiting)).Scan(&t); err != nil {
		return time.Time{}, fmt.Errorf("jdbc store: next fire time: %w", err)
	}
	return t.Time, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func (*jdbcStore) scanJob(sc scanner) (*store.JobRecord, error) {
	var rec store.JobRecord
	var timeoutSecs int64
	var state string
	var nextFireTime sql.NullTime
	var instanceID sql.NullString
	var acquiredAt sql.NullTime
	if err := sc.Scan(&rec.ID, &rec.Name, &rec.TriggerType, &rec.TriggerValue, &timeoutSecs, &nextFireTime, &state, &instanceID, &acquiredAt, &rec.CreatedAt, &rec.UpdatedAt); err != nil { //nolint:lll // column list
		return nil, err
	}
	rec.State = store.JobState(state)
	rec.NextFireTime = nextFireTime.Time
	rec.Timeout = time.Duration(timeoutSecs) * time.Second
	rec.InstanceID = instanceID.String
	rec.AcquiredAt = acquiredAt.Time
	return &rec, nil
}
