package scheduler

import (
	"context"
	"time"
)

// JobStore is the single persistence and coordination interface.
// Implementations handle storage and state transitions internally.
//
// Two built-in implementations:
//   - memory.Store — in-memory, single-instance
//   - jdbc.Store   — SQL-backed, safe for multi-instance deployments
type JobStore interface {
	// SaveJob persists or updates a job definition.
	SaveJob(ctx context.Context, rec *JobRecord) error

	// DeleteJob removes a job by ID.
	DeleteJob(ctx context.Context, id JobID) error

	// GetJob retrieves a single job record.
	GetJob(ctx context.Context, id JobID) (*JobRecord, error)

	// AcquireNextJobs atomically finds due jobs and claims them for execution
	// (WAITING → ACQUIRED) in a single operation. Returns already-acquired records.
	// Called each poll cycle by the scheduler's run loop.
	AcquireNextJobs(ctx context.Context, now time.Time, instanceID string) ([]*JobRecord, error)

	// ReleaseJob transitions a job back to WAITING with updated next fire time.
	// Called after execution completes.
	ReleaseJob(ctx context.Context, id JobID, nextFireTime time.Time) error

	// RecordExecution logs a completed execution for audit.
	RecordExecution(ctx context.Context, exec *ExecutionRecord) error

	// RecoverStaleJobs resets jobs stuck in ACQUIRED state longer than the
	// given threshold back to WAITING. Returns the number of recovered jobs.
	RecoverStaleJobs(ctx context.Context, threshold time.Duration) (int, error)

	// NextFireTime returns the earliest next_fire_time among WAITING enabled jobs.
	// Returns zero time if no jobs are scheduled.
	NextFireTime(ctx context.Context) (time.Time, error)

	// PurgeExecutions deletes execution records older than the given time.
	// Returns the number of deleted records.
	PurgeExecutions(ctx context.Context, before time.Time) (int, error)

	// Close releases any resources held by the store.
	Close() error
}

// JobStoreInitializer is an optional interface that a JobStore may implement
// to perform initialization (e.g., schema creation) before the scheduler starts.
type JobStoreInitializer interface {
	// Init is called once by the scheduler before the run loop begins.
	Init(ctx context.Context) error
}
