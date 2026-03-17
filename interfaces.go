package scheduler

import (
	"context"
	"time"
)

// JobState represents the lifecycle state of a job.
type JobState string

// Job states.
const (
	StateWaiting  JobState = "WAITING"
	StateAcquired JobState = "ACQUIRED"
	StateComplete JobState = "COMPLETE"
)

// JobStore is the single persistence and coordination interface.
// Implementations handle storage, state transitions, and distributed locking internally.
//
// Two built-in implementations:
//   - memory.Store — in-memory, single-instance
//   - jdbc.Store   — SQL-backed, optional clustering via WithClustered()
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

	// Close releases any resources held by the store.
	Close() error
}

// Logger is a minimal structured logger.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// JobRecord is the serializable representation of a job in the store.
type JobRecord struct {
	ID           JobID
	Name         string
	TriggerType  string // "cron", "once", "interval"
	TriggerValue string // cron expr, RFC3339 time, or duration string
	NextFireTime time.Time
	State        JobState
	InstanceID   string // which instance owns it (when ACQUIRED)
	AcquiredAt   time.Time
	Enabled      bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ExecutionRecord captures a single run of a job.
type ExecutionRecord struct {
	JobID      JobID
	Instance   string
	StartedAt  time.Time
	FinishedAt time.Time
	Err        string
}

// JobStoreInitializer is an optional interface that a JobStore may implement
// to perform initialization (e.g., schema creation) before the scheduler starts.
type JobStoreInitializer interface {
	// Init is called once by the scheduler before the run loop begins.
	Init(ctx context.Context) error
}
