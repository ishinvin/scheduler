package store

import (
	"context"
	"errors"
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

// ErrJobNotFound is returned when a job is not found in the store.
var ErrJobNotFound = errors.New("scheduler: job not found")

// InitializeSchema controls whether the store creates tables on startup.
type InitializeSchema string

const (
	InitSchemaNever  InitializeSchema = "never"
	InitSchemaAlways InitializeSchema = "always"
)

// JobRecord is the serializable representation of a job in the store.
type JobRecord struct {
	ID           string
	Name         string
	TriggerType  string        // "cron", "once", "interval"
	TriggerValue string        // cron expr, RFC3339 time, or duration string
	Timeout      time.Duration // per-execution timeout; 0 = no timeout
	NextFireTime time.Time
	State        JobState
	InstanceID   string // which instance owns it (when ACQUIRED)
	AcquiredAt   time.Time
	Enabled      bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// JobStore is the persistence interface for job scheduling.
type JobStore interface {
	// CreateJob persists a new job record to the store.
	CreateJob(ctx context.Context, rec *JobRecord) error

	// UpdateJob updates an existing job record in the store.
	UpdateJob(ctx context.Context, rec *JobRecord) error

	// DeleteJob removes a job by ID.
	DeleteJob(ctx context.Context, id string) error

	// GetJob retrieves a single job record.
	GetJob(ctx context.Context, id string) (*JobRecord, error)

	// AcquireNextJobs finds due jobs and claims them (WAITING -> ACQUIRED).
	AcquireNextJobs(ctx context.Context, now time.Time, instanceID string) ([]*JobRecord, error)

	// ReleaseJob transitions a job back to WAITING with updated next fire time.
	ReleaseJob(ctx context.Context, id string, nextFireTime time.Time) error

	// RecoverStaleJobs resets jobs stuck in ACQUIRED longer than the threshold.
	RecoverStaleJobs(ctx context.Context, threshold time.Duration) (int, error)

	// NextFireTime returns the earliest next_fire_time among WAITING enabled jobs.
	NextFireTime(ctx context.Context) (time.Time, error)
}

// JobStoreInitializer is an optional interface for store initialization.
type JobStoreInitializer interface {
	// Init is called once by the scheduler before the run loop begins.
	Init(ctx context.Context) error
}
