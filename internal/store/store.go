package store

import (
	"context"
	"errors"
	"time"
)

// ErrJobNotFound is returned when a job is not found in the store.
var ErrJobNotFound = errors.New("scheduler: job not found")

// JobState represents the lifecycle state of a job.
type JobState string

// Job states.
const (
	StateWaiting  JobState = "WAITING"
	StateAcquired JobState = "ACQUIRED"
	StateComplete JobState = "COMPLETE"
)

// JobRecord is the serializable representation of a job in the store.
type JobRecord struct {
	ID           string
	Name         string
	InstanceID   string // which instance owns it (when ACQUIRED)
	TriggerType  string // "cron", "once", "interval"
	TriggerValue string // cron expr, RFC3339 time, or duration string
	State        JobState
	Timeout      time.Duration // per-execution timeout; 0 = no timeout
	AcquiredAt   time.Time
	NextFireTime time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// JobStore is the persistence interface for job scheduling.
type JobStore interface {
	// CreateSchema creates the required tables/schema. No-op if not applicable.
	CreateSchema(ctx context.Context) error

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

	// NextFireTime returns the earliest next_fire_time among WAITING jobs.
	NextFireTime(ctx context.Context) (time.Time, error)
}
