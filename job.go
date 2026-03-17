package scheduler

import (
	"context"
	"time"
)

// JobID is a unique string identifier for a job.
type JobID string

// JobState represents the lifecycle state of a job.
type JobState string

// Job states.
const (
	StateWaiting  JobState = "WAITING"
	StateAcquired JobState = "ACQUIRED"
	StateComplete JobState = "COMPLETE"
)

// Job is the unit of work the scheduler manages.
type Job struct {
	ID      JobID
	Name    string
	Trigger Trigger
	Fn      func(ctx context.Context) error
	Timeout time.Duration // per-execution timeout; 0 = no timeout
}

// JobRecord is the serializable representation of a job in the store.
type JobRecord struct {
	ID           JobID
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

// ExecutionRecord captures a single run of a job.
type ExecutionRecord struct {
	JobID      JobID
	Instance   string
	StartedAt  time.Time
	FinishedAt time.Time
	Err        string
}
