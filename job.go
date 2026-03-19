package scheduler

import (
	"context"
	"time"
)

// JobID is a unique string identifier for a job.
type JobID string

// Job is the unit of work the scheduler manages.
type Job struct {
	ID      JobID
	Name    string
	Trigger Trigger
	Fn      func(ctx context.Context) error
	Timeout time.Duration // per-execution timeout; 0 = no timeout
}
