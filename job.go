package scheduler

import (
	"context"
	"time"
)

// Job is the unit of work the scheduler manages.
type Job struct {
	ID      string
	Name    string
	Trigger Trigger
	Fn      func(ctx context.Context) error
	Timeout time.Duration // per-execution timeout; 0 = no timeout
}
