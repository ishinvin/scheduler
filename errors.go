package scheduler

import "errors"

var (
	ErrNoStore             = errors.New("scheduler: no job store configured")
	ErrEmptyJob            = errors.New("scheduler: job must have at least a Fn or Trigger")
	ErrNilTrigger          = errors.New("scheduler: reschedule requires a trigger")
	ErrEmptyJobID          = errors.New("scheduler: job ID must not be empty")
	ErrJobNotFound         = errors.New("scheduler: job not found")
	ErrInvalidCron         = errors.New("scheduler: invalid cron expression")
	ErrAlreadyRunning      = errors.New("scheduler: already running")
	ErrNegativeTimeout     = errors.New("scheduler: job timeout must not be negative")
	ErrNonPositiveInterval = errors.New("scheduler: interval must be positive")
	ErrUnsupportedTrigger  = errors.New("scheduler: unsupported trigger type")
)
