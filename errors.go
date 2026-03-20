package scheduler

import "errors"

var (
	ErrJobNotFound         = errors.New("scheduler: job not found")
	ErrInvalidCron         = errors.New("scheduler: invalid cron expression")
	ErrEmptyJobID          = errors.New("scheduler: job ID must not be empty")
	ErrNegativeTimeout     = errors.New("scheduler: job timeout must not be negative")
	ErrUnsupportedTrigger  = errors.New("scheduler: unsupported trigger type")
	ErrEmptyJob            = errors.New("scheduler: job must have at least a Fn or Trigger")
	ErrNonPositiveInterval = errors.New("scheduler: interval must be positive")
	ErrNoStore             = errors.New("scheduler: no job store configured")
	ErrNilTrigger          = errors.New("scheduler: reschedule requires a trigger")
	ErrAlreadyRunning      = errors.New("scheduler: already running")
)
