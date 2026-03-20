package scheduler

import "errors"

var (
	ErrJobNotFound        = errors.New("scheduler: job not found")
	ErrInvalidCron        = errors.New("scheduler: invalid cron expression")
	ErrEmptyJobID         = errors.New("scheduler: job ID must not be empty")
	ErrNegativeTimeout    = errors.New("scheduler: job timeout must not be negative")
	ErrUnsupportedTrigger = errors.New("scheduler: unsupported trigger type")
)
