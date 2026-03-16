package scheduler

import "errors"

var (
	ErrJobNotFound      = errors.New("scheduler: job not found")
	ErrJobAlreadyExists = errors.New("scheduler: job already exists")
	ErrLockNotAcquired  = errors.New("scheduler: lock not acquired")
	ErrHandlerNotFound  = errors.New("scheduler: no handler registered for job")
	ErrInvalidCron      = errors.New("scheduler: invalid cron expression")
)
