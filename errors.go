package scheduler

import "errors"

var (
	ErrJobNotFound = errors.New("scheduler: job not found")
	ErrInvalidCron = errors.New("scheduler: invalid cron expression")
)
