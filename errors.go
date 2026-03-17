package scheduler

import "errors"

var (
	ErrJobNotFound      = errors.New("scheduler: job not found")
	ErrJobAlreadyExists = errors.New("scheduler: job already exists")
	ErrInvalidCron      = errors.New("scheduler: invalid cron expression")
)
