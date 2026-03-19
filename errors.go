package scheduler

import (
	"errors"

	"github.com/ishinvin/scheduler/internal/store"
)

var (
	ErrJobNotFound = store.ErrJobNotFound
	ErrInvalidCron = errors.New("scheduler: invalid cron expression")
)
