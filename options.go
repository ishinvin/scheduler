package scheduler

import "time"

// Option configures a Scheduler.
type Option func(*Scheduler)

// WithJobStore sets the job store implementation.
func WithJobStore(s JobStore) Option {
	return func(sc *Scheduler) { sc.store = s }
}

// WithVerbose enables logging via slog. Default is silent.
func WithVerbose() Option {
	return func(sc *Scheduler) { sc.verbose = true }
}

// WithLocation sets the time zone for cron evaluation.
func WithLocation(loc *time.Location) Option {
	return func(sc *Scheduler) { sc.location = loc }
}

// WithInstanceID sets the instance identifier for distributed tracking.
func WithInstanceID(id string) Option {
	return func(sc *Scheduler) { sc.instanceID = id }
}

// WithMisfireThreshold sets how long a job can stay ACQUIRED before recovery. Default 1m.
// Set to 0 to disable.
func WithMisfireThreshold(d time.Duration) Option {
	return func(sc *Scheduler) { sc.misfireThreshold = d }
}

// WithShutdownTimeout sets how long Run() waits for in-flight jobs on shutdown. Default 30s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.shutdownTimeout = d
		}
	}
}

// WithCleanupTimeout sets the timeout for ReleaseJob after execution. Default 5s.
func WithCleanupTimeout(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.cleanupTimeout = d
		}
	}
}

// WithPollInterval sets the maximum time between store polls. Default 15s.
func WithPollInterval(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.pollInterval = d
		}
	}
}
