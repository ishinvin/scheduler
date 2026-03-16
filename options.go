package scheduler

import "time"

// Option configures a Scheduler.
type Option func(*Scheduler)

// WithJobStore sets the job store implementation.
// Use memory.New() for single-instance or jdbc.New() for clustered deployments.
func WithJobStore(s JobStore) Option {
	return func(sc *Scheduler) { sc.store = s }
}

// WithLogger sets the logger.
func WithLogger(l Logger) Option {
	return func(sc *Scheduler) { sc.logger = l }
}

// WithLocation sets the time zone for cron evaluation.
func WithLocation(loc *time.Location) Option {
	return func(sc *Scheduler) { sc.location = loc }
}

// WithInstanceID sets the instance identifier for distributed tracking.
func WithInstanceID(id string) Option {
	return func(sc *Scheduler) { sc.instanceID = id }
}

// WithMisfireThreshold sets how long a job can stay in ACQUIRED state before
// being considered stale and recovered back to WAITING. Default is 10 minutes.
// Set to 0 to disable stale job recovery.
func WithMisfireThreshold(d time.Duration) Option {
	return func(sc *Scheduler) { sc.misfireThreshold = d }
}

// WithPollInterval sets how often the scheduler polls the store for due jobs.
// Lower values reduce job fire latency but increase store load. Default is 15s.
func WithPollInterval(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.pollInterval = d
		}
	}
}
