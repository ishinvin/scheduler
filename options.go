package scheduler

import "time"

// Option configures a Scheduler.
type Option func(*Scheduler)

// WithJobStore sets the job store implementation.
// Use ram.New() for single-instance or jdbc.New() for clustered deployments.
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

// WithClusterMode enables DB-driven dispatch for multi-instance deployments.
// The checkinInterval controls how often the scheduler polls the database for
// due jobs. Lower values reduce job
// fire latency but increase database load. Pass 0 to use the default (15s).
//
// Without cluster mode (default), the scheduler persists to the store but uses
// the in-memory map for scheduling decisions — faster, but schedule changes from
// other instances are not visible until restart.
func WithClusterMode(checkinInterval time.Duration) Option {
	return func(sc *Scheduler) {
		sc.clusterMode = true
		if checkinInterval > 0 {
			sc.clusterCheckinInterval = checkinInterval
		}
	}
}
