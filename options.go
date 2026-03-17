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
// being considered stale and recovered back to WAITING. Default is 1 minute.
// Jobs with a timeout longer than this threshold use their timeout instead.
// Set to 0 to disable stale job recovery.
func WithMisfireThreshold(d time.Duration) Option {
	return func(sc *Scheduler) { sc.misfireThreshold = d }
}

// WithShutdownTimeout sets the maximum time Run() waits for in-flight jobs
// to finish after the context is canceled. Default is 30s.
// If the timeout is exceeded, Run() returns and logs a warning.
func WithShutdownTimeout(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.shutdownTimeout = d
		}
	}
}

// WithCleanupTimeout sets the maximum time for post-execution cleanup
// (ReleaseJob + RecordExecution) after a job finishes. Default is 5s.
// Uses a detached context so cleanup completes even during shutdown.
func WithCleanupTimeout(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.cleanupTimeout = d
		}
	}
}

// WithPollInterval sets the maximum time between store polls. Default is 15s.
// Signals handle prompt wake-up for job completion, register, reschedule, and delete.
// This interval acts as a safety fallback for missed signals or cross-instance pickup.
func WithPollInterval(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.pollInterval = d
		}
	}
}

// WithExecutionRetention sets how long execution records are kept before
// automatic purging. Default is 0 (no purge — records are kept forever).
// When set, the scheduler periodically deletes records older than the retention period.
func WithExecutionRetention(d time.Duration) Option {
	return func(sc *Scheduler) {
		if d > 0 {
			sc.executionRetention = d
		}
	}
}
