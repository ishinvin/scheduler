package scheduler

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/ishinvin/scheduler/dialect"
	"github.com/ishinvin/scheduler/internal/store/jdbc"
	"github.com/ishinvin/scheduler/internal/store/memory"
)

// Option configures a scheduler.
type Option func(*scheduler)

// WithMemory uses an in-memory job store. Suitable for single-instance use.
func WithMemory() Option {
	return func(sc *scheduler) { sc.store = memory.NewMemory() }
}

// WithJDBC uses a SQL-backed job store with a built-in dialect ("postgres", "oracle", "mysql").
// Falls back to postgres if the dialect is unknown.
func WithJDBC(db *sql.DB, dialectName, tablePrefix string) Option {
	return func(sc *scheduler) {
		var d dialect.Dialect
		switch dialectName {
		case "postgres":
			d = jdbc.Postgres{}
		case "oracle":
			d = jdbc.Oracle{}
		case "mysql":
			d = jdbc.MySQL{}
		default:
			sc.logWarn("unknown dialect, falling back to postgres", "dialect", dialectName)
			d = jdbc.Postgres{}
		}
		sc.store = jdbc.NewJDBC(db, d, tablePrefix)
	}
}

// WithCustomJDBC uses a SQL-backed job store with a custom Dialect implementation.
func WithCustomJDBC(db *sql.DB, d dialect.Dialect, tablePrefix string) Option {
	return func(sc *scheduler) {
		sc.store = jdbc.NewJDBC(db, d, tablePrefix)
	}
}

// WithLogger sets a custom slog.Logger. Default is slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(sc *scheduler) { sc.logger = logger }
}

// WithVerbose enables info-level logging. Default only logs warn/error.
func WithVerbose() Option {
	return func(sc *scheduler) { sc.verbose = true }
}

// WithInstanceID sets the instance identifier for distributed tracking.
func WithInstanceID(id string) Option {
	return func(sc *scheduler) { sc.instanceID = id }
}

// WithMisfireThreshold sets how long a job can stay ACQUIRED before recovery. Default 1m.
// Set to 0 to disable.
func WithMisfireThreshold(d time.Duration) Option {
	return func(sc *scheduler) { sc.misfireThreshold = d }
}

// WithShutdownTimeout sets how long Run() waits for in-flight jobs on shutdown. Default 30s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(sc *scheduler) {
		if d > 0 {
			sc.shutdownTimeout = d
		} else {
			sc.logWarn("ignoring non-positive shutdown timeout", "value", d)
		}
	}
}

// WithCleanupTimeout sets the timeout for ReleaseJob after execution. Default 5s.
func WithCleanupTimeout(d time.Duration) Option {
	return func(sc *scheduler) {
		if d > 0 {
			sc.cleanupTimeout = d
		} else {
			sc.logWarn("ignoring non-positive cleanup timeout", "value", d)
		}
	}
}

// WithOnError sets a callback invoked when a job execution returns an error.
func WithOnError(fn func(jobID string, err error)) Option {
	return func(sc *scheduler) { sc.onError = fn }
}

// WithPollInterval sets the maximum time between store polls. Default 15s.
func WithPollInterval(d time.Duration) Option {
	return func(sc *scheduler) {
		if d > 0 {
			sc.pollInterval = d
		} else {
			sc.logWarn("ignoring non-positive poll interval", "value", d)
		}
	}
}
