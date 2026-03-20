package scheduler

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/ishinvin/scheduler/dialect"
	"github.com/ishinvin/scheduler/internal/store/jdbc"
	"github.com/ishinvin/scheduler/internal/store/memory"
)

// Option configures a scheduler.
type Option func(*scheduler)

// WithMemoryStore uses an in-memory job store. Suitable for single-instance use.
func WithMemoryStore() Option {
	return func(sc *scheduler) { sc.store = memory.NewMemory() }
}

// WithJDBC uses a SQL-backed job store with a built-in dialect ("postgres", "oracle").
func WithJDBC(db *sql.DB, dialectName, tablePrefix string) Option {
	return func(sc *scheduler) {
		var d dialect.Dialect
		switch dialectName {
		case "postgres":
			d = jdbc.Postgres{}
		case "oracle":
			d = jdbc.Oracle{}
		default:
			panic(fmt.Sprintf("scheduler: unknown dialect %q", dialectName))
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

// WithInitializeSchema enables automatic table creation on startup for the JDBC store.
func WithInitializeSchema() Option {
	return func(sc *scheduler) { sc.initSchema = true }
}

// WithVerbose enables logging via slog. Default is silent.
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
		}
	}
}

// WithCleanupTimeout sets the timeout for ReleaseJob after execution. Default 5s.
func WithCleanupTimeout(d time.Duration) Option {
	return func(sc *scheduler) {
		if d > 0 {
			sc.cleanupTimeout = d
		}
	}
}

// WithPollInterval sets the maximum time between store polls. Default 15s.
func WithPollInterval(d time.Duration) Option {
	return func(sc *scheduler) {
		if d > 0 {
			sc.pollInterval = d
		}
	}
}
