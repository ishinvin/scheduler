# scheduler

A job scheduling library for Go with pluggable job stores and multi-instance support.

## Features

- **Cron, interval, and one-time triggers** — 6-field cron expressions (second granularity), fixed intervals, or fire-once at a specific time
- **Register / Reschedule / Delete** jobs at runtime
- **Built-in stores** — memory (single-instance) or JDBC (SQL-backed, multi-instance safe)
- **Custom database support** — implement the `Dialect` interface to add any SQL database
- **Store-driven dispatch** — the scheduler polls the store for due jobs each cycle, so schedule changes are immediately visible
- **Adaptive polling** — uses `NextFireTime` to sleep exactly until the next job is due, with `pollInterval` as a safety fallback
- **Fair job distribution** — `FOR UPDATE SKIP LOCKED` ensures concurrent instances each get disjoint subsets of due jobs
- **Per-job optimistic locking** — `WHERE state = 'WAITING'` ensures only one instance acquires each job, no separate lock table needed
- **Crash recovery** — stale jobs stuck in ACQUIRED state are automatically recovered back to WAITING, respecting per-job timeouts
- **Context-based lifecycle** — `New(ctx)` accepts the server context; `Run()` blocks until the context is canceled, with graceful shutdown

## Installation

```bash
go get github.com/ishinvin/scheduler
```

## Quick Start

### Single Instance (Memory Store)

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os/signal"
    "syscall"
    "time"

    "github.com/ishinvin/scheduler"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    sched, err := scheduler.New(ctx,
        scheduler.WithMemoryStore(),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Register a cron job with a 30s execution timeout
    sched.Register(scheduler.Job{
        ID:      "daily-report",
        Name:    "Generate daily report",
        Trigger: must(scheduler.NewCronTrigger("0 0 9 * * *")),
        Timeout: 30 * time.Second,
        Fn: func(ctx context.Context) error {
            fmt.Println("generating report...")
            return nil
        },
    })

    // Register an interval job
    sched.Register(scheduler.Job{
        ID:      "health-check",
        Name:    "Health check",
        Trigger: must(scheduler.NewIntervalTrigger(30 * time.Second)),
        Fn: func(ctx context.Context) error {
            fmt.Println("checking health...")
            return nil
        },
    })

    // Start the scheduler. Blocks until signal.
    log.Fatal(sched.Run())
}

func must[T any](v T, err error) T {
    if err != nil {
        panic(err)
    }
    return v
}
```

### Multi-Instance (PostgreSQL)

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "os/signal"
    "syscall"

    _ "github.com/lib/pq"
    "github.com/ishinvin/scheduler"
)

func main() {
    db, _ := sql.Open("postgres", "postgres://localhost/mydb?sslmode=disable")

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    sched, err := scheduler.New(ctx,
        scheduler.WithJDBC(db, "postgres", ""),
        scheduler.WithInitializeSchema(),
        scheduler.WithInstanceID("worker-1"),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Register a job. Idempotent — if the job already exists in the store,
    // only the Fn handler is registered (safe for multi-instance restarts).
    sched.Register(scheduler.Job{
        ID:      "welcome-email",
        Name:    "Send welcome emails",
        Trigger: must(scheduler.NewCronTrigger("0 */5 * * * *")),
        Fn: func(ctx context.Context) error {
            // send email logic
            return nil
        },
    })

    // Start the scheduler. Blocks until signal.
    log.Fatal(sched.Run())
}
```

### Custom Dialect

Implement `dialect.Dialect` and use `WithCustomJDBC`:

```go
sched, _ := scheduler.New(ctx,
    scheduler.WithCustomJDBC(db, MyDialect{}, ""),
    scheduler.WithInitializeSchema(),
)
```

## Architecture

### How It Works

The scheduler uses a **store-driven dispatch** model:

1. The run loop queries `NextFireTime` to sleep precisely until the next job is due (capped by `pollInterval` as a safety fallback)
2. `AcquireNextJobs(now, instanceID)` atomically selects due jobs and claims them (WAITING -> ACQUIRED) in a single transaction
3. `FOR UPDATE SKIP LOCKED` distributes jobs fairly across concurrent instances — each sees only unlocked rows
4. Each job's UPDATE uses `WHERE state = 'WAITING'` as a safety net against edge cases
5. For each acquired job, the scheduler resolves the `Fn` handler from an in-memory registry
6. Executes the function, then releases the job with the next fire time

The store is always the source of truth — there is no in-memory scheduling state.

### Supported Databases

| Database   | Option                                     |
| ---------- | ------------------------------------------ |
| Memory     | `WithMemoryStore()`                        |
| PostgreSQL | `WithJDBC(db, "postgres", tablePrefix)`    |
| Oracle     | `WithJDBC(db, "oracle", tablePrefix)`      |
| MySQL      | `WithJDBC(db, "mysql", tablePrefix)`       |
| Custom     | `WithCustomJDBC(db, dialect, tablePrefix)` |

### JDBC Store — Acquire Flow

```
BEGIN tx
  SELECT ... FROM scheduler_jobs
    WHERE state = 'WAITING' AND next_fire_time <= now
    FOR UPDATE SKIP LOCKED
  UPDATE scheduler_jobs SET state = 'ACQUIRED' ...
    WHERE job_id = ? AND state = 'WAITING'  (per job)
COMMIT
```

`FOR UPDATE SKIP LOCKED` ensures concurrent instances each get disjoint subsets of due jobs. The `AND state = 'WAITING'` condition on each UPDATE acts as a safety net — if two instances somehow see the same job, only one gets `RowsAffected = 1`.

## API Reference

### Scheduler

```go
// Create a new scheduler
sched, err := scheduler.New(ctx, opts ...Option)

// Register a new job (idempotent — safe for multi-instance restarts)
sched.Register(job Job) error

// Create or update a job with a new trigger
sched.Reschedule(job Job) error

// Check if a job exists
sched.Exists(id string) (bool, error)

// Remove a job
sched.Delete(id string) error

// Run the scheduler (blocks until the context is canceled)
sched.Run() error
```

### Options

```go
// Store options (pick one)
scheduler.WithMemoryStore()                          // In-memory store (single-instance)
scheduler.WithJDBC(db, "postgres", "")               // PostgreSQL store
scheduler.WithJDBC(db, "oracle", "")                 // Oracle store
scheduler.WithJDBC(db, "mysql", "")                  // MySQL store
scheduler.WithCustomJDBC(db, dialect, "")            // Custom SQL dialect

// Schema
scheduler.WithInitializeSchema()                     // Auto-create tables on startup

// Logging
scheduler.WithLogger(logger)                         // Custom *slog.Logger (default: slog.Default())
scheduler.WithVerbose()                              // Enable info-level logs (default: warn/error only)

// Scheduler options
scheduler.WithInstanceID(id)                         // Set instance ID (default: hostname-pid)
scheduler.WithPollInterval(d)                        // Safety fallback poll interval (default: 15s)
scheduler.WithMisfireThreshold(d)                    // Stale job recovery threshold (default: 1m)
scheduler.WithShutdownTimeout(d)                     // Max wait for in-flight jobs on shutdown (default: 30s)
scheduler.WithCleanupTimeout(d)                      // Max wait for post-execution DB cleanup (default: 5s)
scheduler.WithOnError(func(jobID string, err error)) // Callback on job execution failure
```

### Triggers

```go
// Cron expression (6-field: second minute hour dom month dow + descriptors like @hourly)
trigger, err := scheduler.NewCronTrigger("0 0 */6 * * *")

// Fixed interval
trigger, err := scheduler.NewIntervalTrigger(30 * time.Second)

// Fire once at a specific time
trigger := scheduler.NewOnceTrigger(time.Now().Add(5 * time.Minute))
```

## Examples

See the [\_examples/](_examples/) directory:

- [\_examples/memory/](_examples/memory/) — single-instance with cron, interval, and once triggers
- [\_examples/postgres/](_examples/postgres/) — multi-instance with PostgreSQL
- [\_examples/oracle/](_examples/oracle/) — multi-instance with Oracle
- [\_examples/mysql/](_examples/mysql/) — MySQL example
- [\_examples/multi-instance/](_examples/multi-instance/) — Docker Compose with 3 replicas sharing PostgreSQL

## License

MIT
