# scheduler

A job scheduling library for Go with pluggable job stores and multi-instance support.

## Features

- **Cron, interval, and one-time triggers** — standard 5-field cron expressions, fixed intervals, or fire-once at a specific time
- **Register / Reschedule / Delete** jobs at runtime
- **Pluggable JobStore** — choose between memory (single-instance) or JDBC (SQL-backed, multi-instance safe)
- **Store-driven dispatch** — the scheduler polls the store for due jobs each cycle, so schedule changes are immediately visible
- **Per-job optimistic locking** — `WHERE state = 'WAITING'` ensures only one instance acquires each job, no separate lock table needed
- **Context-based lifecycle** — `Run(ctx)` blocks until the context is canceled, with graceful shutdown

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
    "github.com/ishinvin/scheduler/jobstore/memory"
)

func main() {
    ctx := context.Background()

    sched, err := scheduler.New(ctx,
        scheduler.WithJobStore(memory.New()),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Register a cron job with a 30s execution timeout
    sched.Register(ctx, scheduler.Job{
        ID:      "daily-report",
        Name:    "Generate daily report",
        Trigger: must(scheduler.NewCronTrigger("0 9 * * *")),
        Timeout: 30 * time.Second, // cancel if execution exceeds 30s
        Fn: func(ctx context.Context) error {
            fmt.Println("generating report...")
            return nil
        },
    })

    // Register an interval job
    sched.Register(ctx, scheduler.Job{
        ID:      "health-check",
        Name:    "Health check",
        Trigger: scheduler.NewIntervalTrigger(30 * time.Second),
        Fn: func(ctx context.Context) error {
            fmt.Println("checking health...")
            return nil
        },
    })

    // Start the scheduler. Blocks until signal.
    ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    log.Fatal(sched.Run(ctx))
}

func must[T any](v T, err error) T {
    if err != nil {
        panic(err)
    }
    return v
}
```

### Multi-Instance (JDBC Store + PostgreSQL)

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
    "github.com/ishinvin/scheduler/jobstore/jdbc"
)

func main() {
    db, _ := sql.Open("postgres", "postgres://localhost/mydb?sslmode=disable")

    store := jdbc.New(db, jdbc.Postgres{},
        jdbc.WithInstanceID("worker-1"),
    )

    ctx := context.Background()

    sched, err := scheduler.New(ctx,
        scheduler.WithJobStore(store),
        scheduler.WithInstanceID("worker-1"),
    )
    if err != nil {
        log.Fatal(err)
    }

    // Register a job. Fn is stored by job ID so rehydrated jobs
    // can resolve it on restart.
    sched.Register(ctx, scheduler.Job{
        ID:      "welcome-email",
        Name:    "Send welcome emails",
        Trigger: must(scheduler.NewCronTrigger("*/5 * * * *")),
        Fn: func(ctx context.Context) error {
            // send email logic
            return nil
        },
    })

    // Start the scheduler. Blocks until signal.
    ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    log.Fatal(sched.Run(ctx))
}
```

## Architecture

### How It Works

The scheduler uses a **store-driven dispatch** model:

1. The run loop polls the store every `pollInterval` (default 15s) via `AcquireNextJobs(now, instanceID)`
2. The store atomically selects due jobs and claims them (WAITING → ACQUIRED) in a single transaction
3. Each job's UPDATE uses `WHERE state = 'WAITING'` as an optimistic lock — only one instance wins
4. For each acquired job, the scheduler resolves the `Fn` handler from an in-memory registry
5. Executes the function, then calls `ReleaseJob` to update the next fire time

The store is always the source of truth — there is no in-memory scheduling state.

### JobStore Interface

The single `JobStore` interface handles both persistence and coordination:

```go
type JobStore interface {
    SaveJob(ctx context.Context, rec *JobRecord) error
    DeleteJob(ctx context.Context, id JobID) error
    GetJob(ctx context.Context, id JobID) (*JobRecord, error)
    AcquireNextJobs(ctx context.Context, now time.Time, instanceID string) ([]*JobRecord, error)
    ReleaseJob(ctx context.Context, id JobID, nextFireTime time.Time) error
    RecordExecution(ctx context.Context, exec *ExecutionRecord) error
    RecoverStaleJobs(ctx context.Context, threshold time.Duration) (int, error)
    Close() error
}
```

### Job Store Types

| Store      | Package           | Multi-Instance | Use Case                     |
| ---------- | ----------------- | -------------- | ---------------------------- |
| **Memory** | `jobstore/memory` | No             | Development, single-instance |
| **JDBC**   | `jobstore/jdbc`   | Yes            | Persistence, multi-instance  |

### JDBC Store — Acquire Flow

```
BEGIN tx
  SELECT ... FROM scheduler_jobs WHERE state = 'WAITING' AND enabled AND next_fire_time <= now
  UPDATE scheduler_jobs SET state = 'ACQUIRED' ... WHERE job_id = ? AND state = 'WAITING'  (per job)
COMMIT
```

The `AND state = 'WAITING'` condition on each UPDATE acts as an optimistic lock. If two instances try to acquire the same job concurrently, only one gets `RowsAffected = 1` — the other sees 0 and skips it.

### JDBC Store Tables

| Table                  | Purpose                                                          |
| ---------------------- | ---------------------------------------------------------------- |
| `scheduler_jobs`       | Job definitions with state (`WAITING` / `ACQUIRED` / `COMPLETE`) |
| `scheduler_executions` | Execution audit log                                              |

### Supported Databases

| Database   | Dialect           |
| ---------- | ----------------- |
| PostgreSQL | `jdbc.Postgres{}` |
| Oracle     | `jdbc.Oracle{}`   |

Custom dialects (e.g., MySQL) can be implemented via the `jdbc.Dialect` interface — see [\_examples/mysql/](_examples/mysql/).

### Database Schema

You control whether the library creates tables:

```go
// Option 1: Library creates tables on startup (uses IF NOT EXISTS, safe to repeat)
store := jdbc.New(db, jdbc.Postgres{},
    jdbc.WithInitializeSchema(jdbc.InitSchemaAlways),
)

// Option 2 (default): You manage schema externally (Liquibase, Flyway, manual DDL)
store := jdbc.New(db, jdbc.Postgres{},
    jdbc.WithInitializeSchema(jdbc.InitSchemaNever), // default, can omit
)
```

To get the DDL for your migration tool:

```go
fmt.Println(jdbc.Postgres{}.SchemaSQL(""))        // PostgreSQL DDL
fmt.Println(jdbc.Oracle{}.SchemaSQL(""))          // Oracle DDL
fmt.Println(jdbc.Postgres{}.SchemaSQL("myapp_"))  // with table prefix
```

You can also call `store.CreateSchema(ctx)` or `store.SchemaSQL()` directly.

## API Reference

### Scheduler

```go
// Create a new scheduler
sched, err := scheduler.New(ctx, opts ...Option)

// Register a new job (Fn is stored by job ID for rehydration)
sched.Register(ctx, job Job) error

// Change a job's trigger
sched.Reschedule(ctx, id JobID, trigger Trigger) error

// Remove a job
sched.Delete(ctx, id JobID) error

// Run the scheduler (blocks until ctx is canceled)
sched.Run(ctx context.Context) error
```

### Options

```go
scheduler.WithJobStore(store)            // Set job store (required)
scheduler.WithLogger(logger)             // Set structured logger
scheduler.WithLocation(loc)              // Set timezone (default: UTC)
scheduler.WithInstanceID(id)             // Set instance ID (default: hostname)
scheduler.WithPollInterval(d)            // Safety fallback poll interval (default: 15s)
scheduler.WithMisfireThreshold(d)        // Stale job recovery threshold (default: 1m)
scheduler.WithShutdownTimeout(d)         // Max wait for in-flight jobs on shutdown (default: 30s)
scheduler.WithCleanupTimeout(d)          // Max wait for post-execution DB cleanup (default: 5s)
```

### Triggers

```go
// Cron expression (5-field standard + descriptors like @hourly)
trigger, err := scheduler.NewCronTrigger("0 */6 * * *")

// Fixed interval
trigger := scheduler.NewIntervalTrigger(30 * time.Second)

// Fire once at a specific time
trigger := scheduler.NewOnceTrigger(time.Now().Add(5 * time.Minute))
```

## Examples

See the [\_examples/](_examples/) directory:

- [\_examples/memory/](_examples/memory/) — single-instance with cron, interval, and once triggers
- [\_examples/postgres/](_examples/postgres/) — multi-instance with PostgreSQL JDBC store
- [\_examples/oracle/](_examples/oracle/) — multi-instance with Oracle JDBC store
- [\_examples/mysql/](_examples/mysql/) — custom MySQL dialect example

## Project Structure

```
scheduler/
├── scheduler.go        # Scheduler core, store-driven run loop
├── job.go              # JobID, Job, Trigger interface
├── trigger.go          # CronTrigger, OnceTrigger, IntervalTrigger
├── interfaces.go       # JobStore interface, JobRecord, ExecutionRecord
├── options.go          # Functional options
├── logger.go           # Default slog logger
├── errors.go           # Sentinel errors
├── scheduler_test.go   # Tests
├── jobstore/
│   ├── memory/
│   │   └── memory.go   # In-memory store
│   └── jdbc/
│       ├── store.go    # SQL store with per-job optimistic locking
│       ├── dialect.go  # Dialect interface + query generators
│       ├── postgres.go # PostgreSQL dialect
│       └── oracle.go   # Oracle dialect
└── _examples/
    ├── memory/         # In-memory example
    ├── postgres/       # PostgreSQL multi-instance example
    ├── oracle/         # Oracle multi-instance example
    └── mysql/          # MySQL custom dialect example
```

## License

MIT
