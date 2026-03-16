# scheduler

A job scheduling library for Go with pluggable job stores and distributed locking.

## Features

- **Cron, interval, and one-time triggers** — standard 5-field cron expressions, fixed intervals, or fire-once at a specific time
- **Register / Reschedule / Delete** jobs at runtime
- **Pluggable JobStore** — choose between memory (single-instance) or JDBC (clustered)
- **Distributed locking** — JDBC store uses `TRIGGER_ACCESS` table lock for safe multi-instance deployments
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
    sched := scheduler.New(
        scheduler.WithJobStore(memory.New()),
    )

    ctx := context.Background()

    // Register a cron job
    sched.Register(ctx, scheduler.Job{
        ID:   "daily-report",
        Name: "Generate daily report",
        Trigger: must(scheduler.NewCronTrigger("0 9 * * *")),
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

### Clustered (JDBC Store + PostgreSQL)

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

    sched := scheduler.New(
        scheduler.WithJobStore(store),
        scheduler.WithInstanceID("worker-1"),
    )

    ctx := context.Background()

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

### JobStore Interface

The single `JobStore` interface handles both persistence and distributed coordination:

```go
type JobStore interface {
    SaveJob(ctx context.Context, rec *JobRecord) error
    DeleteJob(ctx context.Context, id JobID) error
    GetJob(ctx context.Context, id JobID) (*JobRecord, error)
    ListJobs(ctx context.Context) ([]*JobRecord, error)
    AcquireJob(ctx context.Context, id JobID, instanceID string) error
    ReleaseJob(ctx context.Context, id JobID, nextFireTime time.Time) error
    RecordExecution(ctx context.Context, exec *ExecutionRecord) error
    Close() error
}
```

### Job Store Types

| Store      | Package           | Clustering | Use Case                     |
| ---------- | ----------------- | ---------- | ---------------------------- |
| **Memory** | `jobstore/memory` | No         | Development, single-instance |
| **JDBC**   | `jobstore/jdbc`   | Yes        | Production, multi-instance   |

### JDBC Store — Table Locking

The JDBC store uses three tables:

| Table                  | Purpose                                                          |
| ---------------------- | ---------------------------------------------------------------- |
| `scheduler_jobs`       | Job definitions with state (`WAITING` / `ACQUIRED` / `COMPLETE`) |
| `scheduler_locks`      | Named lock rows (`TRIGGER_ACCESS`)                               |
| `scheduler_executions` | Execution audit log                                              |

**Acquire flow:**

```
BEGIN tx
  → SELECT ... FROM scheduler_locks WHERE lock_name = 'TRIGGER_ACCESS' FOR UPDATE NOWAIT
  → UPDATE scheduler_jobs SET state = 'ACQUIRED' WHERE job_id = ? AND state = 'WAITING'
COMMIT
```

This ensures only one instance claims a job, even under concurrent load.

### Supported Databases

| Database   | Dialect           | Lock Error    |
| ---------- | ----------------- | ------------- |
| PostgreSQL | `jdbc.Postgres{}` | `55P03`       |
| Oracle     | `jdbc.Oracle{}`   | `ORA-00054`   |
| SQLite     | `jdbc.SQLite{}`   | `SQLITE_BUSY` |

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
fmt.Println(jdbc.SQLite{}.SchemaSQL(""))          // SQLite DDL
fmt.Println(jdbc.Postgres{}.SchemaSQL("myapp_"))  // with table prefix
```

You can also call `store.CreateSchema(ctx)` or `store.SchemaSQL()` directly.

## API Reference

### Scheduler

```go
// Create a new scheduler
sched := scheduler.New(opts ...Option)

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
scheduler.WithJobStore(store)        // Set job store (memory or jdbc)
scheduler.WithLogger(logger)         // Set structured logger
scheduler.WithLocation(loc)          // Set timezone (default: UTC)
scheduler.WithInstanceID(id)         // Set instance ID (default: hostname)
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
- [\_examples/postgres/](_examples/postgres/) — clustered with PostgreSQL JDBC store
- [\_examples/oracle/](_examples/oracle/) — clustered with Oracle JDBC store
- [\_examples/sqlite/](_examples/sqlite/) — single-instance with SQLite persistence

## Project Structure

```
scheduler/
├── scheduler.go        # Scheduler core, run loop, Run()
├── job.go              # Job, Trigger, CronTrigger, OnceTrigger, IntervalTrigger
├── interfaces.go       # JobStore interface, JobRecord, ExecutionRecord
├── options.go          # Functional options
├── errors.go           # Sentinel errors
├── scheduler_test.go   # Tests
├── jobstore/
│   ├── memory/
│   │   └── memory.go   # In-memory store
│   └── jdbc/
│       ├── store.go    # SQL store with table locks
│       ├── dialect.go  # Dialect interface + query generators
│       ├── postgres.go # PostgreSQL dialect
│       ├── oracle.go   # Oracle dialect
│       └── sqlite.go   # SQLite dialect
└── _examples/
    ├── memory/         # In-memory example
    ├── postgres/       # PostgreSQL clustered example
    ├── oracle/         # Oracle clustered example
    ├── sqlite/         # SQLite example
    └── mysql/          # MySQL custom dialect example
```

## License

MIT
