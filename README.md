# scheduler

A job scheduling library for Go with pluggable job stores and multi-instance support.

## Features

- **Cron, interval, and one-time triggers** with second-level granularity
- **Register / Reschedule / Delete** jobs at runtime
- **Built-in stores** — memory (single-instance) or SQL (Postgres, Oracle, MySQL)
- **Multi-instance safe** — `FOR UPDATE SKIP LOCKED` + optimistic locking
- **Crash recovery** — stale jobs automatically recovered
- **Graceful shutdown** — waits for in-flight jobs on context cancellation

## Installation

```bash
go get github.com/ishinvin/scheduler
```

## Quick Start

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

sched, err := scheduler.New(ctx, scheduler.WithMemory())
if err != nil {
    log.Fatal(err)
}

cronTrigger, _ := scheduler.NewCronTrigger("0 0 9 * * *")
sched.Register(scheduler.Job{
    ID:      "daily-report",
    Name:    "Generate daily report",
    Trigger: cronTrigger,
    Timeout: 30 * time.Second,
    Fn: func(ctx context.Context) error {
        fmt.Println("generating report...")
        return nil
    },
})

log.Fatal(sched.Run())
```

For SQL-backed multi-instance usage:

```go
sched, _ := scheduler.New(ctx,
    scheduler.WithJDBC(db, "postgres", ""),
    scheduler.WithInitializeSchema(),
    scheduler.WithInstanceID("worker-1"),
)
```

## Supported Databases

| Database   | Option                                     |
| ---------- | ------------------------------------------ |
| Memory     | `WithMemory()`                             |
| PostgreSQL | `WithJDBC(db, "postgres", tablePrefix)`    |
| Oracle     | `WithJDBC(db, "oracle", tablePrefix)`      |
| MySQL      | `WithJDBC(db, "mysql", tablePrefix)`       |
| Custom     | `WithCustomJDBC(db, dialect, tablePrefix)` |

## API

```go
sched.Register(job Job) error            // Add job (idempotent)
sched.Reschedule(job Job) error          // Update trigger
sched.Exists(id string) (bool, error)    // Check if job exists
sched.Delete(id string) error            // Remove job
sched.Run() error                        // Start scheduler (blocks)
```

### Triggers

```go
trigger, err := scheduler.NewCronTrigger("0 0 */6 * * *")    // 6-field cron
trigger, err := scheduler.NewIntervalTrigger(30 * time.Second) // fixed interval
trigger := scheduler.NewOnceTrigger(time.Now().Add(5 * time.Minute)) // fire once
```

### Options

```go
scheduler.WithMemory()                               // in-memory store
scheduler.WithJDBC(db, "postgres", "")               // SQL store
scheduler.WithCustomJDBC(db, dialect, "")             // custom dialect
scheduler.WithInitializeSchema()                      // auto-create tables
scheduler.WithLogger(logger)                          // custom *slog.Logger
scheduler.WithVerbose()                               // enable info-level logs
scheduler.WithInstanceID(id)                          // instance ID (default: hostname-pid)
scheduler.WithPollInterval(d)                         // poll interval (default: 30s)
scheduler.WithMisfireThreshold(d)                     // recovery threshold (default: 1m)
scheduler.WithShutdownTimeout(d)                      // shutdown wait (default: 30s)
scheduler.WithCleanupTimeout(d)                       // cleanup wait (default: 5s)
scheduler.WithOnError(func(jobID string, err error))  // error callback
```

## Examples

See [\_examples/](_examples/) for memory, postgres, oracle, mysql, and multi-instance examples.

## License

MIT
