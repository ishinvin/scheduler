package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/ishinvin/scheduler"
	"github.com/ishinvin/scheduler/jobstore/jdbc"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://localhost:5432/scheduler?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Create JDBC store with PostgreSQL dialect.
	//
	// initialize-schema options:
	//   - jdbc.InitSchemaNever  (default) — you manage schema via Liquibase/Flyway/manual DDL
	//   - jdbc.InitSchemaAlways — the library creates tables on startup (uses IF NOT EXISTS)
	//
	// To get the DDL for external migration tools:
	//   fmt.Println(jdbc.Postgres{}.SchemaSQL(""))       // no prefix
	//   fmt.Println(jdbc.Postgres{}.SchemaSQL("myapp_")) // with prefix
	store := jdbc.New(db, jdbc.Postgres{},
		jdbc.WithInstanceID("worker-1"),
		jdbc.WithInitializeSchema(jdbc.InitSchemaAlways), // opt-in schema creation
		// jdbc.WithTablePrefix("myapp_"),                // optional table prefix
	)

	sched := scheduler.New(
		scheduler.WithJobStore(store),
		scheduler.WithInstanceID("worker-1"),
	)

	// Register named handlers for dynamic/persisted jobs.
	sched.RegisterHandler("cleanup", func(ctx context.Context) error {
		fmt.Println(time.Now().Format(time.RFC3339), "running cleanup...")
		return nil
	})

	sched.RegisterHandler("report", func(ctx context.Context) error {
		fmt.Println(time.Now().Format(time.RFC3339), "generating report...")
		return nil
	})

	ctx := context.Background()

	// Register jobs. These are persisted to PostgreSQL and survive restarts.
	sched.Register(ctx, scheduler.Job{
		ID:      "cleanup-job",
		Name:    "Periodic cleanup",
		Trigger: must(scheduler.NewCronTrigger("*/5 * * * *")), // every 5 minutes
		Metadata: map[string]string{
			"handler": "cleanup",
		},
	})

	sched.Register(ctx, scheduler.Job{
		ID:      "daily-report",
		Name:    "Daily report",
		Trigger: must(scheduler.NewCronTrigger("0 9 * * *")), // 9 AM daily
		Metadata: map[string]string{
			"handler": "report",
		},
	})

	// You can also register jobs with inline functions (not persisted across restarts).
	sched.Register(ctx, scheduler.Job{
		ID:      "heartbeat",
		Name:    "Heartbeat",
		Trigger: scheduler.NewIntervalTrigger(10 * time.Second),
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "heartbeat")
			return nil
		},
	})

	// Start the scheduler.
	execute, interrupt := sched.Actor()
	go func() {
		if err := execute(); err != nil {
			log.Fatalf("scheduler: %v", err)
		}
	}()

	fmt.Println("scheduler started (ctrl+c to stop)")

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("shutting down...")
	interrupt(nil)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
