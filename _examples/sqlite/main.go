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

	_ "github.com/mattn/go-sqlite3"

	"github.com/ishinvin/scheduler"
	"github.com/ishinvin/scheduler/jobstore/jdbc"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "scheduler.db"
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Create JDBC store with SQLite dialect.
	//
	// initialize-schema options:
	//   - jdbc.InitSchemaNever  (default) — you manage schema via migration tools
	//   - jdbc.InitSchemaAlways — the library creates tables on startup (uses IF NOT EXISTS)
	//
	// To get the DDL for external migration tools:
	//   fmt.Println(jdbc.SQLite{}.SchemaSQL(""))       // no prefix
	//   fmt.Println(jdbc.SQLite{}.SchemaSQL("myapp_")) // with prefix
	store := jdbc.New(db, jdbc.SQLite{},
		jdbc.WithInstanceID("worker-1"),
		jdbc.WithInitializeSchema(jdbc.InitSchemaAlways), // opt-in schema creation
		// jdbc.WithTablePrefix("myapp_"),                // optional table prefix
	)

	sched := scheduler.New(
		scheduler.WithJobStore(store),
		scheduler.WithInstanceID("worker-1"),
	)

	ctx := context.Background()

	// Register jobs. These are persisted to SQLite and survive restarts.
	// The Fn is stored by job ID so rehydrated jobs can resolve it on restart.
	sched.Register(ctx, scheduler.Job{
		ID:      "cleanup-job",
		Name:    "Periodic cleanup",
		Trigger: must(scheduler.NewCronTrigger("*/5 * * * *")), // every 5 minutes
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "running cleanup...")
			return nil
		},
	})

	sched.Register(ctx, scheduler.Job{
		ID:      "daily-report",
		Name:    "Daily report",
		Trigger: must(scheduler.NewCronTrigger("0 9 * * *")), // 9 AM daily
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "generating report...")
			return nil
		},
	})

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
