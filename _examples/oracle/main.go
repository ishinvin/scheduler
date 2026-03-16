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

	_ "github.com/sijms/go-ora/v2"

	"github.com/ishinvin/scheduler"
	"github.com/ishinvin/scheduler/jobstore/jdbc"
)

func main() {
	dsn := os.Getenv("ORACLE_DSN")
	if dsn == "" {
		dsn = "oracle://user:pass@localhost:1521/ORCLPDB1"
	}

	db, err := sql.Open("oracle", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Create JDBC store with Oracle dialect.
	//
	// initialize-schema options:
	//   - jdbc.InitSchemaNever  (default) — you manage schema via Liquibase/Flyway/manual DDL
	//   - jdbc.InitSchemaAlways — the library creates tables on startup
	//
	// To get the DDL for external migration tools:
	//   fmt.Println(jdbc.Oracle{}.SchemaSQL(""))       // no prefix
	//   fmt.Println(jdbc.Oracle{}.SchemaSQL("myapp_")) // with prefix
	store := jdbc.New(db, jdbc.Oracle{},
		jdbc.WithInstanceID("worker-1"),
		jdbc.WithInitializeSchema(jdbc.InitSchemaAlways), // opt-in schema creation
		// jdbc.WithTablePrefix("myapp_"),                // optional table prefix
	)

	sched := scheduler.New(
		scheduler.WithJobStore(store),
		scheduler.WithInstanceID("worker-1"),
	)

	ctx := context.Background()

	// Register jobs. These are persisted to Oracle and survive restarts.
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

	// Fire-once job: schedule a one-time task.
	sched.Register(ctx, scheduler.Job{
		ID:      "welcome-task",
		Name:    "One-time welcome",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(30 * time.Second)),
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "welcome task executed!")
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
