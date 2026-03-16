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

	_ "github.com/go-sql-driver/mysql"

	"github.com/ishinvin/scheduler"
	"github.com/ishinvin/scheduler/jobstore/jdbc"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "root:@tcp(localhost:3306)/scheduler?parseTime=true"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Create JDBC store with custom MySQL dialect.
	//
	// The MySQL struct in dialect.go implements the jdbc.Dialect interface,
	// demonstrating how to add support for any database.
	store := jdbc.New(db, MySQL{},
		jdbc.WithInstanceID("worker-1"),
		jdbc.WithInitializeSchema(jdbc.InitSchemaAlways),
	)

	sched := scheduler.New(
		scheduler.WithJobStore(store),
		scheduler.WithInstanceID("worker-1"),
	)

	// Register named handlers for dynamic/persisted jobs.
	sched.RegisterHandler("cleanup", func(_ context.Context) error {
		fmt.Println(time.Now().Format(time.RFC3339), "running cleanup...")
		return nil
	})

	sched.RegisterHandler("report", func(_ context.Context) error {
		fmt.Println(time.Now().Format(time.RFC3339), "generating report...")
		return nil
	})

	ctx := context.Background()

	// Register jobs. These are persisted to MySQL and survive restarts.
	_ = sched.Register(ctx, scheduler.Job{
		ID:      "cleanup-job",
		Name:    "Periodic cleanup",
		Trigger: must(scheduler.NewCronTrigger("*/5 * * * *")),
		Metadata: map[string]string{
			"handler": "cleanup",
		},
	})

	_ = sched.Register(ctx, scheduler.Job{
		ID:      "daily-report",
		Name:    "Daily report",
		Trigger: must(scheduler.NewCronTrigger("0 9 * * *")),
		Metadata: map[string]string{
			"handler": "report",
		},
	})

	_ = sched.Register(ctx, scheduler.Job{
		ID:      "heartbeat",
		Name:    "Heartbeat",
		Trigger: scheduler.NewIntervalTrigger(10 * time.Second),
		Fn: func(_ context.Context) error {
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
