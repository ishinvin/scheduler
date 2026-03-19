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
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "root:scheduler@tcp(localhost:3306)/scheduler?parseTime=true"
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Use WithJDBC with a custom MySQL dialect.
	// This demonstrates how to add support for any database.
	sched, err := scheduler.New(ctx,
		scheduler.WithJDBC(db, MySQL{}),
		scheduler.WithInitializeSchema(),
		scheduler.WithInstanceID("worker-1"),
	)
	if err != nil {
		log.Fatalf("init scheduler: %v", err)
	}

	// Register jobs. These are persisted to MySQL and survive restarts.
	_ = sched.Register(ctx, scheduler.Job{
		ID:      "cleanup-job",
		Name:    "Periodic cleanup",
		Trigger: must(scheduler.NewCronTrigger("*/5 * * * *")),
		Fn: func(_ context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "running cleanup...")
			return nil
		},
	})

	_ = sched.Register(ctx, scheduler.Job{
		ID:      "daily-report",
		Name:    "Daily report",
		Trigger: must(scheduler.NewCronTrigger("0 9 * * *")),
		Fn: func(_ context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "generating report...")
			return nil
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

	// Start the scheduler. Blocks until the context is canceled.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Println("scheduler started (ctrl+c to stop)")
	if err := sched.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
