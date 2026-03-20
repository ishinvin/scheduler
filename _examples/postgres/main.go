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
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sched, err := scheduler.New(ctx,
		scheduler.WithJDBC(db, "postgres", ""),
		scheduler.WithInitializeSchema(),
		scheduler.WithInstanceID("worker-1"),
	)
	if err != nil {
		log.Fatalf("init scheduler: %v", err)
	}

	// Register jobs. These are persisted to PostgreSQL and survive restarts.
	// The Fn is stored by job ID so rehydrated jobs can resolve it on restart.
	sched.Register(scheduler.Job{
		ID:      "cleanup-job",
		Name:    "Periodic cleanup",
		Trigger: must(scheduler.NewCronTrigger("0 */1 * * * *")), // every 1 minute
		Timeout: 2 * time.Minute,                               // timeout persists to DB
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "running cleanup...")
			return nil
		},
	})

	sched.Register(scheduler.Job{
		ID:      "daily-report",
		Name:    "Daily report",
		Trigger: must(scheduler.NewCronTrigger("0 0 9 * * *")), // 9 AM daily
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "generating report...")
			return nil
		},
	})

	sched.Register(scheduler.Job{
		ID:      "heartbeat",
		Name:    "Heartbeat",
		Trigger: scheduler.NewIntervalTrigger(10 * time.Second),
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "heartbeat")
			return nil
		},
	})

	// Start the scheduler. Blocks until the context is canceled.
	fmt.Println("scheduler started (ctrl+c to stop)")
	if err := sched.Run(); err != nil {
		log.Fatal(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
