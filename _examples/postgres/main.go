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
	cleanupTrigger, _ := scheduler.NewCronTrigger("0 */1 * * * *")
	sched.Register(scheduler.Job{
		ID:      "cleanup-job",
		Name:    "Periodic cleanup",
		Trigger: cleanupTrigger,
		Timeout: 2 * time.Minute,
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "running cleanup...")
			return nil
		},
	})

	reportTrigger, _ := scheduler.NewCronTrigger("0 0 9 * * *")
	sched.Register(scheduler.Job{
		ID:      "daily-report",
		Name:    "Daily report",
		Trigger: reportTrigger,
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "generating report...")
			return nil
		},
	})

	heartbeatTrigger, _ := scheduler.NewIntervalTrigger(10 * time.Second)
	sched.Register(scheduler.Job{
		ID:      "heartbeat",
		Name:    "Heartbeat",
		Trigger: heartbeatTrigger,
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
