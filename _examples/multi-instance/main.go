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
		dsn = "postgres://scheduler:scheduler@postgres:5432/scheduler?sslmode=disable"
	}

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		hostname, _ := os.Hostname()
		instanceID = hostname
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
		scheduler.WithInstanceID(instanceID),
	)
	if err != nil {
		log.Fatalf("init scheduler: %v", err)
	}

	// Each replica registers the same jobs.
	// The first to start persists them; the rest just register their handlers.
	jobs := []scheduler.Job{
		{
			ID:      "heartbeat",
			Name:    "Heartbeat",
			Trigger: must(scheduler.NewIntervalTrigger(1 * time.Second)),
			Fn: func(_ context.Context) error {
				fmt.Printf("[%s] %s  heartbeat\n", instanceID, time.Now().Format(time.TimeOnly))
				return nil
			},
		},
		{
			ID:      "cleanup",
			Name:    "Periodic cleanup",
			Trigger: must(scheduler.NewCronTrigger("0 */1 * * * *")),
			Timeout: 30 * time.Second,
			Fn: func(_ context.Context) error {
				fmt.Printf("[%s] %s  cleanup\n", instanceID, time.Now().Format(time.TimeOnly))
				return nil
			},
		},
		{
			ID:      "report",
			Name:    "One-time report",
			Trigger: scheduler.NewOnceTrigger(time.Now().Add(10 * time.Second)),
			Fn: func(_ context.Context) error {
				fmt.Printf("[%s] %s  report (once)\n", instanceID, time.Now().Format(time.TimeOnly))
				return nil
			},
		},
	}

	for _, job := range jobs {
		if err := sched.Register(job); err != nil {
			log.Fatalf("register %s: %v", job.ID, err)
		}
	}

	fmt.Printf("[%s] scheduler started\n", instanceID)
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
