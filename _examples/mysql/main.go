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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sched, err := scheduler.New(ctx,
		scheduler.WithJDBC(db, "mysql", "myapp_"),
		scheduler.WithInitializeSchema(),
		scheduler.WithInstanceID("worker-1"),
	)
	if err != nil {
		log.Fatalf("init scheduler: %v", err)
	}

	// Cron trigger: every second, with a 10s execution timeout.
	cronTrigger, _ := scheduler.NewCronTrigger("*/1 * * * * *")
	sched.Register(scheduler.Job{
		ID:      "cron-job",
		Name:    "Every 1s",
		Trigger: cronTrigger,
		Timeout: 10 * time.Second,
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "cron job fired")
			return nil
		},
	})

	// Interval trigger: every 5 seconds.
	intervalTrigger, _ := scheduler.NewIntervalTrigger(5 * time.Second)
	sched.Register(scheduler.Job{
		ID:      "interval-job",
		Name:    "Every 5s",
		Trigger: intervalTrigger,
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "interval job fired")
			return nil
		},
	})

	// Once trigger: 10 seconds from now.
	sched.Register(scheduler.Job{
		ID:      "once-job",
		Name:    "Fire once",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(10 * time.Second)),
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "once job fired!")
			return nil
		},
	})

	// Start the scheduler. Blocks until the context is canceled.
	fmt.Println("scheduler started (ctrl+c to stop)")
	if err := sched.Run(); err != nil {
		log.Fatal(err)
	}
}
