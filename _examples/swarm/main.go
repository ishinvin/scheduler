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
	_ "github.com/sijms/go-ora/v2"

	"github.com/ishinvin/scheduler"
)

func main() {
	dialectName := os.Getenv("DB_DIALECT")
	if dialectName == "" {
		dialectName = "postgres"
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://scheduler:scheduler@postgres:5432/scheduler?sslmode=disable"
	}

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		hostname, _ := os.Hostname()
		instanceID = hostname
	}

	db, err := sql.Open(dialectName, dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sched, err := scheduler.New(
		scheduler.WithJDBC(db, dialectName, ""),
		scheduler.WithInstanceID(instanceID),
		// scheduler.WithVerbose(),
	)
	if err != nil {
		log.Fatalf("init scheduler: %v", err)
	}

	if err := sched.InitSchema(ctx); err != nil {
		log.Fatalf("init schema: %v", err)
	}

	cronTrigger, _ := scheduler.NewCronTrigger("*/1 * * * * *")
	intervalTrigger, _ := scheduler.NewIntervalTrigger(5 * time.Second)

	jobs := []scheduler.Job{
		{
			ID:      "cron-job",
			Name:    "Every 1s",
			Trigger: cronTrigger,
			Timeout: 10 * time.Second,
			Fn: func(ctx context.Context) error {
				fmt.Printf("[%s] %s cron job fired\n", instanceID, time.Now().Format(time.TimeOnly))
				return nil
			},
		},
		{
			ID:      "interval-job",
			Name:    "Every 5s",
			Trigger: intervalTrigger,
			Fn: func(ctx context.Context) error {
				fmt.Printf("[%s] %s interval job fired\n", instanceID, time.Now().Format(time.TimeOnly))
				return nil
			},
		},
		{
			ID:      "once-job",
			Name:    "Fire once",
			Trigger: scheduler.NewOnceTrigger(time.Now().Add(10 * time.Second)),
			Fn: func(ctx context.Context) error {
				fmt.Printf("[%s] %s once job fired!\n", instanceID, time.Now().Format(time.TimeOnly))
				return nil
			},
		},
	}

	for _, job := range jobs {
		if err := sched.Register(ctx, job); err != nil {
			log.Fatalf("register %s: %v", job.ID, err)
		}
	}

	fmt.Printf("[%s] scheduler started (dialect=%s)\n", instanceID, dialectName)
	if err := sched.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
