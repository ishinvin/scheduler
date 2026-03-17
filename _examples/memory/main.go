package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/ishinvin/scheduler"
	"github.com/ishinvin/scheduler/jobstore/memory"
)

func main() {
	ctx := context.Background()

	sched, err := scheduler.New(ctx,
		scheduler.WithJobStore(memory.New()),
	)
	if err != nil {
		log.Fatalf("init scheduler: %v", err)
	}

	// Cron trigger: every minute, with a 10s execution timeout.
	sched.Register(ctx, scheduler.Job{
		ID:      "cron-job",
		Name:    "Every minute",
		Trigger: must(scheduler.NewCronTrigger("* * * * *")),
		Timeout: 10 * time.Second,
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "cron job fired")
			return nil
		},
	})

	// Interval trigger: every 5 seconds.
	sched.Register(ctx, scheduler.Job{
		ID:      "interval-job",
		Name:    "Every 5s",
		Trigger: scheduler.NewIntervalTrigger(5 * time.Second),
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "interval job fired")
			return nil
		},
	})

	// Once trigger: 10 seconds from now.
	sched.Register(ctx, scheduler.Job{
		ID:      "once-job",
		Name:    "Fire once",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(10 * time.Second)),
		Fn: func(ctx context.Context) error {
			fmt.Println(time.Now().Format(time.RFC3339), "once job fired!")
			return nil
		},
	})

	// Demonstrate reschedule after 15 seconds.
	go func() {
		time.Sleep(15 * time.Second)
		fmt.Println("rescheduling interval-job to every 2s...")
		sched.Reschedule(ctx, "interval-job", scheduler.NewIntervalTrigger(2*time.Second))
	}()

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
