package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ishinvin/scheduler"
	"github.com/ishinvin/scheduler/jobstore/memory"
)

func main() {
	sched := scheduler.New(
		scheduler.WithJobStore(memory.New()),
	)

	ctx := context.Background()

	// Cron trigger: every minute.
	sched.Register(ctx, scheduler.Job{
		ID:      "cron-job",
		Name:    "Every minute",
		Trigger: must(scheduler.NewCronTrigger("* * * * *")),
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

	// Start the scheduler.
	execute, interrupt := sched.Actor()
	go func() {
		if err := execute(); err != nil {
			log.Fatalf("scheduler: %v", err)
		}
	}()

	fmt.Println("scheduler started (ctrl+c to stop)")

	// Demonstrate reschedule after 15 seconds.
	go func() {
		time.Sleep(15 * time.Second)
		fmt.Println("rescheduling interval-job to every 2s...")
		sched.Reschedule(ctx, "interval-job", scheduler.NewIntervalTrigger(2*time.Second))
	}()

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
