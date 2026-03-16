package scheduler_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ishinvin/scheduler"
	"github.com/ishinvin/scheduler/jobstore/memory"
)

func newTestScheduler() *scheduler.Scheduler {
	return scheduler.New(
		scheduler.WithJobStore(memory.New()),
	)
}

func TestRegisterAndExecute(t *testing.T) {
	s := newTestScheduler()

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "test-1",
		Name:    "interval job",
		Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(context.Background(), job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Duplicate should fail.
	err := s.Register(context.Background(), job)
	if !errors.Is(err, scheduler.ErrJobAlreadyExists) {
		t.Fatalf("expected ErrJobAlreadyExists, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to have fired at least once")
	}
}

func TestReschedule(t *testing.T) {
	s := newTestScheduler()

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "resched-1",
		Name:    "slow job",
		Trigger: scheduler.NewIntervalTrigger(1 * time.Hour),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	ctx := context.Background()
	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Reschedule to fire fast.
	err := s.Reschedule(ctx, "resched-1", scheduler.NewIntervalTrigger(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Reschedule: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to fire after reschedule")
	}
}

func TestDelete(t *testing.T) {
	s := newTestScheduler()

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "delete-1",
		Name:    "deletable",
		Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	ctx := context.Background()
	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(150 * time.Millisecond)

	if err := s.Delete(ctx, "delete-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	countAfterDelete := count.Load()
	time.Sleep(150 * time.Millisecond)

	// Count should not increase significantly after delete.
	if count.Load()-countAfterDelete > 1 {
		t.Fatal("job kept running after delete")
	}

	cancel()

	// Delete non-existent job.
	err := s.Delete(ctx, "nonexistent")
	if !errors.Is(err, scheduler.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestOnceTrigger(t *testing.T) {
	s := newTestScheduler()

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "once-1",
		Name:    "fire once",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(50 * time.Millisecond)),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(context.Background(), job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(300 * time.Millisecond)
	cancel()

	if count.Load() != 1 {
		t.Fatalf("expected once trigger to fire exactly 1 time, got %d", count.Load())
	}
}

func TestHandlerRegistry(t *testing.T) {
	s := newTestScheduler()

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "handler-1",
		Name:    "handler test",
		Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(context.Background(), job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected handler to have been invoked at least once")
	}
}

func TestCronTriggerInvalid(t *testing.T) {
	_, err := scheduler.NewCronTrigger("not a cron")
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
}

func TestIntervalTrigger(t *testing.T) {
	trigger := scheduler.NewIntervalTrigger(5 * time.Second)
	now := time.Now()
	next := trigger.NextFireTime(now)
	if !next.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("expected %v, got %v", now.Add(5*time.Second), next)
	}
}

func TestRecoverStaleJobs(t *testing.T) {
	store := memory.New()
	s := scheduler.New(
		scheduler.WithJobStore(store),
		scheduler.WithMisfireThreshold(100*time.Millisecond),
	)

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "stale-1",
		Name:    "stale test",
		Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	ctx := context.Background()
	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Simulate a crashed instance: acquire the job but never release it.
	if err := store.AcquireJob(ctx, "stale-1", "crashed-instance"); err != nil {
		t.Fatalf("AcquireJob: %v", err)
	}

	// Verify job is stuck in ACQUIRED.
	rec, _ := store.GetJob(ctx, "stale-1")
	if rec.State != scheduler.StateAcquired {
		t.Fatalf("expected ACQUIRED, got %s", rec.State)
	}

	// Wait for misfire threshold to pass.
	time.Sleep(150 * time.Millisecond)

	// Recovery should reset it to WAITING.
	n, err := store.RecoverStaleJobs(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("RecoverStaleJobs: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 recovered job, got %d", n)
	}

	rec, _ = store.GetJob(ctx, "stale-1")
	if rec.State != scheduler.StateWaiting {
		t.Fatalf("expected WAITING after recovery, got %s", rec.State)
	}
}

func TestRescheduleNotFound(t *testing.T) {
	s := newTestScheduler()
	err := s.Reschedule(context.Background(), "nope", scheduler.NewIntervalTrigger(time.Second))
	if !errors.Is(err, scheduler.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func newClusterScheduler() *scheduler.Scheduler {
	return scheduler.New(
		scheduler.WithJobStore(memory.New()),
		scheduler.WithClusterMode(50*time.Millisecond),
	)
}

func TestClusterModeDispatch(t *testing.T) {
	s := newClusterScheduler()

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "cluster-1",
		Name:    "cluster job",
		Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(context.Background(), job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(300 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected cluster mode job to have fired at least once")
	}
}

func TestClusterModeOnceTrigger(t *testing.T) {
	s := newClusterScheduler()

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "cluster-once-1",
		Name:    "fire once cluster",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(50 * time.Millisecond)),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(context.Background(), job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(400 * time.Millisecond)
	cancel()

	if count.Load() != 1 {
		t.Fatalf("expected once trigger to fire exactly 1 time in cluster mode, got %d", count.Load())
	}
}

func TestClusterModeReschedule(t *testing.T) {
	s := newClusterScheduler()

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "cluster-resched-1",
		Name:    "slow cluster job",
		Trigger: scheduler.NewIntervalTrigger(1 * time.Hour),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	ctx := context.Background()
	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	// Reschedule to fire fast.
	time.Sleep(100 * time.Millisecond)
	if err := s.Reschedule(ctx, "cluster-resched-1", scheduler.NewIntervalTrigger(50*time.Millisecond)); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to fire after reschedule in cluster mode")
	}
}
