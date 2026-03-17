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

func newTestScheduler(t *testing.T) *scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(context.Background(),
		scheduler.WithJobStore(memory.New()),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestRegisterAndExecute(t *testing.T) {
	s := newTestScheduler(t)

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

	// Duplicate registration should be idempotent (no error).
	if err := s.Register(context.Background(), job); err != nil {
		t.Fatalf("duplicate Register should be idempotent, got %v", err)
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
	s := newTestScheduler(t)

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
	s := newTestScheduler(t)

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
	if err == nil {
		t.Fatal("expected error for deleting non-existent job")
	}
}

func TestOnceTrigger(t *testing.T) {
	s := newTestScheduler(t)

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
	s := newTestScheduler(t)

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
	s, err := scheduler.New(context.Background(),
		scheduler.WithJobStore(store),
		scheduler.WithMisfireThreshold(100*time.Millisecond),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

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
	// Wait for the job to become due, then acquire it.
	time.Sleep(60 * time.Millisecond)
	acquired, err := store.AcquireNextJobs(ctx, time.Now(), "crashed-instance")
	if err != nil || len(acquired) == 0 {
		t.Fatalf("AcquireNextJobs: err=%v, acquired=%d", err, len(acquired))
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

func TestMultiInstanceNoDuplicate(t *testing.T) {
	// Two scheduler instances share the same store.
	// A once-trigger job must fire exactly once, even with concurrent instances.
	store := memory.New()

	var count atomic.Int32

	fn := func(_ context.Context) error {
		count.Add(1)
		time.Sleep(50 * time.Millisecond) // hold ACQUIRED state briefly
		return nil
	}

	s1, err := scheduler.New(context.Background(),
		scheduler.WithJobStore(store),
		scheduler.WithInstanceID("instance-1"),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New s1: %v", err)
	}

	s2, err := scheduler.New(context.Background(),
		scheduler.WithJobStore(store),
		scheduler.WithInstanceID("instance-2"),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New s2: %v", err)
	}

	ctx := context.Background()
	if err := s1.Register(ctx, scheduler.Job{
		ID:      "once-shared",
		Name:    "shared once job",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(50 * time.Millisecond)),
		Fn:      fn,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Also register the same job on instance 2 (idempotent — only registers handler).
	if err := s2.Register(ctx, scheduler.Job{
		ID:      "once-shared",
		Name:    "shared once job",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(50 * time.Millisecond)),
		Fn:      fn,
	}); err != nil {
		t.Fatalf("Register s2: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s1.Run(ctx) }()
	go func() { _ = s2.Run(ctx) }()

	time.Sleep(400 * time.Millisecond)
	cancel()

	if c := count.Load(); c != 1 {
		t.Fatalf("expected exactly 1 execution across 2 instances, got %d", c)
	}
}

func TestMultiInstanceIntervalDistribution(t *testing.T) {
	// Two instances share the same store with an interval job.
	// Both instances should execute the job (taking turns) with no duplicates per cycle.
	store := memory.New()

	var instance1Count, instance2Count atomic.Int32

	fn1 := func(_ context.Context) error {
		instance1Count.Add(1)
		return nil
	}
	fn2 := func(_ context.Context) error {
		instance2Count.Add(1)
		return nil
	}

	s1, _ := scheduler.New(context.Background(),
		scheduler.WithJobStore(store),
		scheduler.WithInstanceID("instance-1"),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	s2, _ := scheduler.New(context.Background(),
		scheduler.WithJobStore(store),
		scheduler.WithInstanceID("instance-2"),
		scheduler.WithPollInterval(25*time.Millisecond),
	)

	ctx := context.Background()
	if err := s1.Register(ctx, scheduler.Job{
		ID:      "interval-shared",
		Name:    "shared interval job",
		Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond),
		Fn:      fn1,
	}); err != nil {
		t.Fatalf("Register s1: %v", err)
	}
	if err := s2.Register(ctx, scheduler.Job{
		ID:      "interval-shared",
		Name:    "shared interval job",
		Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond),
		Fn:      fn2,
	}); err != nil {
		t.Fatalf("Register s2: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s1.Run(ctx) }()
	go func() { _ = s2.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)
	cancel()

	total := instance1Count.Load() + instance2Count.Load()
	if total == 0 {
		t.Fatal("expected at least one execution across instances")
	}
	t.Logf("instance-1: %d, instance-2: %d, total: %d", instance1Count.Load(), instance2Count.Load(), total)
}

func TestRescheduleNotFound(t *testing.T) {
	s := newTestScheduler(t)
	err := s.Reschedule(context.Background(), "nope", scheduler.NewIntervalTrigger(time.Second))
	if !errors.Is(err, scheduler.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}
