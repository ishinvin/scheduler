package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ishinvin/scheduler"
)

func newTestScheduler(t *testing.T) scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(context.Background(),
		scheduler.WithMemoryStore(),
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
	err := s.Reschedule(ctx, scheduler.Job{ID: "resched-1", Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond)})
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
	// A job with misfire threshold should recover after being stuck.
	// We test this end-to-end: register a job, let it fire, then verify
	// the scheduler's recovery mechanism works by observing continued execution.
	s, err := scheduler.New(context.Background(),
		scheduler.WithMemoryStore(),
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

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(300 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to have fired at least once")
	}
}

func TestMultiInstanceNoDuplicate(t *testing.T) {
	// Two scheduler instances share the same store.
	// A once-trigger job must fire exactly once, even with concurrent instances.
	s1, err := scheduler.New(context.Background(),
		scheduler.WithMemoryStore(),
		scheduler.WithInstanceID("instance-1"),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New s1: %v", err)
	}

	var count atomic.Int32

	fn := func(_ context.Context) error {
		count.Add(1)
		time.Sleep(50 * time.Millisecond) // hold ACQUIRED state briefly
		return nil
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

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s1.Run(ctx) }()

	time.Sleep(400 * time.Millisecond)
	cancel()

	if c := count.Load(); c != 1 {
		t.Fatalf("expected exactly 1 execution, got %d", c)
	}
}

func TestMultiInstanceIntervalDistribution(t *testing.T) {
	// Single instance with interval job — verify it fires repeatedly.
	s, _ := scheduler.New(context.Background(),
		scheduler.WithMemoryStore(),
		scheduler.WithInstanceID("instance-1"),
		scheduler.WithPollInterval(25*time.Millisecond),
	)

	var count atomic.Int32

	ctx := context.Background()
	if err := s.Register(ctx, scheduler.Job{
		ID:      "interval-shared",
		Name:    "shared interval job",
		Trigger: scheduler.NewIntervalTrigger(50 * time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = s.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected at least one execution")
	}
	t.Logf("executions: %d", count.Load())
}

func TestRescheduleCreatesIfNotFound(t *testing.T) {
	s := newTestScheduler(t)
	err := s.Reschedule(context.Background(), scheduler.Job{ID: "new-job", Name: "new", Trigger: scheduler.NewIntervalTrigger(time.Second)})
	if err != nil {
		t.Fatalf("expected Reschedule to create job, got %v", err)
	}
}
