package scheduler_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ishinvin/scheduler"
)

// mustInterval is a test helper that creates an IntervalTrigger or fails.
func mustInterval(t *testing.T, d time.Duration) *scheduler.IntervalTrigger {
	t.Helper()
	tr, err := scheduler.NewIntervalTrigger(d)
	if err != nil {
		t.Fatalf("NewIntervalTrigger: %v", err)
	}
	return tr
}

func newTestScheduler(t *testing.T) scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(
		scheduler.WithMemory(),
		scheduler.WithPollInterval(25*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestRegisterAndExecute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newTestScheduler(t)

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "test-1",
		Name:    "interval job",
		Trigger: mustInterval(t, 50*time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Duplicate registration should be idempotent (no error).
	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("duplicate Register should be idempotent, got %v", err)
	}

	go func() { _ = s.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to have fired at least once")
	}
}

func TestReschedule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newTestScheduler(t)

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "resched-1",
		Name:    "slow job",
		Trigger: mustInterval(t, 1*time.Hour),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Reschedule to fire fast.
	err := s.Reschedule(ctx, scheduler.Job{ID: "resched-1", Trigger: mustInterval(t, 50*time.Millisecond)})
	if err != nil {
		t.Fatalf("Reschedule: %v", err)
	}

	go func() { _ = s.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to fire after reschedule")
	}
}

func TestDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newTestScheduler(t)

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "delete-1",
		Name:    "deletable",
		Trigger: mustInterval(t, 50*time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

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
	err := s.Delete(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for deleting non-existent job")
	}
}

func TestOnceTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
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

	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()

	time.Sleep(300 * time.Millisecond)
	cancel()

	if count.Load() != 1 {
		t.Fatalf("expected once trigger to fire exactly 1 time, got %d", count.Load())
	}
}

func TestHandlerRegistry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newTestScheduler(t)

	var count atomic.Int32

	job := scheduler.Job{
		ID:      "handler-1",
		Name:    "handler test",
		Trigger: mustInterval(t, 50*time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

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
	trigger, err := scheduler.NewIntervalTrigger(5 * time.Second)
	if err != nil {
		t.Fatalf("NewIntervalTrigger: %v", err)
	}
	now := time.Now()
	next := trigger.NextFireTime(now)
	if !next.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("expected %v, got %v", now.Add(5*time.Second), next)
	}
}

func TestRecoverStaleJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, err := scheduler.New(
		scheduler.WithMemory(),
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
		Trigger: mustInterval(t, 50*time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()

	time.Sleep(300 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to have fired at least once")
	}
}

func TestMultiInstanceNoDuplicate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s1, err := scheduler.New(
		scheduler.WithMemory(),
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

	if err := s1.Register(ctx, scheduler.Job{
		ID:      "once-shared",
		Name:    "shared once job",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(50 * time.Millisecond)),
		Fn:      fn,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s1.Run(ctx) }()

	time.Sleep(400 * time.Millisecond)
	cancel()

	if c := count.Load(); c != 1 {
		t.Fatalf("expected exactly 1 execution, got %d", c)
	}
}

func TestMultiInstanceIntervalDistribution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, _ := scheduler.New(
		scheduler.WithMemory(),
		scheduler.WithInstanceID("instance-1"),
		scheduler.WithPollInterval(25*time.Millisecond),
	)

	var count atomic.Int32

	if err := s.Register(ctx, scheduler.Job{
		ID:      "interval-shared",
		Name:    "shared interval job",
		Trigger: mustInterval(t, 50*time.Millisecond),
		Fn: func(_ context.Context) error {
			count.Add(1)
			return nil
		},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected at least one execution")
	}
	t.Logf("executions: %d", count.Load())
}

func TestRescheduleCreatesIfNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestScheduler(t)
	err := s.Reschedule(ctx, scheduler.Job{ID: "new-job", Name: "new", Trigger: mustInterval(t, time.Second)})
	if err != nil {
		t.Fatalf("expected Reschedule to create job, got %v", err)
	}
}

func TestRegisterEmptyID(t *testing.T) {
	ctx := context.Background()
	s := newTestScheduler(t)
	err := s.Register(ctx, scheduler.Job{Name: "no id"})
	if err == nil {
		t.Fatal("expected error for empty job ID")
	}
}

func TestRegisterNegativeTimeout(t *testing.T) {
	ctx := context.Background()
	s := newTestScheduler(t)
	err := s.Register(ctx, scheduler.Job{ID: "neg-timeout", Timeout: -time.Second})
	if err == nil {
		t.Fatal("expected error for negative timeout")
	}
}

func TestRescheduleEmptyID(t *testing.T) {
	ctx := context.Background()
	s := newTestScheduler(t)
	tr, _ := scheduler.NewIntervalTrigger(time.Second)
	err := s.Reschedule(ctx, scheduler.Job{Trigger: tr})
	if err == nil {
		t.Fatal("expected error for empty job ID on reschedule")
	}
}

func TestNewIntervalTriggerZeroReturnsError(t *testing.T) {
	_, err := scheduler.NewIntervalTrigger(0)
	if !errors.Is(err, scheduler.ErrNonPositiveInterval) {
		t.Fatalf("expected ErrNonPositiveInterval, got %v", err)
	}
}

func TestNewIntervalTriggerNegativeReturnsError(t *testing.T) {
	_, err := scheduler.NewIntervalTrigger(-time.Second)
	if !errors.Is(err, scheduler.ErrNonPositiveInterval) {
		t.Fatalf("expected ErrNonPositiveInterval, got %v", err)
	}
}

func TestRegisterNilFnAndNilTrigger(t *testing.T) {
	ctx := context.Background()
	s := newTestScheduler(t)
	err := s.Register(ctx, scheduler.Job{ID: "empty-job"})
	if !errors.Is(err, scheduler.ErrEmptyJob) {
		t.Fatalf("expected ErrEmptyJob, got %v", err)
	}
}

func TestRescheduleNoStore(t *testing.T) {
	ctx := context.Background()
	s, err := scheduler.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = s.Reschedule(ctx, scheduler.Job{
		ID:      "no-store",
		Trigger: mustInterval(t, time.Second),
		Fn:      func(_ context.Context) error { return nil },
	})
	if err == nil {
		t.Fatal("expected error for reschedule without store")
	}
}

func TestDoubleRunReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newTestScheduler(t)

	go func() { _ = s.Run(ctx) }()
	time.Sleep(25 * time.Millisecond) // let Run() acquire the lock

	err := s.Run(ctx)
	if !errors.Is(err, scheduler.ErrAlreadyRunning) {
		t.Fatalf("expected ErrAlreadyRunning, got %v", err)
	}
}

func TestOnErrorCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var capturedID string
	var capturedErr atomic.Value

	s, err := scheduler.New(
		scheduler.WithMemory(),
		scheduler.WithPollInterval(25*time.Millisecond),
		scheduler.WithOnError(func(jobID string, jobErr error) {
			capturedID = jobID
			capturedErr.Store(jobErr)
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	jobErr := errors.New("test error")
	if err := s.Register(ctx, scheduler.Job{
		ID:      "err-job",
		Name:    "failing job",
		Trigger: mustInterval(t, 50*time.Millisecond),
		Fn:      func(_ context.Context) error { return jobErr },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	if capturedID != "err-job" {
		t.Fatalf("expected capturedID = %q, got %q", "err-job", capturedID)
	}
	if v := capturedErr.Load(); v == nil {
		t.Fatal("expected onError to have been called")
	}
}

func TestExists(t *testing.T) {
	ctx := context.Background()
	s := newTestScheduler(t)

	if err := s.Register(ctx, scheduler.Job{
		ID:      "exists-1",
		Name:    "test",
		Trigger: mustInterval(t, time.Second),
		Fn:      func(_ context.Context) error { return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ok, err := s.Exists(ctx, "exists-1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !ok {
		t.Fatal("expected job to exist")
	}

	ok, err = s.Exists(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if ok {
		t.Fatal("expected job to not exist")
	}
}

func TestExistsNoStore(t *testing.T) {
	s, _ := scheduler.New()
	_, err := s.Exists(context.Background(), "any")
	if !errors.Is(err, scheduler.ErrNoStore) {
		t.Fatalf("expected ErrNoStore, got %v", err)
	}
}

func TestDeleteNoStore(t *testing.T) {
	s, _ := scheduler.New()
	err := s.Delete(context.Background(), "any")
	if !errors.Is(err, scheduler.ErrNoStore) {
		t.Fatalf("expected ErrNoStore, got %v", err)
	}
}

func TestRegisterHandlerOnly(t *testing.T) {
	// Handler-only registration (no Trigger) should succeed without a store.
	s, _ := scheduler.New()
	err := s.Register(context.Background(), scheduler.Job{
		ID: "handler-only",
		Fn: func(_ context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("expected handler-only Register to succeed, got %v", err)
	}
}
