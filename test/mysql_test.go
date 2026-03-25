package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ishinvin/scheduler"
)

func TestMySQL_InitSchemaIdempotent(t *testing.T) {
	ctx := t.Context()
	s := newJDBCScheduler(t, myDB, "mysql", "my_init_")

	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema (idempotent): %v", err)
	}
}

func TestMySQL_RegisterAndExecute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newJDBCScheduler(t, myDB, "mysql", "my_regexec_")

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "my-exec-1",
		Name:    "interval job",
		Trigger: mustInterval(t, 100*time.Millisecond),
		Fn:      func(_ context.Context) error { count.Add(1); return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()
	time.Sleep(500 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to have fired at least once")
	}
}

func TestMySQL_DuplicateRegister(t *testing.T) {
	ctx := t.Context()
	s := newJDBCScheduler(t, myDB, "mysql", "my_dup_")

	job := scheduler.Job{
		ID:      "my-dup-1",
		Name:    "dup test",
		Trigger: mustInterval(t, time.Second),
		Fn:      func(_ context.Context) error { return nil },
	}
	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := s.Register(ctx, job); err != nil {
		t.Fatalf("duplicate Register should be idempotent, got %v", err)
	}
}

func TestMySQL_Reschedule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newJDBCScheduler(t, myDB, "mysql", "my_resched_")

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "my-resched-1",
		Name:    "slow job",
		Trigger: mustInterval(t, 1*time.Hour),
		Fn:      func(_ context.Context) error { count.Add(1); return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := s.Reschedule(ctx, scheduler.Job{
		ID:      "my-resched-1",
		Trigger: mustInterval(t, 100*time.Millisecond),
	})
	if err != nil {
		t.Fatalf("Reschedule: %v", err)
	}

	go func() { _ = s.Run(ctx) }()
	time.Sleep(500 * time.Millisecond)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected job to fire after reschedule")
	}
}

func TestMySQL_Exists(t *testing.T) {
	ctx := t.Context()
	s := newJDBCScheduler(t, myDB, "mysql", "my_exists_")

	if err := s.Register(ctx, scheduler.Job{
		ID:      "my-exists-1",
		Name:    "test",
		Trigger: mustInterval(t, time.Second),
		Fn:      func(_ context.Context) error { return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ok, err := s.Exists(ctx, "my-exists-1")
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

func TestMySQL_Delete(t *testing.T) {
	ctx := t.Context()
	s := newJDBCScheduler(t, myDB, "mysql", "my_del_")

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "my-del-1",
		Name:    "deletable",
		Trigger: mustInterval(t, 100*time.Millisecond),
		Fn:      func(_ context.Context) error { count.Add(1); return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)

	if err := s.Delete(ctx, "my-del-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	ok, err := s.Exists(ctx, "my-del-1")
	if err != nil {
		t.Fatalf("Exists after delete: %v", err)
	}
	if ok {
		t.Fatal("expected job to not exist after delete")
	}
}

func TestMySQL_OnceTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newJDBCScheduler(t, myDB, "mysql", "my_once_")

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "my-once-1",
		Name:    "fire once",
		Trigger: scheduler.NewOnceTrigger(time.Now().Add(100 * time.Millisecond)),
		Fn:      func(_ context.Context) error { count.Add(1); return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()
	time.Sleep(500 * time.Millisecond)
	cancel()

	if count.Load() != 1 {
		t.Fatalf("expected once trigger to fire exactly 1 time, got %d", count.Load())
	}
}

func TestMySQL_CronTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newJDBCScheduler(t, myDB, "mysql", "my_cron_")

	cronTrigger, err := scheduler.NewCronTrigger("*/1 * * * * *")
	if err != nil {
		t.Fatalf("NewCronTrigger: %v", err)
	}

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "my-cron-1",
		Name:    "every second",
		Trigger: cronTrigger,
		Fn:      func(_ context.Context) error { count.Add(1); return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()
	time.Sleep(3 * time.Second)
	cancel()

	if count.Load() == 0 {
		t.Fatal("expected cron job to have fired at least once")
	}
}
