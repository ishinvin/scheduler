package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ishinvin/scheduler"
)

func TestPostgres_InitSchemaIdempotent(t *testing.T) {
	ctx := t.Context()
	s := newJDBCScheduler(t, pgDB, "postgres", "pg_init_")

	if err := s.InitSchema(ctx); err != nil {
		t.Fatalf("InitSchema (idempotent): %v", err)
	}
}

func TestPostgres_RegisterAndExecute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newJDBCScheduler(t, pgDB, "postgres", "pg_regexec_")

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "pg-exec-1",
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

func TestPostgres_DuplicateRegister(t *testing.T) {
	ctx := t.Context()
	s := newJDBCScheduler(t, pgDB, "postgres", "pg_dup_")

	job := scheduler.Job{
		ID:      "pg-dup-1",
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

func TestPostgres_Reschedule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newJDBCScheduler(t, pgDB, "postgres", "pg_resched_")

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "pg-resched-1",
		Name:    "slow job",
		Trigger: mustInterval(t, 1*time.Hour),
		Fn:      func(_ context.Context) error { count.Add(1); return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	err := s.Reschedule(ctx, scheduler.Job{
		ID:      "pg-resched-1",
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

func TestPostgres_Exists(t *testing.T) {
	ctx := t.Context()
	s := newJDBCScheduler(t, pgDB, "postgres", "pg_exists_")

	if err := s.Register(ctx, scheduler.Job{
		ID:      "pg-exists-1",
		Name:    "test",
		Trigger: mustInterval(t, time.Second),
		Fn:      func(_ context.Context) error { return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ok, err := s.Exists(ctx, "pg-exists-1")
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

func TestPostgres_Delete(t *testing.T) {
	ctx := t.Context()
	s := newJDBCScheduler(t, pgDB, "postgres", "pg_del_")

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "pg-del-1",
		Name:    "deletable",
		Trigger: mustInterval(t, 100*time.Millisecond),
		Fn:      func(_ context.Context) error { count.Add(1); return nil },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _ = s.Run(ctx) }()
	time.Sleep(300 * time.Millisecond)

	if err := s.Delete(ctx, "pg-del-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	ok, err := s.Exists(ctx, "pg-del-1")
	if err != nil {
		t.Fatalf("Exists after delete: %v", err)
	}
	if ok {
		t.Fatal("expected job to not exist after delete")
	}
}

func TestPostgres_OnceTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newJDBCScheduler(t, pgDB, "postgres", "pg_once_")

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "pg-once-1",
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

func TestPostgres_CronTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := newJDBCScheduler(t, pgDB, "postgres", "pg_cron_")

	cronTrigger, err := scheduler.NewCronTrigger("*/1 * * * * *")
	if err != nil {
		t.Fatalf("NewCronTrigger: %v", err)
	}

	var count atomic.Int32
	if err := s.Register(ctx, scheduler.Job{
		ID:      "pg-cron-1",
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
