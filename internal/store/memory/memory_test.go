package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ishinvin/scheduler/internal/store"
)

func newTestJob(id string, fireAt time.Time) *store.JobRecord {
	return &store.JobRecord{
		ID:           id,
		Name:         "test-" + id,
		TriggerType:  "interval",
		TriggerValue: "1s",
		State:        store.StateWaiting,
		NextFireTime: fireAt,
	}
}

func TestCreateAndGetJob(t *testing.T) {
	s := NewMemory()
	ctx := context.Background()

	job := newTestJob("j1", time.Now().Add(time.Second))
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	got, err := s.GetJob(ctx, "j1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ID != "j1" {
		t.Errorf("ID = %q, want j1", got.ID)
	}
}

func TestGetJobNotFound(t *testing.T) {
	s := NewMemory()
	_, err := s.GetJob(context.Background(), "missing")
	if !errors.Is(err, store.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestUpdateJob(t *testing.T) {
	s := NewMemory()
	ctx := context.Background()

	job := newTestJob("j1", time.Now().Add(time.Second))
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	job.Name = "updated"
	if err := s.UpdateJob(ctx, job); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	got, _ := s.GetJob(ctx, "j1")
	if got.Name != "updated" {
		t.Errorf("Name = %q, want updated", got.Name)
	}
}

func TestUpdateJobNotFound(t *testing.T) {
	s := NewMemory()
	job := newTestJob("missing", time.Now())
	err := s.UpdateJob(context.Background(), job)
	if !errors.Is(err, store.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestDeleteJob(t *testing.T) {
	s := NewMemory()
	ctx := context.Background()

	job := newTestJob("j1", time.Now().Add(time.Second))
	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if err := s.DeleteJob(ctx, "j1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}

	_, err := s.GetJob(ctx, "j1")
	if !errors.Is(err, store.ErrJobNotFound) {
		t.Fatal("expected job to be deleted")
	}
}

func TestDeleteJobNotFound(t *testing.T) {
	s := NewMemory()
	err := s.DeleteJob(context.Background(), "missing")
	if !errors.Is(err, store.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestAcquireNextJobs(t *testing.T) {
	s := NewMemory()
	ctx := context.Background()

	past := time.Now().Add(-time.Second)
	future := time.Now().Add(time.Hour)

	if err := s.CreateJob(ctx, newTestJob("due", past)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := s.CreateJob(ctx, newTestJob("not-due", future)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	acquired, err := s.AcquireNextJobs(ctx, time.Now(), "inst-1")
	if err != nil {
		t.Fatalf("AcquireNextJobs: %v", err)
	}
	if len(acquired) != 1 {
		t.Fatalf("expected 1 acquired job, got %d", len(acquired))
	}
	if acquired[0].ID != "due" {
		t.Errorf("acquired job ID = %q, want due", acquired[0].ID)
	}
	if acquired[0].State != store.StateAcquired {
		t.Errorf("state = %q, want ACQUIRED", acquired[0].State)
	}
}

func TestReleaseJob(t *testing.T) {
	s := NewMemory()
	ctx := context.Background()

	past := time.Now().Add(-time.Second)
	if err := s.CreateJob(ctx, newTestJob("j1", past)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := s.AcquireNextJobs(ctx, time.Now(), "inst-1"); err != nil {
		t.Fatalf("AcquireNextJobs: %v", err)
	}

	nextFire := time.Now().Add(time.Minute)
	if err := s.ReleaseJob(ctx, "j1", nextFire); err != nil {
		t.Fatalf("ReleaseJob: %v", err)
	}

	got, _ := s.GetJob(ctx, "j1")
	if got.State != store.StateWaiting {
		t.Errorf("state = %q, want WAITING", got.State)
	}
}

func TestReleaseJobComplete(t *testing.T) {
	s := NewMemory()
	ctx := context.Background()

	past := time.Now().Add(-time.Second)
	if err := s.CreateJob(ctx, newTestJob("j1", past)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := s.AcquireNextJobs(ctx, time.Now(), "inst-1"); err != nil {
		t.Fatalf("AcquireNextJobs: %v", err)
	}

	// Zero next fire time → COMPLETE
	if err := s.ReleaseJob(ctx, "j1", time.Time{}); err != nil {
		t.Fatalf("ReleaseJob: %v", err)
	}

	got, _ := s.GetJob(ctx, "j1")
	if got.State != store.StateComplete {
		t.Errorf("state = %q, want COMPLETE", got.State)
	}
}

func TestRecoverStaleJobs(t *testing.T) {
	s := NewMemory()
	ctx := context.Background()

	past := time.Now().Add(-time.Second)
	if err := s.CreateJob(ctx, newTestJob("j1", past)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := s.AcquireNextJobs(ctx, time.Now(), "inst-1"); err != nil {
		t.Fatalf("AcquireNextJobs: %v", err)
	}

	// Not stale yet (threshold 1 hour)
	n, _ := s.RecoverStaleJobs(ctx, time.Hour)
	if n != 0 {
		t.Fatalf("expected 0 recovered, got %d", n)
	}

	// Stale (threshold 0)
	n, _ = s.RecoverStaleJobs(ctx, 0)
	if n != 1 {
		t.Fatalf("expected 1 recovered, got %d", n)
	}

	got, _ := s.GetJob(ctx, "j1")
	if got.State != store.StateWaiting {
		t.Errorf("state = %q, want WAITING", got.State)
	}
}

func TestNextFireTime(t *testing.T) {
	s := NewMemory()
	ctx := context.Background()

	// No jobs → zero time
	next, err := s.NextFireTime(ctx)
	if err != nil {
		t.Fatalf("NextFireTime: %v", err)
	}
	if !next.IsZero() {
		t.Errorf("expected zero time, got %v", next)
	}

	t1 := time.Now().Add(time.Hour)
	t2 := time.Now().Add(time.Minute)
	if err := s.CreateJob(ctx, newTestJob("j1", t1)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := s.CreateJob(ctx, newTestJob("j2", t2)); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	next, _ = s.NextFireTime(ctx)
	if !next.Equal(t2) {
		t.Errorf("NextFireTime = %v, want %v", next, t2)
	}
}

func TestCreateSchema(t *testing.T) {
	s := NewMemory()
	if err := s.CreateSchema(context.Background()); err != nil {
		t.Fatalf("CreateSchema: %v", err)
	}
}
