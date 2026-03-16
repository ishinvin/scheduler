package memory

import (
	"context"
	"sync"
	"time"

	"github.com/ishinvin/scheduler"
)

// Store is an in-memory JobStore implementation.
// Suitable for single-instance deployments — no distributed locking.
type Store struct {
	mu         sync.RWMutex
	jobs       map[scheduler.JobID]*scheduler.JobRecord
	executions []*scheduler.ExecutionRecord
}

// New creates a new in-memory job store.
func New() *Store {
	return &Store{
		jobs: make(map[scheduler.JobID]*scheduler.JobRecord),
	}
}

func (s *Store) SaveJob(_ context.Context, job *scheduler.JobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if existing, ok := s.jobs[job.ID]; ok {
		job.CreatedAt = existing.CreatedAt
		job.UpdatedAt = now
	} else {
		job.CreatedAt = now
		job.UpdatedAt = now
	}

	cp := *job
	s.jobs[job.ID] = &cp
	return nil
}

func (s *Store) DeleteJob(_ context.Context, id scheduler.JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return scheduler.ErrJobNotFound
	}
	delete(s.jobs, id)
	return nil
}

func (s *Store) GetJob(_ context.Context, id scheduler.JobID) (*scheduler.JobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, scheduler.ErrJobNotFound
	}
	cp := *job
	return &cp, nil
}

func (s *Store) ListJobs(_ context.Context) ([]*scheduler.JobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*scheduler.JobRecord, 0, len(s.jobs))
	for _, job := range s.jobs {
		cp := *job
		result = append(result, &cp)
	}
	return result, nil
}

// ListDueJobs returns jobs in WAITING state whose next fire time is at or before now.
func (s *Store) ListDueJobs(_ context.Context, now time.Time) ([]*scheduler.JobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*scheduler.JobRecord
	for _, job := range s.jobs {
		if job.State == scheduler.StateWaiting && job.Enabled && !job.NextFireTime.IsZero() && !job.NextFireTime.After(now) {
			cp := *job
			result = append(result, &cp)
		}
	}
	return result, nil
}

// AcquireJob always succeeds for RAM store (single-instance, no contention).
func (s *Store) AcquireJob(_ context.Context, id scheduler.JobID, instanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return scheduler.ErrJobNotFound
	}
	if job.State == scheduler.StateAcquired {
		return scheduler.ErrLockNotAcquired
	}
	now := time.Now()
	job.State = scheduler.StateAcquired
	job.InstanceID = instanceID
	job.AcquiredAt = now
	job.UpdatedAt = now
	return nil
}

// ReleaseJob transitions a job back to WAITING with updated next fire time.
func (s *Store) ReleaseJob(_ context.Context, id scheduler.JobID, nextFireTime time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return scheduler.ErrJobNotFound
	}
	if nextFireTime.IsZero() {
		job.State = scheduler.StateComplete
	} else {
		job.State = scheduler.StateWaiting
	}
	job.InstanceID = ""
	job.AcquiredAt = time.Time{}
	job.NextFireTime = nextFireTime
	job.UpdatedAt = time.Now()
	return nil
}

func (s *Store) RecordExecution(_ context.Context, exec *scheduler.ExecutionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *exec
	s.executions = append(s.executions, &cp)
	return nil
}

// RecoverStaleJobs resets jobs stuck in ACQUIRED state longer than the threshold.
func (s *Store) RecoverStaleJobs(_ context.Context, threshold time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-threshold)
	recovered := 0
	for _, job := range s.jobs {
		if job.State != scheduler.StateAcquired || job.AcquiredAt.IsZero() || !job.AcquiredAt.Before(cutoff) {
			continue
		}
		job.State = scheduler.StateWaiting
		job.InstanceID = ""
		job.AcquiredAt = time.Time{}
		job.UpdatedAt = time.Now()
		recovered++
	}
	return recovered, nil
}

func (*Store) Close() error {
	return nil
}
