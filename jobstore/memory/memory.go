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
	mu   sync.RWMutex
	jobs map[scheduler.JobID]*scheduler.JobRecord
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

// AcquireNextJobs finds due jobs and marks them ACQUIRED in a single operation.
func (s *Store) AcquireNextJobs(_ context.Context, now time.Time, instanceID string) ([]*scheduler.JobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*scheduler.JobRecord
	ts := time.Now()
	for _, job := range s.jobs {
		if job.State != scheduler.StateWaiting || !job.Enabled || job.NextFireTime.IsZero() || job.NextFireTime.After(now) {
			continue
		}
		job.State = scheduler.StateAcquired
		job.InstanceID = instanceID
		job.AcquiredAt = ts
		job.UpdatedAt = ts
		cp := *job
		result = append(result, &cp)
	}
	return result, nil
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

func (*Store) RecordExecution(_ context.Context, _ *scheduler.ExecutionRecord) error {
	return nil
}

// RecoverStaleJobs resets jobs stuck in ACQUIRED state longer than the threshold.
// For jobs with a Timeout longer than the threshold, the timeout is used instead
// to avoid recovering jobs that are still legitimately running.
func (s *Store) RecoverStaleJobs(_ context.Context, threshold time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	recovered := 0
	for _, job := range s.jobs {
		if job.State != scheduler.StateAcquired || job.AcquiredAt.IsZero() {
			continue
		}
		// Use the longer of misfireThreshold and job timeout.
		staleAfter := threshold
		if job.Timeout > staleAfter {
			staleAfter = job.Timeout
		}
		if !job.AcquiredAt.Before(now.Add(-staleAfter)) {
			continue
		}
		job.State = scheduler.StateWaiting
		job.InstanceID = ""
		job.AcquiredAt = time.Time{}
		job.UpdatedAt = now
		recovered++
	}
	return recovered, nil
}

func (s *Store) NextFireTime(_ context.Context) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var earliest time.Time
	for _, job := range s.jobs {
		if job.State != scheduler.StateWaiting || !job.Enabled || job.NextFireTime.IsZero() {
			continue
		}
		if earliest.IsZero() || job.NextFireTime.Before(earliest) {
			earliest = job.NextFireTime
		}
	}
	return earliest, nil
}

func (*Store) PurgeExecutions(_ context.Context, _ time.Time) (int, error) {
	return 0, nil
}

func (*Store) Close() error {
	return nil
}
