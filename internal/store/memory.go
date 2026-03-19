package store

import (
	"context"
	"sync"
	"time"
)

// MemoryStore is an in-memory JobStore implementation.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[JobID]*JobRecord
}

// NewMemory creates a new in-memory job store.
func NewMemory() *MemoryStore {
	return &MemoryStore{
		jobs: make(map[JobID]*JobRecord),
	}
}

func (s *MemoryStore) CreateJob(_ context.Context, job *JobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	job.CreatedAt = now
	job.UpdatedAt = now

	cp := *job
	s.jobs[job.ID] = &cp
	return nil
}

func (s *MemoryStore) UpdateJob(_ context.Context, job *JobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.jobs[job.ID]
	if !ok {
		return ErrJobNotFound
	}

	job.CreatedAt = existing.CreatedAt
	job.UpdatedAt = time.Now()

	cp := *job
	s.jobs[job.ID] = &cp
	return nil
}

func (s *MemoryStore) DeleteJob(_ context.Context, id JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return ErrJobNotFound
	}
	delete(s.jobs, id)
	return nil
}

func (s *MemoryStore) GetJob(_ context.Context, id JobID) (*JobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	cp := *job
	return &cp, nil
}

// AcquireNextJobs finds due jobs and marks them ACQUIRED.
func (s *MemoryStore) AcquireNextJobs(_ context.Context, now time.Time, instanceID string) ([]*JobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*JobRecord
	ts := time.Now()
	for _, job := range s.jobs {
		if job.State != StateWaiting || !job.Enabled || job.NextFireTime.IsZero() || job.NextFireTime.After(now) {
			continue
		}
		job.State = StateAcquired
		job.InstanceID = instanceID
		job.AcquiredAt = ts
		job.UpdatedAt = ts
		cp := *job
		result = append(result, &cp)
	}
	return result, nil
}

// ReleaseJob transitions a job back to WAITING, or COMPLETE if nextFireTime is zero.
func (s *MemoryStore) ReleaseJob(_ context.Context, id JobID, nextFireTime time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if nextFireTime.IsZero() {
		job.State = StateComplete
	} else {
		job.State = StateWaiting
	}
	job.InstanceID = ""
	job.AcquiredAt = time.Time{}
	job.NextFireTime = nextFireTime
	job.UpdatedAt = time.Now()
	return nil
}

// RecoverStaleJobs resets jobs stuck in ACQUIRED longer than the threshold.
func (s *MemoryStore) RecoverStaleJobs(_ context.Context, threshold time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	recovered := 0
	for _, job := range s.jobs {
		if job.State != StateAcquired || job.AcquiredAt.IsZero() {
			continue
		}
		staleAfter := threshold
		if job.Timeout > staleAfter {
			staleAfter = job.Timeout
		}
		if !job.AcquiredAt.Before(now.Add(-staleAfter)) {
			continue
		}
		job.State = StateWaiting
		job.InstanceID = ""
		job.AcquiredAt = time.Time{}
		job.UpdatedAt = now
		recovered++
	}
	return recovered, nil
}

func (s *MemoryStore) NextFireTime(_ context.Context) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var earliest time.Time
	for _, job := range s.jobs {
		if job.State != StateWaiting || !job.Enabled || job.NextFireTime.IsZero() {
			continue
		}
		if earliest.IsZero() || job.NextFireTime.Before(earliest) {
			earliest = job.NextFireTime
		}
	}
	return earliest, nil
}
