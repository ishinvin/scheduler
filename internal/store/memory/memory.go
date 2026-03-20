package memory

import (
	"context"
	"sync"
	"time"

	"github.com/ishinvin/scheduler/internal/store"
)

// MemoryStore is an in-memory JobStore implementation.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*store.JobRecord
}

// NewMemory creates a new in-memory job store.
func NewMemory() store.JobStore {
	return &MemoryStore{
		jobs: make(map[string]*store.JobRecord),
	}
}

func (*MemoryStore) CreateSchema(_ context.Context) error { return nil }

func (s *MemoryStore) CreateJob(_ context.Context, job *store.JobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	job.CreatedAt = now
	job.UpdatedAt = now

	cp := *job
	s.jobs[job.ID] = &cp
	return nil
}

func (s *MemoryStore) UpdateJob(_ context.Context, job *store.JobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.jobs[job.ID]
	if !ok {
		return store.ErrJobNotFound
	}

	job.CreatedAt = existing.CreatedAt
	job.UpdatedAt = time.Now()

	cp := *job
	s.jobs[job.ID] = &cp
	return nil
}

func (s *MemoryStore) DeleteJob(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return store.ErrJobNotFound
	}
	delete(s.jobs, id)
	return nil
}

func (s *MemoryStore) GetJob(_ context.Context, id string) (*store.JobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, store.ErrJobNotFound
	}
	cp := *job
	return &cp, nil
}

// AcquireNextJobs finds due jobs and marks them ACQUIRED.
func (s *MemoryStore) AcquireNextJobs(_ context.Context, now time.Time, instanceID string) ([]*store.JobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*store.JobRecord
	ts := time.Now()
	for _, job := range s.jobs {
		if job.State != store.StateWaiting || job.NextFireTime.IsZero() || job.NextFireTime.After(now) {
			continue
		}
		job.State = store.StateAcquired
		job.InstanceID = instanceID
		job.AcquiredAt = ts
		job.UpdatedAt = ts
		cp := *job
		result = append(result, &cp)
	}
	return result, nil
}

// ReleaseJob transitions a job back to WAITING, or COMPLETE if nextFireTime is zero.
func (s *MemoryStore) ReleaseJob(_ context.Context, id string, nextFireTime time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return store.ErrJobNotFound
	}
	if nextFireTime.IsZero() {
		job.State = store.StateComplete
	} else {
		job.State = store.StateWaiting
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
		if job.State != store.StateAcquired || job.AcquiredAt.IsZero() {
			continue
		}
		staleAfter := threshold
		if job.Timeout > staleAfter {
			staleAfter = job.Timeout
		}
		if !job.AcquiredAt.Before(now.Add(-staleAfter)) {
			continue
		}
		job.State = store.StateWaiting
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
		if job.State != store.StateWaiting || job.NextFireTime.IsZero() {
			continue
		}
		if earliest.IsZero() || job.NextFireTime.Before(earliest) {
			earliest = job.NextFireTime
		}
	}
	return earliest, nil
}
