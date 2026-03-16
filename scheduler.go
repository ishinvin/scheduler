package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

const (
	defaultMisfireThreshold       = 10 * time.Minute
	defaultIdleWait               = 24 * time.Hour
	defaultClusterCheckinInterval = 15 * time.Second
	cleanupTimeout                = 30 * time.Second
)

// entry is the internal runtime representation of a scheduled job.
type entry struct {
	job     Job
	nextRun time.Time
}

// Scheduler orchestrates job scheduling using a pluggable JobStore.
type Scheduler struct {
	mu               sync.RWMutex
	jobs             map[JobID]*entry
	handlers         map[string]func(ctx context.Context) error
	store            JobStore
	logger           Logger
	location         *time.Location
	instanceID       string
	misfireThreshold time.Duration

	// Cluster mode: DB-driven dispatch instead of in-memory map.
	clusterMode            bool
	clusterCheckinInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wakeUp chan struct{}
	wg     sync.WaitGroup
}

// New creates a new Scheduler with the given options.
// Default: RAM job store, slog logger, UTC timezone.
func New(opts ...Option) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		jobs:                   make(map[JobID]*entry),
		handlers:               make(map[string]func(ctx context.Context) error),
		logger:                 &slogLogger{},
		location:               time.UTC,
		misfireThreshold:       defaultMisfireThreshold,
		clusterCheckinInterval: defaultClusterCheckinInterval,
		ctx:                    ctx,
		cancel:                 cancel,
		wakeUp:                 make(chan struct{}, 1),
	}

	hostname, _ := os.Hostname()
	s.instanceID = hostname

	for _, o := range opts {
		o(s)
	}
	return s
}

// Register adds a job to the scheduler and persists it to the job store.
//
// If Fn is set, it is also stored as the handler for this job ID, so that
// rehydrated jobs (loaded from the DB on restart) can resolve it.
//
// If Trigger is nil, only the handler is registered (no job is scheduled).
// This is useful on restart to provide Fn for jobs that will be rehydrated.
func (s *Scheduler) Register(ctx context.Context, job Job) error {
	// Store Fn as handler keyed by job ID.
	if job.Fn != nil {
		s.mu.Lock()
		s.handlers[string(job.ID)] = job.Fn
		s.mu.Unlock()
	}

	// Handler-only registration — no scheduling.
	if job.Trigger == nil {
		return nil
	}

	now := time.Now().In(s.location)
	next := job.Trigger.NextFireTime(now)

	// Persist to store.
	if s.store != nil {
		record := jobToRecord(&job, next)
		if err := s.store.SaveJob(ctx, record); err != nil {
			return fmt.Errorf("scheduler: register job: %w", err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[job.ID]; exists {
		return ErrJobAlreadyExists
	}
	entryNext := next
	if s.clusterMode {
		entryNext = time.Time{} // DB drives timing in cluster mode
	}
	s.jobs[job.ID] = &entry{job: job, nextRun: entryNext}
	s.signal()
	return nil
}

// Reschedule updates the trigger of an existing job.
func (s *Scheduler) Reschedule(ctx context.Context, id JobID, trigger Trigger) error {
	s.mu.Lock()
	e, exists := s.jobs[id]
	if !exists {
		s.mu.Unlock()
		return ErrJobNotFound
	}

	e.job.Trigger = trigger
	now := time.Now().In(s.location)
	nextRun := trigger.NextFireTime(now)
	if s.clusterMode {
		e.nextRun = time.Time{} // DB drives timing in cluster mode
	} else {
		e.nextRun = nextRun
	}
	job := e.job
	s.mu.Unlock()

	if s.store != nil {
		record := jobToRecord(&job, nextRun)
		if err := s.store.SaveJob(ctx, record); err != nil {
			return fmt.Errorf("scheduler: reschedule job: %w", err)
		}
	}
	s.signal()
	return nil
}

// Delete removes a job from the scheduler and the job store.
func (s *Scheduler) Delete(ctx context.Context, id JobID) error {
	s.mu.Lock()
	if _, exists := s.jobs[id]; !exists {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	delete(s.jobs, id)
	s.mu.Unlock()

	if s.store != nil {
		if err := s.store.DeleteJob(ctx, id); err != nil {
			return fmt.Errorf("scheduler: delete job: %w", err)
		}
	}
	s.signal()
	return nil
}

// Actor returns execute and interrupt functions compatible with oklog/run.Group.
func (s *Scheduler) Actor() (execute func() error, interrupt func(error)) {
	execute = func() error {
		return s.run()
	}
	interrupt = func(error) {
		s.cancel()
	}
	return
}

// run is the core scheduling loop.
func (s *Scheduler) run() error {
	// If the store implements JobStoreInitializer, call Init (e.g., schema creation).
	if s.store != nil {
		if init, ok := s.store.(JobStoreInitializer); ok {
			if err := init.Init(s.ctx); err != nil {
				return fmt.Errorf("scheduler: store init: %w", err)
			}
		}
	}

	if s.clusterMode {
		s.logger.Info("scheduler starting in cluster mode",
			"checkinInterval", s.clusterCheckinInterval)
	}

	if err := s.rehydrate(); err != nil {
		s.logger.Error("failed to rehydrate jobs from store", "error", err)
	}

	// Start stale job recovery ticker.
	var recoveryTicker *time.Ticker
	var recoveryC <-chan time.Time
	if s.store != nil && s.misfireThreshold > 0 {
		recoveryTicker = time.NewTicker(s.misfireThreshold / 2)
		recoveryC = recoveryTicker.C
		// Run recovery once at startup.
		s.recoverStaleJobs()
	}
	defer func() {
		if recoveryTicker != nil {
			recoveryTicker.Stop()
		}
	}()

	for {
		now := time.Now().In(s.location)

		var wait time.Duration
		if s.clusterMode {
			s.dispatchDueJobsFromDB(now)
			wait = s.clusterCheckinInterval
		} else {
			nextWake := s.dispatchDueJobs(now)
			if nextWake.IsZero() {
				wait = defaultIdleWait
			} else {
				wait = time.Until(nextWake)
				if wait < 0 {
					wait = 0
				}
			}
		}

		if done := s.waitForEvent(wait, recoveryC); done {
			return nil
		}
	}
}

// waitForEvent blocks until the timer expires, a signal arrives, recovery fires,
// or the context is canceled. Returns true if the scheduler should stop.
func (s *Scheduler) waitForEvent(wait time.Duration, recoveryC <-chan time.Time) bool {
	timer := time.NewTimer(wait)
	select {
	case <-timer.C:
		return false
	case <-s.wakeUp:
		timer.Stop()
		return false
	case <-recoveryC:
		timer.Stop()
		s.recoverStaleJobs()
		return false
	case <-s.ctx.Done():
		timer.Stop()
		s.wg.Wait()
		return true
	}
}

// dispatchDueJobs runs all jobs where nextRun <= now.
func (s *Scheduler) dispatchDueJobs(now time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	var earliest time.Time

	for _, e := range s.jobs {
		if e.nextRun.IsZero() {
			continue
		}

		if !e.nextRun.After(now) {
			s.wg.Add(1)
			go s.executeJob(e.job)

			if ot, ok := e.job.Trigger.(*OnceTrigger); ok {
				ot.MarkDone()
			}

			e.nextRun = e.job.Trigger.NextFireTime(now)
		}

		if !e.nextRun.IsZero() && (earliest.IsZero() || e.nextRun.Before(earliest)) {
			earliest = e.nextRun
		}
	}

	return earliest
}

// dispatchDueJobsFromDB queries the store for due jobs and dispatches them.
// Used in cluster mode where the DB is the source of truth.
func (s *Scheduler) dispatchDueJobsFromDB(now time.Time) {
	records, err := s.store.ListDueJobs(s.ctx, now)
	if err != nil {
		s.logger.Error("failed to query due jobs from store", "error", err)
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, rec := range records {
		e, exists := s.jobs[rec.ID]
		if !exists {
			// Job exists in DB but not locally — no Fn/handler available on this instance.
			continue
		}

		s.wg.Add(1)
		go s.executeJob(e.job)
	}
}

// executeJob runs a single job with store-level acquire/release.
func (s *Scheduler) executeJob(job Job) {
	defer s.wg.Done()

	// Acquire job via store (distributed lock for JDBC, no-op for RAM).
	if s.store != nil {
		if err := s.store.AcquireJob(s.ctx, job.ID, s.instanceID); err != nil {
			if !errors.Is(err, ErrLockNotAcquired) {
				s.logger.Error("failed to acquire job", "job", job.ID, "error", err)
			}
			return
		}
	}

	// Resolve the function to call.
	fn := job.Fn
	if fn == nil {
		s.mu.RLock()
		h, ok := s.handlers[string(job.ID)]
		s.mu.RUnlock()
		if !ok {
			s.logger.Error("handler not found", "job", job.ID)
			// Release the acquired lock so the job doesn't stay stuck.
			if s.store != nil {
				now := time.Now().In(s.location)
				_ = s.store.ReleaseJob(context.Background(), job.ID, job.Trigger.NextFireTime(now))
			}
			return
		}
		fn = h
	}

	// Build execution context with optional timeout.
	execCtx, execCancel := context.WithCancel(s.ctx)
	if job.Timeout > 0 {
		execCtx, execCancel = context.WithTimeout(s.ctx, job.Timeout)
	}
	defer execCancel()

	startedAt := time.Now()
	err := fn(execCtx)
	finishedAt := time.Now()

	// Compute next fire time for release.
	now := time.Now().In(s.location)
	nextFire := job.Trigger.NextFireTime(now)

	// Release job back to WAITING with updated next fire time.
	// Use a detached context so cleanup completes even during shutdown
	// (s.ctx may already be canceled when interrupt() was called).
	if s.store != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cleanupCancel()

		if releaseErr := s.store.ReleaseJob(cleanupCtx, job.ID, nextFire); releaseErr != nil {
			s.logger.Error("failed to release job", "job", job.ID, "error", releaseErr)
		}

		// Record execution.
		rec := &ExecutionRecord{
			JobID:      job.ID,
			Instance:   s.instanceID,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
		}
		if err != nil {
			rec.Err = err.Error()
		}
		if storeErr := s.store.RecordExecution(cleanupCtx, rec); storeErr != nil {
			s.logger.Error("failed to record execution", "job", job.ID, "error", storeErr)
		}
	}

	if err != nil {
		s.logger.Error("job execution failed", "job", job.ID, "error", err)
	}
}

// rehydrate loads jobs from the store on startup.
func (s *Scheduler) rehydrate() error {
	if s.store == nil {
		return nil
	}

	records, err := s.store.ListJobs(s.ctx)
	if err != nil {
		return fmt.Errorf("scheduler: list jobs: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().In(s.location)
	for _, rec := range records {
		if !rec.Enabled || rec.State == StateComplete {
			continue
		}
		if _, exists := s.jobs[rec.ID]; exists {
			continue
		}

		trigger, err := triggerFromRecord(rec)
		if err != nil {
			s.logger.Error("failed to parse trigger from store", "job", rec.ID, "error", err)
			continue
		}

		job := Job{
			ID:       rec.ID,
			Name:     rec.Name,
			Trigger:  trigger,
			Metadata: rec.Metadata,
		}

		var next time.Time
		if !s.clusterMode {
			// Single-instance: compute next fire time locally.
			next = trigger.NextFireTime(now)
		}
		// Cluster mode: next stays zero; DB drives scheduling decisions.
		s.jobs[rec.ID] = &entry{job: job, nextRun: next}
	}
	return nil
}

// recoverStaleJobs resets jobs stuck in ACQUIRED state past the misfire threshold.
func (s *Scheduler) recoverStaleJobs() {
	n, err := s.store.RecoverStaleJobs(s.ctx, s.misfireThreshold)
	if err != nil {
		s.logger.Error("failed to recover stale jobs", "error", err)
		return
	}
	if n > 0 {
		s.logger.Info("recovered stale jobs", "count", n)
	}
}

func (s *Scheduler) signal() {
	select {
	case s.wakeUp <- struct{}{}:
	default:
	}
}

// jobToRecord converts a Job to a JobRecord for the store.
func jobToRecord(job *Job, nextFire time.Time) *JobRecord {
	rec := &JobRecord{
		ID:           job.ID,
		Name:         job.Name,
		Metadata:     job.Metadata,
		NextFireTime: nextFire,
		State:        StateWaiting,
		Enabled:      true,
	}

	switch t := job.Trigger.(type) {
	case *CronTrigger:
		rec.TriggerType = "cron"
		rec.TriggerValue = t.Expr
	case *OnceTrigger:
		rec.TriggerType = "once"
		rec.TriggerValue = t.At.Format(time.RFC3339)
	case *IntervalTrigger:
		rec.TriggerType = "interval"
		rec.TriggerValue = t.Every.String()
	}

	return rec
}

// triggerFromRecord reconstructs a Trigger from a stored JobRecord.
func triggerFromRecord(rec *JobRecord) (Trigger, error) {
	switch rec.TriggerType {
	case "cron":
		return NewCronTrigger(rec.TriggerValue)
	case "once":
		t, err := time.Parse(time.RFC3339, rec.TriggerValue)
		if err != nil {
			return nil, fmt.Errorf("invalid once trigger: %w", err)
		}
		return NewOnceTrigger(t), nil
	case "interval":
		d, err := time.ParseDuration(rec.TriggerValue)
		if err != nil {
			return nil, fmt.Errorf("invalid interval trigger: %w", err)
		}
		return NewIntervalTrigger(d), nil
	default:
		return nil, fmt.Errorf("unknown trigger type: %s", rec.TriggerType)
	}
}

type slogLogger struct{}

func (*slogLogger) Info(msg string, keysAndValues ...any) {
	slog.Info(msg, keysAndValues...)
}

func (*slogLogger) Error(msg string, keysAndValues ...any) {
	slog.Error(msg, keysAndValues...)
}
