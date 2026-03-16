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
	defaultMisfireThreshold = 10 * time.Minute
	defaultPollInterval     = 15 * time.Second
	cleanupTimeout          = 30 * time.Second
)

// Scheduler orchestrates job scheduling using a pluggable JobStore.
// The store is the sole source of truth for scheduling state.
type Scheduler struct {
	handlers         sync.Map // JobID → func(ctx context.Context) error
	store            JobStore
	logger           Logger
	location         *time.Location
	instanceID       string
	misfireThreshold time.Duration
	pollInterval     time.Duration

	ctx    context.Context
	wakeUp chan struct{}
	wg     sync.WaitGroup
}

// New creates a new Scheduler with the given options.
// A JobStore must be provided via WithJobStore (e.g., memory.New() or jdbc.New()).
// Default: slog logger, UTC timezone, 15s poll interval.
func New(opts ...Option) *Scheduler {
	s := &Scheduler{
		logger:           &slogLogger{},
		location:         time.UTC,
		misfireThreshold: defaultMisfireThreshold,
		pollInterval:     defaultPollInterval,
		wakeUp:           make(chan struct{}, 1),
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
// If Fn is set, it is stored as the handler for this job ID, so that
// jobs loaded from the store can resolve their execution function.
//
// If Trigger is nil, only the handler is registered (no job is scheduled).
// This is useful on restart to provide Fn for jobs already in the store.
func (s *Scheduler) Register(ctx context.Context, job Job) error {
	// Store Fn as handler keyed by job ID.
	if job.Fn != nil {
		s.handlers.Store(job.ID, job.Fn)
	}

	// Handler-only registration — no scheduling.
	if job.Trigger == nil {
		return nil
	}

	if s.store == nil {
		return fmt.Errorf("scheduler: no job store configured (use WithJobStore)")
	}

	// Check for duplicates in the store.
	if _, err := s.store.GetJob(ctx, job.ID); err == nil {
		return ErrJobAlreadyExists
	}

	now := time.Now().In(s.location)
	next := job.Trigger.NextFireTime(now)

	record := jobToRecord(&job, next)
	if err := s.store.SaveJob(ctx, record); err != nil {
		return fmt.Errorf("scheduler: register job: %w", err)
	}

	s.signal()
	return nil
}

// Reschedule updates the trigger of an existing job.
func (s *Scheduler) Reschedule(ctx context.Context, id JobID, trigger Trigger) error {
	rec, err := s.store.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return ErrJobNotFound
		}
		return fmt.Errorf("scheduler: reschedule job: %w", err)
	}

	now := time.Now().In(s.location)
	nextRun := trigger.NextFireTime(now)

	// Build a temporary Job to convert trigger to record fields.
	job := Job{
		ID:      rec.ID,
		Name:    rec.Name,
		Trigger: trigger,
	}
	updated := jobToRecord(&job, nextRun)
	// Preserve state — don't reset an ACQUIRED job.
	updated.State = rec.State
	updated.InstanceID = rec.InstanceID
	updated.AcquiredAt = rec.AcquiredAt

	if err := s.store.SaveJob(ctx, updated); err != nil {
		return fmt.Errorf("scheduler: reschedule job: %w", err)
	}

	s.signal()
	return nil
}

// Delete removes a job from the scheduler and the job store.
func (s *Scheduler) Delete(ctx context.Context, id JobID) error {
	if err := s.store.DeleteJob(ctx, id); err != nil {
		return fmt.Errorf("scheduler: delete job: %w", err)
	}
	s.handlers.Delete(id)
	s.signal()
	return nil
}

// Run starts the scheduling loop. It blocks until ctx is canceled,
// then waits for in-flight jobs to complete before returning.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.store == nil {
		return fmt.Errorf("scheduler: no job store configured (use WithJobStore)")
	}

	s.ctx = ctx

	// If the store implements JobStoreInitializer, call Init (e.g., schema creation).
	if init, ok := s.store.(JobStoreInitializer); ok {
		if err := init.Init(s.ctx); err != nil {
			return fmt.Errorf("scheduler: store init: %w", err)
		}
	}

	// Start stale job recovery ticker.
	var recoveryTicker *time.Ticker
	var recoveryC <-chan time.Time
	if s.misfireThreshold > 0 {
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
		s.dispatchDueJobs(now)

		if done := s.waitForEvent(s.pollInterval, recoveryC); done {
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

// dispatchDueJobs queries the store for due jobs and dispatches them.
func (s *Scheduler) dispatchDueJobs(now time.Time) {
	records, err := s.store.ListDueJobs(s.ctx, now)
	if err != nil {
		s.logger.Error("failed to query due jobs from store", "error", err)
		return
	}

	for _, rec := range records {
		fn := s.resolveHandler(rec.ID)
		if fn == nil {
			// No handler registered on this instance.
			continue
		}

		trigger, err := triggerFromRecord(rec)
		if err != nil {
			s.logger.Error("failed to parse trigger from store", "job", rec.ID, "error", err)
			continue
		}

		job := Job{
			ID:      rec.ID,
			Name:    rec.Name,
			Trigger: trigger,
			Fn:      fn,
		}

		s.wg.Add(1)
		go s.executeJob(job)
	}
}

// resolveHandler looks up the Fn for a job ID from the handler registry.
func (s *Scheduler) resolveHandler(id JobID) func(ctx context.Context) error {
	v, ok := s.handlers.Load(id)
	if !ok {
		return nil
	}
	return v.(func(ctx context.Context) error)
}

// executeJob runs a single job with store-level acquire/release.
func (s *Scheduler) executeJob(job Job) {
	defer s.wg.Done()

	// Acquire job via store (distributed lock for JDBC, contention check for RAM).
	if err := s.store.AcquireJob(s.ctx, job.ID, s.instanceID); err != nil {
		if !errors.Is(err, ErrLockNotAcquired) {
			s.logger.Error("failed to acquire job", "job", job.ID, "error", err)
		}
		return
	}

	// Build execution context with optional timeout.
	var execCtx context.Context
	var execCancel context.CancelFunc
	if job.Timeout > 0 {
		execCtx, execCancel = context.WithTimeout(s.ctx, job.Timeout)
	} else {
		execCtx, execCancel = context.WithCancel(s.ctx)
	}
	defer execCancel()

	startedAt := time.Now()
	err := job.Fn(execCtx)
	finishedAt := time.Now().In(s.location)

	// Compute next fire time for release.
	nextFire := job.Trigger.NextFireTime(finishedAt)

	// Release job back to WAITING with updated next fire time.
	// Use a detached context so cleanup completes even during shutdown.
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

	if err != nil {
		s.logger.Error("job execution failed", "job", job.ID, "error", err)
	}
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
