package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	defaultMisfireThreshold = 1 * time.Minute
	defaultPollInterval     = 15 * time.Second
	defaultShutdownTimeout  = 30 * time.Second
	defaultCleanupTimeout   = 5 * time.Second
)

// Scheduler orchestrates job scheduling using a pluggable JobStore.
// The store is the sole source of truth for scheduling state.
type Scheduler struct {
	handlers         sync.Map // JobID → func(ctx context.Context) error
	store            JobStore
	verbose          bool
	location         *time.Location
	instanceID       string
	misfireThreshold time.Duration
	pollInterval     time.Duration
	shutdownTimeout  time.Duration
	cleanupTimeout   time.Duration

	ctx    context.Context
	wakeUp chan struct{}
	wg     sync.WaitGroup
}

// New creates a new Scheduler with the given options.
// A JobStore must be provided via WithJobStore (e.g., memory.New() or jdbc.New()).
// If the store implements JobStoreInitializer, Init is called during construction
// (e.g., schema creation for JDBC stores with InitSchemaAlways).
// Default: slog logger, UTC timezone, 30s poll interval.
func New(ctx context.Context, opts ...Option) (*Scheduler, error) {
	s := &Scheduler{
		verbose:          false,
		location:         time.UTC,
		misfireThreshold: defaultMisfireThreshold,
		pollInterval:     defaultPollInterval,
		shutdownTimeout:  defaultShutdownTimeout,
		cleanupTimeout:   defaultCleanupTimeout,
		wakeUp:           make(chan struct{}, 1),
	}

	hostname, _ := os.Hostname()
	s.instanceID = hostname

	for _, o := range opts {
		o(s)
	}

	// Initialize the store (e.g., schema creation) if it supports it.
	if init, ok := s.store.(JobStoreInitializer); ok {
		if err := init.Init(ctx); err != nil {
			return nil, fmt.Errorf("scheduler: store init: %w", err)
		}
	}

	return s, nil
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

	// Idempotent: if the job already exists, just keep the handler registered above.
	if _, err := s.store.GetJob(ctx, job.ID); !errors.Is(err, ErrJobNotFound) {
		return err
	}

	now := time.Now().In(s.location)
	next := job.Trigger.NextFireTime(now)

	record := jobToRecord(&job, next)
	if err := s.store.SaveJob(ctx, record); err != nil {
		return fmt.Errorf("scheduler: register job: %w", err)
	}

	s.logInfo("job registered", "job", job.ID, "trigger", record.TriggerType, "next", next)
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

	s.logInfo("job rescheduled", "job", id, "trigger", updated.TriggerType, "next", nextRun)
	s.signal()
	return nil
}

// Delete removes a job from the scheduler and the job store.
func (s *Scheduler) Delete(ctx context.Context, id JobID) error {
	if err := s.store.DeleteJob(ctx, id); err != nil {
		return fmt.Errorf("scheduler: delete job: %w", err)
	}
	s.handlers.Delete(id)
	s.logInfo("job deleted", "job", id)
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
	s.logInfo("scheduler starting", "instance", s.instanceID, "poll_interval", s.pollInterval, "misfire_threshold", s.misfireThreshold)

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

		records, err := s.store.AcquireNextJobs(s.ctx, now, s.instanceID)
		if err != nil {
			s.logError("failed to acquire due jobs from store", "error", err)
		}

		if len(records) > 0 {
			s.logInfo("acquired jobs", "count", len(records))
			// Sort by fire time for precise scheduling.
			sort.Slice(records, func(i, j int) bool {
				return records[i].NextFireTime.Before(records[j].NextFireTime)
			})

			reloop := s.dispatchBatch(records, recoveryC)
			if s.ctx.Err() != nil {
				s.waitForInFlight()
				return nil
			}
			if reloop {
				continue // batch done or interrupted — re-poll immediately
			}
		}

		if done := s.waitForEvent(s.nextPollWait(), recoveryC); done {
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
		s.waitForInFlight()
		return true
	}
}

// dispatchBatch processes a sorted batch of acquired jobs.
// For each job, it waits until the fire time, then dispatches.
// Returns true if the caller should re-poll immediately.
func (s *Scheduler) dispatchBatch(records []*JobRecord, recoveryC <-chan time.Time) bool {
	for i, rec := range records {
		// Wait until fire time.
		if delay := time.Until(rec.NextFireTime); delay > 0 {
			interrupted, reloop := s.waitUntil(delay, recoveryC)
			if interrupted {
				s.releaseRemaining(records[i:])
				return reloop
			}
		}
		s.dispatchJob(rec)
	}
	return true // batch complete — re-poll immediately for next batch
}

// waitUntil blocks until duration elapses. Returns (interrupted, shouldReloop).
// interrupted=true if ctx canceled or wakeUp received.
// shouldReloop=true if the caller should re-poll (wakeUp), false if shutting down (ctx).
func (s *Scheduler) waitUntil(d time.Duration, recoveryC <-chan time.Time) (interrupted, reloop bool) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return false, false
		case <-s.ctx.Done():
			return true, false
		case <-s.wakeUp:
			return true, true
		case <-recoveryC:
			s.recoverStaleJobs()
			// Continue waiting — recovery doesn't affect current batch.
		}
	}
}

// dispatchJob resolves the handler and launches the job goroutine.
func (s *Scheduler) dispatchJob(rec *JobRecord) {
	fn := s.resolveHandler(rec.ID)
	if fn == nil {
		_ = s.store.ReleaseJob(s.ctx, rec.ID, rec.NextFireTime)
		return
	}

	trigger, err := triggerFromRecord(rec)
	if err != nil {
		s.logError("failed to parse trigger from store", "job", rec.ID, "error", err)
		_ = s.store.ReleaseJob(s.ctx, rec.ID, rec.NextFireTime)
		return
	}

	job := Job{
		ID:      rec.ID,
		Name:    rec.Name,
		Trigger: trigger,
		Fn:      fn,
		Timeout: rec.Timeout,
	}

	s.logInfo("dispatching job", "job", rec.ID, "name", rec.Name)
	s.wg.Add(1)
	go s.executeJob(job)
}

// releaseRemaining releases acquired-but-unfired jobs back to WAITING.
func (s *Scheduler) releaseRemaining(records []*JobRecord) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), s.cleanupTimeout)
	defer cancel()
	for _, rec := range records {
		if err := s.store.ReleaseJob(cleanupCtx, rec.ID, rec.NextFireTime); err != nil && !errors.Is(err, ErrJobNotFound) {
			s.logError("failed to release unfired job", "job", rec.ID, "error", err)
		}
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

// executeJob runs a single already-acquired job.
func (s *Scheduler) executeJob(job Job) {
	defer s.wg.Done()

	// Build execution context with optional timeout.
	var execCtx context.Context
	var execCancel context.CancelFunc
	if job.Timeout > 0 {
		execCtx, execCancel = context.WithTimeout(s.ctx, job.Timeout)
	} else {
		execCtx, execCancel = context.WithCancel(s.ctx)
	}
	defer execCancel()

	err := job.Fn(execCtx)

	// Compute next fire time for release.
	nextFire := job.Trigger.NextFireTime(time.Now().In(s.location))

	// Release job back to WAITING with updated next fire time.
	// Use a detached context so cleanup completes even during shutdown.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), s.cleanupTimeout)
	defer cleanupCancel()

	s.logInfo("job completed", "job", job.ID, "next", nextFire)
	if releaseErr := s.store.ReleaseJob(cleanupCtx, job.ID, nextFire); releaseErr != nil {
		s.logError("failed to release job", "job", job.ID, "error", releaseErr)
	} else {
		s.signal() // wake the poll loop so it picks up the next fire time promptly
	}

	if err != nil {
		s.logError("job execution failed", "job", job.ID, "error", err)
	}
}

// recoverStaleJobs resets jobs stuck in ACQUIRED state past the misfire threshold.
func (s *Scheduler) recoverStaleJobs() {
	n, err := s.store.RecoverStaleJobs(s.ctx, s.misfireThreshold)
	if err != nil {
		s.logError("failed to recover stale jobs", "error", err)
		return
	}
	if n > 0 {
		s.logInfo("recovered stale jobs", "count", n)
	}
}

// waitForInFlight waits for in-flight jobs to finish, bounded by shutdownTimeout.
func (s *Scheduler) waitForInFlight() {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	s.logInfo("shutting down, waiting for in-flight jobs")
	select {
	case <-done:
		s.logInfo("all jobs finished")
	case <-time.After(s.shutdownTimeout):
		s.logError("shutdown timeout exceeded, some jobs may still be running", "timeout", s.shutdownTimeout)
	}
}

// nextPollWait returns how long to sleep before the next poll.
// Uses the store's earliest fire time for precision; falls back to pollInterval.
func (s *Scheduler) nextPollWait() time.Duration {
	next, err := s.store.NextFireTime(s.ctx)
	if err != nil || next.IsZero() {
		return s.pollInterval
	}
	wait := time.Until(next)
	if wait <= 0 {
		return 0
	}
	if wait > s.pollInterval {
		return s.pollInterval
	}
	return wait
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
		Timeout:      job.Timeout,
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

func (s *Scheduler) logInfo(msg string, keysAndValues ...any) {
	if s.verbose {
		slog.Info(msg, keysAndValues...)
	}
}

func (s *Scheduler) logError(msg string, keysAndValues ...any) {
	if s.verbose {
		slog.Error(msg, keysAndValues...)
	}
}
