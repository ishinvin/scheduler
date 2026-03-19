package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ishinvin/scheduler/internal/store"
)

const (
	defaultMisfireThreshold = 1 * time.Minute
	defaultPollInterval     = 15 * time.Second
	defaultShutdownTimeout  = 30 * time.Second
	defaultCleanupTimeout   = 5 * time.Second
)

type signal = struct{}

// Scheduler orchestrates job scheduling using a pluggable job store.
type Scheduler struct {
	handlers         sync.Map // job ID -> func(ctx context.Context) error
	store            store.JobStore
	verbose          bool
	location         *time.Location
	instanceID       string
	misfireThreshold time.Duration
	pollInterval     time.Duration
	shutdownTimeout  time.Duration
	cleanupTimeout   time.Duration

	// JDBC store config (used in New to build the store).
	storeDB      *sql.DB
	storeDialect any // implements Dialect
	tablePrefix  string
	initSchema   store.InitializeSchema

	ctx    context.Context
	wakeUp chan signal
	wg     sync.WaitGroup
}

// New creates a new Scheduler.
func New(ctx context.Context, opts ...Option) (*Scheduler, error) {
	s := &Scheduler{
		verbose:          false,
		location:         time.UTC,
		misfireThreshold: defaultMisfireThreshold,
		pollInterval:     defaultPollInterval,
		shutdownTimeout:  defaultShutdownTimeout,
		cleanupTimeout:   defaultCleanupTimeout,
		wakeUp:           make(chan signal, 1),
	}

	hostname, _ := os.Hostname()
	s.instanceID = hostname

	for _, o := range opts {
		o(s)
	}

	// Build JDBC store from config if no store was set directly.
	if s.store == nil && s.storeDB != nil && s.storeDialect != nil {
		s.store = store.NewJDBC(s.storeDB, s.storeDialect, s.tablePrefix, s.initSchema)
	}

	// Initialize the store if it supports it.
	if init, ok := s.store.(store.JobStoreInitializer); ok {
		if err := init.Init(ctx); err != nil {
			return nil, fmt.Errorf("scheduler: store init: %w", err)
		}
	}

	return s, nil
}

// Register adds a job to the scheduler and persists it to the store.
// If Trigger is nil, only the handler is registered (no job is scheduled).
func (s *Scheduler) Register(ctx context.Context, job Job) error {
	if job.Fn != nil {
		s.handlers.Store(job.ID, job.Fn)
	}

	if job.Trigger == nil {
		return nil
	}

	if s.store == nil {
		return fmt.Errorf("scheduler: no job store configured")
	}

	// Idempotent: skip if the job already exists.
	if _, err := s.store.GetJob(ctx, job.ID); !errors.Is(err, store.ErrJobNotFound) {
		return err
	}

	now := time.Now().In(s.location)
	next := job.Trigger.NextFireTime(now)

	record := jobToRecord(&job, next)
	if err := s.store.CreateJob(ctx, record); err != nil {
		return fmt.Errorf("scheduler: register job: %w", err)
	}

	s.logInfo("job registered", "job", job.ID, "trigger", record.TriggerType, "next", next)
	s.signal()
	return nil
}

// Reschedule creates or updates a job with the given trigger.
func (s *Scheduler) Reschedule(ctx context.Context, job Job) error {
	if job.Fn != nil {
		s.handlers.Store(job.ID, job.Fn)
	}

	if job.Trigger == nil {
		return fmt.Errorf("scheduler: reschedule requires a trigger")
	}

	now := time.Now().In(s.location)
	next := job.Trigger.NextFireTime(now)
	record := jobToRecord(&job, next)

	rec, err := s.store.GetJob(ctx, job.ID)
	if errors.Is(err, store.ErrJobNotFound) {
		if err := s.store.CreateJob(ctx, record); err != nil {
			return fmt.Errorf("scheduler: reschedule job: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("scheduler: reschedule job: %w", err)
	} else {
		// Preserve state — don't reset an ACQUIRED job.
		record.State = rec.State
		record.InstanceID = rec.InstanceID
		record.AcquiredAt = rec.AcquiredAt
		if err := s.store.UpdateJob(ctx, record); err != nil {
			return fmt.Errorf("scheduler: reschedule job: %w", err)
		}
	}

	s.logInfo("job rescheduled", "job", job.ID, "trigger", record.TriggerType, "next", next)
	s.signal()
	return nil
}

// Delete removes a job from the scheduler and the job store.
func (s *Scheduler) Delete(ctx context.Context, id string) error {
	if err := s.store.DeleteJob(ctx, id); err != nil {
		return fmt.Errorf("scheduler: delete job: %w", err)
	}
	s.handlers.Delete(id)
	s.logInfo("job deleted", "job", id)
	s.signal()
	return nil
}

// Run starts the scheduling loop. Blocks until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) error {
	if s.store == nil {
		return fmt.Errorf("scheduler: no job store configured")
	}

	s.ctx = ctx
	s.logInfo("scheduler starting", "instance", s.instanceID, "poll_interval", s.pollInterval, "misfire_threshold", s.misfireThreshold)

	// Stale job recovery ticker.
	var recoveryTicker *time.Ticker
	var recoveryC <-chan time.Time
	if s.misfireThreshold > 0 {
		recoveryTicker = time.NewTicker(s.misfireThreshold / 2)
		recoveryC = recoveryTicker.C
		// Recovery once at startup.
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

// waitForEvent blocks until timer, signal, recovery, or ctx cancel. Returns true to stop.
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

// dispatchBatch waits for each job's fire time and dispatches it. Returns true to re-poll.
func (s *Scheduler) dispatchBatch(records []*store.JobRecord, recoveryC <-chan time.Time) bool {
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
			// Recovery doesn't affect current batch.
		}
	}
}

// dispatchJob resolves the handler and launches the job.
func (s *Scheduler) dispatchJob(rec *store.JobRecord) {
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

// releaseRemaining releases unfired jobs back to WAITING.
func (s *Scheduler) releaseRemaining(records []*store.JobRecord) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), s.cleanupTimeout)
	defer cancel()
	for _, rec := range records {
		if err := s.store.ReleaseJob(cleanupCtx, rec.ID, rec.NextFireTime); err != nil && !errors.Is(err, store.ErrJobNotFound) {
			s.logError("failed to release unfired job", "job", rec.ID, "error", err)
		}
	}
}

// resolveHandler looks up the Fn for a job ID.
func (s *Scheduler) resolveHandler(id string) func(ctx context.Context) error {
	v, ok := s.handlers.Load(id)
	if !ok {
		return nil
	}
	return v.(func(ctx context.Context) error)
}

// executeJob runs an acquired job.
func (s *Scheduler) executeJob(job Job) {
	defer s.wg.Done()

	var execCtx context.Context
	var execCancel context.CancelFunc
	if job.Timeout > 0 {
		execCtx, execCancel = context.WithTimeout(s.ctx, job.Timeout)
	} else {
		execCtx, execCancel = context.WithCancel(s.ctx)
	}
	defer execCancel()

	err := job.Fn(execCtx)

	nextFire := job.Trigger.NextFireTime(time.Now().In(s.location))
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

// recoverStaleJobs resets jobs stuck in ACQUIRED past the misfire threshold.
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

// waitForInFlight waits for running jobs to finish, bounded by shutdownTimeout.
func (s *Scheduler) waitForInFlight() {
	done := make(chan signal)
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

// nextPollWait returns the wait duration until the next poll.
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
	case s.wakeUp <- signal{}:
	default:
	}
}

// jobToRecord converts a Job to a store.JobRecord.
func jobToRecord(job *Job, nextFire time.Time) *store.JobRecord {
	rec := &store.JobRecord{
		ID:           job.ID,
		Name:         job.Name,
		Timeout:      job.Timeout,
		NextFireTime: nextFire,
		State:        store.StateWaiting,
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
func triggerFromRecord(rec *store.JobRecord) (Trigger, error) {
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
