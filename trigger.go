package scheduler

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/ishinvin/scheduler/internal/store"
)

// Trigger type constants.
const (
	TriggerTypeCron     = "cron"
	TriggerTypeOnce     = "once"
	TriggerTypeInterval = "interval"
)

// Trigger determines when a Job should fire next.
type Trigger interface {
	// NextFireTime returns the next time the trigger should fire after the given time.
	// Returns the zero time if the trigger will not fire again.
	NextFireTime(after time.Time) time.Time
	fmt.Stringer
}

// ---------------------------------------------------------------------------
// CronTrigger
// ---------------------------------------------------------------------------

// CronTrigger wraps a parsed cron expression.
type CronTrigger struct {
	Expr     string
	schedule cron.Schedule
}

// NewCronTrigger parses a cron expression and returns a CronTrigger.
// Supports 6-field cron (second minute hour dom month dow) and descriptors.
func NewCronTrigger(expr string) (*CronTrigger, error) {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCron, err)
	}
	return &CronTrigger{Expr: expr, schedule: schedule}, nil
}

func (c *CronTrigger) NextFireTime(after time.Time) time.Time {
	return c.schedule.Next(after)
}

func (c *CronTrigger) String() string {
	return fmt.Sprintf("cron(%s)", c.Expr)
}

// ---------------------------------------------------------------------------
// OnceTrigger
// ---------------------------------------------------------------------------

// OnceTrigger fires exactly once at a specific time.
type OnceTrigger struct {
	At time.Time
}

// NewOnceTrigger creates a trigger that fires once at the given time.
func NewOnceTrigger(at time.Time) *OnceTrigger {
	return &OnceTrigger{At: at}
}

func (o *OnceTrigger) NextFireTime(after time.Time) time.Time {
	if !o.At.After(after) {
		return time.Time{}
	}
	return o.At
}

func (o *OnceTrigger) String() string {
	return fmt.Sprintf("once(%s)", o.At.Format(time.RFC3339))
}

// ---------------------------------------------------------------------------
// IntervalTrigger
// ---------------------------------------------------------------------------

// IntervalTrigger fires repeatedly at a fixed interval.
type IntervalTrigger struct {
	Every time.Duration
}

// NewIntervalTrigger creates a trigger that fires at the given interval.
// Panics if every <= 0.
func NewIntervalTrigger(every time.Duration) *IntervalTrigger {
	if every <= 0 {
		panic("scheduler: interval must be positive")
	}
	return &IntervalTrigger{Every: every}
}

func (i *IntervalTrigger) NextFireTime(after time.Time) time.Time {
	return after.Add(i.Every)
}

func (i *IntervalTrigger) String() string {
	return fmt.Sprintf("every(%s)", i.Every)
}

// ---------------------------------------------------------------------------
// Record conversion
// ---------------------------------------------------------------------------

// jobToRecord converts a Job to a store.JobRecord.
func jobToRecord(job *Job, nextFire time.Time) (*store.JobRecord, error) {
	rec := &store.JobRecord{
		ID:           job.ID,
		Name:         job.Name,
		Timeout:      job.Timeout,
		NextFireTime: nextFire,
		State:        store.StateWaiting,
	}

	switch t := job.Trigger.(type) {
	case *CronTrigger:
		rec.TriggerType = TriggerTypeCron
		rec.TriggerValue = t.Expr
	case *OnceTrigger:
		rec.TriggerType = TriggerTypeOnce
		rec.TriggerValue = t.At.Format(time.RFC3339)
	case *IntervalTrigger:
		rec.TriggerType = TriggerTypeInterval
		rec.TriggerValue = t.Every.String()
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedTrigger, job.Trigger)
	}

	return rec, nil
}

// triggerFromRecord reconstructs a Trigger from a stored JobRecord.
func triggerFromRecord(rec *store.JobRecord) (Trigger, error) {
	switch rec.TriggerType {
	case TriggerTypeCron:
		return NewCronTrigger(rec.TriggerValue)
	case TriggerTypeOnce:
		t, err := time.Parse(time.RFC3339, rec.TriggerValue)
		if err != nil {
			return nil, fmt.Errorf("invalid once trigger: %w", err)
		}
		return NewOnceTrigger(t), nil
	case TriggerTypeInterval:
		d, err := time.ParseDuration(rec.TriggerValue)
		if err != nil {
			return nil, fmt.Errorf("invalid interval trigger: %w", err)
		}
		return NewIntervalTrigger(d), nil
	default:
		return nil, fmt.Errorf("unknown trigger type: %s", rec.TriggerType)
	}
}
