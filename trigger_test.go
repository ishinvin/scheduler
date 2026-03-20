package scheduler

import (
	"errors"
	"testing"
	"time"

	"github.com/ishinvin/scheduler/internal/store"
)

func TestCronTrigger(t *testing.T) {
	trigger, err := NewCronTrigger("0 0 9 * * *")
	if err != nil {
		t.Fatalf("NewCronTrigger: %v", err)
	}

	ref := time.Date(2026, 3, 20, 8, 0, 0, 0, time.UTC)
	next := trigger.NextFireTime(ref)
	want := time.Date(2026, 3, 20, 9, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("NextFireTime = %v, want %v", next, want)
	}

	if s := trigger.String(); s != "cron(0 0 9 * * *)" {
		t.Errorf("String() = %q, want %q", s, "cron(0 0 9 * * *)")
	}
}

func TestCronTrigger_Invalid(t *testing.T) {
	_, err := NewCronTrigger("bad")
	if err == nil {
		t.Fatal("expected error for invalid cron")
	}
	if !errors.Is(err, ErrInvalidCron) {
		t.Errorf("error = %v, want wrapping ErrInvalidCron", err)
	}
}

func TestOnceTrigger(t *testing.T) {
	at := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	trigger := NewOnceTrigger(at)

	// Before the target time — should return the fire time.
	before := at.Add(-time.Hour)
	if next := trigger.NextFireTime(before); !next.Equal(at) {
		t.Errorf("NextFireTime(before) = %v, want %v", next, at)
	}

	// At the target time — should return zero (already past).
	if next := trigger.NextFireTime(at); !next.IsZero() {
		t.Errorf("NextFireTime(at) = %v, want zero", next)
	}

	// After the target time — should return zero.
	after := at.Add(time.Hour)
	if next := trigger.NextFireTime(after); !next.IsZero() {
		t.Errorf("NextFireTime(after) = %v, want zero", next)
	}

	if s := trigger.String(); s != "once(2026-03-20T12:00:00Z)" {
		t.Errorf("String() = %q, want %q", s, "once(2026-03-20T12:00:00Z)")
	}
}

func TestIntervalTrigger(t *testing.T) {
	trigger, err := NewIntervalTrigger(30 * time.Second)
	if err != nil {
		t.Fatalf("NewIntervalTrigger: %v", err)
	}

	ref := time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC)
	next := trigger.NextFireTime(ref)
	want := ref.Add(30 * time.Second)
	if !next.Equal(want) {
		t.Errorf("NextFireTime = %v, want %v", next, want)
	}

	if s := trigger.String(); s != "every(30s)" {
		t.Errorf("String() = %q, want %q", s, "every(30s)")
	}
}

func TestJobToRecord_Cron(t *testing.T) {
	trigger, _ := NewCronTrigger("0 */5 * * * *")
	nextFire := time.Date(2026, 3, 20, 0, 5, 0, 0, time.UTC)
	job := &Job{
		ID:      "j1",
		Name:    "cron job",
		Trigger: trigger,
		Timeout: 10 * time.Second,
	}

	rec, err := jobToRecord(job, nextFire)
	if err != nil {
		t.Fatalf("jobToRecord: %v", err)
	}

	if rec.ID != "j1" {
		t.Errorf("ID = %q, want %q", rec.ID, "j1")
	}
	if rec.TriggerType != TriggerTypeCron {
		t.Errorf("TriggerType = %q, want %q", rec.TriggerType, TriggerTypeCron)
	}
	if rec.TriggerValue != "0 */5 * * * *" {
		t.Errorf("TriggerValue = %q, want %q", rec.TriggerValue, "0 */5 * * * *")
	}
	if rec.State != store.StateWaiting {
		t.Errorf("State = %q, want %q", rec.State, store.StateWaiting)
	}
	if rec.Timeout != 10*time.Second {
		t.Errorf("Timeout = %v, want %v", rec.Timeout, 10*time.Second)
	}
	if !rec.NextFireTime.Equal(nextFire) {
		t.Errorf("NextFireTime = %v, want %v", rec.NextFireTime, nextFire)
	}
}

func TestJobToRecord_Once(t *testing.T) {
	at := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	job := &Job{
		ID:      "j2",
		Name:    "once job",
		Trigger: NewOnceTrigger(at),
	}

	rec, err := jobToRecord(job, at)
	if err != nil {
		t.Fatalf("jobToRecord: %v", err)
	}

	if rec.TriggerType != TriggerTypeOnce {
		t.Errorf("TriggerType = %q, want %q", rec.TriggerType, TriggerTypeOnce)
	}
	if rec.TriggerValue != at.Format(time.RFC3339) {
		t.Errorf("TriggerValue = %q, want %q", rec.TriggerValue, at.Format(time.RFC3339))
	}
}

func TestJobToRecord_Interval(t *testing.T) {
	trigger, _ := NewIntervalTrigger(5 * time.Minute)
	job := &Job{
		ID:      "j3",
		Name:    "interval job",
		Trigger: trigger,
	}

	rec, err := jobToRecord(job, time.Now())
	if err != nil {
		t.Fatalf("jobToRecord: %v", err)
	}

	if rec.TriggerType != TriggerTypeInterval {
		t.Errorf("TriggerType = %q, want %q", rec.TriggerType, TriggerTypeInterval)
	}
	if rec.TriggerValue != "5m0s" {
		t.Errorf("TriggerValue = %q, want %q", rec.TriggerValue, "5m0s")
	}
}

func TestTriggerFromRecord_Cron(t *testing.T) {
	rec := &store.JobRecord{TriggerType: TriggerTypeCron, TriggerValue: "0 0 9 * * *"}
	trigger, err := triggerFromRecord(rec)
	if err != nil {
		t.Fatalf("triggerFromRecord: %v", err)
	}
	ct, ok := trigger.(*CronTrigger)
	if !ok {
		t.Fatalf("expected *CronTrigger, got %T", trigger)
	}
	if ct.Expr != "0 0 9 * * *" {
		t.Errorf("Expr = %q, want %q", ct.Expr, "0 0 9 * * *")
	}
}

func TestTriggerFromRecord_Once(t *testing.T) {
	at := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	rec := &store.JobRecord{TriggerType: TriggerTypeOnce, TriggerValue: at.Format(time.RFC3339)}
	trigger, err := triggerFromRecord(rec)
	if err != nil {
		t.Fatalf("triggerFromRecord: %v", err)
	}
	ot, ok := trigger.(*OnceTrigger)
	if !ok {
		t.Fatalf("expected *OnceTrigger, got %T", trigger)
	}
	if !ot.At.Equal(at) {
		t.Errorf("At = %v, want %v", ot.At, at)
	}
}

func TestTriggerFromRecord_Interval(t *testing.T) {
	rec := &store.JobRecord{TriggerType: TriggerTypeInterval, TriggerValue: "1m0s"}
	trigger, err := triggerFromRecord(rec)
	if err != nil {
		t.Fatalf("triggerFromRecord: %v", err)
	}
	it, ok := trigger.(*IntervalTrigger)
	if !ok {
		t.Fatalf("expected *IntervalTrigger, got %T", trigger)
	}
	if it.Every != time.Minute {
		t.Errorf("Every = %v, want %v", it.Every, time.Minute)
	}
}

func TestTriggerFromRecord_Unknown(t *testing.T) {
	rec := &store.JobRecord{TriggerType: "unknown", TriggerValue: "x"}
	_, err := triggerFromRecord(rec)
	if err == nil {
		t.Fatal("expected error for unknown trigger type")
	}
}

func TestTriggerFromRecord_InvalidOnce(t *testing.T) {
	rec := &store.JobRecord{TriggerType: TriggerTypeOnce, TriggerValue: "not-a-time"}
	_, err := triggerFromRecord(rec)
	if err == nil {
		t.Fatal("expected error for invalid once value")
	}
}

func TestTriggerFromRecord_InvalidInterval(t *testing.T) {
	rec := &store.JobRecord{TriggerType: TriggerTypeInterval, TriggerValue: "bad"}
	_, err := triggerFromRecord(rec)
	if err == nil {
		t.Fatal("expected error for invalid interval value")
	}
}

// customTrigger is a test-only Trigger that is not one of the built-in types.
type customTrigger struct{}

func (customTrigger) NextFireTime(after time.Time) time.Time { return after.Add(time.Second) }
func (customTrigger) String() string                         { return "custom" }

func TestJobToRecord_UnsupportedTrigger(t *testing.T) {
	job := &Job{ID: "j4", Name: "custom", Trigger: customTrigger{}}
	_, err := jobToRecord(job, time.Now())
	if !errors.Is(err, ErrUnsupportedTrigger) {
		t.Fatalf("expected ErrUnsupportedTrigger, got %v", err)
	}
}
