package scheduler_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ishinvin/scheduler"
)

func mustInterval(t *testing.T, d time.Duration) *scheduler.IntervalTrigger {
	t.Helper()
	tr, err := scheduler.NewIntervalTrigger(d)
	if err != nil {
		t.Fatalf("NewIntervalTrigger: %v", err)
	}
	return tr
}

func newJDBCScheduler(t *testing.T, db *sql.DB, dialectName, prefix string) scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(
		scheduler.WithJDBC(db, dialectName, prefix),
		scheduler.WithInstanceID("test-instance"),
		scheduler.WithPollInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.InitSchema(context.Background()); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return s
}
