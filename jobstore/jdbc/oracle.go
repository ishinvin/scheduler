package jdbc

import (
	"fmt"
	"strings"
)

// Oracle implements Dialect for Oracle Database.
type Oracle struct{}

func (Oracle) Placeholder(index int) string { return fmt.Sprintf(":%d", index) }
func (Oracle) BooleanTrue() string          { return "1" }

func (Oracle) DateAddSQL(col, secondsExpr string) string {
	return fmt.Sprintf("%s + NUMTODSINTERVAL(%s, 'SECOND')", col, secondsExpr)
}

func (Oracle) SchemaSQL(prefix string) string {
	p := strings.ToUpper(prefix)
	return `
CREATE TABLE ` + p + `SCHEDULER_JOBS (
    JOB_ID         VARCHAR2(256) PRIMARY KEY,
    NAME           VARCHAR2(512) NOT NULL,
    TRIGGER_TYPE   VARCHAR2(32)  NOT NULL,
    TRIGGER_VALUE  VARCHAR2(512) NOT NULL,
    TIMEOUT_SECS   NUMBER        DEFAULT 0 NOT NULL,
    NEXT_FIRE_TIME TIMESTAMP WITH TIME ZONE,
    STATE          VARCHAR2(32)  DEFAULT 'WAITING' NOT NULL,
    INSTANCE_ID    VARCHAR2(256),
    ACQUIRED_AT    TIMESTAMP WITH TIME ZONE,
    ENABLED        NUMBER(1)     DEFAULT 1 NOT NULL,
    CREATED_AT     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    UPDATED_AT     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX IDX_` + p + `SCHED_JOBS_FIRE
    ON ` + p + `SCHEDULER_JOBS (NEXT_FIRE_TIME, STATE, ENABLED);
`
}
