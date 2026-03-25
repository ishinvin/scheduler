package jdbc

import (
	"fmt"
	"strings"
)

// Oracle implements dialect.Dialect for Oracle Database.
type Oracle struct{}

func (Oracle) Placeholder(index int) string { return fmt.Sprintf(":%d", index) }
func (Oracle) DateAddSQL(col, secondsExpr string) string {
	return fmt.Sprintf("%s + NUMTODSINTERVAL(%s, 'SECOND')", col, secondsExpr)
}

func (Oracle) SchemaSQL(prefix string) string {
	p := strings.ToUpper(prefix)
	return `
CREATE TABLE ` + p + `SCHEDULER_JOBS (
    JOB_ID         VARCHAR2(255) PRIMARY KEY,
    NAME           VARCHAR2(255) NOT NULL,
    TRIGGER_TYPE   VARCHAR2(32)  NOT NULL,
    TRIGGER_VALUE  VARCHAR2(255) NOT NULL,
    TIMEOUT_SECS   INTEGER       DEFAULT 0 NOT NULL,
    NEXT_FIRE_TIME TIMESTAMP WITH TIME ZONE,
    STATE          VARCHAR2(32)  DEFAULT 'WAITING' NOT NULL,
    INSTANCE_ID    VARCHAR2(255),
    ACQUIRED_AT    TIMESTAMP WITH TIME ZONE,
    CREATED_AT     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    UPDATED_AT     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX IDX_` + p + `SCHED_JOBS_FIRE
    ON ` + p + `SCHEDULER_JOBS (NEXT_FIRE_TIME, STATE);
`
}
