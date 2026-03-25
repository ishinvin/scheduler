package scheduler_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora/v2"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	pgDB  *sql.DB
	myDB  *sql.DB
	oraDB *sql.DB
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgCtr, pgConn := mustStartPostgres(ctx)
	pgDB = pgConn

	myCtr, myConn := mustStartMySQL(ctx)
	myDB = myConn

	oraCtr, oraConn := mustStartOracle(ctx)
	oraDB = oraConn

	code := m.Run()

	pgDB.Close()
	myDB.Close()
	oraDB.Close()
	_ = pgCtr.Terminate(ctx)
	_ = myCtr.Terminate(ctx)
	_ = oraCtr.Terminate(ctx)

	os.Exit(code)
}

func mustStartPostgres(ctx context.Context) (testcontainers.Container, *sql.DB) {
	ctr, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase("scheduler"),
		tcpostgres.WithUsername("scheduler"),
		tcpostgres.WithPassword("scheduler"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("start postgres: %v", err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("postgres dsn: %v", err)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	return ctr, db
}

func mustStartMySQL(ctx context.Context) (testcontainers.Container, *sql.DB) {
	ctr, err := tcmysql.Run(ctx, "mysql:8.4",
		tcmysql.WithDatabase("scheduler"),
		tcmysql.WithUsername("scheduler"),
		tcmysql.WithPassword("scheduler"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("3306/tcp").WithStartupTimeout(120*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("start mysql: %v", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		log.Fatalf("mysql host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "3306/tcp")
	if err != nil {
		log.Fatalf("mysql port: %v", err)
	}
	dsn := fmt.Sprintf("scheduler:scheduler@tcp(%s:%s)/scheduler?parseTime=true", host, port.Port())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	return ctr, db
}

func mustStartOracle(ctx context.Context) (testcontainers.Container, *sql.DB) {
	req := testcontainers.ContainerRequest{
		Image:        "gvenzl/oracle-free:23-slim",
		ExposedPorts: []string{"1521/tcp"},
		Env: map[string]string{
			"ORACLE_PASSWORD":   "scheduler",
			"APP_USER":          "scheduler",
			"APP_USER_PASSWORD": "scheduler",
		},
		WaitingFor: wait.ForLog("DATABASE IS READY TO USE").WithStartupTimeout(5 * time.Minute),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("start oracle: %v", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		log.Fatalf("oracle host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "1521/tcp")
	if err != nil {
		log.Fatalf("oracle port: %v", err)
	}
	dsn := fmt.Sprintf("oracle://scheduler:scheduler@%s:%s/FREEPDB1", host, port.Port())
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		log.Fatalf("open oracle: %v", err)
	}
	return ctr, db
}
