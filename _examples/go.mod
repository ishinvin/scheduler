module github.com/ishinvin/scheduler/_examples

go 1.23.0

replace github.com/ishinvin/scheduler => ..

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/ishinvin/scheduler v0.0.0
	github.com/lib/pq v1.10.9
	github.com/sijms/go-ora/v2 v2.9.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
)
