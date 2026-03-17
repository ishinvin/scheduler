# Install development tools
tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1
	go install github.com/evilmartians/lefthook/v2@v2.1.1

# Setup git hooks
hooks:
	lefthook install

# Run tests with race detector
test:
	go test -race -v -count=1 ./...

# Run linter
lint:
	golangci-lint run ./...

# Start all databases
db-up:
	docker compose -f _examples/compose.yml up -d

# Stop all databases
db-down:
	docker compose -f _examples/compose.yml down

# Stop all databases and remove volumes
db-clean:
	docker compose -f _examples/compose.yml down -v

# Run example: make run-example APP=postgres
run-example:
	cd _examples/$(APP) && go run .

# Run lint and tests
check: lint test

# Setup development environment
setup: tools hooks
