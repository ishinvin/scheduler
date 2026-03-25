.PHONY: tools hooks test lint e2e check setup up down clean logs

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

# Run lint and tests
check: lint test

# Run e2e tests (requires Docker)
e2e:
	cd test && go test -race -v -count=1 -timeout=10m ./...

# Setup development environment
setup: tools hooks

# Docker: make up APP=postgres | make down APP=postgres | make clean APP=postgres
up:
	docker compose -f _examples/$(APP)/compose.yml up -d

down:
	docker compose -f _examples/$(APP)/compose.yml down

clean:
	docker compose -f _examples/$(APP)/compose.yml down -v

logs:
	docker compose -f _examples/$(APP)/compose.yml logs -f