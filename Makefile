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

# Setup development environment
setup: tools hooks
