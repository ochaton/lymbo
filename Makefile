.PHONY: test test-memory test-postgres test-race build clean help

# Default target
help:
	@echo "Available targets:"
	@echo "  make test          - Run all tests"
	@echo "  make test-memory   - Run memory store tests only"
	@echo "  make test-postgres - Run PostgreSQL store tests only"
	@echo "  make test-race     - Run tests with race detector"
	@echo "  make build         - Build the project"
	@echo "  make clean         - Clean build artifacts"

# Run all tests
test:
	cd test && go test -v ./...

# Run memory store tests only
test-memory:
	cd test && go test -v -run TestMemoryStore ./...

# Run PostgreSQL store tests only
test-postgres:
	cd test && go test -v -run TestPostgresStore ./...

# Run tests with race detector
test-race:
	cd test && go test -v -race ./...

# Build project
build:
	go build -v ./...

# Clean
clean:
	go clean
	rm -f coverage.out
