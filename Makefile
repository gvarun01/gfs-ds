# GFS Distributed Storage System
# Professional Makefile with proper build, test, and deployment targets

# Build variables
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GO_VERSION := $(shell go version | awk '{print $$3}')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# LDFLAGS for embedding build information
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME) -X main.goVersion=$(GO_VERSION) -X main.gitCommit=$(GIT_COMMIT)"

# Directories
BIN_DIR := bin
PROTO_DIR := api/proto
BUILD_DIR := build
CONFIG_DIR := configs
TEST_DIR := test_scripts

# Proto files
PROTO_FILES := $(PROTO_DIR)/chunk_master/chunk_master.proto \
               $(PROTO_DIR)/chunk_operations/chunk_operations.proto \
               $(PROTO_DIR)/client_master/client_master.proto \
               $(PROTO_DIR)/common/common.proto \
               $(PROTO_DIR)/chunk/chunk.proto

PROTOC_BIN ?= /home/gvarun01/protoc/bin/protoc
PROTOC_PLUGIN_PATH ?= /home/gvarun01/go/bin

# Go flags for protoc
GO_FLAGS := --go_out=. --go_opt=paths=source_relative \
            --go-grpc_out=. --go-grpc_opt=paths=source_relative

# Default target
.DEFAULT_GOAL := help

.PHONY: help build clean test proto install dev docker docker-compose lint format vet deps check

## help: Show this help message
help:
	@echo "GFS Distributed Storage System"
	@echo ""
	@echo "Available commands:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

## build: Build all binaries
build: clean proto deps
	@echo "Building GFS binaries..."
	@mkdir -p $(BIN_DIR)
	go build $(LDFLAGS) -o $(BIN_DIR)/master ./cmd/master
	go build $(LDFLAGS) -o $(BIN_DIR)/chunkserver ./cmd/chunkserver
	go build $(LDFLAGS) -o $(BIN_DIR)/client ./cmd/client
	@echo "Build completed successfully!"

## build-master: Build only master binary
build-master: proto deps
	@mkdir -p $(BIN_DIR)
	go build $(LDFLAGS) -o $(BIN_DIR)/master ./cmd/master

## build-chunkserver: Build only chunkserver binary
build-chunkserver: proto deps
	@mkdir -p $(BIN_DIR)
	go build $(LDFLAGS) -o $(BIN_DIR)/chunkserver ./cmd/chunkserver

## build-client: Build only client binary
build-client: proto deps
	@mkdir -p $(BIN_DIR)
	go build $(LDFLAGS) -o $(BIN_DIR)/client ./cmd/client

## test: Run all tests
test:
	@echo "Running tests..."
	go test -v ./...
	@echo "Running integration tests..."
	@if [ -f $(TEST_DIR)/edge_case_tests.go ]; then \
		go run $(TEST_DIR)/edge_case_tests.go; \
	fi

## test-unit: Run only unit tests
test-unit:
	@echo "Running unit tests..."
	go test -v ./internal/... ./pkg/...

## test-integration: Run integration tests
test-integration:
	@echo "Running integration tests..."
	@if [ -f test_gfs_system.go ]; then \
		go run test_gfs_system.go; \
	fi

## proto: Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	PATH="$(PROTOC_PLUGIN_PATH):$$PATH" $(PROTOC_BIN) $(GO_FLAGS) $(PROTO_FILES)
	@echo "Protobuf generation completed!"

## clean: Remove build artifacts and logs
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BIN_DIR)
	rm -rf $(BUILD_DIR)
	rm -f *.log
	rm -f cmd/*/*.log
	@echo "Clean completed!"

## clean-generated: Remove generated protobuf code
clean-generated:
	@echo "Cleaning generated protobuf files..."
	find $(PROTO_DIR) -type f -name '*.pb.go' -exec rm -f {} +

## deps: Download and verify dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod verify
	go mod tidy

## install: Install binaries to GOPATH/bin
install: build
	@echo "Installing binaries..."
	cp $(BIN_DIR)/* $(GOPATH)/bin/

## dev: Start development environment
dev: build
	@echo "Starting development environment..."
	@echo "Starting master server..."
	@$(BIN_DIR)/master &
	@sleep 2
	@echo "Starting chunkservers..."
	@$(BIN_DIR)/chunkserver -port 8001 &
	@$(BIN_DIR)/chunkserver -port 8002 &
	@$(BIN_DIR)/chunkserver -port 8003 &
	@echo "GFS cluster started in development mode"

## dev-stop: Stop development environment
dev-stop:
	@echo "Stopping development environment..."
	@pkill -f "$(BIN_DIR)/master" || true
	@pkill -f "$(BIN_DIR)/chunkserver" || true
	@echo "Development environment stopped"

## lint: Run linting tools
lint:
	@echo "Running linting..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not installed, skipping linting"; \
	fi

## format: Format Go code
format:
	@echo "Formatting code..."
	go fmt ./...
	@if command -v goimports >/dev/null 2>&1; then \
		goimports -w .; \
	fi

## vet: Run go vet
vet:
	@echo "Running go vet..."
	go vet ./...

## check: Run all quality checks
check: format vet lint test

## docker: Build Docker image
docker:
	@echo "Building Docker image..."
	docker build -t gfs:$(VERSION) .

## docker-compose: Start with docker-compose
docker-compose:
	@echo "Starting GFS with docker-compose..."
	docker-compose up -d

## docker-compose-down: Stop docker-compose
docker-compose-down:
	@echo "Stopping GFS docker-compose..."
	docker-compose down

## benchmark: Run performance benchmarks
benchmark:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

## security: Run security checks
security:
	@echo "Running security checks..."
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "gosec not installed, skipping security checks"; \
	fi

## profile: Run with CPU profiling
profile: build
	@echo "Starting master with CPU profiling..."
	$(BIN_DIR)/master -cpuprofile=cpu.prof

## race: Build with race detector
race: clean deps proto
	@echo "Building with race detector..."
	@mkdir -p $(BIN_DIR)
	go build -race $(LDFLAGS) -o $(BIN_DIR)/master ./cmd/master
	go build -race $(LDFLAGS) -o $(BIN_DIR)/chunkserver ./cmd/chunkserver
	go build -race $(LDFLAGS) -o $(BIN_DIR)/client ./cmd/client

# Development shortcuts
.PHONY: m c cl
m: build-master
c: build-chunkserver  
cl: build-client
