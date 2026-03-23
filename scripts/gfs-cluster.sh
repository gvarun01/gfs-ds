#!/bin/bash

# GFS Cluster Management Script
# This script provides a centralized way to manage the GFS cluster
# removing hardcoded values and providing a professional interface

set -euo pipefail

# Configuration
DEFAULT_ENVIRONMENT="development"
DEFAULT_MASTER_PORT="50051"
DEFAULT_CHUNKSERVER_COUNT=3
DEFAULT_CHUNKSERVER_START_PORT=8001

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Usage information
usage() {
    cat << EOF
GFS Cluster Management Script

USAGE:
    $0 [COMMAND] [OPTIONS]

COMMANDS:
    start       Start the GFS cluster
    stop        Stop the GFS cluster
    restart     Restart the GFS cluster
    status      Check cluster status
    test        Run system tests
    clean       Clean up storage and logs
    build       Build all binaries
    help        Show this help message

OPTIONS:
    -e, --env ENV           Environment (development, staging, production) [default: development]
    -m, --master-port PORT  Master server port [default: 50051]
    -c, --chunk-count NUM   Number of chunk servers [default: 3]
    -p, --chunk-port PORT   Starting port for chunk servers [default: 8001]
    -d, --data-dir DIR      Data directory [default: storage]
    -v, --verbose           Verbose output
    -h, --help              Show help message

EXAMPLES:
    $0 start                          # Start cluster with defaults
    $0 start -c 5 -p 8001            # Start with 5 chunk servers starting at port 8001
    $0 start -e production           # Start in production environment
    $0 test                          # Run comprehensive tests
    $0 clean && $0 build && $0 start # Full restart with rebuild
    $0 status                        # Check if services are running

ENVIRONMENT VARIABLES:
    GFS_ENVIRONMENT      Default environment
    GFS_MASTER_PORT      Default master port
    GFS_CHUNK_COUNT      Default chunk server count
    GFS_DATA_DIR         Default data directory

EOF
}

# Parse command line arguments
parse_arguments() {
    ENVIRONMENT="${GFS_ENVIRONMENT:-$DEFAULT_ENVIRONMENT}"
    MASTER_PORT="${GFS_MASTER_PORT:-$DEFAULT_MASTER_PORT}"
    CHUNK_COUNT="${GFS_CHUNK_COUNT:-$DEFAULT_CHUNKSERVER_COUNT}"
    CHUNK_START_PORT="${GFS_CHUNK_START_PORT:-$DEFAULT_CHUNKSERVER_START_PORT}"
    DATA_DIR="${GFS_DATA_DIR:-storage}"
    VERBOSE=false

    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--env)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -m|--master-port)
                MASTER_PORT="$2"
                shift 2
                ;;
            -c|--chunk-count)
                CHUNK_COUNT="$2"
                shift 2
                ;;
            -p|--chunk-port)
                CHUNK_START_PORT="$2"
                shift 2
                ;;
            -d|--data-dir)
                DATA_DIR="$2"
                shift 2
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                break
                ;;
        esac
    done

    COMMAND="${1:-help}"
}

# Check if binaries exist
check_binaries() {
    local missing=false
    
    if [[ ! -f "bin/master" ]]; then
        log_error "Master binary not found. Run: make build"
        missing=true
    fi
    
    if [[ ! -f "bin/chunkserver" ]]; then
        log_error "Chunkserver binary not found. Run: make build"
        missing=true
    fi
    
    if [[ ! -f "bin/client" ]]; then
        log_error "Client binary not found. Run: make build"
        missing=true
    fi
    
    if [[ "$missing" == "true" ]]; then
        exit 1
    fi
}

# Create necessary directories
create_directories() {
    log_info "Creating necessary directories..."
    mkdir -p "$DATA_DIR/master"
    mkdir -p "$DATA_DIR/chunks"
    mkdir -p logs
}

# Start master server
start_master() {
    log_info "Starting master server on port $MASTER_PORT..."
    
    # Check if master is already running
    if pgrep -f "bin/master" > /dev/null; then
        log_warning "Master server is already running"
        return 0
    fi
    
    # Start master in background (no command line args needed)
    nohup ./bin/master > "logs/master.log" 2>&1 &
    
    local master_pid=$!
    echo "$master_pid" > "logs/master.pid"
    
    # Wait a moment and check if it started successfully
    sleep 2
    if ps -p "$master_pid" > /dev/null; then
        log_success "Master server started successfully (PID: $master_pid)"
        return 0
    else
        log_error "Failed to start master server"
        cat logs/master.log
        return 1
    fi
}

# Start chunk servers
start_chunkservers() {
    log_info "Starting $CHUNK_COUNT chunk servers..."
    
    for ((i=0; i<CHUNK_COUNT; i++)); do
        local port=$((CHUNK_START_PORT + i))
        local chunk_id="chunkserver-$((i+1))"
        
        # Check if this chunkserver is already running
        if pgrep -f "bin/chunkserver.*$port" > /dev/null; then
            log_warning "Chunkserver on port $port is already running"
            continue
        fi
        
        log_info "Starting chunkserver $((i+1)) on port $port..."
        
        # Create chunkserver-specific data directory
        mkdir -p "$DATA_DIR/chunks/$chunk_id"
        
        # Start chunkserver in background (using flag that works)
        nohup ./bin/chunkserver -port $port > "logs/$chunk_id.log" 2>&1 &
        
        local chunk_pid=$!
        echo "$chunk_pid" > "logs/$chunk_id.pid"
        
        # Brief pause between starting chunkservers
        sleep 1
        
        if ps -p "$chunk_pid" > /dev/null; then
            log_success "Chunkserver $((i+1)) started successfully on port $port (PID: $chunk_pid)"
        else
            log_error "Failed to start chunkserver $((i+1)) on port $port"
            cat "logs/$chunk_id.log"
        fi
    done
}

# Stop all services
stop_cluster() {
    log_info "Stopping GFS cluster..."
    
    # Stop master
    if [[ -f "logs/master.pid" ]]; then
        local master_pid=$(cat logs/master.pid)
        if ps -p "$master_pid" > /dev/null 2>&1; then
            log_info "Stopping master server (PID: $master_pid)..."
            kill "$master_pid"
            rm -f "logs/master.pid"
        fi
    fi
    
    # Stop all chunkservers
    for ((i=1; i<=10; i++)); do  # Check up to 10 possible chunkservers
        local chunk_id="chunkserver-$i"
        if [[ -f "logs/$chunk_id.pid" ]]; then
            local chunk_pid=$(cat "logs/$chunk_id.pid")
            if ps -p "$chunk_pid" > /dev/null 2>&1; then
                log_info "Stopping $chunk_id (PID: $chunk_pid)..."
                kill "$chunk_pid"
            fi
            rm -f "logs/$chunk_id.pid"
        fi
    done
    
    # Wait for processes to terminate
    sleep 2
    
    # Force kill if any are still running
    pkill -f "bin/master" 2>/dev/null || true
    pkill -f "bin/chunkserver" 2>/dev/null || true
    
    log_success "GFS cluster stopped"
}

# Check cluster status
check_status() {
    log_info "Checking GFS cluster status..."
    
    local master_running=false
    local chunk_count_running=0
    
    # Check master
    if pgrep -f "bin/master" > /dev/null; then
        log_success "Master server is running"
        master_running=true
    else
        log_error "Master server is not running"
    fi
    
    # Check chunkservers
    local chunk_pids=($(pgrep -f "bin/chunkserver" || true))
    chunk_count_running=${#chunk_pids[@]}
    
    if [[ $chunk_count_running -gt 0 ]]; then
        log_success "$chunk_count_running chunkserver(s) running"
        if [[ "$VERBOSE" == "true" ]]; then
            for pid in "${chunk_pids[@]}"; do
                log_info "  Chunkserver PID: $pid"
            done
        fi
    else
        log_error "No chunkservers are running"
    fi
    
    # Summary
    if [[ "$master_running" == "true" ]] && [[ $chunk_count_running -gt 0 ]]; then
        log_success "GFS cluster is operational"
        return 0
    else
        log_error "GFS cluster is not fully operational"
        return 1
    fi
}

# Run tests
run_tests() {
    log_info "Running GFS system tests..."
    
    # Check if cluster is running
    if ! check_status > /dev/null 2>&1; then
        log_error "Cluster is not running. Start it first with: $0 start"
        return 1
    fi
    
    # Run basic test
    if [[ -f "test_gfs_system.go" ]]; then
        log_info "Running basic system test..."
        go run test_gfs_system.go
    fi
    
    # Run edge case tests
    if [[ -f "test_scripts/edge_case_tests.go" ]]; then
        log_info "Running edge case tests..."
        go run test_scripts/edge_case_tests.go
    fi
    
    log_success "Tests completed"
}

# Clean up storage and logs
clean_cluster() {
    log_info "Cleaning up storage and logs..."
    
    # Stop cluster first
    stop_cluster
    
    # Remove storage and logs
    rm -rf storage/
    rm -rf logs/
    rm -f *.log
    rm -f cmd/*/*.log
    
    log_success "Cleanup completed"
}

# Build binaries
build_binaries() {
    log_info "Building GFS binaries..."
    
    if command -v make > /dev/null; then
        make build
    else
        log_warning "Make not available, building manually..."
        mkdir -p bin
        go build -o bin/master ./cmd/master
        go build -o bin/chunkserver ./cmd/chunkserver
        go build -o bin/client ./cmd/client
    fi
    
    log_success "Build completed"
}

# Main function
main() {
    parse_arguments "$@"
    
    case "$COMMAND" in
        start)
            create_directories
            check_binaries
            start_master
            start_chunkservers
            log_success "GFS cluster started successfully"
            log_info "Use '$0 status' to check cluster health"
            ;;
        stop)
            stop_cluster
            ;;
        restart)
            stop_cluster
            sleep 2
            create_directories
            check_binaries
            start_master
            start_chunkservers
            log_success "GFS cluster restarted successfully"
            ;;
        status)
            check_status
            ;;
        test)
            run_tests
            ;;
        clean)
            clean_cluster
            ;;
        build)
            build_binaries
            ;;
        help)
            usage
            ;;
        *)
            log_error "Unknown command: $COMMAND"
            log_info "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"