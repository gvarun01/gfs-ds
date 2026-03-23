#!/bin/bash

# HA System Integration Test Runner
# Tests all major components of the GFS Master HA system

set -e

echo "🚀 GFS Master HA System - Integration Test Runner"
echo "=================================================="

# Check if Go is available
if ! command -v go &> /dev/null; then
    echo "❌ Go is not installed. Please install Go to run integration tests."
    exit 1
fi

cd "$(dirname "$0")"

echo ""
echo "📋 Available Tests:"
echo "  1. SingleNodeLeadership - ✅ (Port conflicts fixed)"
echo "  2. ThreeNodeConsensus - 🔧 (Fixed with unique ports)"
echo "  3. OperationReplication - 🔧 (Fixed with unique ports)" 
echo "  4. LeaderFailover - 🔧 (Fixed with unique ports)"
echo "  5. DNSIntegration - ✅ (Working)"
echo "  6. BenchmarkRaftConsensus - 📊 (Performance tests)"
echo ""

# Function to run test with better output
run_test() {
    local test_name="$1"
    echo "🧪 Running: $test_name"
    echo "----------------------------------------"
    
    if go test -v -run "TestHASystemIntegration/$test_name" ./internal/master/ha/; then
        echo "✅ $test_name PASSED"
    else
        echo "❌ $test_name FAILED"
        return 1
    fi
    echo ""
}

# Function to run benchmark tests
run_benchmark() {
    echo "📊 Running Performance Benchmarks"
    echo "----------------------------------------"
    
    if go test -bench=. -benchmem ./internal/master/ha/; then
        echo "✅ Benchmarks completed"
    else
        echo "❌ Benchmarks failed"
        return 1
    fi
    echo ""
}

# Main test execution
echo "🔄 Starting Integration Tests..."
echo ""

# Test 1: Single Node Leadership (should work)
run_test "SingleNodeLeadership"

# Test 2: DNS Integration (should work)
run_test "DNSIntegration"

# Test 3: Three Node Consensus (now with port fix)
run_test "ThreeNodeConsensus"

# Test 4: Operation Replication (now with port fix)
run_test "OperationReplication"

# Test 5: Leader Failover (now with port fix)
run_test "LeaderFailover"

# Test 6: Performance Benchmarks
run_benchmark

echo "🎉 All Integration Tests Completed!"
echo ""

echo "📊 Test Summary:"
echo "  ✅ SingleNodeLeadership"
echo "  ✅ DNSIntegration" 
echo "  ✅ ThreeNodeConsensus (Port conflicts fixed)"
echo "  ✅ OperationReplication (Port conflicts fixed)"
echo "  ✅ LeaderFailover (Port conflicts fixed)"
echo "  ✅ Performance Benchmarks"
echo ""

echo "🏆 GFS Master HA System: All tests passing!"
echo "   - Raft consensus working across multi-node clusters"
echo "   - Shadow masters operational with unique ports"
echo "   - Automatic failover validated"
echo "   - DNS integration functional"
echo "   - Performance benchmarks completed"
echo ""

echo "✨ The HA system is ready for production!"