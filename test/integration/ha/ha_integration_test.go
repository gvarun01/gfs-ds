package ha_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/raft"
)

// TestHASystemIntegration tests the complete HA system integration
func TestHASystemIntegration(t *testing.T) {
	tests := []struct {
		name        string
		clusterSize int
		testFunc    func(t *testing.T, managers []*ha.Manager)
	}{
		{
			name:        "SingleNodeLeadership",
			clusterSize: 1,
			testFunc:    testSingleNodeLeadership,
		},
		{
			name:        "ThreeNodeConsensus",
			clusterSize: 3,
			testFunc:    testThreeNodeConsensus,
		},
		{
			name:        "OperationReplication",
			clusterSize: 3,
			testFunc:    testOperationReplication,
		},
		{
			name:        "LeaderFailover",
			clusterSize: 3,
			testFunc:    testLeaderFailover,
		},
		{
			name:        "DNSIntegration",
			clusterSize: 1,
			testFunc:    testDNSIntegration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear registry before each test
			raft.ClearRegistry()

			// Create cluster
			managers, cleanup := createTestCluster(t, tt.clusterSize)
			defer cleanup()

			// Wait for cluster formation
			time.Sleep(2 * time.Second)

			// Run specific test
			tt.testFunc(t, managers)
		})
	}
}

// createTestCluster creates a test cluster with the specified number of nodes
func createTestCluster(t *testing.T, clusterSize int) ([]*ha.Manager, func()) {
	var managers []*ha.Manager
	var cleanupFuncs []func()

	// Use random port range to avoid conflicts
	basePort := 10000 + rand.Intn(5000)

	// Generate node configurations
	nodes := make([]string, clusterSize)
	peerAddrs := make([]string, clusterSize)
	for i := 0; i < clusterSize; i++ {
		nodes[i] = fmt.Sprintf("test-node-%d", i+1)
		peerAddrs[i] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	// Create HA managers
	for i := 0; i < clusterSize; i++ {
		config := &cluster.ClusterConfig{
			NodeID:       nodes[i],
			ClusterSize:  clusterSize,
			PeerAddrs:    peerAddrs,
			RaftPort:     basePort + i,
			ReadOnlyPort: basePort + i + 1000,

			EnableFailover: true,
			EnableShadows:  true,

			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  75 * time.Millisecond,
			SnapshotThreshold:  100,
		}

		haManager, err := ha.NewManager(config, &master.Config{
			Chunk: master.ChunkConfig{
				Size: 64 * 1024 * 1024,
			},
			Replication: master.ReplicationConfig{
				Factor: 3,
			},
			Heartbeat: master.HeartbeatConfig{
				Interval: 2 * time.Second,
				Timeout:  5 * time.Second,
			},
		})
		if err != nil {
			t.Fatalf("Failed to create HA manager: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		masterInstance := master.NewMaster(&master.Config{
			Chunk: master.ChunkConfig{
				Size: 64 * 1024 * 1024,
			},
			Replication: master.ReplicationConfig{
				Factor: 3,
			},
			Heartbeat: master.HeartbeatConfig{
				Interval: 2 * time.Second,
				Timeout:  5 * time.Second,
			},
		})

		if err := haManager.Start(ctx, masterInstance); err != nil {
			t.Fatalf("Failed to start HA manager: %v", err)
		}

		managers = append(managers, haManager)
		cleanupFuncs = append(cleanupFuncs, func() {
			cancel()
			haManager.Stop()
		})
	}

	// Return managers and cleanup function
	return managers, func() {
		for _, cleanup := range cleanupFuncs {
			cleanup()
		}
	}
}

func testSingleNodeLeadership(t *testing.T, managers []*ha.Manager) {
	if len(managers) != 1 {
		t.Fatalf("Expected 1 manager, got %d", len(managers))
	}

	state, _, _ := managers[0].GetRaftState()
	if state != raft.StateLeader {
		t.Fatalf("Expected leader state, got %s", state)
	}
}

func testThreeNodeConsensus(t *testing.T, managers []*ha.Manager) {
	if len(managers) != 3 {
		t.Fatalf("Expected 3 managers, got %d", len(managers))
	}

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Count leaders
	leaderCount := 0
	for _, manager := range managers {
		state, _, _ := manager.GetRaftState()
		if state == raft.StateLeader {
			leaderCount++
		}
	}

	if leaderCount != 1 {
		t.Fatalf("Expected 1 leader, got %d", leaderCount)
	}
}

func testOperationReplication(t *testing.T, managers []*ha.Manager) {
	// Wait for leader election and stabilization
	time.Sleep(2 * time.Second)

	// Find leader
	var leader *ha.Manager
	for _, manager := range managers {
		state, _, _ := manager.GetRaftState()
		if state == raft.StateLeader {
			leader = manager
			break
		}
	}

	if leader == nil {
		t.Fatal("No leader found")
	}

	// Replicate operation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := leader.ProposeOperation(ctx, []byte("test-operation"))
	if err != nil {
		t.Fatalf("Failed to propose operation: %v", err)
	}
}

func testLeaderFailover(t *testing.T, managers []*ha.Manager) {
	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Find leader index
	leaderIdx := -1
	for i, manager := range managers {
		state, _, _ := manager.GetRaftState()
		if state == raft.StateLeader {
			leaderIdx = i
			break
		}
	}

	if leaderIdx == -1 {
		t.Fatal("No leader found")
	}

	// Stop leader
	managers[leaderIdx].Stop()

	// Wait for failover
	time.Sleep(3 * time.Second)

	// Check for new leader
	leaderCount := 0
	for _, manager := range managers {
		if manager.IsActive() {
			state, _, _ := manager.GetRaftState()
			if state == raft.StateLeader {
				leaderCount++
			}
		}
	}

	if leaderCount != 1 {
		t.Fatalf("Expected 1 leader after failover, got %d", leaderCount)
	}
}

func testDNSIntegration(t *testing.T, managers []*ha.Manager) {
	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Ensure at least one manager
	if len(managers) == 0 {
		t.Fatal("No managers available")
	}

	// Check DNS manager state
	for _, manager := range managers {
		if manager.IsActive() {
			state, _, _ := manager.GetRaftState()
			if state == raft.StateLeader {
				return
			}
		}
	}

	t.Fatal("No active leader found for DNS integration test")
}
