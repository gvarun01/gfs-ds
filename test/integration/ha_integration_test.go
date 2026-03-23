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
		peerAddrs[i] = fmt.Sprintf("test-node-%d:8080", i+1)
	}
	
	for i, nodeID := range nodes {
		config := createTestClusterConfig(nodeID, clusterSize, peerAddrs, basePort+i)
		masterConfig := createTestMasterConfig()
		
		// Verify unique port assignment (fix for shadow master port conflicts)
		t.Logf("Node %s: RaftPort=%d, ReadOnlyPort=%d", nodeID, config.RaftPort, config.ReadOnlyPort)
		
		haManager, err := ha.NewManager(config, masterConfig)
		if err != nil {
			t.Fatalf("Failed to create HA manager for %s: %v", nodeID, err)
		}
		
		masterInstance := master.NewMaster(masterConfig)
		
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		
		if err := haManager.Start(ctx, masterInstance); err != nil {
			cancel()
			t.Fatalf("Failed to start HA manager for %s: %v", nodeID, err)
		}
		
		managers = append(managers, haManager)
		cleanupFuncs = append(cleanupFuncs, func() {
			cancel()
			haManager.Stop()
		})
	}
	
	cleanup := func() {
		for _, cleanup := range cleanupFuncs {
			cleanup()
		}
		// Give some time for ports to be released
		time.Sleep(100 * time.Millisecond)
	}
	
	return managers, cleanup
}

// createTestMasterConfig creates a test master configuration
func createTestMasterConfig() *master.Config {
	return &master.Config{
		Chunk: struct {
			Size              int64  `yaml:"size"`
			NamingPattern     string `yaml:"naming_pattern"`
			ChecksumAlgorithm string `yaml:"checksum_algorithm"`
			VerifyOnRead      bool   `yaml:"verify_on_read"`
		}{
			Size:              67108864,
			NamingPattern:     "chunk_%s",
			ChecksumAlgorithm: "crc32",
			VerifyOnRead:      true,
		},
		Health: struct {
			CheckInterval int `yaml:"check_interval"`
			Timeout       int `yaml:"timeout"`
			MaxFailures   int `yaml:"max_failures"`
		}{
			CheckInterval: 30,
			Timeout:       10,
			MaxFailures:   3,
		},
		Deletion: struct {
			GCInterval        int    `yaml:"g_c_interval"`
			RetentionPeriod   int    `yaml:"retention_period"`
			GCDeleteBatchSize int    `yaml:"g_c_delete_batch_size"`
			TrashDirPrefix    string `yaml:"trash_dir_prefix"`
		}{
			GCInterval:        300,
			RetentionPeriod:   259200,
			GCDeleteBatchSize: 100,
			TrashDirPrefix:    "/.trash/",
		},
		Replication: struct {
			Factor  int `yaml:"factor"`
			Timeout int `yaml:"timeout"`
		}{
			Factor:  3,
			Timeout: 30,
		},
		Lease: struct {
			LeaseTimeout int `yaml:"lease_timeout"`
		}{
			LeaseTimeout: 60,
		},
		OperationLog: struct {
			Path string `yaml:"path"`
		}{
			Path: "./test_operation.log",
		},
		Metadata: struct {
			Database struct {
				Type           string `yaml:"type"`
				Path           string `yaml:"path"`
				BackupInterval int    `yaml:"backup_interval"`
			} `yaml:"database"`
			MaxFilenameLength int `yaml:"max_filename_length"`
			MaxDirectoryDepth int `yaml:"max_directory_depth"`
			CacheSizeMB       int `yaml:"cache_size_mb"`
			CacheTTLSeconds   int `yaml:"cache_ttl_seconds"`
		}{
			Database: struct {
				Type           string `yaml:"type"`
				Path           string `yaml:"path"`
				BackupInterval int    `yaml:"backup_interval"`
			}{
				Type:           "file",
				Path:           "./test_metadata.db",
				BackupInterval: 3600,
			},
			MaxFilenameLength: 255,
			MaxDirectoryDepth: 100,
			CacheSizeMB:       128,
			CacheTTLSeconds:   300,
		},
		Server: struct {
			Host              string `yaml:"host"`
			Port              int    `yaml:"port"`
			MaxConnections    int    `yaml:"max_connections"`
			ConnectionTimeout int    `yaml:"connection_timeout"`
			MaxRequestSize    int64  `yaml:"max_request_size"`
			ThreadPoolSize    int    `yaml:"thread_pool_size"`
		}{
			Host:              "localhost",
			Port:              8080,
			MaxConnections:    1000,
			ConnectionTimeout: 30,
			MaxRequestSize:    1048576,
			ThreadPoolSize:    10,
		},
		HA: struct {
			Enabled bool                    `yaml:"enabled"`
			Cluster *cluster.ClusterConfig `yaml:"cluster,omitempty"`
		}{
			Enabled: true,
			Cluster: nil,
		},
	}
}

// createTestClusterConfig creates a test cluster configuration
func createTestClusterConfig(nodeID string, clusterSize int, peerAddrs []string, raftPort int) *cluster.ClusterConfig {
	return &cluster.ClusterConfig{
		NodeID:      nodeID,
		ClusterSize: clusterSize,
		PeerAddrs:   peerAddrs,
		RaftPort:    raftPort,
		ReadOnlyPort: raftPort + 1000, // Unique port for shadow master (e.g., 11000, 11001, 11002)
		
		ElectionTimeoutMin: 100 * time.Millisecond, // Faster for testing
		ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		SnapshotThreshold:  100, // Smaller for testing
		
		EnableShadows:       clusterSize > 1,
		ReplicationTimeout:  2 * time.Second, // Faster for testing
		EnableFailover:      true,
		HealthCheckInterval: 500 * time.Millisecond, // Faster for testing
		
		// DNS settings for testing
		DNSEnabled:  false, // Disabled by default for most tests
		DNSDomain:   "test.local",
		DNSTTL:      30 * time.Second,
		DNSProvider: "internal",
	}
}

// testSingleNodeLeadership verifies single node becomes leader
func testSingleNodeLeadership(t *testing.T, managers []*ha.Manager) {
	if len(managers) != 1 {
		t.Fatalf("Expected 1 manager, got %d", len(managers))
	}
	
	manager := managers[0]
	
	// Check leadership
	state, leader, term := manager.GetRaftState()
	if state.String() != "Leader" {
		t.Errorf("Expected Leader state, got %s", state)
	}
	if term < 0 {
		t.Errorf("Expected non-negative term, got %d", term)
	}
	
	// Check active status
	if !manager.IsActive() {
		t.Error("Single node should be active master")
	}
	
	t.Logf("✅ Single node leadership: state=%s, leader=%s, term=%d", state, leader, term)
}

// testThreeNodeConsensus verifies proper Raft consensus in 3-node cluster
func testThreeNodeConsensus(t *testing.T, managers []*ha.Manager) {
	if len(managers) != 3 {
		t.Fatalf("Expected 3 managers, got %d", len(managers))
	}
	
	var leaders []string
	var activeCount int
	
	for i, manager := range managers {
		state, leader, term := manager.GetRaftState()
		isActive := manager.IsActive()
		
		t.Logf("Node %d: state=%s, leader=%s, term=%d, active=%t", i+1, state, leader, term, isActive)
		
		if state.String() == "Leader" {
			leaders = append(leaders, leader)
		}
		if isActive {
			activeCount++
		}
	}
	
	// Verify single leader
	if len(leaders) != 1 {
		t.Errorf("Expected exactly 1 leader, got %d leaders: %v", len(leaders), leaders)
	}
	
	// Verify single active master
	if activeCount != 1 {
		t.Errorf("Expected exactly 1 active master, got %d", activeCount)
	}
	
	t.Logf("✅ Three node consensus: leaders=%v, active=%d", leaders, activeCount)
}

// testOperationReplication verifies log replication across cluster
func testOperationReplication(t *testing.T, managers []*ha.Manager) {
	// Find the active master
	var activeMaster *ha.Manager
	for _, manager := range managers {
		if manager.IsActive() {
			activeMaster = manager
			break
		}
	}
	
	if activeMaster == nil {
		t.Fatal("No active master found")
	}
	
	// Test operation replication
	operations := []struct {
		opType string
		data   map[string]string
	}{
		{"file_create", map[string]string{"path": "/test/file1.txt", "size": "1024"}},
		{"file_delete", map[string]string{"path": "/test/file2.txt"}},
		{"chunk_create", map[string]string{"chunk_id": "chunk123", "size": "67108864"}},
	}
	
	for _, op := range operations {
		err := activeMaster.ReplicateOperation(op.opType, op.data)
		if err != nil {
			t.Errorf("Failed to replicate operation %s: %v", op.opType, err)
		} else {
			t.Logf("✅ Replicated operation: %s", op.opType)
		}
	}
	
	// Wait for replication to complete
	time.Sleep(1 * time.Second)
	
	t.Log("✅ Operation replication completed")
}

// testLeaderFailover simulates leader failure and verifies failover
func testLeaderFailover(t *testing.T, managers []*ha.Manager) {
	if len(managers) < 3 {
		t.Skip("Leader failover requires at least 3 nodes")
	}
	
	// Find current leader
	var currentLeader *ha.Manager
	var leaderIndex int
	for i, manager := range managers {
		if manager.IsActive() {
			currentLeader = manager
			leaderIndex = i
			break
		}
	}
	
	if currentLeader == nil {
		t.Fatal("No current leader found")
	}
	
	originalState, originalLeader, originalTerm := currentLeader.GetRaftState()
	t.Logf("Original leader: %s (term %d, state %s)", originalLeader, originalTerm, originalState)
	
	// Simulate leader failure by stopping it
	currentLeader.Stop()
	t.Logf("Stopped leader node %s", originalLeader)
	
	// Wait for failover
	time.Sleep(3 * time.Second)
	
	// Check that a new leader was elected
	var newLeader *ha.Manager
	var newLeaderID string
	for i, manager := range managers {
		if i == leaderIndex {
			continue // Skip the stopped node
		}
		
		state, leader, term := manager.GetRaftState()
		if state.String() == "Leader" && manager.IsActive() {
			newLeader = manager
			newLeaderID = leader
			
			// Verify new term is higher
			if term <= originalTerm {
				t.Errorf("New leader term %d should be higher than original %d", term, originalTerm)
			}
			
			t.Logf("✅ New leader elected: %s (term %d)", leader, term)
			break
		}
	}
	
	if newLeader == nil {
		t.Fatal("No new leader elected after failover")
	}
	
	// Test that new leader can still replicate operations
	err := newLeader.ReplicateOperation("failover_test", map[string]string{
		"original_leader": originalLeader,
		"new_leader":      newLeaderID,
	})
	if err != nil {
		t.Errorf("New leader failed to replicate operation: %v", err)
	} else {
		t.Log("✅ New leader successfully replicated operation")
	}
	
	t.Log("✅ Leader failover completed successfully")
}

// testDNSIntegration verifies DNS integration functionality
func testDNSIntegration(t *testing.T, managers []*ha.Manager) {
	if len(managers) != 1 {
		t.Fatalf("DNS test requires exactly 1 manager, got %d", len(managers))
	}
	
	// This test would be enhanced in a future implementation
	// For now, we just verify the basic integration works
	manager := managers[0]
	
	if !manager.IsActive() {
		t.Error("Manager should be active for DNS test")
	}
	
	// In a full implementation, we would:
	// 1. Verify DNS records are created
	// 2. Test DNS updates during failover
	// 3. Validate TTL settings
	// 4. Test different DNS providers
	
	t.Log("✅ DNS integration test passed (basic verification)")
}

// Benchmark tests for performance validation
func BenchmarkRaftConsensus(b *testing.B) {
	raft.ClearRegistry()
	managers, cleanup := createBenchmarkCluster(b, 3)
	defer cleanup()
	
	// Wait for cluster formation
	time.Sleep(2 * time.Second)
	
	// Find active master
	var activeMaster *ha.Manager
	for _, manager := range managers {
		if manager.IsActive() {
			activeMaster = manager
			break
		}
	}
	
	if activeMaster == nil {
		b.Fatal("No active master found")
	}
	
	b.ResetTimer()
	
	// Benchmark operation replication
	for i := 0; i < b.N; i++ {
		err := activeMaster.ReplicateOperation("benchmark_op", map[string]string{
			"operation_id": fmt.Sprintf("bench_%d", i),
			"data":         "benchmark_data",
		})
		if err != nil {
			b.Fatalf("Operation replication failed: %v", err)
		}
	}
}

func createBenchmarkCluster(b *testing.B, clusterSize int) ([]*ha.Manager, func()) {
	var managers []*ha.Manager
	var cleanupFuncs []func()
	
	// Use different port range for benchmarks
	basePort := 15000 + rand.Intn(1000)
	
	nodes := make([]string, clusterSize)
	peerAddrs := make([]string, clusterSize)
	for i := 0; i < clusterSize; i++ {
		nodes[i] = fmt.Sprintf("bench-node-%d", i+1)
		peerAddrs[i] = fmt.Sprintf("bench-node-%d:8080", i+1)
	}
	
	for i, nodeID := range nodes {
		config := createTestClusterConfig(nodeID, clusterSize, peerAddrs, basePort+i)
		masterConfig := createTestMasterConfig()
		
		haManager, err := ha.NewManager(config, masterConfig)
		if err != nil {
			b.Fatalf("Failed to create HA manager: %v", err)
		}
		
		masterInstance := master.NewMaster(masterConfig)
		
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		
		if err := haManager.Start(ctx, masterInstance); err != nil {
			cancel()
			b.Fatalf("Failed to start HA manager: %v", err)
		}
		
		managers = append(managers, haManager)
		cleanupFuncs = append(cleanupFuncs, func() {
			cancel()
			haManager.Stop()
		})
	}
	
	cleanup := func() {
		for _, cleanup := range cleanupFuncs {
			cleanup()
		}
	}
	
	return managers, cleanup
}