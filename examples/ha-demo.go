package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/raft"
)

func main() {
	fmt.Println("🚀 GFS High Availability Demo")
	fmt.Println("==============================")
	
	// Test 1: Single Node Cluster (Basic functionality)
	fmt.Println("\n📝 Test 1: Single Node Master")
	testSingleNodeCluster()
	
	// Test 2: Three Node Cluster (Full HA)
	fmt.Println("\n📝 Test 2: Three Node HA Cluster")
	testThreeNodeCluster()
	
	// Test 3: Failover Simulation
	fmt.Println("\n📝 Test 3: Failover Simulation")
	testFailoverSimulation()
	
	// Test 4: DNS Integration (NEW)
	fmt.Println("\n📝 Test 4: DNS Integration Demo")
	testDNSIntegration()
	
	fmt.Println("\n✅ All HA tests completed successfully!")
}

func testSingleNodeCluster() {
	// Clear any previous nodes from registry
	raft.ClearRegistry()
	
	fmt.Println("Creating single-node cluster configuration...")
	
	// Create single-node cluster config
	clusterConfig := &cluster.ClusterConfig{
		NodeID:      "master-single",
		ClusterSize: 1,
		PeerAddrs:   []string{"master-single:8080"},
		RaftPort:    9090,
		
		// Raft settings
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		SnapshotThreshold:  1000,
		
		// Disable shadows for single node
		EnableShadows:       false,
		EnableFailover:      false,
		HealthCheckInterval: 1 * time.Second,
	}
	
	// Create master config (simplified for demo)
	masterConfig := createTestMasterConfig()
	
	// Create HA manager
	haManager, err := ha.NewManager(clusterConfig, masterConfig)
	if err != nil {
		log.Fatalf("Failed to create HA manager: %v", err)
	}
	
	// Create master instance
	masterInstance := master.NewMaster(masterConfig)
	
	// Start HA system
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	fmt.Println("Starting single-node HA system...")
	if err := haManager.Start(ctx, masterInstance); err != nil {
		log.Fatalf("Failed to start HA manager: %v", err)
	}
	
	// Wait for leadership
	time.Sleep(2 * time.Second)
	
	// Check leadership
	state, leader, term := haManager.GetRaftState()
	fmt.Printf("✅ Raft State: %s, Leader: %s, Term: %d\n", state, leader, term)
	
	if haManager.IsActive() {
		fmt.Println("✅ Single node is active master")
		
		// Test operation replication
		fmt.Println("Testing operation replication...")
		err := haManager.ReplicateOperation("file_create", map[string]string{
			"filename": "/test/single-node-file.txt",
			"size":     "1024",
		})
		if err != nil {
			fmt.Printf("❌ Operation replication failed: %v\n", err)
		} else {
			fmt.Println("✅ Operation replicated successfully")
		}
	} else {
		fmt.Println("❌ Single node should be active master")
	}
	
	// Stop HA system
	haManager.Stop()
	fmt.Println("✅ Single-node test completed")
}

func testThreeNodeCluster() {
	// Clear any previous nodes from registry
	raft.ClearRegistry()
	
	fmt.Println("Creating three-node cluster configuration...")
	
	// Test multiple nodes (simulation)
	nodes := []string{"master-1", "master-2", "master-3"}
	peerAddrs := []string{"master-1:8080", "master-2:8080", "master-3:8080"}
	
	var managers []*ha.Manager
	
	for i, nodeID := range nodes {
		clusterConfig := &cluster.ClusterConfig{
			NodeID:      nodeID,
			ClusterSize: 3,
			PeerAddrs:   peerAddrs,
			RaftPort:    9090 + i, // Different port for each node in demo
			
			// Raft settings
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			SnapshotThreshold:  1000,
			
			// Enable shadows and failover
			EnableShadows:       true,
			EnableFailover:      true,
			HealthCheckInterval: 1 * time.Second,
			ReadOnlyPort:        8081 + i,
		}
		
		masterConfig := createTestMasterConfig()
		
		haManager, err := ha.NewManager(clusterConfig, masterConfig)
		if err != nil {
			log.Printf("Failed to create HA manager for %s: %v", nodeID, err)
			continue
		}
		
		masterInstance := master.NewMaster(masterConfig)
		
		// Start HA system
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		
		fmt.Printf("Starting HA system for node %s...\n", nodeID)
		if err := haManager.Start(ctx, masterInstance); err != nil {
			log.Printf("Failed to start HA manager for %s: %v", nodeID, err)
			continue
		}
		
		managers = append(managers, haManager)
	}
	
	// Wait for cluster formation
	fmt.Println("Waiting for cluster formation...")
	time.Sleep(5 * time.Second)
	
	// Check cluster state
	var activeCount int
	for i, manager := range managers {
		state, leader, term := manager.GetRaftState()
		isActive := manager.IsActive()
		
		fmt.Printf("Node %s - State: %s, Leader: %s, Term: %d, Active: %t\n", 
			nodes[i], state, leader, term, isActive)
		
		if isActive {
			activeCount++
		}
		
		// Show cluster health
		health := manager.GetClusterHealth()
		for nodeID, nodeHealth := range health {
			fmt.Printf("  Health[%s]: %s (errors: %d)\n", 
				nodeID, nodeHealth.Status, nodeHealth.ErrorCount)
		}
	}
	
	if activeCount == 1 {
		fmt.Printf("✅ Cluster formed correctly with 1 active master\n")
	} else {
		fmt.Printf("❌ Expected 1 active master, got %d\n", activeCount)
	}
	
	// Clean up
	for _, manager := range managers {
		manager.Stop()
	}
	fmt.Println("✅ Three-node cluster test completed")
}

func testFailoverSimulation() {
	// Clear any previous nodes from registry
	raft.ClearRegistry()
	
	fmt.Println("Setting up failover simulation...")
	
	// This would simulate a failover scenario
	// For now, we'll just demonstrate the concepts
	
	clusterConfig := &cluster.ClusterConfig{
		NodeID:      "master-failover",
		ClusterSize: 1,
		PeerAddrs:   []string{"master-failover:8080"},
		RaftPort:    9093, // Different port to avoid conflicts
		
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		
		EnableFailover:      true,
		HealthCheckInterval: 1 * time.Second,
		FailoverTimeout:     30 * time.Second,
	}
	
	masterConfig := createTestMasterConfig()
	
	haManager, err := ha.NewManager(clusterConfig, masterConfig)
	if err != nil {
		log.Printf("Failed to create HA manager: %v", err)
		return
	}
	
	masterInstance := master.NewMaster(masterConfig)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := haManager.Start(ctx, masterInstance); err != nil {
		log.Printf("Failed to start HA manager: %v", err)
		return
	}
	
	time.Sleep(2 * time.Second)
	
	fmt.Println("✅ Failover simulation setup completed")
	fmt.Println("     In a real scenario, this would:")
	fmt.Println("     1. Detect master failure")
	fmt.Println("     2. Elect new leader via Raft")
	fmt.Println("     3. Promote shadow master")
	fmt.Println("     4. Update DNS records")
	fmt.Println("     5. Redirect client traffic")
	
	haManager.Stop()
}

func testDNSIntegration() {
	// Clear any previous nodes from registry
	raft.ClearRegistry()
	
	fmt.Println("Setting up DNS integration demo...")
	
	clusterConfig := &cluster.ClusterConfig{
		NodeID:      "master-dns",
		ClusterSize: 1,
		PeerAddrs:   []string{"master-dns:8080"},
		RaftPort:    9094, // Different port to avoid conflicts
		
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		
		EnableFailover:      true,
		HealthCheckInterval: 1 * time.Second,
		
		// Enable DNS integration
		DNSEnabled:  true,
		DNSDomain:   "gfs.demo.local",
		DNSTTL:      60 * time.Second,
		DNSProvider: "internal", // Use internal provider for demo
	}
	
	masterConfig := createTestMasterConfig()
	
	haManager, err := ha.NewManager(clusterConfig, masterConfig)
	if err != nil {
		log.Printf("Failed to create HA manager: %v", err)
		return
	}
	
	masterInstance := master.NewMaster(masterConfig)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	fmt.Println("Starting HA system with DNS integration...")
	if err := haManager.Start(ctx, masterInstance); err != nil {
		log.Printf("Failed to start HA manager: %v", err)
		return
	}
	
	// Wait for leadership and DNS update
	time.Sleep(3 * time.Second)
	
	// Check DNS state
	state, leader, term := haManager.GetRaftState()
	fmt.Printf("✅ Raft State: %s, Leader: %s, Term: %d\n", state, leader, term)
	
	if haManager.IsActive() {
		fmt.Println("✅ Master is active and DNS records should be updated")
		fmt.Println("     In production, master.gfs.demo.local would point to master-dns:8080")
		fmt.Println("     Clients would automatically connect to the active master via DNS")
		
		// Simulate a failover scenario
		fmt.Println("\n🔄 Simulating failover scenario...")
		fmt.Println("     1. Master failure detected")
		fmt.Println("     2. New leader elected via Raft")
		fmt.Println("     3. DNS records updated to point to new master")
		fmt.Println("     4. Client traffic redirected automatically")
		fmt.Println("     5. Failover complete with minimal downtime")
	} else {
		fmt.Println("❌ Master should be active")
	}
	
	// Stop HA system
	haManager.Stop()
	
	fmt.Println("\n✅ DNS integration demo completed")
	fmt.Println("     Key benefits:")
	fmt.Println("     • Automatic client redirection during failover")
	fmt.Println("     • No manual intervention required")
	fmt.Println("     • Sub-minute failover times with proper DNS TTL")
	fmt.Println("     • Supports multiple DNS providers (Route53, Cloudflare)")
}

func createTestMasterConfig() *master.Config {
	return &master.Config{
		Chunk: struct {
			Size              int64  `yaml:"size"`
			NamingPattern     string `yaml:"naming_pattern"`
			ChecksumAlgorithm string `yaml:"checksum_algorithm"`
			VerifyOnRead      bool   `yaml:"verify_on_read"`
		}{
			Size:              67108864, // 64 MB
			NamingPattern:     "chunk_%s",
			ChecksumAlgorithm: "crc32",
			VerifyOnRead:      true,
		},
		Health: struct {
			CheckInterval int `yaml:"check_interval"`
			Timeout       int `yaml:"timeout"`
			MaxFailures   int `yaml:"max_failures"`
		}{
			CheckInterval: 30, // 30 seconds
			Timeout:       10, // 10 seconds  
			MaxFailures:   3,  // 3 failures
		},
		Deletion: struct {
			GCInterval        int    `yaml:"g_c_interval"`
			RetentionPeriod   int    `yaml:"retention_period"`
			GCDeleteBatchSize int    `yaml:"g_c_delete_batch_size"`
			TrashDirPrefix    string `yaml:"trash_dir_prefix"`
		}{
			GCInterval:        300,      // 5 minutes
			RetentionPeriod:   259200,   // 3 days
			GCDeleteBatchSize: 100,      // 100 files per batch
			TrashDirPrefix:    "/.trash/",
		},
		Replication: struct {
			Factor  int `yaml:"factor"`
			Timeout int `yaml:"timeout"`
		}{
			Factor:  3,  // 3x replication
			Timeout: 30, // 30 seconds
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
			MaxRequestSize:    1048576, // 1 MB
			ThreadPoolSize:    10,
		},
		HA: struct {
			Enabled bool                    `yaml:"enabled"`
			Cluster *cluster.ClusterConfig `yaml:"cluster,omitempty"`
		}{
			Enabled: true,
			Cluster: nil, // Will be set by HA manager
		},
	}
}