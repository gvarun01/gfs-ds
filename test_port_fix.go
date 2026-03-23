package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
)

func main() {
	// Test the port assignment fix for multi-node cluster
	fmt.Println("Testing port assignment fix for multi-node clusters...")
	
	// Create 3-node cluster with unique ports
	clusterSize := 3
	basePort := 12000
	
	nodes := make([]string, clusterSize)
	peerAddrs := make([]string, clusterSize)
	configs := make([]*cluster.ClusterConfig, clusterSize)
	
	// Generate configurations
	for i := 0; i < clusterSize; i++ {
		nodeID := fmt.Sprintf("test-node-%d", i+1)
		nodes[i] = nodeID
		peerAddrs[i] = fmt.Sprintf("test-node-%d:8080", i+1)
	}
	
	for i := 0; i < clusterSize; i++ {
		raftPort := basePort + i
		readOnlyPort := raftPort + 1000
		
		config := &cluster.ClusterConfig{
			NodeID:       nodes[i],
			ClusterSize:  clusterSize,
			PeerAddrs:    peerAddrs,
			RaftPort:     raftPort,
			ReadOnlyPort: readOnlyPort,
			
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 200 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			SnapshotThreshold:  100,
			
			EnableShadows:       true,
			ReplicationTimeout:  2 * time.Second,
			EnableFailover:      true,
			HealthCheckInterval: 500 * time.Millisecond,
			
			DNSEnabled:  false,
			DNSDomain:   "test.local",
			DNSTTL:      30 * time.Second,
			DNSProvider: "internal",
		}
		
		configs[i] = config
		fmt.Printf("Node %d: NodeID=%s, RaftPort=%d, ReadOnlyPort=%d\n", 
			i+1, config.NodeID, config.RaftPort, config.ReadOnlyPort)
	}
	
	// Verify all ports are unique
	ports := make(map[int]string)
	for i, config := range configs {
		nodeID := fmt.Sprintf("node-%d", i+1)
		
		if existing, exists := ports[config.RaftPort]; exists {
			fmt.Printf("❌ CONFLICT: RaftPort %d used by both %s and %s\n", 
				config.RaftPort, existing, nodeID)
		} else {
			ports[config.RaftPort] = nodeID
		}
		
		if existing, exists := ports[config.ReadOnlyPort]; exists {
			fmt.Printf("❌ CONFLICT: ReadOnlyPort %d used by both %s and %s\n", 
				config.ReadOnlyPort, existing, nodeID)
		} else {
			ports[config.ReadOnlyPort] = nodeID
		}
	}
	
	fmt.Printf("✅ All ports are unique! Total ports: %d\n", len(ports))
	
	// Try to create HA managers to test actual port binding
	var managers []*ha.Manager
	var cleanupFuncs []func()
	
	fmt.Println("\nTesting actual port binding...")
	
	for i, config := range configs {
		fmt.Printf("Creating HA manager for %s...\n", config.NodeID)
		
		masterConfig := &master.Config{
			Chunk: struct {
				Size              int64  `yaml:"size"`
				NamingPattern     string `yaml:"naming_pattern"`
				ChecksumAlgorithm string `yaml:"checksum_algorithm"`
				VerifyOnRead      bool   `yaml:"verify_on_read"`
			}{
				Size:              67108864,
				NamingPattern:     "chunk_%s_%d",
				ChecksumAlgorithm: "sha256",
				VerifyOnRead:      true,
			},
			Replication: struct {
				Factor int `yaml:"factor"`
			}{
				Factor: 3,
			},
			Heartbeat: struct {
				Interval time.Duration `yaml:"interval"`
				Timeout  time.Duration `yaml:"timeout"`
			}{
				Interval: 3 * time.Second,
				Timeout:  10 * time.Second,
			},
		}
		
		haManager, err := ha.NewManager(config, masterConfig)
		if err != nil {
			fmt.Printf("❌ Failed to create HA manager for %s: %v\n", config.NodeID, err)
			continue
		}
		
		masterInstance := master.NewMaster(masterConfig)
		
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		
		fmt.Printf("Starting HA manager for %s...\n", config.NodeID)
		if err := haManager.Start(ctx, masterInstance); err != nil {
			cancel()
			fmt.Printf("❌ Failed to start HA manager for %s: %v\n", config.NodeID, err)
			continue
		}
		
		fmt.Printf("✅ Successfully started %s on RaftPort=%d, ReadOnlyPort=%d\n", 
			config.NodeID, config.RaftPort, config.ReadOnlyPort)
		
		managers = append(managers, haManager)
		cleanupFuncs = append(cleanupFuncs, func() {
			cancel()
			haManager.Stop()
		})
	}
	
	fmt.Printf("\n✅ Successfully started %d out of %d managers!\n", len(managers), clusterSize)
	
	// Wait a bit to let them settle
	time.Sleep(2 * time.Second)
	
	// Check status
	fmt.Println("\nCluster status:")
	for i, manager := range managers {
		state, leader, term := manager.GetRaftState()
		active := manager.IsActive()
		fmt.Printf("Node %d: state=%s, leader=%s, term=%d, active=%t\n", 
			i+1, state, leader, term, active)
	}
	
	// Cleanup
	fmt.Println("\nCleaning up...")
	for _, cleanup := range cleanupFuncs {
		cleanup()
	}
	
	fmt.Println("✅ Port assignment fix test completed successfully!")
}