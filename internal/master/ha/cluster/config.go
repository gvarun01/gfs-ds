package cluster

import (
	"fmt"
	"time"
)

// ClusterConfig defines the configuration for master cluster
type ClusterConfig struct {
	// Cluster settings
	NodeID       string   `yaml:"node_id"`       // Unique identifier for this master node
	ClusterSize  int      `yaml:"cluster_size"`  // Total number of masters in cluster (1, 3, 5, 7)
	PeerAddrs    []string `yaml:"peer_addrs"`    // List of peer master addresses
	RaftPort     int      `yaml:"raft_port"`     // gRPC port for Raft communication
	
	// Raft consensus settings
	ElectionTimeoutMin time.Duration `yaml:"election_timeout_min"` // Min election timeout (150ms recommended)
	ElectionTimeoutMax time.Duration `yaml:"election_timeout_max"` // Max election timeout (300ms recommended)
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval"`   // Leader heartbeat interval (50ms recommended)
	SnapshotThreshold  int           `yaml:"snapshot_threshold"`   // Log entries before snapshot (1000 recommended)
	
	// Shadow master settings
	EnableShadows      bool          `yaml:"enable_shadows"`       // Enable shadow master functionality
	ReplicationTimeout time.Duration `yaml:"replication_timeout"`  // Timeout for log replication
	ReadOnlyPort       int           `yaml:"readonly_port"`        // Port for read-only shadow operations
	
	// Failover settings
	EnableFailover     bool          `yaml:"enable_failover"`      // Enable automatic failover
	HealthCheckInterval time.Duration `yaml:"health_check_interval"` // Health monitoring interval
	FailoverTimeout    time.Duration `yaml:"failover_timeout"`     // Max time for failover completion
	
	// DNS settings
	DNSEnabled         bool          `yaml:"dns_enabled"`          // Enable DNS record management
	DNSDomain          string        `yaml:"dns_domain"`           // DNS domain for master discovery
	DNSTTL             time.Duration `yaml:"dns_ttl"`              // DNS record TTL
	DNSProvider        string        `yaml:"dns_provider"`         // DNS provider (route53, cloudflare, internal)
}

// DefaultClusterConfig returns a default cluster configuration
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		// Cluster defaults
		ClusterSize: 1, // Single master by default
		RaftPort:    9090, // Default Raft gRPC port
		
		// Raft defaults (based on Raft paper recommendations)
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		SnapshotThreshold:  1000,
		
		// Shadow master defaults
		EnableShadows:      false,
		ReplicationTimeout: 5 * time.Second,
		ReadOnlyPort:       8081,
		
		// Failover defaults
		EnableFailover:      false,
		HealthCheckInterval: 1 * time.Second,
		FailoverTimeout:     30 * time.Second,
		
		// DNS defaults
		DNSEnabled:  false,
		DNSDomain:   "gfs-master.cluster.local",
		DNSTTL:      30 * time.Second,
		DNSProvider: "internal",
	}
}

// Validate checks if the cluster configuration is valid
func (c *ClusterConfig) Validate() error {
	if c.ClusterSize < 1 || c.ClusterSize > 7 {
		return fmt.Errorf("cluster_size must be between 1 and 7, got %d", c.ClusterSize)
	}
	
	if c.ClusterSize%2 == 0 {
		return fmt.Errorf("cluster_size should be odd for Raft consensus, got %d", c.ClusterSize)
	}
	
	if c.NodeID == "" {
		return fmt.Errorf("node_id cannot be empty")
	}
	
	if c.ClusterSize > 1 && len(c.PeerAddrs) != c.ClusterSize {
		return fmt.Errorf("peer_addrs length (%d) must match cluster_size (%d)", len(c.PeerAddrs), c.ClusterSize)
	}
	
	if c.ElectionTimeoutMin >= c.ElectionTimeoutMax {
		return fmt.Errorf("election_timeout_min must be less than election_timeout_max")
	}
	
	if c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return fmt.Errorf("heartbeat_interval must be less than election_timeout_min")
	}
	
	return nil
}

// IsClustered returns true if this is a multi-master cluster
func (c *ClusterConfig) IsClustered() bool {
	return c.ClusterSize > 1
}

// MajorityCount returns the number of nodes needed for majority consensus
func (c *ClusterConfig) MajorityCount() int {
	return (c.ClusterSize / 2) + 1
}