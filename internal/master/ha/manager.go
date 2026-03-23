package ha

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/dns"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/failover"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/raft"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/shadow"
)

// Manager coordinates all high availability components
type Manager struct {
	config *cluster.ClusterConfig
	masterConfig *master.Config
	
	// HA Components
	raftSystem    *raft.Raft
	shadowMaster  *shadow.ShadowMaster
	healthMonitor *failover.HealthMonitor
	dnsManager    *dns.DNSManager
	
	// Master integration
	masterInstance *master.Master
	
	// State
	isActive  bool
	activeMu  sync.RWMutex
	running   bool
	runMu     sync.Mutex
}

// NewManager creates a new HA manager
func NewManager(clusterConfig *cluster.ClusterConfig, masterConfig *master.Config) (*Manager, error) {
	if err := clusterConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid cluster configuration: %v", err)
	}
	
	// Create Raft consensus system
	raftSystem, err := raft.NewRaft(clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft system: %v", err)
	}
	
	// Create health monitor
	healthMonitor := failover.NewHealthMonitor(clusterConfig, raftSystem)
	
	// Create DNS manager if enabled
	var dnsManager *dns.DNSManager
	if clusterConfig.DNSEnabled {
		var err error
		dnsManager, err = dns.NewDNSManager(clusterConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create DNS manager: %v", err)
		}
	}
	
	manager := &Manager{
		config:        clusterConfig,
		masterConfig:  masterConfig,
		raftSystem:    raftSystem,
		healthMonitor: healthMonitor,
		dnsManager:    dnsManager,
	}
	
	// Set up Raft callbacks
	raftSystem.SetApplyCallback(manager.applyOperation)
	
	// Create shadow master if enabled
	if clusterConfig.EnableShadows && clusterConfig.ClusterSize > 1 {
		shadowMaster, err := shadow.NewShadowMaster(clusterConfig, masterConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create shadow master: %v", err)
		}
		manager.shadowMaster = shadowMaster
		
		// Register shadow master with health monitor
		healthMonitor.RegisterShadowMaster(clusterConfig.NodeID, shadowMaster)
	}
	
	return manager, nil
}

// Start begins HA operation
func (m *Manager) Start(ctx context.Context, masterInstance *master.Master) error {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	
	if m.running {
		return fmt.Errorf("HA manager already running")
	}
	
	m.masterInstance = masterInstance
	
	log.Printf("Starting HA manager for node %s in %d-node cluster", 
		m.config.NodeID, m.config.ClusterSize)
	
	// Start Raft consensus system
	if err := m.raftSystem.Start(ctx); err != nil {
		return fmt.Errorf("failed to start Raft system: %v", err)
	}
	
	// Start health monitoring
	if err := m.healthMonitor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start health monitor: %v", err)
	}
	
	// Start DNS manager if enabled
	if m.dnsManager != nil {
		if err := m.dnsManager.Start(ctx); err != nil {
			return fmt.Errorf("failed to start DNS manager: %v", err)
		}
	}
	
	// Start shadow master if configured
	if m.shadowMaster != nil {
		if err := m.shadowMaster.Start(ctx); err != nil {
			return fmt.Errorf("failed to start shadow master: %v", err)
		}
	}
	
	// Monitor Raft leadership changes
	go m.monitorLeadership(ctx)
	
	m.running = true
	log.Printf("HA manager started successfully")
	
	return nil
}

// Stop gracefully shuts down HA components
func (m *Manager) Stop() error {
	m.runMu.Lock()
	defer m.runMu.Unlock()
	
	if !m.running {
		return nil
	}
	
	log.Printf("Stopping HA manager")
	
	// Stop DNS manager
	if m.dnsManager != nil {
		if err := m.dnsManager.Stop(); err != nil {
			log.Printf("Error stopping DNS manager: %v", err)
		}
	}
	
	// Stop shadow master
	if m.shadowMaster != nil {
		if err := m.shadowMaster.Stop(); err != nil {
			log.Printf("Error stopping shadow master: %v", err)
		}
	}
	
	// Stop health monitor
	if err := m.healthMonitor.Stop(); err != nil {
		log.Printf("Error stopping health monitor: %v", err)
	}
	
	// Stop Raft system
	if err := m.raftSystem.Stop(); err != nil {
		log.Printf("Error stopping Raft system: %v", err)
	}
	
	m.running = false
	return nil
}

// IsActive returns true if this node is the active master
func (m *Manager) IsActive() bool {
	m.activeMu.RLock()
	defer m.activeMu.RUnlock()
	return m.isActive
}

// GetClusterHealth returns the current cluster health status
func (m *Manager) GetClusterHealth() map[string]*failover.NodeHealth {
	return m.healthMonitor.GetClusterHealth()
}

// GetRaftState returns the current Raft state
func (m *Manager) GetRaftState() (raft.NodeState, string, int64) {
	return m.raftSystem.GetState()
}

// ReplicateOperation replicates an operation through Raft consensus
func (m *Manager) ReplicateOperation(opType string, data interface{}) error {
	if !m.IsActive() {
		leader, _ := m.raftSystem.GetLeader()
		return fmt.Errorf("not active master, current leader: %s", leader)
	}
	
	return m.raftSystem.AppendOperation(opType, data)
}

// monitorLeadership monitors Raft leadership changes
func (m *Manager) monitorLeadership(ctx context.Context) {
	// This would monitor for Raft state changes
	// For now, we'll check periodically
	
	for {
		select {
		case <-ctx.Done():
			return
		default:
			isLeader := m.raftSystem.IsLeader()
			
			m.activeMu.Lock()
			wasActive := m.isActive
			m.isActive = isLeader
			m.activeMu.Unlock()
			
			if isLeader != wasActive {
				if isLeader {
					log.Printf("Node %s became active master", m.config.NodeID)
					m.onBecomeActive()
				} else {
					log.Printf("Node %s no longer active master", m.config.NodeID)
					m.onBecomeInactive()
				}
			}
			
			// Check every second
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				continue
			}
		}
	}
}

// onBecomeActive handles becoming the active master
func (m *Manager) onBecomeActive() {
	log.Printf("Activating master operations for node %s", m.config.NodeID)
	
	// Enable write operations on master
	if m.masterInstance != nil {
		// In a full implementation, this would:
		// 1. Enable write operations
		// 2. Update DNS records
		// 3. Start accepting client connections
		// 4. Begin background tasks (GC, replication, etc.)
	}
	
	// Update DNS records to point to this master
	if m.dnsManager != nil {
		// Get the master service address (IP:port)
		masterAddr := m.getMasterServiceAddress()
		if err := m.dnsManager.UpdateMasterRecord(masterAddr); err != nil {
			log.Printf("Failed to update DNS record for active master: %v", err)
		}
	}
}

// onBecomeInactive handles becoming inactive (shadow)
func (m *Manager) onBecomeInactive() {
	log.Printf("Deactivating master operations for node %s", m.config.NodeID)
	
	// Disable write operations on master
	if m.masterInstance != nil {
		// In a full implementation, this would:
		// 1. Disable write operations
		// 2. Stop background tasks
		// 3. Switch to read-only mode
		// 4. Update DNS records if needed
	}
	
	// Note: We don't remove DNS records here as the new leader will update them
}

// getMasterServiceAddress returns the service address for this master
func (m *Manager) getMasterServiceAddress() string {
	// In a full implementation, this would return the public service address
	// For demo, we'll use the node ID with the service port
	if m.masterConfig != nil {
		return fmt.Sprintf("%s:%d", m.config.NodeID, m.masterConfig.Server.Port)
	}
	return fmt.Sprintf("%s:8080", m.config.NodeID) // Default port
}

// applyOperation applies a replicated operation to the master state
func (m *Manager) applyOperation(entry *raft.LogEntry) error {
	log.Printf("Applying operation: %s (index: %d, term: %d)", 
		entry.Type, entry.Index, entry.Term)
	
	// Apply the operation to the master state
	if m.masterInstance != nil {
		// In a full implementation, this would deserialize the operation
		// and apply it to the master's state machine
		return m.masterInstance.ApplyOperation(entry.Type, entry.Data)
	}
	
	return nil
}