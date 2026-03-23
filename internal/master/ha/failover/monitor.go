package failover

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/raft"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/shadow"
)

// HealthStatus represents the health status of a master node
type HealthStatus int

const (
	Healthy   HealthStatus = iota // Node is healthy and responsive
	Degraded                     // Node is responding but with issues
	Unhealthy                    // Node is not responding
	Unknown                      // Node status unknown
)

func (hs HealthStatus) String() string {
	switch hs {
	case Healthy:
		return "Healthy"
	case Degraded:
		return "Degraded"
	case Unhealthy:
		return "Unhealthy"
	default:
		return "Unknown"
	}
}

// NodeHealth represents the health information for a single node
type NodeHealth struct {
	NodeID      string        `json:"node_id"`
	Status      HealthStatus  `json:"status"`
	LastSeen    time.Time     `json:"last_seen"`
	ResponseTime time.Duration `json:"response_time"`
	ErrorCount  int           `json:"error_count"`
}

// HealthMonitor monitors the health of all masters in the cluster
type HealthMonitor struct {
	config *cluster.ClusterConfig
	
	// Health tracking
	nodeHealth map[string]*NodeHealth
	healthMu   sync.RWMutex
	
	// Raft integration
	raftSystem *raft.Raft
	
	// Shadow masters (for failover targets)
	shadowMasters map[string]*shadow.ShadowMaster
	shadowMu      sync.RWMutex
	
	// Failover state
	failoverInProgress bool
	failoverMu         sync.RWMutex
	
	// Control channels
	stopCh   chan struct{}
	running  bool
	runMu    sync.Mutex
}

// NewHealthMonitor creates a new health monitoring system
func NewHealthMonitor(config *cluster.ClusterConfig, raftSystem *raft.Raft) *HealthMonitor {
	hm := &HealthMonitor{
		config:        config,
		nodeHealth:    make(map[string]*NodeHealth),
		raftSystem:    raftSystem,
		shadowMasters: make(map[string]*shadow.ShadowMaster),
		stopCh:        make(chan struct{}),
	}
	
	// Initialize health tracking for all nodes
	for _, peerAddr := range config.PeerAddrs {
		hm.nodeHealth[peerAddr] = &NodeHealth{
			NodeID:   peerAddr,
			Status:   Unknown,
			LastSeen: time.Time{},
		}
	}
	
	return hm
}

// Start begins health monitoring
func (hm *HealthMonitor) Start(ctx context.Context) error {
	hm.runMu.Lock()
	defer hm.runMu.Unlock()
	
	if hm.running {
		return fmt.Errorf("health monitor already running")
	}
	
	log.Printf("Starting health monitor for cluster")
	
	// Start monitoring goroutines
	go hm.monitorClusterHealth(ctx)
	go hm.checkForFailover(ctx)
	
	hm.running = true
	return nil
}

// Stop gracefully shuts down health monitoring
func (hm *HealthMonitor) Stop() error {
	hm.runMu.Lock()
	defer hm.runMu.Unlock()
	
	if !hm.running {
		return nil
	}
	
	log.Printf("Stopping health monitor")
	close(hm.stopCh)
	hm.running = false
	return nil
}

// RegisterShadowMaster registers a shadow master for potential failover
func (hm *HealthMonitor) RegisterShadowMaster(nodeID string, shadow *shadow.ShadowMaster) {
	hm.shadowMu.Lock()
	defer hm.shadowMu.Unlock()
	
	hm.shadowMasters[nodeID] = shadow
	log.Printf("Registered shadow master %s for failover", nodeID)
}

// GetClusterHealth returns the current health status of all nodes
func (hm *HealthMonitor) GetClusterHealth() map[string]*NodeHealth {
	hm.healthMu.RLock()
	defer hm.healthMu.RUnlock()
	
	// Return a copy to prevent external modification
	healthCopy := make(map[string]*NodeHealth)
	for nodeID, health := range hm.nodeHealth {
		healthCopy[nodeID] = &NodeHealth{
			NodeID:       health.NodeID,
			Status:       health.Status,
			LastSeen:     health.LastSeen,
			ResponseTime: health.ResponseTime,
			ErrorCount:   health.ErrorCount,
		}
	}
	
	return healthCopy
}

// monitorClusterHealth continuously monitors the health of all cluster nodes
func (hm *HealthMonitor) monitorClusterHealth(ctx context.Context) {
	ticker := time.NewTicker(hm.config.HealthCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-hm.stopCh:
			return
		case <-ticker.C:
			hm.performHealthChecks()
		}
	}
}

// performHealthChecks checks the health of all cluster nodes
func (hm *HealthMonitor) performHealthChecks() {
	currentLeader, _ := hm.raftSystem.GetLeader()
	
	for _, nodeID := range hm.config.PeerAddrs {
		if nodeID == hm.config.NodeID {
			// Skip self health check
			continue
		}
		
		go hm.checkNodeHealth(nodeID, nodeID == currentLeader)
	}
}

// checkNodeHealth checks the health of a specific node
func (hm *HealthMonitor) checkNodeHealth(nodeID string, isLeader bool) {
	start := time.Now()
	
	// Perform health check (in real implementation, this would be an RPC call)
	healthy := hm.performHealthCheck(nodeID)
	responseTime := time.Since(start)
	
	hm.healthMu.Lock()
	defer hm.healthMu.Unlock()
	
	health := hm.nodeHealth[nodeID]
	health.ResponseTime = responseTime
	
	if healthy {
		health.Status = Healthy
		health.LastSeen = time.Now()
		health.ErrorCount = 0
	} else {
		health.ErrorCount++
		
		// Determine status based on error count and time since last seen
		timeSinceLastSeen := time.Since(health.LastSeen)
		
		if health.ErrorCount > 3 || timeSinceLastSeen > 30*time.Second {
			health.Status = Unhealthy
		} else {
			health.Status = Degraded
		}
	}
	
	// Log health status changes for leaders
	if isLeader && health.Status != Healthy {
		log.Printf("Leader %s health status: %s (errors: %d, last seen: %v ago)",
			nodeID, health.Status, health.ErrorCount, time.Since(health.LastSeen))
	}
}

// performHealthCheck performs the actual health check on a node
func (hm *HealthMonitor) performHealthCheck(nodeID string) bool {
	// In a real implementation, this would:
	// 1. Send a health check RPC to the node
	// 2. Check if the node responds within timeout
	// 3. Verify the node is functioning correctly
	
	// For simulation, we'll randomly fail some health checks
	// In reality, this would be based on actual network connectivity and node responsiveness
	
	// Simulate network delay
	time.Sleep(time.Duration(10) * time.Millisecond)
	
	// Simulate 95% success rate
	return time.Now().UnixNano()%100 < 95
}

// checkForFailover monitors for failover conditions
func (hm *HealthMonitor) checkForFailover(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second) // Check for failover every 5 seconds
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-hm.stopCh:
			return
		case <-ticker.C:
			hm.evaluateFailoverNeed()
		}
	}
}

// evaluateFailoverNeed checks if failover is needed and triggers it
func (hm *HealthMonitor) evaluateFailoverNeed() {
	if !hm.config.EnableFailover {
		return
	}
	
	hm.failoverMu.Lock()
	if hm.failoverInProgress {
		hm.failoverMu.Unlock()
		return
	}
	hm.failoverMu.Unlock()
	
	// Check if current leader is unhealthy
	currentLeader, _ := hm.raftSystem.GetLeader()
	if currentLeader == "" {
		return
	}
	
	hm.healthMu.RLock()
	leaderHealth := hm.nodeHealth[currentLeader]
	hm.healthMu.RUnlock()
	
	if leaderHealth != nil && leaderHealth.Status == Unhealthy {
		log.Printf("Leader %s is unhealthy, evaluating failover options", currentLeader)
		go hm.initiateFailover(currentLeader)
	}
}

// initiateFailover starts the failover process
func (hm *HealthMonitor) initiateFailover(failedLeader string) {
	hm.failoverMu.Lock()
	if hm.failoverInProgress {
		hm.failoverMu.Unlock()
		return
	}
	hm.failoverInProgress = true
	hm.failoverMu.Unlock()
	
	defer func() {
		hm.failoverMu.Lock()
		hm.failoverInProgress = false
		hm.failoverMu.Unlock()
	}()
	
	log.Printf("Initiating failover from failed leader: %s", failedLeader)
	
	// Find the best shadow master for promotion
	bestCandidate := hm.findBestFailoverCandidate()
	if bestCandidate == "" {
		log.Printf("No suitable failover candidate found")
		return
	}
	
	log.Printf("Selected failover candidate: %s", bestCandidate)
	
	// Promote the best candidate
	if err := hm.promoteCandidate(bestCandidate); err != nil {
		log.Printf("Failed to promote candidate %s: %v", bestCandidate, err)
		return
	}
	
	log.Printf("Failover completed successfully, new leader: %s", bestCandidate)
}

// findBestFailoverCandidate finds the best shadow master to promote
func (hm *HealthMonitor) findBestFailoverCandidate() string {
	hm.shadowMu.RLock()
	defer hm.shadowMu.RUnlock()
	
	hm.healthMu.RLock()
	defer hm.healthMu.RUnlock()
	
	var bestCandidate string
	var bestHealth *NodeHealth
	
	for nodeID, shadowMaster := range hm.shadowMasters {
		if !shadowMaster.CanPromote() {
			continue
		}
		
		health := hm.nodeHealth[nodeID]
		if health == nil || health.Status != Healthy {
			continue
		}
		
		if bestHealth == nil || health.ResponseTime < bestHealth.ResponseTime {
			bestCandidate = nodeID
			bestHealth = health
		}
	}
	
	return bestCandidate
}

// promoteCandidate promotes a shadow master to primary
func (hm *HealthMonitor) promoteCandidate(nodeID string) error {
	hm.shadowMu.RLock()
	shadowMaster := hm.shadowMasters[nodeID]
	hm.shadowMu.RUnlock()
	
	if shadowMaster == nil {
		return fmt.Errorf("shadow master %s not found", nodeID)
	}
	
	return shadowMaster.Promote()
}