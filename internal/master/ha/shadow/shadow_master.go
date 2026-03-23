package shadow

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/raft"
	"google.golang.org/grpc"
)

// ShadowMaster represents a read-only replica of the master
type ShadowMaster struct {
	config *cluster.ClusterConfig
	
	// Master state (read-only replica)
	masterState *master.Master
	stateMu     sync.RWMutex
	
	// Replication from primary
	raftSystem *raft.Raft
	replicationCh chan *raft.LogEntry
	
	// gRPC server for read-only operations
	grpcServer *grpc.Server
	
	// Status
	isReady    bool
	readyMu    sync.RWMutex
	shutdownCh chan struct{}
	running    bool
	runMu      sync.Mutex
}

// NewShadowMaster creates a new shadow master instance
func NewShadowMaster(config *cluster.ClusterConfig, masterConfig *master.Config) (*ShadowMaster, error) {
	if !config.EnableShadows {
		return nil, fmt.Errorf("shadow masters not enabled in configuration")
	}
	
	// Create a master instance for state replication
	masterState := master.NewMaster(masterConfig)
	
	// Create Raft system for receiving replicated logs
	raftSystem, err := raft.NewRaft(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft system: %v", err)
	}
	
	shadow := &ShadowMaster{
		config:        config,
		masterState:   masterState,
		raftSystem:    raftSystem,
		replicationCh: make(chan *raft.LogEntry, 100),
		shutdownCh:    make(chan struct{}),
	}
	
	// Set up log replication callback
	raftSystem.SetApplyCallback(shadow.applyLogEntry)
	
	return shadow, nil
}

// Start begins shadow master operation
func (sm *ShadowMaster) Start(ctx context.Context) error {
	sm.runMu.Lock()
	defer sm.runMu.Unlock()
	
	if sm.running {
		return fmt.Errorf("shadow master already running")
	}
	
	log.Printf("Starting shadow master %s", sm.config.NodeID)
	
	// Start Raft system (as follower)
	if err := sm.raftSystem.Start(ctx); err != nil {
		return fmt.Errorf("failed to start Raft system: %v", err)
	}
	
	// Start log replication processor
	go sm.processLogReplication(ctx)
	
	// Start read-only gRPC server
	if err := sm.startReadOnlyServer(); err != nil {
		return fmt.Errorf("failed to start read-only server: %v", err)
	}
	
	// Wait for initial synchronization
	go sm.waitForInitialSync()
	
	sm.running = true
	log.Printf("Shadow master %s started successfully", sm.config.NodeID)
	
	return nil
}

// Stop gracefully shuts down the shadow master
func (sm *ShadowMaster) Stop() error {
	sm.runMu.Lock()
	defer sm.runMu.Unlock()
	
	if !sm.running {
		return nil
	}
	
	log.Printf("Stopping shadow master %s", sm.config.NodeID)
	
	close(sm.shutdownCh)
	
	// Stop gRPC server
	if sm.grpcServer != nil {
		sm.grpcServer.GracefulStop()
	}
	
	// Stop Raft system
	if err := sm.raftSystem.Stop(); err != nil {
		log.Printf("Error stopping Raft system: %v", err)
	}
	
	sm.running = false
	return nil
}

// IsReady returns true if the shadow master is ready to serve read requests
func (sm *ShadowMaster) IsReady() bool {
	sm.readyMu.RLock()
	defer sm.readyMu.RUnlock()
	return sm.isReady
}

// waitForInitialSync waits for initial synchronization with primary
func (sm *ShadowMaster) waitForInitialSync() {
	// Wait for Raft to establish connection with leader
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeout:
			log.Printf("Shadow master %s: initial sync timeout", sm.config.NodeID)
			return
		case <-ticker.C:
			_, leader, _ := sm.raftSystem.GetState()
			if leader != "" && leader != sm.config.NodeID {
				log.Printf("Shadow master %s: connected to leader %s", sm.config.NodeID, leader)
				
				sm.readyMu.Lock()
				sm.isReady = true
				sm.readyMu.Unlock()
				
				log.Printf("Shadow master %s is ready to serve read requests", sm.config.NodeID)
				return
			}
		case <-sm.shutdownCh:
			return
		}
	}
}

// processLogReplication processes replicated log entries from the primary
func (sm *ShadowMaster) processLogReplication(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sm.shutdownCh:
			return
		case entry := <-sm.replicationCh:
			if err := sm.applyLogEntryToState(entry); err != nil {
				log.Printf("Shadow master %s: error applying log entry: %v", sm.config.NodeID, err)
			}
		}
	}
}

// applyLogEntry is called by Raft when a log entry should be applied
func (sm *ShadowMaster) applyLogEntry(entry *raft.LogEntry) error {
	// Queue for processing in the background
	select {
	case sm.replicationCh <- entry:
		return nil
	default:
		log.Printf("Shadow master %s: replication channel full, dropping entry", sm.config.NodeID)
		return fmt.Errorf("replication channel full")
	}
}

// applyLogEntryToState applies a log entry to the shadow master's state
func (sm *ShadowMaster) applyLogEntryToState(entry *raft.LogEntry) error {
	sm.stateMu.Lock()
	defer sm.stateMu.Unlock()
	
	log.Printf("Shadow master %s applying %s operation (index: %d, term: %d)", 
		sm.config.NodeID, entry.Type, entry.Index, entry.Term)
	
	// Apply the operation to the local master state
	// This would deserialize the operation data and apply it to the master
	// For now, we'll simulate this process
	
	switch entry.Type {
	case "file_create":
		// Apply file creation
		log.Printf("Shadow: Applied file creation")
	case "file_delete":
		// Apply file deletion
		log.Printf("Shadow: Applied file deletion")
	case "chunk_create":
		// Apply chunk creation
		log.Printf("Shadow: Applied chunk creation")
	default:
		log.Printf("Shadow: Unknown operation type: %s", entry.Type)
	}
	
	return nil
}

// startReadOnlyServer starts the read-only gRPC server
func (sm *ShadowMaster) startReadOnlyServer() error {
	// This would start a gRPC server on the read-only port
	// For now, we'll just log that it's starting
	log.Printf("Shadow master %s: Starting read-only gRPC server on port %d", 
		sm.config.NodeID, sm.config.ReadOnlyPort)
	
	// In a full implementation, this would:
	// 1. Create a gRPC server
	// 2. Register read-only services (GetFileInfo, GetChunkLocations, etc.)
	// 3. Start listening on the read-only port
	
	return nil
}

// CanPromote returns true if this shadow master can be promoted to primary
func (sm *ShadowMaster) CanPromote() bool {
	sm.readyMu.RLock()
	defer sm.readyMu.RUnlock()
	
	return sm.isReady && sm.running
}

// Promote promotes this shadow master to primary master
func (sm *ShadowMaster) Promote() error {
	if !sm.CanPromote() {
		return fmt.Errorf("shadow master not ready for promotion")
	}
	
	log.Printf("Promoting shadow master %s to primary", sm.config.NodeID)
	
	// This would involve:
	// 1. Becoming Raft leader
	// 2. Starting write operations
	// 3. Taking over DNS records
	// 4. Notifying other components
	
	return fmt.Errorf("promotion not yet implemented")
}