package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
)

// Raft represents the Raft consensus system for master replication
type Raft struct {
	config *cluster.ClusterConfig
	node   *RaftNode
	
	// Operation log replication
	operationLog []*LogEntry
	logMu        sync.RWMutex
	
	// State machine callbacks
	applyCallback func(entry *LogEntry) error
	
	// Leader information
	isLeader     bool
	leaderInfo   string
	leaderTerm   int64
	leaderMu     sync.RWMutex
	
	// Shutdown
	shutdownCh chan struct{}
	running    bool
	runMu      sync.Mutex
}

// NewRaft creates a new Raft consensus system
func NewRaft(config *cluster.ClusterConfig) (*Raft, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid cluster config: %v", err)
	}
	
	nodeConfig := NodeConfig{
		ElectionTimeoutMin: config.ElectionTimeoutMin,
		ElectionTimeoutMax: config.ElectionTimeoutMax,
		HeartbeatInterval:  config.HeartbeatInterval,
		Port:               config.RaftPort,
	}
	
	node := NewRaftNode(config.NodeID, config.PeerAddrs, nodeConfig)
	
	raft := &Raft{
		config:       config,
		node:         node,
		operationLog: make([]*LogEntry, 0),
		shutdownCh:   make(chan struct{}),
	}
	
	// Set up callbacks
	node.SetStateChangeCallback(raft.onStateChange)
	node.SetLogApplyCallback(raft.onLogApply)
	
	return raft, nil
}

// Start begins the Raft consensus system
func (r *Raft) Start(ctx context.Context) error {
	r.runMu.Lock()
	defer r.runMu.Unlock()
	
	if r.running {
		return fmt.Errorf("Raft already running")
	}
	
	log.Printf("Starting Raft node %s in %d-node cluster", r.config.NodeID, r.config.ClusterSize)
	
	if err := r.node.Start(ctx); err != nil {
		return fmt.Errorf("failed to start Raft node: %v", err)
	}
	
	r.running = true
	
	// If this is a single-node cluster, immediately become leader
	if r.config.ClusterSize == 1 {
		log.Printf("Single-node cluster detected, becoming leader immediately")
		go func() {
			time.Sleep(100 * time.Millisecond) // Allow initialization to complete
			r.node.becomeLeader()
		}()
	}
	
	return nil
}

// Stop gracefully shuts down the Raft system
func (r *Raft) Stop() error {
	r.runMu.Lock()
	defer r.runMu.Unlock()
	
	if !r.running {
		return nil
	}
	
	log.Printf("Stopping Raft node %s", r.config.NodeID)
	
	close(r.shutdownCh)
	
	if err := r.node.Stop(); err != nil {
		return fmt.Errorf("failed to stop Raft node: %v", err)
	}
	
	r.running = false
	return nil
}

// IsLeader returns true if this node is the current Raft leader
func (r *Raft) IsLeader() bool {
	r.leaderMu.RLock()
	defer r.leaderMu.RUnlock()
	return r.isLeader
}

// GetLeader returns the current leader node ID and term
func (r *Raft) GetLeader() (string, int64) {
	r.leaderMu.RLock()
	defer r.leaderMu.RUnlock()
	return r.leaderInfo, r.leaderTerm
}

// GetState returns the current Raft state
func (r *Raft) GetState() (NodeState, string, int64) {
	return r.node.GetState()
}

// AppendOperation appends a new operation to the Raft log
// This should only be called on the leader
func (r *Raft) AppendOperation(opType string, data interface{}) error {
	if !r.IsLeader() {
		leader, _ := r.GetLeader()
		return fmt.Errorf("not leader, current leader: %s", leader)
	}
	
	// Serialize operation data
	opData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to serialize operation data: %v", err)
	}
	
	r.logMu.Lock()
	defer r.logMu.Unlock()
	
	// Create new log entry
	entry := &LogEntry{
		Term:    r.leaderTerm,
		Index:   int64(len(r.operationLog) + 1),
		Type:    opType,
		Data:    opData,
		Created: time.Now(),
	}
	
	// Append to local log
	r.operationLog = append(r.operationLog, entry)
	
	log.Printf("Leader %s appended operation %s at index %d, term %d", 
		r.config.NodeID, opType, entry.Index, entry.Term)
	
	// In a full implementation, this would replicate to followers
	// For now, we'll just apply it locally
	if r.applyCallback != nil {
		if err := r.applyCallback(entry); err != nil {
			log.Printf("Error applying log entry: %v", err)
			return err
		}
	}
	
	return nil
}

// SetApplyCallback sets the callback function for applying log entries to the state machine
func (r *Raft) SetApplyCallback(callback func(entry *LogEntry) error) {
	r.applyCallback = callback
}

// GetOperationLog returns a copy of the current operation log
func (r *Raft) GetOperationLog() []*LogEntry {
	r.logMu.RLock()
	defer r.logMu.RUnlock()
	
	// Return a copy to prevent external modification
	logCopy := make([]*LogEntry, len(r.operationLog))
	copy(logCopy, r.operationLog)
	return logCopy
}

// onStateChange handles Raft node state changes
func (r *Raft) onStateChange(oldState, newState NodeState) {
	r.leaderMu.Lock()
	defer r.leaderMu.Unlock()
	
	log.Printf("Raft node %s state change: %s -> %s", r.config.NodeID, oldState, newState)
	
	switch newState {
	case Leader:
		r.isLeader = true
		r.leaderInfo = r.config.NodeID
		r.leaderTerm = r.node.currentTerm
		log.Printf("Node %s is now the leader for term %d", r.config.NodeID, r.leaderTerm)
		
	case Follower, Candidate:
		r.isLeader = false
		if newState == Follower {
			leader := r.node.GetLeader()
			r.leaderInfo = leader
			r.leaderTerm = r.node.currentTerm
			log.Printf("Node %s is now following leader %s for term %d", r.config.NodeID, leader, r.leaderTerm)
		} else {
			r.leaderInfo = ""
		}
	}
}

// onLogApply handles applying log entries to the state machine
func (r *Raft) onLogApply(entry *LogEntry) error {
	log.Printf("Applying log entry: index=%d, term=%d, type=%s", entry.Index, entry.Term, entry.Type)
	
	if r.applyCallback != nil {
		return r.applyCallback(entry)
	}
	
	return nil
}