package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
	
	"google.golang.org/grpc"
)

// NodeState represents the current state of a Raft node
type NodeState int

const (
	Follower  NodeState = iota // Node is following a leader
	Candidate                  // Node is campaigning to become leader
	Leader                     // Node is the current leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate" 
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int64       `json:"term"`     // Term when entry was received by leader
	Index   int64       `json:"index"`    // Position in the log
	Type    string      `json:"type"`     // Type of operation (file_create, file_delete, etc.)
	Data    []byte      `json:"data"`     // Serialized operation data
	Created time.Time   `json:"created"`  // Timestamp when entry was created
}

// RaftNode represents a single node in the Raft cluster
type RaftNode struct {
	// Node identity
	nodeID    string
	peerAddrs []string
	port      int // gRPC server port for this node
	
	// Raft state (persistent)
	currentTerm int64     // Latest term server has seen
	votedFor    string    // CandidateId that received vote in current term
	log         []*LogEntry // Log entries
	
	// Raft state (volatile)
	commitIndex int64 // Index of highest log entry known to be committed
	lastApplied int64 // Index of highest log entry applied to state machine
	
	// Leader state (volatile, reinitialized after election)
	nextIndex  map[string]int64 // For each server, index of the next log entry to send
	matchIndex map[string]int64 // For each server, index of highest log entry known to be replicated
	
	// Node state
	state       NodeState
	currentLeader string
	
	// Timing
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	
	// Channels and control
	electionTimer *time.Timer
	heartbeatTicker *time.Ticker
	stopCh        chan struct{}
	
	// gRPC components
	grpcServer   *grpc.Server
	grpcListener net.Listener
	grpcClient   *RaftClient
	
	// Thread safety
	mu sync.RWMutex
	
	// Callbacks
	onStateChange func(oldState, newState NodeState)
	onLogApply    func(entry *LogEntry) error
}

// NewRaftNode creates a new Raft node
func NewRaftNode(nodeID string, peerAddrs []string, config NodeConfig) *RaftNode {
	node := &RaftNode{
		nodeID:    nodeID,
		peerAddrs: make([]string, len(peerAddrs)),
		port:      config.Port,
		
		// Initialize Raft state
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*LogEntry, 0),
		
		commitIndex: 0,
		lastApplied: 0,
		
		nextIndex:  make(map[string]int64),
		matchIndex: make(map[string]int64),
		
		state: Follower,
		
		electionTimeoutMin: config.ElectionTimeoutMin,
		electionTimeoutMax: config.ElectionTimeoutMax,
		heartbeatInterval:  config.HeartbeatInterval,
		
		stopCh:     make(chan struct{}),
		grpcClient: NewRaftClient(),
	}
	
	copy(node.peerAddrs, peerAddrs)
	
	// Initialize leader state
	for _, addr := range peerAddrs {
		if !isSameNode(addr, nodeID) {
			node.nextIndex[addr] = 1
			node.matchIndex[addr] = 0
		}
	}
	
	return node
}

// NodeConfig contains configuration for a Raft node
type NodeConfig struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	Port               int // gRPC server port
}

// Start begins the Raft node operation
func (rn *RaftNode) Start(ctx context.Context) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	// Start gRPC server
	if err := rn.startGRPCServer(rn.port); err != nil {
		return fmt.Errorf("failed to start gRPC server: %v", err)
	}
	
	// Register this node in the global registry for demo communication
	RegisterNode(rn.nodeID, rn)
	
	// Start election timer
	rn.resetElectionTimer()
	
	// Start main Raft loop
	go rn.run(ctx)
	
	return nil
}

// Stop gracefully shuts down the Raft node
func (rn *RaftNode) Stop() error {
	// Stop gRPC server
	rn.stopGRPCServer()
	
	// Close gRPC client connections
	if rn.grpcClient != nil {
		rn.grpcClient.Close()
	}
	
	// Unregister this node from the global registry
	UnregisterNode(rn.nodeID)
	
	close(rn.stopCh)
	
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	
	if rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
	}
	
	return nil
}

// GetState returns the current node state and leader
func (rn *RaftNode) GetState() (NodeState, string, int64) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	
	return rn.state, rn.currentLeader, rn.currentTerm
}

// IsLeader returns true if this node is the current leader
func (rn *RaftNode) IsLeader() bool {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	
	return rn.state == Leader
}

// GetLeader returns the current leader node ID
func (rn *RaftNode) GetLeader() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	
	return rn.currentLeader
}

// SetStateChangeCallback sets the callback for state changes
func (rn *RaftNode) SetStateChangeCallback(callback func(oldState, newState NodeState)) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	rn.onStateChange = callback
}

// SetLogApplyCallback sets the callback for applying log entries
func (rn *RaftNode) SetLogApplyCallback(callback func(entry *LogEntry) error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	rn.onLogApply = callback
}