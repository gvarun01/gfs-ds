package raft

import (
	"log"
	"strings"
	"sync"
	
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/raft/proto"
)

// NodeRegistry provides a simple in-memory registry for node communication during demos
// In production, this would be replaced with gRPC services
type NodeRegistry struct {
	nodes map[string]*RaftNode
	mu    sync.RWMutex
}

var globalNodeRegistry = &NodeRegistry{
	nodes: make(map[string]*RaftNode),
}

// extractNodeID extracts node ID from address (e.g., "master-1:8080" -> "master-1")
func extractNodeID(addr string) string {
	if idx := strings.Index(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// isSameNode checks if an address refers to the same node ID
func isSameNode(addr, nodeID string) bool {
	return extractNodeID(addr) == nodeID
}

// RegisterNode registers a node in the global registry
func RegisterNode(nodeID string, node *RaftNode) {
	globalNodeRegistry.mu.Lock()
	defer globalNodeRegistry.mu.Unlock()
	globalNodeRegistry.nodes[nodeID] = node
	log.Printf("Registered node %s in global registry", nodeID)
}

// UnregisterNode removes a node from the global registry
func UnregisterNode(nodeID string) {
	globalNodeRegistry.mu.Lock()
	defer globalNodeRegistry.mu.Unlock()
	delete(globalNodeRegistry.nodes, nodeID)
	log.Printf("Unregistered node %s from global registry", nodeID)
}

// GetNode retrieves a node from the global registry
func GetNode(addr string) *RaftNode {
	nodeID := extractNodeID(addr)
	globalNodeRegistry.mu.RLock()
	defer globalNodeRegistry.mu.RUnlock()
	return globalNodeRegistry.nodes[nodeID]
}

// ClearRegistry clears all nodes from the registry (for testing)
func ClearRegistry() {
	globalNodeRegistry.mu.Lock()
	defer globalNodeRegistry.mu.Unlock()
	globalNodeRegistry.nodes = make(map[string]*RaftNode)
	log.Printf("Cleared node registry")
}

// requestVoteFromNode sends a vote request to a specific node (fallback for demos)
func (rn *RaftNode) requestVoteFromNode(peerAddr string, req *proto.VoteRequest) *proto.VoteResponse {
	node := GetNode(peerAddr)
	if node == nil {
		log.Printf("Node %s not found in registry for vote request", extractNodeID(peerAddr))
		return &proto.VoteResponse{Term: 0, VoteGranted: false}
	}
	
	// Process vote request using the gRPC server implementation
	server := NewRaftServer(node)
	response, _ := server.RequestVote(nil, req)
	return response
}

// sendAppendEntriesToNode sends append entries to a specific node (fallback for demos)
func (rn *RaftNode) sendAppendEntriesToNode(peerAddr string, req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
	node := GetNode(peerAddr)
	if node == nil {
		log.Printf("Node %s not found in registry for append entries", extractNodeID(peerAddr))
		return &proto.AppendEntriesResponse{Term: 0, Success: false}
	}
	
	// Process append entries using the gRPC server implementation
	server := NewRaftServer(node)
	response, _ := server.AppendEntries(nil, req)
	return response
}