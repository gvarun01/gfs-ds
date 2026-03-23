package raft

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/raft/proto"
	"google.golang.org/grpc"
)

// RaftServer implements the gRPC Raft service
type RaftServer struct {
	proto.UnimplementedRaftServiceServer
	node *RaftNode
}

// NewRaftServer creates a new gRPC Raft server
func NewRaftServer(node *RaftNode) *RaftServer {
	return &RaftServer{
		node: node,
	}
}

// RequestVote handles vote requests from candidates
func (rs *RaftServer) RequestVote(ctx context.Context, req *proto.VoteRequest) (*proto.VoteResponse, error) {
	rs.node.mu.Lock()
	defer rs.node.mu.Unlock()
	
	response := &proto.VoteResponse{
		Term:        rs.node.currentTerm,
		VoteGranted: false,
	}
	
	// If request term is older, reject
	if req.Term < rs.node.currentTerm {
		return response, nil
	}
	
	// If request term is newer, become follower
	if req.Term > rs.node.currentTerm {
		rs.node.currentTerm = req.Term
		rs.node.votedFor = ""
		if rs.node.state != Follower {
			rs.node.state = Follower
			rs.node.currentLeader = ""
		}
	}
	
	// Check if we can grant the vote
	lastLogIndex := int64(len(rs.node.log))
	var lastLogTerm int64
	if lastLogIndex > 0 {
		lastLogTerm = rs.node.log[lastLogIndex-1].Term
	}
	
	logOk := (req.LastLogTerm > lastLogTerm) || 
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)
	
	if (rs.node.votedFor == "" || rs.node.votedFor == req.CandidateId) && logOk {
		rs.node.votedFor = req.CandidateId
		response.VoteGranted = true
		rs.node.resetElectionTimer() // Reset election timer when granting vote
		log.Printf("Node %s granted vote to %s for term %d", rs.node.nodeID, req.CandidateId, req.Term)
	} else {
		log.Printf("Node %s denied vote to %s for term %d (already voted for %s, logOk=%v)", 
			rs.node.nodeID, req.CandidateId, req.Term, rs.node.votedFor, logOk)
	}
	
	return response, nil
}

// AppendEntries handles append entries requests from leaders
func (rs *RaftServer) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	rs.node.mu.Lock()
	defer rs.node.mu.Unlock()
	
	response := &proto.AppendEntriesResponse{
		Term:    rs.node.currentTerm,
		Success: false,
	}
	
	// If request term is older, reject
	if req.Term < rs.node.currentTerm {
		return response, nil
	}
	
	// If request term is newer or equal, become follower
	if req.Term >= rs.node.currentTerm {
		rs.node.currentTerm = req.Term
		if rs.node.state != Follower {
			oldState := rs.node.state
			rs.node.state = Follower
			if rs.node.onStateChange != nil && oldState != Follower {
				go rs.node.onStateChange(oldState, Follower)
			}
		}
		rs.node.currentLeader = req.LeaderId
		rs.node.votedFor = ""
	}
	
	// Reset election timer since we heard from leader
	rs.node.resetElectionTimer()
	
	// For heartbeats (empty entries), just acknowledge
	if len(req.Entries) == 0 {
		response.Success = true
		log.Printf("Node %s received heartbeat from leader %s (term %d)", 
			rs.node.nodeID, req.LeaderId, req.Term)
		return response, nil
	}
	
	// Handle log replication (simplified for demo)
	log.Printf("Node %s received %d log entries from leader %s (term %d)", 
		rs.node.nodeID, len(req.Entries), req.LeaderId, req.Term)
	
	// Convert proto entries to internal format and append
	for _, protoEntry := range req.Entries {
		entry := &LogEntry{
			Term:    protoEntry.Term,
			Index:   protoEntry.Index,
			Type:    protoEntry.Type,
			Data:    protoEntry.Data,
			Created: time.Unix(0, protoEntry.Created),
		}
		rs.node.log = append(rs.node.log, entry)
		
		// Apply the entry if we have the callback
		if rs.node.onLogApply != nil {
			go rs.node.onLogApply(entry)
		}
	}
	
	response.Success = true
	return response, nil
}

// RaftClient provides gRPC client for Raft operations
type RaftClient struct {
	clients map[string]proto.RaftServiceClient
	conns   map[string]*grpc.ClientConn
	mu      sync.RWMutex
}

// NewRaftClient creates a new Raft gRPC client
func NewRaftClient() *RaftClient {
	return &RaftClient{
		clients: make(map[string]proto.RaftServiceClient),
		conns:   make(map[string]*grpc.ClientConn),
	}
}

// getClient gets or creates a gRPC client for a peer address
func (rc *RaftClient) getClient(peerAddr string) (proto.RaftServiceClient, error) {
	rc.mu.RLock()
	client, exists := rc.clients[peerAddr]
	rc.mu.RUnlock()
	
	if exists {
		return client, nil
	}
	
	rc.mu.Lock()
	defer rc.mu.Unlock()
	
	// Double-check in case another goroutine created the client
	if client, exists = rc.clients[peerAddr]; exists {
		return client, nil
	}
	
	// Create new gRPC connection
	conn, err := grpc.Dial(peerAddr, grpc.WithInsecure(), 
		grpc.WithTimeout(5*time.Second),
		grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", peerAddr, err)
	}
	
	client = proto.NewRaftServiceClient(conn)
	rc.clients[peerAddr] = client
	rc.conns[peerAddr] = conn
	
	log.Printf("Created gRPC client for peer %s", peerAddr)
	return client, nil
}

// RequestVote sends a vote request to a peer via gRPC
func (rc *RaftClient) RequestVote(ctx context.Context, peerAddr string, req *proto.VoteRequest) (*proto.VoteResponse, error) {
	client, err := rc.getClient(peerAddr)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	
	return client.RequestVote(ctx, req)
}

// AppendEntries sends an append entries request to a peer via gRPC
func (rc *RaftClient) AppendEntries(ctx context.Context, peerAddr string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	client, err := rc.getClient(peerAddr)
	if err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	
	return client.AppendEntries(ctx, req)
}

// Close closes all gRPC connections
func (rc *RaftClient) Close() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	
	var lastErr error
	for addr, conn := range rc.conns {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %s: %v", addr, err)
			lastErr = err
		}
	}
	
	rc.clients = make(map[string]proto.RaftServiceClient)
	rc.conns = make(map[string]*grpc.ClientConn)
	
	return lastErr
}

// startGRPCServer starts the gRPC server for this Raft node
func (rn *RaftNode) startGRPCServer(port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", port, err)
	}
	
	grpcServer := grpc.NewServer()
	raftServer := NewRaftServer(rn)
	proto.RegisterRaftServiceServer(grpcServer, raftServer)
	
	rn.grpcServer = grpcServer
	rn.grpcListener = lis
	
	log.Printf("Starting Raft gRPC server for node %s on port %d", rn.nodeID, port)
	
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error for node %s: %v", rn.nodeID, err)
		}
	}()
	
	return nil
}

// stopGRPCServer stops the gRPC server
func (rn *RaftNode) stopGRPCServer() {
	if rn.grpcServer != nil {
		rn.grpcServer.GracefulStop()
		rn.grpcServer = nil
	}
	if rn.grpcListener != nil {
		rn.grpcListener.Close()
		rn.grpcListener = nil
	}
}