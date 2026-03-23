package raft

import (
	"context"
	"log"
	"math/rand"
	"time"
	
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/raft/proto"
)

// run is the main Raft node loop
func (rn *RaftNode) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-rn.stopCh:
			return
		default:
			rn.mu.RLock()
			state := rn.state
			rn.mu.RUnlock()
			
			switch state {
			case Follower:
				rn.runFollower(ctx)
			case Candidate:
				rn.runCandidate(ctx)
			case Leader:
				rn.runLeader(ctx)
			}
		}
	}
}

// runFollower handles follower state logic
func (rn *RaftNode) runFollower(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-rn.stopCh:
		return
	case <-rn.electionTimer.C:
		// Election timeout - become candidate
		rn.becomeCandidate()
	}
}

// runCandidate handles candidate state logic
func (rn *RaftNode) runCandidate(ctx context.Context) {
	// Start election
	rn.startElection()
	
	select {
	case <-ctx.Done():
		return
	case <-rn.stopCh:
		return
	case <-rn.electionTimer.C:
		// Election timeout - start new election
		rn.startElection()
	}
}

// runLeader handles leader state logic
func (rn *RaftNode) runLeader(ctx context.Context) {
	// Send heartbeats to all followers
	rn.sendHeartbeats()
	
	select {
	case <-ctx.Done():
		return
	case <-rn.stopCh:
		return
	case <-rn.heartbeatTicker.C:
		// Send regular heartbeats
		rn.sendHeartbeats()
	}
}

// becomeCandidate transitions to candidate state
func (rn *RaftNode) becomeCandidate() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	oldState := rn.state
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.nodeID
	rn.currentLeader = ""
	
	log.Printf("Node %s became candidate for term %d", rn.nodeID, rn.currentTerm)
	
	rn.resetElectionTimer()
	
	if rn.onStateChange != nil {
		go rn.onStateChange(oldState, Candidate)
	}
}

// becomeLeader transitions to leader state
func (rn *RaftNode) becomeLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	oldState := rn.state
	rn.state = Leader
	rn.currentLeader = rn.nodeID
	
	// Initialize leader state
	lastLogIndex := int64(len(rn.log))
	for peerAddr := range rn.nextIndex {
		rn.nextIndex[peerAddr] = lastLogIndex + 1
		rn.matchIndex[peerAddr] = 0
	}
	
	// Stop election timer and start heartbeat ticker
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	rn.heartbeatTicker = time.NewTicker(rn.heartbeatInterval)
	
	log.Printf("Node %s became leader for term %d", rn.nodeID, rn.currentTerm)
	
	if rn.onStateChange != nil {
		go rn.onStateChange(oldState, Leader)
	}
}

// becomeFollower transitions to follower state
func (rn *RaftNode) becomeFollower(term int64, leader string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	
	oldState := rn.state
	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = ""
	rn.currentLeader = leader
	
	// Stop heartbeat ticker if we were leader
	if rn.heartbeatTicker != nil {
		rn.heartbeatTicker.Stop()
		rn.heartbeatTicker = nil
	}
	
	rn.resetElectionTimer()
	
	log.Printf("Node %s became follower for term %d, leader: %s", rn.nodeID, rn.currentTerm, leader)
	
	if rn.onStateChange != nil {
		go rn.onStateChange(oldState, Follower)
	}
}

// startElection initiates a new election
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	term := rn.currentTerm
	lastLogIndex := int64(len(rn.log))
	var lastLogTerm int64
	if lastLogIndex > 0 {
		lastLogTerm = rn.log[lastLogIndex-1].Term
	}
	rn.mu.Unlock()
	
	log.Printf("Node %s starting election for term %d", rn.nodeID, term)
	
	// Vote for ourselves
	votes := 1
	totalNodes := len(rn.peerAddrs)
	
	// Request votes from all peers
	voteCh := make(chan bool, totalNodes-1)
	
	for _, peerAddr := range rn.peerAddrs {
		if !isSameNode(peerAddr, rn.nodeID) {
			go rn.requestVote(peerAddr, term, lastLogIndex, lastLogTerm, voteCh)
		}
	}
	
	// Count votes
	majority := (totalNodes / 2) + 1
	
	// Set election timeout
	timeout := time.After(rn.getRandomElectionTimeout())
	
	for votes < majority {
		select {
		case vote := <-voteCh:
			if vote {
				votes++
				log.Printf("Node %s received vote, total votes: %d/%d", rn.nodeID, votes, majority)
			}
		case <-timeout:
			log.Printf("Node %s election timeout, received %d/%d votes", rn.nodeID, votes, majority)
			return
		case <-rn.stopCh:
			return
		}
	}
	
	// Won election
	if votes >= majority {
		log.Printf("Node %s won election with %d/%d votes", rn.nodeID, votes, majority)
		rn.becomeLeader()
	}
}

// sendHeartbeats sends heartbeat messages to all followers
func (rn *RaftNode) sendHeartbeats() {
	rn.mu.RLock()
	term := rn.currentTerm
	rn.mu.RUnlock()
	
	for _, peerAddr := range rn.peerAddrs {
		if !isSameNode(peerAddr, rn.nodeID) {
			go rn.sendAppendEntries(peerAddr, term, nil) // Empty append entries = heartbeat
		}
	}
}

// resetElectionTimer resets the election timeout with random jitter
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	
	timeout := rn.getRandomElectionTimeout()
	rn.electionTimer = time.NewTimer(timeout)
}

// getRandomElectionTimeout returns a random election timeout between min and max
func (rn *RaftNode) getRandomElectionTimeout() time.Duration {
	min := rn.electionTimeoutMin.Nanoseconds()
	max := rn.electionTimeoutMax.Nanoseconds()
	duration := min + rand.Int63n(max-min)
	return time.Duration(duration)
}

// requestVote sends a vote request to a peer
func (rn *RaftNode) requestVote(peerAddr string, term, lastLogIndex, lastLogTerm int64, voteCh chan bool) {
	// Create vote request
	req := &proto.VoteRequest{
		Term:         term,
		CandidateId:  rn.nodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	
	// Try gRPC first, fall back to registry for demo compatibility
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	response, err := rn.grpcClient.RequestVote(ctx, peerAddr, req)
	if err != nil {
		// Fall back to registry-based communication for demo
		log.Printf("gRPC vote request to %s failed, falling back to registry: %v", peerAddr, err)
		response = rn.requestVoteFromNode(peerAddr, req)
	}
	
	// Check if our term is still current
	rn.mu.RLock()
	currentTerm := rn.currentTerm
	rn.mu.RUnlock()
	
	if response.Term > currentTerm {
		// Newer term discovered, become follower
		rn.becomeFollower(response.Term, "")
		select {
		case voteCh <- false:
		case <-rn.stopCh:
		}
		return
	}
	
	select {
	case voteCh <- response.VoteGranted:
	case <-rn.stopCh:
	}
}

// sendAppendEntries sends an append entries RPC to a peer
func (rn *RaftNode) sendAppendEntries(peerAddr string, term int64, entries []*LogEntry) {
	// Convert internal log entries to proto format
	var protoEntries []*proto.LogEntry
	for _, entry := range entries {
		protoEntries = append(protoEntries, &proto.LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Type:    entry.Type,
			Data:    entry.Data,
			Created: entry.Created.UnixNano(),
		})
	}
	
	// Create append entries request
	req := &proto.AppendEntriesRequest{
		Term:         term,
		LeaderId:     rn.nodeID,
		PrevLogIndex: 0, // Simplified for demo
		PrevLogTerm:  0,
		Entries:      protoEntries,
		LeaderCommit: rn.commitIndex,
	}
	
	// Try gRPC first, fall back to registry for demo compatibility
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	response, err := rn.grpcClient.AppendEntries(ctx, peerAddr, req)
	if err != nil {
		// Fall back to registry-based communication for demo
		log.Printf("gRPC append entries to %s failed, falling back to registry: %v", peerAddr, err)
		response = rn.sendAppendEntriesToNode(peerAddr, req)
	}
	
	// Check if our term is still current
	rn.mu.RLock()
	currentTerm := rn.currentTerm
	isLeader := rn.state == Leader
	rn.mu.RUnlock()
	
	if response.Term > currentTerm {
		// Newer term discovered, become follower
		rn.becomeFollower(response.Term, "")
		return
	}
	
	if isLeader && len(entries) == 0 {
		log.Printf("Leader %s sent heartbeat to %s for term %d", rn.nodeID, peerAddr, term)
	}
}