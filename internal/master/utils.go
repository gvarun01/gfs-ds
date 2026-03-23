package master

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"path"
	"sort"
	"strings"
	"time"

	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
)

func (m *Master) addPendingOperation(serverId string, op *PendingOperation) {
	m.pendingOpsMu.Lock()
	defer m.pendingOpsMu.Unlock()

	if _, exists := m.pendingOps[serverId]; !exists {
		m.pendingOps[serverId] = make([]*PendingOperation, 0)
	}
	m.pendingOps[serverId] = append(m.pendingOps[serverId], op)
}

func (m *Master) assignNewPrimary(chunkHandle string) error {
	m.chunksMu.Lock()
	defer m.chunksMu.Unlock()

	chunkInfo, exists := m.chunks[chunkHandle]
	if !exists {
		return fmt.Errorf("chunk-handle does not exist: %s", chunkHandle)
	}

	if chunkInfo == nil {
		return fmt.Errorf("chunk not found: %s", chunkHandle)
	}

	chunkInfo.mu.Lock()
	defer chunkInfo.mu.Unlock()

	// Get list of available servers that have this chunk
	availableServers := make([]string, 0)
	m.serversMu.RLock()
	for serverId := range chunkInfo.Locations {
		if server, exists := m.servers[serverId]; exists {
			server.mu.RLock()
			if server.Status == "ACTIVE" && server.ActiveOps < 100 {
				availableServers = append(availableServers, serverId)
			}
			server.mu.RUnlock()
		}
	}
	m.serversMu.RUnlock()

	if len(availableServers) == 0 {
		return fmt.Errorf("no available servers for chunk: %s", chunkHandle)
	}

	newPrimary := availableServers[rand.Intn(len(availableServers))]
	chunkInfo.Primary = newPrimary
	chunkInfo.LeaseExpiration = time.Now().Add(time.Duration(m.Config.Lease.LeaseTimeout) * time.Second)

	command := &chunk_pb.ChunkCommand{
		Type:        chunk_pb.ChunkCommand_BECOME_PRIMARY,
		ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
	}

	if err := m.chunkServerMgr.SendCommandToServer(newPrimary, command); err != nil {
		return fmt.Errorf("failed to send become primary command: %v", err)
	}

	chunkInfo.Primary = newPrimary
	chunkInfo.LeaseExpiration = time.Now().Add(time.Duration(m.Config.Lease.LeaseTimeout) * time.Second)

	return nil
}

func (m *Master) cleanupExpiredOperations() {
	m.pendingOpsMu.Lock()
	defer m.pendingOpsMu.Unlock()

	for serverId, ops := range m.pendingOps {
		var validOps []*PendingOperation
		for _, op := range ops {
			if time.Since(op.CreatedAt) > time.Hour || op.AttemptCount >= 5 {
				log.Printf("Operation failed permanently: type=%v, chunk=%s, server=%s, attempts=%d",
					op.Type, op.ChunkHandle, serverId, op.AttemptCount)
			} else {
				validOps = append(validOps, op)
			}
		}
		if len(validOps) == 0 {
			delete(m.pendingOps, serverId)
		} else {
			m.pendingOps[serverId] = validOps
		}
	}
}

func (s *MasterServer) updateServerStatus(serverId string, req *chunk_pb.HeartBeatRequest) error {
	s.Master.serversMu.Lock()
	defer s.Master.serversMu.Unlock()

	serverInfo, exists := s.Master.servers[serverId]
	if !exists {
		return fmt.Errorf("server %s not found", serverId)
	}

	serverInfo.mu.Lock()
	defer serverInfo.mu.Unlock()

	serverInfo.LastHeartbeat = time.Now()
	serverInfo.CPUUsage = req.CpuUsage
	serverInfo.ActiveOps = req.ActiveOperations
	serverInfo.LastUpdated = time.Now()
	serverInfo.Status = "ACTIVE"
	serverInfo.FailureCount = 0

	// Update chunk information
	s.Master.chunksMu.Lock()
	defer s.Master.chunksMu.Unlock()

	for _, chunkStatus := range req.Chunks {
		chunkHandle := chunkStatus.ChunkHandle.Handle
		s.Master.deletedChunksMu.Lock()
		if _, exists := s.Master.deletedChunks[chunkHandle]; exists {
			s.Master.deletedChunksMu.Unlock()
			continue
		}
		s.Master.deletedChunksMu.Unlock()
		
		// Check if chunk exists in master's registry
		s.Master.chunksMu.RLock()
		chunkInfo, exists := s.Master.chunks[chunkHandle]
		s.Master.chunksMu.RUnlock()
		
		if !exists {
			// This is an orphaned chunk - mark it for tracking and eventual deletion
			// Per GFS paper Section 4.4: chunks not known to master are garbage
			s.Master.markChunkAsOrphan(chunkHandle, serverId, chunkStatus.Size, chunkStatus.Version)
			continue // Don't adopt unknown chunks
		}

		// Update known chunk information
		chunkInfo.mu.Lock()
		chunkInfo.Size = chunkStatus.Size
		if chunkInfo.Locations == nil {
			chunkInfo.Locations = make(map[string]bool)
		}
		if chunkInfo.ServerAddresses == nil {
			chunkInfo.ServerAddresses = make(map[string]string)
		}
		chunkInfo.Locations[serverId] = true
		chunkInfo.ServerAddresses[serverId] = s.Master.servers[serverId].Address
		chunkInfo.mu.Unlock()

		serverInfo.Chunks[chunkHandle] = true
	}

	return nil
}

func (m *Master) handleServerFailure(serverId string) {
	if serverId == "" {
		return
	}

	m.serversMu.Lock()
	defer m.serversMu.Unlock()

	serverInfo, exists := m.servers[serverId]
	if !exists {
		return
	}

	serverInfo.mu.RLock()
	defer serverInfo.mu.RUnlock()

	m.chunksMu.Lock()
	defer m.chunksMu.Unlock()

	// Update chunk locations and trigger re-replication if needed
	for chunkHandle := range serverInfo.Chunks {
		if chunkInfo, exists := m.chunks[chunkHandle]; exists {
			chunkInfo.mu.Lock()
			delete(chunkInfo.Locations, serverId)
			if chunkInfo.Primary == serverId {
				chunkInfo.Primary = ""
				chunkInfo.LeaseExpiration = time.Time{}
			}

			// Trigger re-replication if needed
			if len(chunkInfo.Locations) < m.Config.Replication.Factor {
				go m.initiateReplication(chunkHandle)
			}
			chunkInfo.mu.Unlock()
		}
	}

	// Remove server from active servers
	log.Print("Failure: ", serverId)
	delete(m.servers, serverId)
}

func (m *Master) initiateReplication(chunkHandle string) {
	m.chunksMu.RLock()
	chunkInfo, exists := m.chunks[chunkHandle]
	if !exists {
		m.chunksMu.RUnlock()
		return
	}

	m.chunksMu.RUnlock()

	chunkInfo.mu.RLock()
	currentReplicas := len(chunkInfo.Locations)
	neededReplicas := m.Config.Replication.Factor - currentReplicas
	chunkInfo.mu.RUnlock()

	if neededReplicas <= 0 {
		return
	}

	targets := m.selectReplicationTargets(chunkHandle, neededReplicas)
	if len(targets) == 0 {
		log.Printf("No suitable targets found for replicating chunk %s", chunkHandle)
		return
	}

	sourceServer := m.selectReplicationSource(chunkHandle)
	if sourceServer == "" {
		log.Printf("No source server available for replicating chunk %s", chunkHandle)
		return
	}

	targetIds := make([]string, len(targets))
	for i, target := range targets {
		targetIds[i] = target.ServerId
	}

	// Add to pending operations
	op := &PendingOperation{
		Type:        chunk_pb.ChunkCommand_REPLICATE,
		ChunkHandle: chunkHandle,
		Targets:     targetIds,
		Source:      sourceServer,
		CreatedAt:   time.Now(),
	}
	m.addPendingOperation(sourceServer, op)
}

func (m *Master) runPendingOpsCleanup() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		m.cleanupExpiredOperations()
	}
}

func (m *Master) selectReplicationTargets(chunkHandle string, count int) []*common_pb.ChunkLocation {
	var targets []*common_pb.ChunkLocation

	m.serversMu.RLock()
	defer m.serversMu.RUnlock()

	chunkInfo := m.chunks[chunkHandle]
	if chunkInfo == nil {
		return targets
	}

	chunkInfo.mu.RLock()

	existingLocations := make(map[string]bool)
	for loc := range chunkInfo.Locations {
		existingLocations[loc] = true
	}
	chunkInfo.mu.RUnlock()

	// Score and sort servers based on multiple criteria
	type serverScore struct {
		id    string
		score float64
	}
	var scoredServers []serverScore

	for serverId, serverInfo := range m.servers {
		// Skip if server already has the chunk
		if existingLocations[serverId] {
			continue
		}

		serverInfo.mu.RLock()

		// Calculate score based on multiple factors
		score := 100.0
		// Prefer servers with lower CPU usage
		score -= serverInfo.CPUUsage
		// Prefer servers with fewer active operations
		score -= float64(serverInfo.ActiveOps) / 100
		serverInfo.mu.RUnlock()

		scoredServers = append(scoredServers, serverScore{serverId, score})
	}

	// Sort servers by score
	sort.Slice(scoredServers, func(i, j int) bool {
		return scoredServers[i].score > scoredServers[j].score
	})

	// Select top N servers
	for i := 0; i < len(scoredServers) && len(targets) < count; i++ {
		targets = append(targets, &common_pb.ChunkLocation{
			ServerId: scoredServers[i].id,
		})
	}

	return targets
}

func (s *MasterServer) selectInitialChunkServers() []string {
	var desiredReplicas = s.Master.Config.Replication.Factor

	s.Master.serversMu.RLock()
	defer s.Master.serversMu.RUnlock()

	type serverLoad struct {
		id        string
		activeOps int32
	}

	// Collect all active servers
	var activeServers []serverLoad
	for serverId, server := range s.Master.servers {
		server.mu.RLock()
		if server.Status == "ACTIVE" {
			activeServers = append(activeServers, serverLoad{
				id:        serverId,
				activeOps: server.ActiveOps,
			})
		}
		server.mu.RUnlock()
	}

	// Sort servers by load (activeOps)
	sort.Slice(activeServers, func(i, j int) bool {
		return activeServers[i].activeOps < activeServers[j].activeOps
	})

	// Select the least loaded servers up to desiredReplicas
	selectedServers := make([]string, 0, desiredReplicas)
	for i := 0; i < len(activeServers) && i < desiredReplicas; i++ {
		selectedServers = append(selectedServers, activeServers[i].id)
	}

	return selectedServers
}

func (cm *ChunkServerManager) SendCommandToServer(serverId string, command *chunk_pb.ChunkCommand) error {
	cm.mu.RLock()
	stream, exists := cm.activeStreams[serverId]
	cm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no active stream for server %s", serverId)
	}

	response := &chunk_pb.HeartBeatResponse{
		Status:   &common_pb.Status{Code: common_pb.Status_OK},
		Commands: []*chunk_pb.ChunkCommand{command},
	}

	select {
	case stream <- response:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout sending command to server %s", serverId)
	}
}

func (m *Master) selectReplicationSource(chunkHandle string) string {
	m.chunksMu.RLock()
	chunkInfo, exists := m.chunks[chunkHandle]
	if !exists {
		m.chunksMu.RUnlock()
		return ""
	}
	m.chunksMu.RUnlock()

	chunkInfo.mu.RLock()
	defer chunkInfo.mu.RUnlock()

	// Prefer primary if it exists and is alive
	if chunkInfo.Primary != "" {
		m.serversMu.RLock()
		if server, exists := m.servers[chunkInfo.Primary]; exists {
			server.mu.RLock()
			if server.Status == "ACTIVE" && server.ActiveOps < 100 { // Configurable threshold
				server.mu.RUnlock()
				m.serversMu.RUnlock()
				return chunkInfo.Primary
			}
			server.mu.RUnlock()
		}
		m.serversMu.RUnlock()
	}

	// Otherwise, select the least loaded replica
	var bestServer string
	var minLoad int32 = math.MaxInt32

	m.serversMu.RLock()
	defer m.serversMu.RUnlock()

	for serverId := range chunkInfo.Locations {
		if server, exists := m.servers[serverId]; exists {
			server.mu.RLock()
			if server.Status == "ACTIVE" && server.ActiveOps < minLoad {
				minLoad = server.ActiveOps
				bestServer = serverId
			}
			server.mu.RUnlock()
		}
	}

	return bestServer
}

func (s *MasterServer) generateChunkCommands(serverId string) []*chunk_pb.ChunkCommand {
	var commands []*chunk_pb.ChunkCommand

	s.Master.pendingOpsMu.Lock()
	pendingOps, exists := s.Master.pendingOps[serverId]
	if exists {
		var remainingOps []*PendingOperation
		for _, op := range pendingOps {
			if op.AttemptCount > 0 {
				continue
			}

			command := &chunk_pb.ChunkCommand{
				Type:        op.Type,
				ChunkHandle: &common_pb.ChunkHandle{Handle: op.ChunkHandle},
			}

			if op.Type == chunk_pb.ChunkCommand_REPLICATE {
				command.TargetLocations = make([]*common_pb.ChunkLocation, len(op.Targets))
				for i, target := range op.Targets {
					command.TargetLocations[i] = &common_pb.ChunkLocation{
						ServerId:      target,
						ServerAddress: s.Master.servers[target].Address,
// processExpeditedDeletions processes files marked for expedited deletion
// This method is called from the garbage collection cycle
func (m *Master) processExpeditedDeletions() {
	m.expeditedDeletionsMu.Lock()
	filesToDelete := make([]*ExpeditedDeletion, 0)
	
	for _, deletion := range m.expeditedDeletions {
		deletion.mu.RLock()
		if !deletion.IsProcessed {
			filesToDelete = append(filesToDelete, deletion)
		}
		deletion.mu.RUnlock()
	}
	m.expeditedDeletionsMu.Unlock()
	
	// Process deletions outside of lock to avoid deadlock
	for _, deletion := range filesToDelete {
		if err := m.performExpeditedDeletion(deletion); err != nil {
			log.Printf("Failed to perform expedited deletion for %s: %v", deletion.TrashPath, err)
		} else {
			// Mark as processed
			deletion.mu.Lock()
			deletion.IsProcessed = true
			deletion.ProcessedAt = time.Now()
			deletion.mu.Unlock()
			
			log.Printf("Successfully performed expedited deletion of %s", deletion.TrashPath)
		}
	}
	
	// Clean up processed expedited deletions
	m.cleanupProcessedExpeditedDeletions()
}

// performExpeditedDeletion immediately deletes a file and its chunks
func (m *Master) performExpeditedDeletion(deletion *ExpeditedDeletion) error {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	
	trashPath := deletion.TrashPath
	
	// Check if file still exists in trash
	if !m.namespace.FileExists(trashPath) {
		return fmt.Errorf("trash file %s no longer exists", trashPath)
	}
	
	// Get file info to identify chunks for deletion
	fileInfo := m.namespace.GetFileInfo(trashPath)
	if fileInfo != nil {
		// Delete all chunks associated with the file
		for _, chunkHandle := range fileInfo.Chunks {
			if err := m.deleteChunkAndReplicas(chunkHandle); err != nil {
				log.Printf("Warning: Failed to delete chunk %s during expedited deletion: %v", 
					chunkHandle, err)
			}
		}
	}
	
	// Remove file from namespace
	if err := m.namespace.DeleteFile(trashPath); err != nil {
		return fmt.Errorf("failed to delete trash file %s: %v", trashPath, err)
	}
	
	return nil
}

// deleteChunkAndReplicas deletes a chunk and all its replicas from chunkservers
func (m *Master) deleteChunkAndReplicas(chunkHandle string) error {
	m.chunksMu.Lock()
	chunkInfo, exists := m.chunks[chunkHandle]
	if !exists {
		m.chunksMu.Unlock()
		return nil // Chunk already deleted
	}
	
	// Get list of servers that have this chunk
	var servers []string
	chunkInfo.mu.RLock()
	for serverID := range chunkInfo.Locations {
		servers = append(servers, serverID)
	}
	chunkInfo.mu.RUnlock()
	
	// Remove chunk from master's registry
	delete(m.chunks, chunkHandle)
	m.chunksMu.Unlock()
	
	// Mark chunk for deletion
	m.deletedChunksMu.Lock()
	m.deletedChunks[chunkHandle] = true
	m.deletedChunksMu.Unlock()
	
	// Send delete commands to all servers
	for _, serverID := range servers {
		if err := m.sendDeleteChunkCommand(serverID, chunkHandle); err != nil {
			log.Printf("Failed to send delete command for chunk %s to server %s: %v", 
				chunkHandle, serverID, err)
		}
	}
	
	return nil
}

// cleanupProcessedExpeditedDeletions removes processed expedited deletion records
func (m *Master) cleanupProcessedExpeditedDeletions() {
	m.expeditedDeletionsMu.Lock()
	defer m.expeditedDeletionsMu.Unlock()
	
	for trashPath, deletion := range m.expeditedDeletions {
		deletion.mu.RLock()
		if deletion.IsProcessed && time.Since(deletion.ProcessedAt) > time.Hour {
			delete(m.expeditedDeletions, trashPath)
		}
		deletion.mu.RUnlock()
	}
}
				}
			}

			commands = append(commands, command)
			op.AttemptCount++
			op.LastAttempt = time.Now()
			remainingOps = append(remainingOps, op)
		}

		if len(remainingOps) == 0 {
			delete(s.Master.pendingOps, serverId)
		} else {
			s.Master.pendingOps[serverId] = remainingOps
		}
	}
	s.Master.pendingOpsMu.Unlock()

	return commands
}

func (m *Master) runGarbageCollection() {
	ticker := time.NewTicker(time.Duration(m.Config.Deletion.GCInterval) * time.Second)
	for range ticker.C {
		m.gcMu.Lock()
		if m.gcInProgress {
			m.gcMu.Unlock()
			continue
		}
		m.gcInProgress = true
		m.gcMu.Unlock()

		log.Printf("Starting garbage collection cycle")

		// First, process expedited deletions (double-deleted files)
		// These should be processed immediately, bypassing grace period
		log.Printf("Processing expedited deletions")
		m.processExpeditedDeletions()

		filesToProcess := m.getExpiredDeletedFiles()

		// Process files in batches
		for i := 0; i < len(filesToProcess); i += m.Config.Deletion.GCDeleteBatchSize {
			end := i + m.Config.Deletion.GCDeleteBatchSize
			if end > len(filesToProcess) {
				end = len(filesToProcess)
			}

			batch := filesToProcess[i:end]
			m.processGCBatch(batch)
		}
		
		// Process orphaned chunks after file cleanup
		// This implements GFS Section 4.4 orphaned chunk detection and cleanup
		log.Printf("Processing orphaned chunks")
		m.processOrphanedChunks()

		m.gcMu.Lock()
		m.gcInProgress = false
		m.gcMu.Unlock()

		log.Printf("Completed garbage collection cycle")
	}
}

func (m *Master) getExpiredDeletedFiles() []string {
	var expiredFiles []string
	cutoffTime, err := time.Parse("2006-01-02T15:04:05", time.Now().Add(-time.Duration(m.Config.Deletion.RetentionPeriod)*time.Second).Format("2006-01-02T15:04:05"))
	if err != nil {
		log.Printf("Error parsing cutoff time: %v", err)
		return expiredFiles
	}

	m.filesMu.RLock()
	defer m.filesMu.RUnlock()

	// Get all files from B-tree namespace
	allFiles := m.namespace.GetAllFiles()
	for deletedPath := range allFiles {
		if strings.HasPrefix(deletedPath, m.Config.Deletion.TrashDirPrefix) {
			parts := strings.Split(strings.TrimPrefix(deletedPath, m.Config.Deletion.TrashDirPrefix), "_")
			if len(parts) != 2 {
				log.Printf("Error parsing delete time from path %s: unexpected format", deletedPath)
				continue
			}
			deleteTime, err := time.Parse("2006-01-02T15:04:05", parts[1])
			if err != nil {
				log.Printf("Error parsing delete time from path %s: %v", deletedPath, err)
				continue
			}

			if deleteTime.Before(cutoffTime) {
				expiredFiles = append(expiredFiles, deletedPath)
			}
		}
	}

	return expiredFiles
}

func (m *Master) processGCBatch(deletedPaths []string) {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()

	for _, deletedPath := range deletedPaths {
		fileInfo, err := m.namespace.GetFile(deletedPath)
		if err != nil {
			continue
		}

		chunksToDelete := make([]string, 0)
		for _, chunkHandle := range fileInfo.Chunks {
			chunksToDelete = append(chunksToDelete, chunkHandle)
		}

		m.chunksMu.Lock()
		defer m.chunksMu.Unlock()
		for _, chunkHandle := range chunksToDelete {
			if chunkInfo, exists := m.chunks[chunkHandle]; exists {
				chunkInfo.mu.Lock()
				for serverId := range chunkInfo.Locations {
					m.sendDeleteChunkCommand(serverId, chunkHandle)
				}
				chunkInfo.mu.Unlock()
				m.deletedChunksMu.Lock()
				m.deletedChunks[chunkHandle] = true
				m.deletedChunksMu.Unlock()
				delete(m.chunks, chunkHandle)
			}
		}

		// Delete file from B-tree namespace
		err = m.namespace.DeleteFile(deletedPath)
		if err != nil {
			log.Printf("Warning: Could not delete file %s from namespace during GC: %v", deletedPath, err)
		}

		log.Printf("GC: Permanently deleted file %s and its %d chunks", deletedPath, len(chunksToDelete))
	}
}

func (m *Master) sendDeleteChunkCommand(serverId, chunkHandle string) {
	op := &PendingOperation{
		Type:        chunk_pb.ChunkCommand_DELETE,
		ChunkHandle: chunkHandle,
		CreatedAt:   time.Now(),
	}
	m.addPendingOperation(serverId, op)
}

func (m *Master) incrementChunkVersion(chunkHandle string, c *ChunkInfo) {
	c.Version++

	metadata := struct {
		Version int32 `json:"version"`
	}{
		Version: c.Version,
	}
	if err := m.opLog.LogOperation(OpUpdateChunkVersion, "", chunkHandle, metadata); err != nil {
		log.Printf("Failed to log chunk version update: %v", err)
	}
}

// Helper function to schedule updates for stale replicas
func (s *MasterServer) scheduleStaleReplicaDelete(chunkHandle string, staleServerId string) {
	chunkInfo := s.Master.chunks[chunkHandle]
	if chunkInfo == nil {
		return
	}
	if _, isStale := chunkInfo.StaleReplicas[staleServerId]; !isStale {
		log.Printf("Server %s already has handle %s up-to-date, skipping replication",
			staleServerId, chunkHandle)
		return
	}

	// First check if we already have enough up-to-date replicas
	upToDateCount := 0
	for serverId := range chunkInfo.Locations {
		if _, isStale := chunkInfo.StaleReplicas[serverId]; !isStale {
			upToDateCount++
		}
	}

	log.Printf("Deleting stale chunk %s on server %d", chunkHandle, upToDateCount)

	s.Master.chunkServerMgr.mu.RLock()
	responseChan, exists := s.Master.chunkServerMgr.activeStreams[staleServerId]
	s.Master.chunkServerMgr.mu.RUnlock()

	if !exists {
		log.Print("No active stream for server")
		return
	}

	response := &chunk_pb.HeartBeatResponse{
		Status: &common_pb.Status{Code: common_pb.Status_OK},
		Commands: []*chunk_pb.ChunkCommand{{
			Type:        chunk_pb.ChunkCommand_DELETE,
			ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
		}},
	}

	select {
	case responseChan <- response:
		// Command sent successfully
	case <-time.After(5 * time.Second):
		log.Print("timeout sending INIT_EMPTY to server")
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
	Validation Utils Start:-
*/

func (s *MasterServer) validateFilename(filename string) error {
	if filename == "" {
		return ErrInvalidFileName
	}

	if len(filename) > s.Master.Config.Metadata.MaxFilenameLength {
		return ErrInvalidFileName
	}

	if strings.Count(filename, "/") > s.Master.Config.Metadata.MaxDirectoryDepth {
		return ErrInvalidFileName
	}

	if !s.Master.validatePath(filename) {
		return ErrInvalidFileName
	}

	return nil
}

func (m *Master) validatePath(filepath string) bool {
	if strings.HasPrefix(filepath, m.Config.Deletion.TrashDirPrefix) {
		// Split on underscore to separate path from timestamp
		parts := strings.SplitN(strings.TrimPrefix(filepath, m.Config.Deletion.TrashDirPrefix), "_", 2)
		if len(parts) == 2 {
			filepath = parts[0]
		}
	}

	cleaned := path.Clean(filepath)

	if path.IsAbs(cleaned) || strings.Contains(cleaned, "..") || strings.Contains(cleaned, "./") {
		return false
	}

	for _, char := range filepath {
		if (char < 32 || char > 126) && char != '/' {
			return false
		}
	}

	return true
}

// markChunkAsOrphan tracks a chunk reported by chunkserver but unknown to master
// This implements proper GFS garbage collection as per Section 4.4 of the GFS paper
func (m *Master) markChunkAsOrphan(chunkHandle, serverID string, chunkSize int64, version int32) {
	m.orphanedChunksMu.Lock()
	defer m.orphanedChunksMu.Unlock()

	now := time.Now()
	
	if orphan, exists := m.orphanedChunks[chunkHandle]; exists {
		// Update existing orphan tracking
		orphan.mu.Lock()
		orphan.LastSeen = now
		orphan.ConfirmCount++
		orphan.Size = chunkSize
		orphan.Version = version
		orphan.mu.Unlock()
		
		log.Printf("Orphaned chunk %s confirmed again by server %s (count: %d)", 
			chunkHandle, serverID, orphan.ConfirmCount)
	} else {
		// First time seeing this orphan
		m.orphanedChunks[chunkHandle] = &OrphanedChunk{
			ChunkHandle:   chunkHandle,
			ServerID:      serverID,
			DetectedAt:    now,
			LastSeen:      now,
			Size:         chunkSize,
			ConfirmCount:  1,
			Version:      version,
		}
		
		log.Printf("New orphaned chunk detected: %s on server %s (size: %d bytes)", 
			chunkHandle, serverID, chunkSize)
	}
}

// processOrphanedChunks cleans up orphaned chunks that have been confirmed multiple times
// This prevents storage waste and implements GFS garbage collection properly
func (m *Master) processOrphanedChunks() {
	m.orphanedChunksMu.Lock()
	orphansToDelete := make([]*OrphanedChunk, 0)
	
	// Identify orphans ready for deletion (confirmed multiple times)
	confirmationThreshold := int32(3) // Require 3 confirmations before deletion
	
	for handle, orphan := range m.orphanedChunks {
		orphan.mu.RLock()
		if orphan.ConfirmCount >= confirmationThreshold {
			orphansToDelete = append(orphansToDelete, orphan)
		}
		orphan.mu.RUnlock()
	}
	m.orphanedChunksMu.Unlock()

	// Process deletions outside of lock to avoid deadlock
	for _, orphan := range orphansToDelete {
		if err := m.deleteOrphanedChunk(orphan); err != nil {
			log.Printf("Failed to delete orphaned chunk %s: %v", orphan.ChunkHandle, err)
		} else {
			// Remove from tracking after successful deletion command
			m.orphanedChunksMu.Lock()
			delete(m.orphanedChunks, orphan.ChunkHandle)
			m.orphanedChunksMu.Unlock()
			
			log.Printf("Successfully marked orphaned chunk %s for deletion", orphan.ChunkHandle)
		}
	}
	
	// Clean up old orphan entries that haven't been seen recently
	m.cleanupStaleOrphanEntries()
}

// deleteOrphanedChunk sends delete command for an orphaned chunk
func (m *Master) deleteOrphanedChunk(orphan *OrphanedChunk) error {
	orphan.mu.RLock()
	serverID := orphan.ServerID
	chunkHandle := orphan.ChunkHandle
	orphan.mu.RUnlock()
	
	// Mark chunk as deleted to prevent re-adoption
	m.deletedChunksMu.Lock()
	m.deletedChunks[chunkHandle] = true
	m.deletedChunksMu.Unlock()
	
	// Send delete command to the server
	return m.sendDeleteChunkCommand(serverID, chunkHandle)
}

// cleanupStaleOrphanEntries removes orphan tracking entries that are too old
func (m *Master) cleanupStaleOrphanEntries() {
	m.orphanedChunksMu.Lock()
	defer m.orphanedChunksMu.Unlock()
	
	staleThreshold := time.Hour * 24 // Remove orphan entries older than 24 hours
	now := time.Now()
	
	for handle, orphan := range m.orphanedChunks {
		orphan.mu.RLock()
		isStale := now.Sub(orphan.LastSeen) > staleThreshold
		orphan.mu.RUnlock()
		
		if isStale {
			delete(m.orphanedChunks, handle)
			log.Printf("Removed stale orphan entry: %s", handle)
		}
	}
}
