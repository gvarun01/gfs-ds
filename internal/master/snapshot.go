package master

import (
	"fmt"
	"log"
	"strings"
	"time"

	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
)

// Snapshot implementation for GFS master
// Provides copy-on-write snapshots as specified in GFS Section 3.4

var (
	ErrSnapshotExists = fmt.Errorf("snapshot already exists")
	ErrSnapshotNotFound = fmt.Errorf("snapshot not found")
)

// generateNewChunkHandle creates a new unique chunk handle for CoW operations
func (m *Master) generateNewChunkHandle() string {
	m.chunkHandleMu.Lock()
	defer m.chunkHandleMu.Unlock()
	
	handle := fmt.Sprintf("chunk_%d", m.nextChunkHandle)
	m.nextChunkHandle++
	return handle
}

// CreateSnapshot creates a copy-on-write snapshot of a file or directory
func (m *Master) CreateSnapshot(sourcePath, snapshotPath string) error {
	m.filesMu.Lock()
	m.snapshotsMu.Lock()
	defer m.filesMu.Unlock()
	defer m.snapshotsMu.Unlock()

	// Check if snapshot already exists
	if _, exists := m.snapshots[snapshotPath]; exists {
		return ErrSnapshotExists
	}

	// Check if source exists
	if !m.namespace.FileExists(sourcePath) && !m.namespace.DirectoryExists(sourcePath) {
		return ErrPathNotFound
	}

	log.Printf("Creating snapshot: %s -> %s", sourcePath, snapshotPath)

	// Step 1: Revoke outstanding leases on chunks to be snapshotted
	affectedChunks, err := m.collectAffectedChunks(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to collect affected chunks: %v", err)
	}

	log.Printf("Revoking leases on %d chunks for snapshot", len(affectedChunks))
	m.revokeLeases(affectedChunks)

	// Step 2: Create snapshot metadata and duplicate file metadata
	snapshotInfo := &SnapshotInfo{
		SnapshotPath:  snapshotPath,
		OriginalPath:  sourcePath,
		CreationTime:  time.Now(),
		ChunkHandles:  affectedChunks,
		IsDirectory:   m.namespace.DirectoryExists(sourcePath),
	}

	// Step 3: Duplicate the namespace structure
	err = m.duplicateNamespaceStructure(sourcePath, snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to duplicate namespace structure: %v", err)
	}

	// Step 4: Increment reference counts for all affected chunks
	m.chunksMu.Lock()
	for _, chunkHandle := range affectedChunks {
		if chunkInfo, exists := m.chunks[chunkHandle]; exists {
			chunkInfo.mu.Lock()
			chunkInfo.RefCount++
			chunkInfo.mu.Unlock()
		}
	}
	m.chunksMu.Unlock()

	// Step 5: Store snapshot metadata
	m.snapshots[snapshotPath] = snapshotInfo

	log.Printf("Snapshot created successfully: %s", snapshotPath)
	return nil
}

// collectAffectedChunks gathers all chunk handles for a file or directory
func (m *Master) collectAffectedChunks(path string) ([]string, error) {
	var chunks []string

	if m.namespace.FileExists(path) {
		// Single file
		fileInfo, err := m.namespace.GetFile(path)
		if err != nil {
			return nil, err
		}

		fileInfo.mu.RLock()
		for _, chunkHandle := range fileInfo.Chunks {
			chunks = append(chunks, chunkHandle)
		}
		fileInfo.mu.RUnlock()
	} else if m.namespace.DirectoryExists(path) {
		// Directory - collect chunks from all files recursively
		allFiles := m.namespace.GetAllFiles()
		for filePath, fileInfo := range allFiles {
			if strings.HasPrefix(filePath, path+"/") || filePath == path {
				fileInfo.mu.RLock()
				for _, chunkHandle := range fileInfo.Chunks {
					chunks = append(chunks, chunkHandle)
				}
				fileInfo.mu.RUnlock()
			}
		}
	}

	return chunks, nil
}

// revokeLeases revokes outstanding leases on specified chunks
func (m *Master) revokeLeases(chunkHandles []string) {
	m.chunksMu.RLock()
	defer m.chunksMu.RUnlock()

	for _, chunkHandle := range chunkHandles {
		if chunkInfo, exists := m.chunks[chunkHandle]; exists {
			chunkInfo.mu.Lock()
			// Immediately expire the lease to force re-negotiation
			chunkInfo.LeaseExpiration = time.Now().Add(-time.Minute)
			chunkInfo.Primary = "" // Clear primary
			chunkInfo.mu.Unlock()
		}
	}
}

// duplicateNamespaceStructure creates snapshot copy of file/directory structure
func (m *Master) duplicateNamespaceStructure(sourcePath, snapshotPath string) error {
	if m.namespace.FileExists(sourcePath) {
		// Single file snapshot
		fileInfo, err := m.namespace.GetFile(sourcePath)
		if err != nil {
			return err
		}

		// Create snapshot file with same chunk references
		snapshotFileInfo := &FileInfo{
			Chunks:       make(map[int64]string),
			IsSnapshot:   true,
			SnapshotTime: time.Now(),
			OriginalPath: sourcePath,
		}

		fileInfo.mu.RLock()
		for chunkIndex, chunkHandle := range fileInfo.Chunks {
			snapshotFileInfo.Chunks[chunkIndex] = chunkHandle
		}
		fileInfo.mu.RUnlock()

		return m.namespace.CreateFile(snapshotPath, snapshotFileInfo)

	} else if m.namespace.DirectoryExists(sourcePath) {
		// Directory snapshot - recursively duplicate structure
		return m.duplicateDirectoryStructure(sourcePath, snapshotPath)
	}

	return ErrPathNotFound
}

// duplicateDirectoryStructure recursively duplicates directory structure for snapshot
func (m *Master) duplicateDirectoryStructure(sourcePath, snapshotPath string) error {
	// Create snapshot directory
	err := m.namespace.CreateDirectory(snapshotPath)
	if err != nil {
		return err
	}

	// Get all files in the namespace and find those under sourcePath
	allFiles := m.namespace.GetAllFiles()
	for filePath, fileInfo := range allFiles {
		if strings.HasPrefix(filePath, sourcePath+"/") {
			// Calculate relative path and create snapshot file
			relativePath := strings.TrimPrefix(filePath, sourcePath)
			snapshotFilePath := snapshotPath + relativePath

			snapshotFileInfo := &FileInfo{
				Chunks:       make(map[int64]string),
				IsSnapshot:   true,
				SnapshotTime: time.Now(),
				OriginalPath: filePath,
			}

			fileInfo.mu.RLock()
			for chunkIndex, chunkHandle := range fileInfo.Chunks {
				snapshotFileInfo.Chunks[chunkIndex] = chunkHandle
			}
			fileInfo.mu.RUnlock()

			err := m.namespace.CreateFile(snapshotFilePath, snapshotFileInfo)
			if err != nil {
				log.Printf("Warning: Could not create snapshot file %s: %v", snapshotFilePath, err)
			}
		}
	}

	return nil
}

// handleCopyOnWrite handles copy-on-write when writing to a chunk with refcount > 1
func (m *Master) handleCopyOnWrite(originalChunkHandle string, targetServers []string) (string, error) {
	m.chunksMu.Lock()
	defer m.chunksMu.Unlock()

	originalChunk, exists := m.chunks[originalChunkHandle]
	if !exists {
		return "", fmt.Errorf("original chunk %s not found", originalChunkHandle)
	}

	originalChunk.mu.Lock()
	defer originalChunk.mu.Unlock()

	// Check if copy-on-write is needed
	if originalChunk.RefCount <= 1 {
		// No need for CoW, return original handle
		return originalChunkHandle, nil
	}

	log.Printf("Copy-on-write triggered for chunk %s (refcount: %d)", originalChunkHandle, originalChunk.RefCount)

	// Generate new chunk handle
	newChunkHandle := m.generateNewChunkHandle()

	// Create new chunk info
	newChunk := &ChunkInfo{
		Size:            originalChunk.Size,
		Version:         1, // New chunk starts at version 1
		Locations:       make(map[string]bool),
		ServerAddresses: make(map[string]string),
		Primary:         "",
		LeaseExpiration: time.Time{},
		StaleReplicas:   make(map[string]bool),
		RefCount:        1, // New chunk has refcount 1
		IsSnapshot:      true,
		OriginalHandle:  originalChunkHandle,
	}

	// Add new chunk to chunks map
	m.chunks[newChunkHandle] = newChunk

	// Decrease refcount of original chunk
	originalChunk.RefCount--

	// Ask chunkservers to copy the chunk locally
	err := m.requestChunkCopy(originalChunkHandle, newChunkHandle, targetServers)
	if err != nil {
		// Cleanup on failure
		delete(m.chunks, newChunkHandle)
		originalChunk.RefCount++
		return "", fmt.Errorf("failed to copy chunk: %v", err)
	}

	log.Printf("Copy-on-write completed: %s -> %s", originalChunkHandle, newChunkHandle)
	return newChunkHandle, nil
}

// requestChunkCopy asks chunkservers to create a copy of a chunk locally
func (m *Master) requestChunkCopy(originalHandle, newHandle string, targetServers []string) error {
	m.chunksMu.RLock()
	originalChunk := m.chunks[originalHandle]
	newChunk := m.chunks[newHandle]
	m.chunksMu.RUnlock()

	if originalChunk == nil || newChunk == nil {
		return fmt.Errorf("chunk not found")
	}

	originalChunk.mu.RLock()
	defer originalChunk.mu.RUnlock()

	// Copy chunk on each target server
	for _, serverId := range targetServers {
		if originalChunk.Locations[serverId] {
			// Server has the original chunk, ask it to copy
			err := m.sendCopyChunkCommand(serverId, originalHandle, newHandle)
			if err != nil {
				log.Printf("Warning: Failed to copy chunk on server %s: %v", serverId, err)
				continue
			}

			// Update new chunk's location info
			newChunk.mu.Lock()
			newChunk.Locations[serverId] = true
			if serverAddr, exists := originalChunk.ServerAddresses[serverId]; exists {
				newChunk.ServerAddresses[serverId] = serverAddr
			}
			newChunk.mu.Unlock()
		}
	}

	// Ensure we have at least one copy
	newChunk.mu.RLock()
	hasReplica := len(newChunk.Locations) > 0
	newChunk.mu.RUnlock()

	if !hasReplica {
		return fmt.Errorf("failed to create any replicas of new chunk")
	}

	return nil
}

// sendCopyChunkCommand sends a copy command to a chunkserver
func (m *Master) sendCopyChunkCommand(serverId, originalHandle, newHandle string) error {
	// Create COPY command to send to chunkserver
	copyCommand := &chunk_pb.ChunkCommand{
		Type: chunk_pb.ChunkCommand_COPY,
		ChunkHandle: &common_pb.ChunkHandle{
			Handle: newHandle,
		},
		SourceChunkHandle: originalHandle,
	}

	// Send command through heartbeat response channel
	m.chunkServerMgr.mu.RLock()
	responseChan, exists := m.chunkServerMgr.activeStreams[serverId]
	m.chunkServerMgr.mu.RUnlock()

	if !exists {
		return fmt.Errorf("server %s not connected", serverId)
	}

	response := &chunk_pb.HeartBeatResponse{
		Status:   &common_pb.Status{Code: common_pb.Status_OK},
		Commands: []*chunk_pb.ChunkCommand{copyCommand},
	}

	select {
	case responseChan <- response:
		log.Printf("Sent COPY command to server %s: %s -> %s", serverId, originalHandle, newHandle)
		return nil
	default:
		return fmt.Errorf("failed to send COPY command to server %s (channel full)", serverId)
	}
}

// DeleteSnapshot removes a snapshot and decreases reference counts
func (m *Master) DeleteSnapshot(snapshotPath string) error {
	m.snapshotsMu.Lock()
	defer m.snapshotsMu.Unlock()

	snapshotInfo, exists := m.snapshots[snapshotPath]
	if !exists {
		return ErrSnapshotNotFound
	}

	log.Printf("Deleting snapshot: %s", snapshotPath)

	// Remove snapshot from namespace
	if snapshotInfo.IsDirectory {
		// For directory snapshots, we need to remove all files recursively
		// This is simplified - in a full implementation, we'd need careful cleanup
		log.Printf("Directory snapshot deletion not fully implemented")
	} else {
		err := m.namespace.DeleteFile(snapshotPath)
		if err != nil {
			log.Printf("Warning: Could not delete snapshot file %s: %v", snapshotPath, err)
		}
	}

	// Decrease reference counts for all chunks
	m.chunksMu.Lock()
	for _, chunkHandle := range snapshotInfo.ChunkHandles {
		if chunkInfo, exists := m.chunks[chunkHandle]; exists {
			chunkInfo.mu.Lock()
			chunkInfo.RefCount--
			
			// If refcount reaches 0 and it's a CoW chunk, mark for deletion
			if chunkInfo.RefCount <= 0 && chunkInfo.IsSnapshot {
				m.deletedChunksMu.Lock()
				m.deletedChunks[chunkHandle] = true
				m.deletedChunksMu.Unlock()
				delete(m.chunks, chunkHandle)
			}
			chunkInfo.mu.Unlock()
		}
	}
	m.chunksMu.Unlock()

	// Remove snapshot metadata
	delete(m.snapshots, snapshotPath)

	log.Printf("Snapshot deleted: %s", snapshotPath)
	return nil
}

// ListSnapshots returns all available snapshots
func (m *Master) ListSnapshots() map[string]*SnapshotInfo {
	m.snapshotsMu.RLock()
	defer m.snapshotsMu.RUnlock()

	result := make(map[string]*SnapshotInfo)
	for path, info := range m.snapshots {
		// Create a copy to avoid race conditions
		result[path] = &SnapshotInfo{
			SnapshotPath:  info.SnapshotPath,
			OriginalPath:  info.OriginalPath,
			CreationTime:  info.CreationTime,
			ChunkHandles:  append([]string(nil), info.ChunkHandles...),
			IsDirectory:   info.IsDirectory,
		}
	}

	return result
}