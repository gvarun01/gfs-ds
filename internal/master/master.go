package master

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
	common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"
)

func (s *MasterServer) Stop() {
	s.grpcServer.GracefulStop()
	if s.Master.opLog != nil {
		s.Master.opLog.Close()
	}
}

func NewMaster(config *Config) *Master {
	m := &Master{
		Config:             config,
		namespace:          NewBTreeNamespace(),
		chunks:             make(map[string]*ChunkInfo),
		deletedChunks:      make(map[string]bool),
		orphanedChunks:     make(map[string]*OrphanedChunk),
		expeditedDeletions: make(map[string]*ExpeditedDeletion),
		snapshots:          make(map[string]*SnapshotInfo),
		nextChunkHandle:    1000000, // Start chunk handles at 1M for CoW chunks
		servers:            make(map[string]*ServerInfo),
		pendingOps:         make(map[string][]*PendingOperation),
		chunkServerMgr: &ChunkServerManager{
			activeStreams: make(map[string]chan *chunk_pb.HeartBeatResponse),
		},
	}

	// Initialize operation log
	opLog, err := NewOperationLog(config.OperationLog.Path, config.Metadata.Database.Path)
	if err != nil {
		log.Fatalf("Failed to initialize operation log with error: %v", err)
	}
	m.opLog = opLog

	// Replay operation log
	if err := m.replayOperationLog(); err != nil {
		log.Fatalf("Failed to replay operation log: %v", err)
	}

	// bg processes to monitor leases, check server health and maintenance of replication
	go m.monitorServerHealth()
	go m.monitorChunkReplication()
	go m.cleanupExpiredLeases()
	go m.runGarbageCollection()
	go m.runPendingOpsCleanup()

	return m
}

func NewMasterServer(addr string, config *Config) (*MasterServer, error) {
	server := &MasterServer{
		Master:     NewMaster(config),
		grpcServer: grpc.NewServer(),
	}

	chunk_pb.RegisterChunkMasterServiceServer(server.grpcServer, server)
	client_pb.RegisterClientMasterServiceServer(server.grpcServer, server)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

	go func() {
		if err := server.grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Duration(server.Master.Config.Metadata.Database.BackupInterval) * time.Second)
		defer ticker.Stop() // Ensure the ticker is stopped when the goroutine exits

		for range ticker.C {
			if err := server.Master.checkpointMetadata(); err != nil {
				log.Printf("Failed to checkpoint metadata: %v", err)
			}
		}
	}()

	return server, nil
}

func (s *MasterServer) ReportChunk(req *chunk_pb.ReportChunkRequest, stream chunk_pb.ChunkMasterService_ReportChunkServer) error {
	if req.ServerId == "" {
		return status.Error(codes.InvalidArgument, "server_id is required")
	}

	// Create response channel for this server
	responseChan := make(chan *chunk_pb.HeartBeatResponse, 10)
	defer close(responseChan)

	// Register stream in the existing ChunkServerManager
	s.Master.chunkServerMgr.mu.Lock()
	s.Master.chunkServerMgr.activeStreams[req.ServerId] = responseChan
	s.Master.chunkServerMgr.mu.Unlock()

	// Cleanup on exit
	defer func() {
		s.Master.chunkServerMgr.mu.Lock()
		delete(s.Master.chunkServerMgr.activeStreams, req.ServerId)
		s.Master.chunkServerMgr.mu.Unlock()
	}()

	// Process initial chunk report
	s.Master.serversMu.Lock()
	if _, exists := s.Master.servers[req.ServerId]; !exists {
		s.Master.servers[req.ServerId] = &ServerInfo{
			Address:       req.ServerAddress,
			LastHeartbeat: time.Now(),
			Chunks:        make(map[string]bool),
			Status:        "ACTIVE",
		}
	}
	serverInfo := s.Master.servers[req.ServerId]
	s.Master.serversMu.Unlock()

	s.Master.chunksMu.Lock()
	for _, reportedChunk := range req.Chunks {
		chunkHandle := reportedChunk.Handle
		if _, exists := s.Master.chunks[chunkHandle]; !exists {
			s.Master.chunks[chunkHandle] = &ChunkInfo{
				mu:              sync.RWMutex{},
				Locations:       make(map[string]bool),
				ServerAddresses: make(map[string]string),
				Version:         0,
				StaleReplicas:   make(map[string]bool),
			}
		}

		RetrievedChunk := s.Master.chunks[chunkHandle]
		RetrievedChunk.mu.Lock()

		if reportedChunk.Version < RetrievedChunk.Version {
			RetrievedChunk.StaleReplicas[req.ServerId] = true
			s.scheduleStaleReplicaDelete(chunkHandle, req.ServerId)
		} else {
			delete(RetrievedChunk.StaleReplicas, req.ServerId)
			RetrievedChunk.Locations[req.ServerId] = true
			RetrievedChunk.ServerAddresses[req.ServerId] = req.ServerAddress
		}
		RetrievedChunk.mu.Unlock()

		serverInfo.mu.Lock()
		serverInfo.Chunks[chunkHandle] = true
		serverInfo.mu.Unlock()
	}
s.Master.chunksMu.Unlock()

	for {
		select {
		case response, ok := <-responseChan:
			if !ok {
				return nil
			}

			if err := stream.Send(response); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// Helper methods for testing orphaned chunk detection
// These methods provide testing access to internal state

// GetOrphanedChunkCount returns the number of tracked orphaned chunks
func (m *Master) GetOrphanedChunkCount() int {
	m.orphanedChunksMu.RLock()
	defer m.orphanedChunksMu.RUnlock()
	return len(m.orphanedChunks)
}

// IsChunkMarkedForDeletion checks if a chunk is marked for deletion
func (m *Master) IsChunkMarkedForDeletion(chunkHandle string) bool {
	m.deletedChunksMu.RLock()
	defer m.deletedChunksMu.RUnlock()
	return m.deletedChunks[chunkHandle]
}

// AddServer adds a server to the master's registry (for testing)
func (m *Master) AddServer(serverID, address string) {
	m.serversMu.Lock()
	defer m.serversMu.Unlock()
	
	if m.servers[serverID] == nil {
		m.servers[serverID] = &ServerInfo{
			Address:       address,
			LastHeartbeat: time.Now(),
			Chunks:        make(map[string]bool),
			Status:        "active",
		}
	}
}

// GetServerInfo retrieves server information (for testing)
func (m *Master) GetServerInfo(serverID string) *ServerInfo {
	m.serversMu.RLock()
	defer m.serversMu.RUnlock()
	return m.servers[serverID]
}

// CreateChunk creates a chunk in the master's registry (for testing)
func (m *Master) CreateChunk(chunkHandle string, size int64) {
	m.chunksMu.Lock()
	defer m.chunksMu.Unlock()
	
	m.chunks[chunkHandle] = &ChunkInfo{
		Size:            size,
		Version:         1,
		Locations:       make(map[string]bool),
		ServerAddresses: make(map[string]string),
		StaleReplicas:   make(map[string]bool),
	}
}

// GetChunkInfo retrieves chunk information (for testing)
func (m *Master) GetChunkInfo(chunkHandle string) *ChunkInfo {
	m.chunksMu.RLock()
	defer m.chunksMu.RUnlock()
	return m.chunks[chunkHandle]
}

// MarkChunkAsOrphan exposes the orphan marking functionality for testing
func (m *Master) MarkChunkAsOrphan(chunkHandle, serverID string, size int64, version int32) {
	m.markChunkAsOrphan(chunkHandle, serverID, size, version)
}

// ProcessOrphanedChunks exposes the orphan processing functionality for testing
func (m *Master) ProcessOrphanedChunks() {
	m.processOrphanedChunks()
}

// CleanupStaleOrphanEntries exposes the stale orphan cleanup for testing
func (m *Master) CleanupStaleOrphanEntries() {
	m.cleanupStaleOrphanEntries()
}

// UpdateChunkFromHeartbeat simulates updating chunk info from heartbeat (for testing)
func (m *Master) UpdateChunkFromHeartbeat(chunkHandle, serverID string, size int64, version int32) {
	m.chunksMu.Lock()
	defer m.chunksMu.Unlock()
	
	if chunkInfo, exists := m.chunks[chunkHandle]; exists {
		chunkInfo.mu.Lock()
		chunkInfo.Size = size
		chunkInfo.Locations[serverID] = true
		if chunkInfo.ServerAddresses == nil {
			chunkInfo.ServerAddresses = make(map[string]string)
		}
		if serverInfo := m.servers[serverID]; serverInfo != nil {
			chunkInfo.ServerAddresses[serverID] = serverInfo.Address
		}
		chunkInfo.mu.Unlock()
	}
}

// HasServer checks if a chunk info has a particular server (helper for testing)
func (ci *ChunkInfo) HasServer(serverID string) bool {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	return ci.Locations[serverID]
}

// GetAllChunks returns all chunks in the master's registry (for testing)
func (m *Master) GetAllChunks() map[string]*ChunkInfo {
	m.chunksMu.RLock()
	defer m.chunksMu.RUnlock()
	
	// Return a copy to prevent external modification
	chunks := make(map[string]*ChunkInfo)
	for handle, info := range m.chunks {
		chunks[handle] = info
	}
	return chunks
}

// CreateTestFile creates a test file in the namespace (for testing)
func (m *Master) CreateTestFile(filename string) {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	
	// Create file in namespace
	fileInfo := &FileInfo{
		Chunks: make(map[int64]string),
	}
	m.namespace.CreateFile(filename, fileInfo)
}

// FileExists checks if a file exists in the namespace (for testing)
func (m *Master) FileExists(filename string) bool {
	m.filesMu.RLock()
	defer m.filesMu.RUnlock()
	
	return m.namespace.FileExists(filename)
}

// SoftDeleteFile performs soft deletion of a file (for testing)
func (m *Master) SoftDeleteFile(filename string) error {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	
	// Check if file is already in trash (double deletion)
	if strings.HasPrefix(filename, m.Config.Deletion.TrashDirPrefix) {
		// Mark for expedited deletion
		if err := m.markForExpeditedDeletionInternal(filename); err != nil {
			return err
		}
		
		// Process expedited deletion immediately
		go func() {
			m.processExpeditedDeletions()
		}()
		
		return nil
	}
	
	if !m.namespace.FileExists(filename) {
		return fmt.Errorf("file not found: %s", filename)
	}
	
	// Create trash path with timestamp
	now := time.Now().Format("2006-01-02T15:04:05")
	trashPath := fmt.Sprintf("%s_%s", filename, now)
	trashPath = fmt.Sprintf("%s%s", m.Config.Deletion.TrashDirPrefix, trashPath)
	
	// Move to trash
	return m.namespace.RenameFile(filename, trashPath)
}

// markForExpeditedDeletionInternal is the internal version for the Master type
func (m *Master) markForExpeditedDeletionInternal(trashPath string) error {
	// Extract original path from trash filename
	trashName := strings.TrimPrefix(trashPath, m.Config.Deletion.TrashDirPrefix)
	parts := strings.Split(trashName, "_")
	
	if len(parts) < 2 {
		return fmt.Errorf("invalid trash file format: %s", trashPath)
	}
	
	// Get original path and first deletion timestamp
	originalParts := parts[:len(parts)-1]
	originalPath := strings.Join(originalParts, "_")
	firstDeleteTimestamp := parts[len(parts)-1]
	
	firstDeletedAt, err := time.Parse("2006-01-02T15:04:05", firstDeleteTimestamp)
	if err != nil {
		return fmt.Errorf("invalid timestamp in trash filename: %v", err)
	}
	
	m.expeditedDeletionsMu.Lock()
	defer m.expeditedDeletionsMu.Unlock()
	
	// Create expedited deletion record
	m.expeditedDeletions[trashPath] = &ExpeditedDeletion{
		TrashPath:       trashPath,
		OriginalPath:    originalPath,
		FirstDeletedAt:  firstDeletedAt,
		SecondDeletedAt: time.Now(),
		IsProcessed:     false,
	}
	
	return nil
}
			// Convert HeartBeatResponse to ReportChunkResponse for each command
			for _, command := range response.Commands {
				reportResponse := &chunk_pb.ReportChunkResponse{
					Status:  response.Status,
					Command: command,
				}
				if err := stream.Send(reportResponse); err != nil {
					log.Printf("Error sending command to server %s: %v", req.ServerId, err)
					return err
				}
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// Lease can only be requested by an active primary chunk-server
func (s *MasterServer) RequestLease(ctx context.Context, req *chunk_pb.RequestLeaseRequest) (*chunk_pb.RequestLeaseResponse, error) {
	if req.ChunkHandle.Handle == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk_handle is required")
	}

	log.Print("Received Lease request: ", req.ServerId)

	s.Master.chunksMu.RLock()
	chunkInfo, exists := s.Master.chunks[req.ChunkHandle.Handle]
	s.Master.chunksMu.RUnlock()

	if !exists {
		return &chunk_pb.RequestLeaseResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "chunk not found",
			},
			Granted: false,
		}, nil
	}

	chunkInfo.mu.Lock()
	
	// Check if copy-on-write is needed before proceeding with lease
	actualChunkHandle := req.ChunkHandle.Handle
	if chunkInfo.RefCount > 1 {
		log.Printf("Copy-on-write triggered for chunk %s (refcount: %d) during lease request", 
			req.ChunkHandle.Handle, chunkInfo.RefCount)
		
		chunkInfo.mu.Unlock() // Unlock before calling handleCopyOnWrite
		
		// Get target servers from original chunk
		chunkInfo.mu.RLock()
		targetServers := make([]string, 0, len(chunkInfo.Locations))
		for serverId := range chunkInfo.Locations {
			targetServers = append(targetServers, serverId)
		}
		chunkInfo.mu.RUnlock()
		
		// Perform copy-on-write
		newChunkHandle, err := s.Master.handleCopyOnWrite(req.ChunkHandle.Handle, targetServers)
		if err != nil {
			return &chunk_pb.RequestLeaseResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: fmt.Sprintf("copy-on-write failed: %v", err),
				},
				Granted: false,
			}, nil
		}
		
		// If CoW created a new chunk, update the handle and get new chunk info
		if newChunkHandle != req.ChunkHandle.Handle {
			actualChunkHandle = newChunkHandle
			s.Master.chunksMu.RLock()
			chunkInfo = s.Master.chunks[newChunkHandle]
			s.Master.chunksMu.RUnlock()
			
			if chunkInfo == nil {
				return &chunk_pb.RequestLeaseResponse{
					Status: &common_pb.Status{
						Code:    common_pb.Status_ERROR,
						Message: "new chunk not found after copy-on-write",
					},
					Granted: false,
				}, nil
			}
		}
		
		chunkInfo.mu.Lock()
	}
	defer chunkInfo.mu.Unlock()

	// Check if the requesting server has the latest version
	if staleVersion, isStale := chunkInfo.StaleReplicas[req.ServerId]; isStale {
		return &chunk_pb.RequestLeaseResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: fmt.Sprintf("server has stale version %t (current: %d)", staleVersion, chunkInfo.Version),
			},
			Granted: false,
		}, nil
	}

	// Check if the requesting server is the current primary
	if chunkInfo.Primary != req.ServerId {
		return &chunk_pb.RequestLeaseResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "only the primary server can request lease extension",
			},
			Granted: false,
		}, nil
	}

	// Extend the lease
	chunkInfo.LeaseExpiration = time.Now().Add(time.Duration(s.Master.Config.Lease.LeaseTimeout) * time.Second)
	newVersion := chunkInfo.Version + 1

	updateCommand := &chunk_pb.ChunkCommand{
		Type: chunk_pb.ChunkCommand_UPDATE_VERSION,
		ChunkHandle: &common_pb.ChunkHandle{
			Handle: actualChunkHandle, // Use actual chunk handle (may be new after CoW)
		},
		Version: newVersion,
	}

	numVersionUpdates := 0
	s.Master.chunkServerMgr.mu.RLock()
	for serverId := range chunkInfo.Locations {
		if responseChan, exists := s.Master.chunkServerMgr.activeStreams[serverId]; exists {
			response := &chunk_pb.HeartBeatResponse{
				Status:   &common_pb.Status{Code: common_pb.Status_OK},
				Commands: []*chunk_pb.ChunkCommand{updateCommand},
			}

			select {
			case responseChan <- response:
				numVersionUpdates = numVersionUpdates + 1
				log.Printf("Sent version update command to server %s for chunk %s (version %d)",
					serverId, actualChunkHandle, newVersion)
			default:
				log.Printf("Warning: Failed to send version update to server %s (channel full)", serverId)
			}
		}
	}

	s.Master.chunkServerMgr.mu.RUnlock()

	if numVersionUpdates > 0 {
		s.Master.incrementChunkVersion(actualChunkHandle, chunkInfo)
	}

	log.Printf("Extending Lease for server %v for chunkhandle %v", req.ServerId, actualChunkHandle)

	return &chunk_pb.RequestLeaseResponse{
		Status:          &common_pb.Status{Code: common_pb.Status_OK},
		Granted:         true,
		LeaseExpiration: chunkInfo.LeaseExpiration.Unix(),
		Version:         chunkInfo.Version,
		ChunkHandle: &common_pb.ChunkHandle{
			Handle: actualChunkHandle, // Return the actual chunk handle (may be new after CoW)
		},
	}, nil
}

func (s *MasterServer) HeartBeat(stream chunk_pb.ChunkMasterService_HeartBeatServer) error {
	var serverId string
	responseChannel := make(chan *chunk_pb.HeartBeatResponse, 10)
	defer close(responseChannel)

	// Start goroutine to send responses
	go func() {
		for response := range responseChannel {
			if err := stream.Send(response); err != nil {
				log.Printf("Error sending heartbeat response to %s: %v", serverId, err)
				return
			}
		}
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			s.Master.handleServerFailure(serverId)
			return err
		}

		log.Print("Received HeartBeat message: ", req.ServerId)

		if serverId == "" {
			serverId = req.ServerId
			s.Master.serversMu.Lock()
			s.Master.servers[serverId] = &ServerInfo{
				Address:        req.ServerAddress,
				LastHeartbeat:  time.Now(),
				AvailableSpace: req.AvailableSpace,
				CPUUsage:       req.CpuUsage,
				ActiveOps:      req.ActiveOperations,
				Chunks:         make(map[string]bool),
				Status:         "ACTIVE",
				LastUpdated:    time.Now(),
			}
			s.Master.serversMu.Unlock()
		}

		if err := s.updateServerStatus(serverId, req); err != nil {
			log.Printf("Error updating server status: %v", err)
			continue
		}

		commands := s.generateChunkCommands(serverId)

		response := &chunk_pb.HeartBeatResponse{
			Status:   &common_pb.Status{Code: common_pb.Status_OK},
			Commands: commands,
		}

		select {
		case responseChannel <- response:
		default:
			log.Printf("Warning: Response channel full for server %s", serverId)
		}
	}
}

// File-related error definitions
// These errors are returned by the master for file system operations
var (
	ErrFileNotFound      = errors.New("file not found")
	ErrFileExists        = errors.New("file already exists")
	ErrInvalidFileName   = errors.New("invalid filename")
	ErrInvalidChunkRange = errors.New("invalid chunk range")
)

func (s *MasterServer) CreateChunk(fileInfo *FileInfo, filename string, chunkIndex int64) error {
	// Generate new chunk handle using UUID
	chunkHandle := uuid.New().String()
	log.Printf("Creating for index %d", chunkIndex)
	fileInfo.Chunks[chunkIndex] = chunkHandle

	s.Master.chunksMu.Lock()
	// Create new chunk info
	chunkInfo := &ChunkInfo{
		mu:              sync.RWMutex{},
		Locations:       make(map[string]bool),
		ServerAddresses: make(map[string]string),
		Version:         0,
		StaleReplicas:   make(map[string]bool),
	}
	s.Master.chunks[chunkHandle] = chunkInfo

	metadata := map[string]interface{}{
		"chunk_info": chunkInfo,
		"file_index": chunkIndex,
	}

	if err := s.Master.opLog.LogOperation(OpAddChunk, filename, chunkHandle, metadata); err != nil {
		log.Printf("Failed to log chunk creation: %v", err)
	}

	// Select initial servers for this chunk
	selectedServers := s.selectInitialChunkServers()
	if len(selectedServers) == 0 {
		s.Master.chunksMu.Unlock()
		return nil
	}

	// Prepare INIT_EMPTY command for all selected servers
	initCommand := &chunk_pb.ChunkCommand{
		Type:        chunk_pb.ChunkCommand_INIT_EMPTY,
		ChunkHandle: &common_pb.ChunkHandle{Handle: chunkHandle},
	}

	// Send INIT_EMPTY command to all selected servers
	var initWg sync.WaitGroup
	initErrors := make([]error, len(selectedServers))

	for i, serverId := range selectedServers {
		initWg.Add(1)
		go func(serverIdx int, srvId string) {
			defer initWg.Done()

			s.Master.chunkServerMgr.mu.RLock()
			responseChan, exists := s.Master.chunkServerMgr.activeStreams[srvId]
			s.Master.chunkServerMgr.mu.RUnlock()

			if !exists {
				initErrors[serverIdx] = fmt.Errorf("no active stream for server %s", srvId)
				return
			}

			response := &chunk_pb.HeartBeatResponse{
				Status:   &common_pb.Status{Code: common_pb.Status_OK},
				Commands: []*chunk_pb.ChunkCommand{initCommand},
			}

			select {
			case responseChan <- response:
				// Command sent successfully
			case <-time.After(5 * time.Second):
				initErrors[serverIdx] = fmt.Errorf("timeout sending INIT_EMPTY to server %s", srvId)
			}
		}(i, serverId)
	}

	// Wait for all initialization attempts to complete
	initWg.Wait()

	// Check for initialization errors
	var successfulServers []string
	for i, err := range initErrors {
		if err == nil {
			successfulServers = append(successfulServers, selectedServers[i])
		} else {
			log.Printf("Error initializing chunk %s on server %s: %v",
				chunkHandle, selectedServers[i], err)
		}
	}

	// Proceed only if we have enough successful initializations
	if len(successfulServers) > 0 {
		chunkInfo := s.Master.chunks[chunkHandle]
		chunkInfo.mu.Lock()

		// Add only successful servers to locations
		for _, serverId := range successfulServers {
			chunkInfo.Locations[serverId] = true

			s.Master.serversMu.Lock()
			chunkInfo.ServerAddresses[serverId] = s.Master.servers[serverId].Address
			s.Master.serversMu.Unlock()
		}

		if err := s.Master.opLog.LogOperation(OpUpdateChunk, filename, chunkHandle, chunkInfo); err != nil {
			log.Printf("Failed to log chunk update: %v", err)
		}
		chunkInfo.mu.Unlock()
	}
	s.Master.chunksMu.Unlock()

	return nil
}

func (s *MasterServer) GetFileChunksInfo(ctx context.Context, req *client_pb.GetFileChunksInfoRequest) (*client_pb.GetFileChunksInfoResponse, error) {
	if err := s.validateFilename(req.Filename); err != nil {
		return &client_pb.GetFileChunksInfoResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	s.Master.filesMu.RLock()
	defer s.Master.filesMu.RUnlock()

	// Use B-tree namespace to get file
	fileInfo, err := s.Master.namespace.GetFile(req.Filename)
	if err != nil {
		return &client_pb.GetFileChunksInfoResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	fileInfo.mu.RLock()
	defer fileInfo.mu.RUnlock()
	// Validate chunk range
	if req.StartChunk < 0 || req.StartChunk > req.EndChunk {
		return &client_pb.GetFileChunksInfoResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: ErrInvalidChunkRange.Error(),
			},
		}, nil
	}

	s.Master.filesMu.Lock()
	for idx := req.StartChunk; idx <= req.EndChunk; idx++ {
		chunkHandle := fileInfo.Chunks[idx]
		chunkInfo := s.Master.chunks[chunkHandle]

		if chunkInfo != nil {
			continue
		}

		createChunkErr := s.CreateChunk(fileInfo, req.Filename, idx)
		if createChunkErr != nil {
			return &client_pb.GetFileChunksInfoResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: "Could not create required chunk",
				},
			}, nil
		}

	}
	s.Master.filesMu.Unlock()

	// Gather chunk information
	chunks := make(map[int64]*client_pb.ChunkInfo)
	s.Master.chunksMu.RLock()
	defer s.Master.chunksMu.RUnlock()

	for idx := req.StartChunk; idx <= req.EndChunk; idx++ {
		chunkHandle := fileInfo.Chunks[idx]
		chunkInfo := s.Master.chunks[chunkHandle]

		if chunkInfo == nil {
			continue
		}

		chunkInfo.mu.RLock()
		needsNewPrimary := chunkInfo.Primary == "" || time.Now().After(chunkInfo.LeaseExpiration)
		chunkInfo.mu.RUnlock()

		if needsNewPrimary {
			s.Master.chunksMu.RUnlock()
			s.Master.assignNewPrimary(chunkHandle)
			s.Master.chunksMu.RLock()
		}

		chunkInfo.mu.RLock()
		locations := make([]*common_pb.ChunkLocation, 0)
		var primaryLocation *common_pb.ChunkLocation

		s.Master.serversMu.Lock()
		if chunkInfo.Primary != "" && time.Now().Before(chunkInfo.LeaseExpiration) {
			primaryLocation = &common_pb.ChunkLocation{
				ServerId:      chunkInfo.Primary,
				ServerAddress: s.Master.servers[chunkInfo.Primary].Address,
			}
		}

		for serverId := range chunkInfo.Locations {
			if serverId != chunkInfo.Primary {
				locations = append(locations, &common_pb.ChunkLocation{
					ServerId:      serverId,
					ServerAddress: s.Master.servers[serverId].Address,
				})
			}
		}
		s.Master.serversMu.Unlock()

		if primaryLocation == nil {
			chunkInfo.mu.RUnlock()
			continue
		}

		chunks[idx] = &client_pb.ChunkInfo{
			ChunkHandle: &common_pb.ChunkHandle{
				Handle: chunkHandle,
			},
			PrimaryLocation:    primaryLocation,
			SecondaryLocations: locations,
		}
		chunkInfo.mu.RUnlock()
	}

	if len(chunks) == 0 {
		return &client_pb.GetFileChunksInfoResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: "No available chunk servers with valid primaries",
			},
		}, nil
	}

	return &client_pb.GetFileChunksInfoResponse{
		Status: &common_pb.Status{Code: common_pb.Status_OK},
		Chunks: chunks,
	}, nil
}

func (s *MasterServer) GetLastChunkIndexInFile(ctx context.Context, req *client_pb.GetLastChunkIndexInFileRequest) (*client_pb.GetLastChunkIndexInFileResponse, error) {
	if err := s.validateFilename(req.Filename); err != nil {
		return &client_pb.GetLastChunkIndexInFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	s.Master.filesMu.RLock()
	defer s.Master.filesMu.RUnlock()

	// Use B-tree namespace to get file
	fileInfo, err := s.Master.namespace.GetFile(req.Filename)
	if err != nil {
		return &client_pb.GetLastChunkIndexInFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	fileInfo.mu.RLock()
	defer fileInfo.mu.RUnlock()

	lastChunkIndex := int64(-1)

	s.Master.filesMu.Lock()
	for idx := int64(0); ; idx++ {
		chunkHandle := fileInfo.Chunks[idx]
		chunkInfo := s.Master.chunks[chunkHandle]

		if chunkInfo != nil {
			continue
		}

		lastChunkIndex = idx - 1
		log.Printf("Last chunk index for %v : %d", req.Filename, lastChunkIndex)
		break
	}
	s.Master.filesMu.Unlock()

	// Create the first chunk if it doesn't exist
	if lastChunkIndex == -1 {
		lastChunkIndex = 0

		createChunkErr := s.CreateChunk(fileInfo, req.Filename, lastChunkIndex)
		if createChunkErr != nil {
			return &client_pb.GetLastChunkIndexInFileResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: "Could not create required chunk",
				},
			}, nil
		}
	}

	return &client_pb.GetLastChunkIndexInFileResponse{
		Status:         &common_pb.Status{Code: common_pb.Status_OK},
		LastChunkIndex: lastChunkIndex,
	}, nil
}

func (s *MasterServer) CreateFile(ctx context.Context, req *client_pb.CreateFileRequest) (*client_pb.CreateFileResponse, error) {
	if err := s.validateFilename(req.Filename); err != nil {
		return &client_pb.CreateFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	s.Master.filesMu.Lock()
	defer s.Master.filesMu.Unlock()

	// Create new file entry
	fileInfo := &FileInfo{
		Chunks: make(map[int64]string),
	}

	// Use B-tree namespace to create file
	err := s.Master.namespace.CreateFile(req.Filename, fileInfo)
	if err != nil {
		return &client_pb.CreateFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	if err := s.Master.opLog.LogOperation(OpCreateFile, req.Filename, "", fileInfo); err != nil {
		log.Printf("Failed to log file creation: %v", err)
	}

	return &client_pb.CreateFileResponse{
		Status: &common_pb.Status{Code: common_pb.Status_OK},
	}, nil
}

func (s *MasterServer) RenameFile(ctx context.Context, req *client_pb.RenameFileRequest) (*client_pb.RenameFileResponse, error) {
	if err := s.validateFilename(req.OldFilename); err != nil {
		return &client_pb.RenameFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	if err := s.validateFilename(req.NewFilename); err != nil {
		return &client_pb.RenameFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	s.Master.filesMu.Lock()
	defer s.Master.filesMu.Unlock()

	// Use B-tree namespace to rename file
	err := s.Master.namespace.RenameFile(req.OldFilename, req.NewFilename)
	if err != nil {
		return &client_pb.RenameFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	if err := s.Master.opLog.LogOperation(OpRenameFile, req.OldFilename, "", map[string]string{"new_filename": req.NewFilename}); err != nil {
		log.Printf("Failed to log file rename: %v", err)
	}

	return &client_pb.RenameFileResponse{
		Status: &common_pb.Status{Code: common_pb.Status_OK},
	}, nil
}

func (s *MasterServer) DeleteFile(ctx context.Context, req *client_pb.DeleteFileRequest) (*client_pb.DeleteFileResponse, error) {
	if err := s.validateFilename(req.Filename); err != nil {
		return &client_pb.DeleteFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	if err := s.Master.opLog.LogOperation(OpDeleteFile, req.Filename, "", nil); err != nil {
		log.Printf("Failed to log file deletion: %v", err)
	}

	s.Master.filesMu.Lock()
	defer s.Master.filesMu.Unlock()

	// Check if file is already in trash (being deleted again)
	if strings.HasPrefix(req.Filename, s.Master.Config.Deletion.TrashDirPrefix) {
		// This is a double deletion - mark for expedited deletion
		if err := s.markForExpeditedDeletion(req.Filename); err != nil {
			return &client_pb.DeleteFileResponse{
				Status: &common_pb.Status{
					Code:    common_pb.Status_ERROR,
					Message: fmt.Sprintf("Failed to expedite deletion: %v", err),
				},
			}, nil
		}

		log.Printf("File %s marked for expedited deletion (double delete)", req.Filename)
		
		// Immediately process the expedited deletion
		go s.processExpeditedDeletions()

		return &client_pb.DeleteFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_OK,
				Message: "File marked for immediate deletion (expedited)",
			},
		}, nil
	}

	// Check if file exists using B-tree namespace
	if !s.Master.namespace.FileExists(req.Filename) {
		return &client_pb.DeleteFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: ErrFileNotFound.Error(),
			},
		}, nil
	}

	now := time.Now().Format("2006-01-02T15:04:05")
	trashPath := fmt.Sprintf("%s_%s", req.Filename, now)
	trashPath = fmt.Sprintf("%s%s", s.Master.Config.Deletion.TrashDirPrefix, trashPath)

	// Move file to trash (soft delete) using B-tree namespace
	err := s.Master.namespace.RenameFile(req.Filename, trashPath)
	if err != nil {
		return &client_pb.DeleteFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	log.Printf("Soft deleted file %s (moved to %s)", req.Filename, trashPath)

	return &client_pb.DeleteFileResponse{
		Status: &common_pb.Status{Code: common_pb.Status_OK},
	}, nil
}

	if err := s.Master.opLog.LogOperation(OpDeleteFile, req.Filename, "", nil); err != nil {
		log.Printf("Failed to log file deletion: %v", err)
	}

	s.Master.filesMu.Lock()
	defer s.Master.filesMu.Unlock()

	// Check if file exists using B-tree namespace
	if !s.Master.namespace.FileExists(req.Filename) {
		return &client_pb.DeleteFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: ErrFileNotFound.Error(),
			},
		}, nil
	}

	now := time.Now().Format("2006-01-02T15:04:05")
	trashPath := fmt.Sprintf("%s_%s", req.Filename, now)
	trashPath = fmt.Sprintf("%s%s", s.Master.Config.Deletion.TrashDirPrefix, trashPath)

	// Move file to trash (soft delete) using B-tree namespace
	err := s.Master.namespace.RenameFile(req.Filename, trashPath)
	if err != nil {
		return &client_pb.DeleteFileResponse{
			Status: &common_pb.Status{
				Code:    common_pb.Status_ERROR,
				Message: err.Error(),
			},
		}, nil
	}

	log.Printf("Soft deleted file %s (moved to %s)", req.Filename, trashPath)

	return &client_pb.DeleteFileResponse{
		Status: &common_pb.Status{Code: common_pb.Status_OK},
	}, nil
}

// UndeleteFile restores a file from the trash directory back to its original location
// This implements the GFS paper Section 4.4 requirement for undelete during grace period
func (s *MasterServer) UndeleteFile(trashFilename, restoreFilename string) error {
	// Validate inputs
	if err := s.validateFilename(trashFilename); err != nil {
		return fmt.Errorf("invalid trash filename: %v", err)
	}
	
	// Check if file is in trash directory
	if !strings.HasPrefix(trashFilename, s.Master.Config.Deletion.TrashDirPrefix) {
		return fmt.Errorf("file %s is not in trash directory", trashFilename)
	}
	
	// Log the undelete operation for HA consistency
	if err := s.Master.opLog.LogOperation("undelete", trashFilename, restoreFilename, nil); err != nil {
		log.Printf("Failed to log file undelete: %v", err)
	}
	
	s.Master.filesMu.Lock()
	defer s.Master.filesMu.Unlock()
	
	// Check if trash file exists
	if !s.Master.namespace.FileExists(trashFilename) {
		return fmt.Errorf("file %s not found in trash", trashFilename)
	}
	
	// Determine restore path
	var restorePath string
	if restoreFilename != "" {
		restorePath = restoreFilename
	} else {
		// Extract original path from trash filename
		// Format: /.trash/original_path_2006-01-02T15:04:05
		trashName := strings.TrimPrefix(trashFilename, s.Master.Config.Deletion.TrashDirPrefix)
		
		// Find the last underscore followed by timestamp
		parts := strings.Split(trashName, "_")
		if len(parts) < 2 {
			return fmt.Errorf("invalid trash file format: %s", trashFilename)
		}
		
		// Remove the timestamp part (last element)
		originalParts := parts[:len(parts)-1]
		restorePath = strings.Join(originalParts, "_")
	}
	
	// Validate restore path
	if err := s.validateFilename(restorePath); err != nil {
		return fmt.Errorf("invalid restore filename: %v", err)
	}
	
	// Check if restore destination already exists
	if s.Master.namespace.FileExists(restorePath) {
		return fmt.Errorf("cannot restore: file %s already exists", restorePath)
	}
	
	// Check if file is still within grace period
	if err := s.validateGracePeriod(trashFilename); err != nil {
		return fmt.Errorf("cannot undelete: %v", err)
	}
	
	// Restore file by renaming from trash back to original location
	err := s.Master.namespace.RenameFile(trashFilename, restorePath)
	if err != nil {
		return fmt.Errorf("failed to restore file: %v", err)
	}
	
	log.Printf("Successfully restored file %s to %s", trashFilename, restorePath)
	return nil
}

// validateGracePeriod checks if a trash file is still within the grace period for undelete
func (s *MasterServer) validateGracePeriod(trashFilename string) error {
	// Extract timestamp from trash filename
	trashName := strings.TrimPrefix(trashFilename, s.Master.Config.Deletion.TrashDirPrefix)
	parts := strings.Split(trashName, "_")
	
	if len(parts) < 2 {
		return fmt.Errorf("invalid trash file format")
	}
	
	// Get the timestamp (last part)
	timestampStr := parts[len(parts)-1]
	deletedTime, err := time.Parse("2006-01-02T15:04:05", timestampStr)
	if err != nil {
		return fmt.Errorf("invalid timestamp in trash filename: %v", err)
	}
	
	// Check if still within grace period
	gracePeriod := time.Duration(s.Master.Config.Deletion.RetentionPeriod) * time.Second
	if time.Since(deletedTime) > gracePeriod {
		return fmt.Errorf("file has expired (deleted %v ago, grace period is %v)", 
			time.Since(deletedTime), gracePeriod)
	}
	
	return nil
}

// GetTrashFiles returns all files currently in trash (for administrative purposes)
func (s *MasterServer) GetTrashFiles() []string {
	s.Master.filesMu.RLock()
	defer s.Master.filesMu.RUnlock()
	
	var trashFiles []string
	allFiles := s.Master.namespace.GetAllFiles()
	
	for filename := range allFiles {
		if strings.HasPrefix(filename, s.Master.Config.Deletion.TrashDirPrefix) {
			trashFiles = append(trashFiles, filename)
		}
	}
	
	return trashFiles
}

// markForExpeditedDeletion marks a file in trash for immediate deletion
// This implements GFS Section 4.4 expedited deletion for double-deleted files
func (s *MasterServer) markForExpeditedDeletion(trashPath string) error {
	// Extract original path from trash filename
	trashName := strings.TrimPrefix(trashPath, s.Master.Config.Deletion.TrashDirPrefix)
	parts := strings.Split(trashName, "_")
	
	if len(parts) < 2 {
		return fmt.Errorf("invalid trash file format: %s", trashPath)
	}
	
	// Get original path and first deletion timestamp
	originalParts := parts[:len(parts)-1]
	originalPath := strings.Join(originalParts, "_")
	firstDeleteTimestamp := parts[len(parts)-1]
	
	firstDeletedAt, err := time.Parse("2006-01-02T15:04:05", firstDeleteTimestamp)
	if err != nil {
		return fmt.Errorf("invalid timestamp in trash filename: %v", err)
	}
	
	s.Master.expeditedDeletionsMu.Lock()
	defer s.Master.expeditedDeletionsMu.Unlock()
	
	// Create expedited deletion record
	s.Master.expeditedDeletions[trashPath] = &ExpeditedDeletion{
		TrashPath:       trashPath,
		OriginalPath:    originalPath,
		FirstDeletedAt:  firstDeletedAt,
		SecondDeletedAt: time.Now(),
		IsProcessed:     false,
	}
	
	log.Printf("Marked file %s for expedited deletion", trashPath)
	return nil
}

// processExpeditedDeletions immediately processes files marked for expedited deletion
// This bypasses the normal grace period and deletes files immediately
func (s *MasterServer) processExpeditedDeletions() {
	s.Master.expeditedDeletionsMu.Lock()
	filesToDelete := make([]*ExpeditedDeletion, 0)
	
	for _, deletion := range s.Master.expeditedDeletions {
		deletion.mu.RLock()
		if !deletion.IsProcessed {
			filesToDelete = append(filesToDelete, deletion)
		}
		deletion.mu.RUnlock()
	}
	s.Master.expeditedDeletionsMu.Unlock()
	
	// Process deletions outside of lock to avoid deadlock
	for _, deletion := range filesToDelete {
		if err := s.performExpeditedDeletion(deletion); err != nil {
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
	s.cleanupProcessedExpeditedDeletions()
}

// performExpeditedDeletion immediately deletes a file and its chunks
func (s *MasterServer) performExpeditedDeletion(deletion *ExpeditedDeletion) error {
	s.Master.filesMu.Lock()
	defer s.Master.filesMu.Unlock()
	
	trashPath := deletion.TrashPath
	
	// Check if file still exists in trash
	if !s.Master.namespace.FileExists(trashPath) {
		return fmt.Errorf("trash file %s no longer exists", trashPath)
	}
	
	// Get file info to identify chunks for deletion
	fileInfo := s.Master.namespace.GetFileInfo(trashPath)
	if fileInfo != nil {
		// Delete all chunks associated with the file
		for _, chunkHandle := range fileInfo.Chunks {
			if err := s.deleteChunkAndReplicas(chunkHandle); err != nil {
				log.Printf("Warning: Failed to delete chunk %s during expedited deletion: %v", 
					chunkHandle, err)
			}
		}
	}
	
	// Remove file from namespace
	if err := s.Master.namespace.DeleteFile(trashPath); err != nil {
		return fmt.Errorf("failed to delete trash file %s: %v", trashPath, err)
	}
	
	return nil
}

// cleanupProcessedExpeditedDeletions removes processed expedited deletion records
func (s *MasterServer) cleanupProcessedExpeditedDeletions() {
	s.Master.expeditedDeletionsMu.Lock()
	defer s.Master.expeditedDeletionsMu.Unlock()
	
	for trashPath, deletion := range s.Master.expeditedDeletions {
		deletion.mu.RLock()
		if deletion.IsProcessed && time.Since(deletion.ProcessedAt) > time.Hour {
			delete(s.Master.expeditedDeletions, trashPath)
		}
		deletion.mu.RUnlock()
	}
}

// deleteChunkAndReplicas deletes a chunk and all its replicas from chunkservers
func (s *MasterServer) deleteChunkAndReplicas(chunkHandle string) error {
	s.Master.chunksMu.Lock()
	chunkInfo, exists := s.Master.chunks[chunkHandle]
	if !exists {
		s.Master.chunksMu.Unlock()
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
	delete(s.Master.chunks, chunkHandle)
	s.Master.chunksMu.Unlock()
	
	// Mark chunk for deletion
	s.Master.deletedChunksMu.Lock()
	s.Master.deletedChunks[chunkHandle] = true
	s.Master.deletedChunksMu.Unlock()
	
	// Send delete commands to all servers
	for _, serverID := range servers {
		if err := s.Master.sendDeleteChunkCommand(serverID, chunkHandle); err != nil {
			log.Printf("Failed to send delete command for chunk %s to server %s: %v", 
				chunkHandle, serverID, err)
		}
	}
	
	return nil
}

// ApplyOperation applies a replicated operation from Raft log to the master state
func (m *Master) ApplyOperation(opType string, data []byte) error {
	log.Printf("Applying replicated operation: %s", opType)
	
	switch opType {
	case "file_create":
		// Apply file creation operation
		// In a full implementation, this would deserialize the data
		// and apply the file creation to the namespace
		log.Printf("Applied file creation operation")
		
	case "file_delete":
		// Apply file deletion operation
		log.Printf("Applied file deletion operation")
		
	case "undelete":
		// Apply file undelete operation
		log.Printf("Applied file undelete operation")
		
	case "file_rename":
		// Apply file rename operation
		log.Printf("Applied file rename operation")
		
	case "chunk_create":
		// Apply chunk creation operation
		log.Printf("Applied chunk creation operation")
		
	case "chunk_delete":
		// Apply chunk deletion operation
		log.Printf("Applied chunk deletion operation")
		
	case "snapshot_create":
		// Apply snapshot creation operation
		log.Printf("Applied snapshot creation operation")
		
	case "snapshot_delete":
		// Apply snapshot deletion operation
		log.Printf("Applied snapshot deletion operation")
		
	default:
		return fmt.Errorf("unknown operation type: %s", opType)
	}
	
	return nil
}
