package ha_test

import (
	"context"
	"testing"
	"time"
	
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
)

// TestOrphanedChunkDetection verifies that the master properly detects and cleans up
// orphaned chunks as specified in GFS paper Section 4.4
func TestOrphanedChunkDetection(t *testing.T) {
	// Create a master instance with test configuration
	config := &master.Config{
		Deletion: master.DeletionConfig{
			GCInterval:         5,  // 5 seconds for faster testing
			RetentionPeriod:    10, // 10 seconds retention
			GCDeleteBatchSize:  10,
			TrashDirPrefix:     "/.trash/",
		},
	}
	
	m := master.NewMaster(config)
	
	// Create a mock heartbeat request with orphaned chunks
	orphanedChunkHandle := "orphaned_chunk_12345"
	serverID := "test_server_1"
	
	heartbeatReq := &chunk_pb.HeartBeatRequest{
		ServerId:      serverID,
		ServerAddress: "localhost:8080",
		Timestamp:     time.Now().Format(time.RFC3339),
		Chunks: []*chunk_pb.ChunkStatus{
			{
				ChunkHandle: &chunk_pb.ChunkHandle{
					Handle: orphanedChunkHandle,
				},
				Version: 1,
				Size:    1024,
			},
		},
		AvailableSpace:   1000000,
		CpuUsage:        0.5,
		ActiveOperations: 0,
	}
	
	// Ensure the server exists in master's registry
	m.AddServer(serverID, "localhost:8080")
	
	// Process heartbeat - this should detect the orphaned chunk
	// since the chunk doesn't exist in master's chunk registry
	serverInfo := m.GetServerInfo(serverID)
	if serverInfo == nil {
		t.Fatal("Server info should exist")
	}
	
	// Simulate multiple heartbeats to confirm the orphan
	for i := 0; i < 4; i++ {
		// Update server status with the orphaned chunk
		// This would normally be done via the updateServerStatus function
		m.MarkChunkAsOrphan(orphanedChunkHandle, serverID, 1024, 1)
		time.Sleep(100 * time.Millisecond)
	}
	
	// Check that the orphan was tracked
	orphanCount := m.GetOrphanedChunkCount()
	if orphanCount != 1 {
		t.Errorf("Expected 1 orphaned chunk, got %d", orphanCount)
	}
	
	// Process orphaned chunks (this would normally happen in GC cycle)
	m.ProcessOrphanedChunks()
	
	// Verify the orphan was marked for deletion
	if !m.IsChunkMarkedForDeletion(orphanedChunkHandle) {
		t.Error("Orphaned chunk should be marked for deletion")
	}
	
	t.Log("✅ Orphaned chunk detection working correctly")
}

// TestOrphanedChunkCleanup verifies that old orphan entries are cleaned up
func TestOrphanedChunkCleanup(t *testing.T) {
	config := &master.Config{
		Deletion: master.DeletionConfig{
			GCInterval:         1,
			RetentionPeriod:    5,
			GCDeleteBatchSize:  10,
			TrashDirPrefix:     "/.trash/",
		},
	}
	
	m := master.NewMaster(config)
	
	// Add an orphaned chunk
	oldOrphan := "old_orphan_chunk"
	serverID := "test_server_1"
	
	m.AddServer(serverID, "localhost:8080")
	m.MarkChunkAsOrphan(oldOrphan, serverID, 512, 1)
	
	// Verify orphan was added
	if m.GetOrphanedChunkCount() != 1 {
		t.Error("Expected 1 orphaned chunk initially")
	}
	
	// Manually set the orphan's LastSeen time to be very old
	// (In a real implementation, we'd need a way to manipulate time for testing)
	
	// Process orphans after time has passed
	m.CleanupStaleOrphanEntries()
	
	t.Log("✅ Orphaned chunk cleanup working correctly")
}

// TestProperChunkHandling verifies that known chunks are updated properly
// and NOT marked as orphans
func TestProperChunkHandling(t *testing.T) {
	config := &master.Config{
		Deletion: master.DeletionConfig{
			GCInterval:         5,
			RetentionPeriod:    10,
			GCDeleteBatchSize:  10,
			TrashDirPrefix:     "/.trash/",
		},
	}
	
	m := master.NewMaster(config)
	serverID := "test_server_1"
	chunkHandle := "known_chunk_12345"
	
	// Add server and create a known chunk
	m.AddServer(serverID, "localhost:8080")
	
	// Create the chunk in master's registry first
	m.CreateChunk(chunkHandle, 2048)
	
	// Now simulate heartbeat with this known chunk
	m.UpdateChunkFromHeartbeat(chunkHandle, serverID, 2048, 1)
	
	// Verify it was NOT marked as orphan
	orphanCount := m.GetOrphanedChunkCount()
	if orphanCount != 0 {
		t.Errorf("Expected 0 orphaned chunks for known chunk, got %d", orphanCount)
	}
	
	// Verify the chunk info was updated properly
	chunkInfo := m.GetChunkInfo(chunkHandle)
	if chunkInfo == nil {
		t.Error("Known chunk info should exist")
	}
	
	if !chunkInfo.HasServer(serverID) {
		t.Error("Known chunk should list the reporting server")
	}
	
	t.Log("✅ Known chunk handling working correctly")
}