package ha_test

import (
	"context"
	"testing"
	"time"

	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
)

// TestOrphanedChunkDetection verifies that the master properly detects and cleans up
// orphaned chunks as specified in GFS paper Section 4.4
func TestOrphanedChunkDetection(t *testing.T) {
	// Create a master instance with test configuration
	config := &master.Config{
		Deletion: master.DeletionConfig{
			GCInterval:        5,  // 5 seconds for faster testing
			RetentionPeriod:   10, // 10 seconds retention
			GCDeleteBatchSize: 10,
			TrashDirPrefix:    "/.trash/",
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
		CpuUsage:         0.5,
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
		err := m.ProcessHeartbeat(context.Background(), heartbeatReq)
		if err != nil {
			t.Fatalf("Heartbeat processing failed: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// After confirmation threshold is reached, the chunk should be marked for deletion
	if !m.IsChunkMarkedForDeletion(orphanedChunkHandle) {
		t.Fatalf("Chunk %s should be marked for deletion after repeated orphan reports", orphanedChunkHandle)
	}

	// Ensure orphaned chunk count is zero after cleanup
	if count := m.GetOrphanedChunkCount(); count != 0 {
		t.Fatalf("Orphaned chunk count should be 0 after processing, got %d", count)
	}
}
