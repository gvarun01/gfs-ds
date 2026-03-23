package master

import (
	"sync"
	"time"

	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	client_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/client_master"
	"google.golang.org/grpc"
)

type ChunkInfo struct {
	Size            int64
	Version         int32
	Locations       map[string]bool
	ServerAddresses map[string]string
	Primary         string
	LeaseExpiration time.Time
	StaleReplicas   map[string]bool
	
	// Snapshot support - reference counting for copy-on-write
	RefCount        int32          // Number of files/snapshots referencing this chunk
	IsSnapshot      bool          // True if this chunk was created via CoW
	OriginalHandle  string        // Original chunk handle for CoW chunks
	
	mu              sync.RWMutex
}

type FileInfo struct {
	Chunks map[int64]string
	
	// Snapshot support
	IsSnapshot     bool      // True if this file is a snapshot
	SnapshotTime   time.Time // When snapshot was created
	OriginalPath   string    // Original file path for snapshots
	
	mu     sync.RWMutex
}

// SnapshotInfo tracks snapshot metadata
type SnapshotInfo struct {
	SnapshotPath   string    // Path where snapshot is stored
	OriginalPath   string    // Original file/directory path
	CreationTime   time.Time // When snapshot was created
	ChunkHandles   []string  // List of chunk handles in snapshot
	IsDirectory    bool      // True if snapshot is of a directory
	mu             sync.RWMutex
}

type ServerInfo struct {
	Address        string
	LastHeartbeat  time.Time
	AvailableSpace int64
	CPUUsage       float64
	ActiveOps      int32
	Chunks         map[string]bool
	LastUpdated    time.Time
	Status         string
	FailureCount   int
	mu             sync.RWMutex
}

// OrphanedChunk represents a chunk reported by chunkserver but unknown to master
// These chunks need to be tracked and eventually deleted to prevent storage waste
type OrphanedChunk struct {
	ChunkHandle   string    // Handle of the orphaned chunk
	ServerID      string    // ID of the server reporting the chunk
	DetectedAt    time.Time // When the orphan was first detected
	LastSeen      time.Time // Last time this orphan was reported
	Size         int64     // Size of the orphaned chunk
	ConfirmCount  int32     // Number of times this orphan has been confirmed
	Version      int32     // Version reported by chunkserver
	mu           sync.RWMutex
}

// ExpeditedDeletion represents a file that was deleted twice and should be immediately removed
// This implements the GFS paper Section 4.4 expedited deletion for double-deleted files
type ExpeditedDeletion struct {
	TrashPath      string    // Path of the file in trash
	OriginalPath   string    // Original path before first deletion
	FirstDeletedAt time.Time // When the file was first deleted
	SecondDeletedAt time.Time // When the file was deleted again
	ProcessedAt    time.Time // When the expedited deletion was processed
	IsProcessed    bool      // Whether this has been processed
	mu            sync.RWMutex
}

type Master struct {
	Config *Config

	// File namespace - B-tree implementation for hierarchical directories
	namespace *BTreeNamespace
	filesMu   sync.RWMutex

	// Chunk management
	chunks   map[string]*ChunkInfo // chunk_handle -> chunk info
	chunksMu sync.RWMutex

	deletedChunks   map[string]bool
	deletedChunksMu sync.RWMutex

	// Orphaned chunk tracking for proper GFS garbage collection
	orphanedChunks   map[string]*OrphanedChunk // chunk_handle -> orphan info
	orphanedChunksMu sync.RWMutex

	// Expedited deletion tracking for double-deleted files
	expeditedDeletions   map[string]*ExpeditedDeletion // trash_path -> expedited deletion info
	expeditedDeletionsMu sync.RWMutex

	// Snapshot management
	snapshots   map[string]*SnapshotInfo // snapshot_path -> snapshot info
	snapshotsMu sync.RWMutex
	nextChunkHandle int64 // For generating new chunk handles during CoW
	chunkHandleMu sync.Mutex

	// Server management
	servers   map[string]*ServerInfo // server_id -> server info
	serversMu sync.RWMutex

	// Chunk server manager
	chunkServerMgr *ChunkServerManager

	gcInProgress bool
	gcMu         sync.Mutex

	pendingOpsMu sync.RWMutex
	pendingOps   map[string][]*PendingOperation // serverId -> pending operations

	opLog *OperationLog
}

type ChunkServerManager struct {
	mu            sync.RWMutex
	activeStreams map[string]chan *chunk_pb.HeartBeatResponse
}

type MasterServer struct {
	client_pb.UnimplementedClientMasterServiceServer
	chunk_pb.UnimplementedChunkMasterServiceServer
	Master     *Master
	grpcServer *grpc.Server
}

type PendingOperation struct {
	Type         chunk_pb.ChunkCommand_CommandType
	ChunkHandle  string
	Targets      []string
	Source       string
	AttemptCount int
	LastAttempt  time.Time
	CreatedAt    time.Time
}
