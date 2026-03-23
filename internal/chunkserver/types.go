package chunkserver

import (
	"sync"
	"time"

	chunkserver_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk"
	chunk_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_master"
	chunk_ops "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/chunk_operations"
	common_pb "github.com/Mit-Vin/GFS-Distributed-Systems/api/proto/common"

	"google.golang.org/grpc"
)

// GFS paper Section 5.2: 64KB checksum blocks
const CHECKSUM_BLOCK_SIZE = 64 * 1024 // 64KB blocks as per GFS paper

type BlockChecksum struct {
	BlockIndex uint32 `json:"block_index"` // Index of 64KB block within chunk
	Checksum   uint32 `json:"checksum"`    // CRC32 checksum of this block
}

type ChunkMetadata struct {
	Size           int64           `json:"size"`
	LastModified   time.Time       `json:"last_modified"`
	Checksum       uint32          `json:"checksum"`        // Legacy: full chunk checksum (will be deprecated)
	BlockChecksums []BlockChecksum `json:"block_checksums"` // NEW: 64KB block-level checksums
	Version        int32           `json:"version"`
}

type ChunkServer struct {
	mu sync.RWMutex

	// Server identification
	serverID string
	address  string

	config *Config

	dataDir   string
	serverDir string // Complete path including serverID
	chunks    map[string]*ChunkMetadata

	// Operation coordination
	operationQueue *OperationQueue
	leases         map[string]time.Time

	// Master connection
	masterClient  chunk_pb.ChunkMasterServiceClient
	heartbeatStop chan struct{}

	// Server state
	availableSpace int64
	isRunning      bool

	// Append request state
	idempotencyIdStatusMap     map[string]AppendStatus
	idempotencyIdStatusMapLock sync.RWMutex

	chunkPrimary map[string]bool

	pendingData     map[string]map[string]*PendingData // operationID -> chunkHandle -> data
	pendingDataLock sync.RWMutex

	grpcServer *grpc.Server

	chunk_ops.UnimplementedChunkOperationServiceServer
	chunkserver_pb.UnimplementedChunkServiceServer
}

type Operation struct {
	OperationId      string
	IdempotentencyId string
	Type             OperationType
	ChunkHandle      string
	Offset           int64
	Data             []byte
	Checksum         uint32
	Secondaries      []*common_pb.ChunkLocation
	ResponseChan     chan OperationResult
}

type OperationType int

const (
	OpWrite OperationType = iota
	OpRead
	OpReplicate
	OpAppend
)

type AppendPhaseType int

const (
	AppendPhaseOne AppendPhaseType = iota
	AppendPhaseTwo
	AppendNullify
)

type AppendStatus int

// AppendStatus represents the current state of an append operation
// Note: Enhanced status reporting could be implemented for detailed operation tracking
const (
	AppendReceived AppendStatus = iota
	AppendCompleted
	AppendFailed
)

type OperationResult struct {
	Status common_pb.Status
	Data   []byte
	Offset int64
	Error  error
}

type OperationQueue struct {
	mu       sync.Mutex
	queue    []*Operation
	notEmpty chan struct{}
}

type PendingData struct {
	Data     []byte
	Checksum uint32
	Offset   int64
}
