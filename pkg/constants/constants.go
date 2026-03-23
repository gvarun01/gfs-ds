// Package constants defines all system-wide constants for the GFS implementation.
// This centralizes configuration values and prevents hardcoded values throughout the codebase.
package constants

import "time"

// Chunk and storage constants
const (
	// DefaultChunkSize is the default size of each chunk in bytes (64MB as per GFS paper)
	DefaultChunkSize = 64 * 1024 * 1024

	// MaxChunkSize is the maximum allowed chunk size
	MaxChunkSize = 64 * 1024 * 1024

	// ChecksumBlockSize is the size of each checksum block (64KB)
	ChecksumBlockSize = 64 * 1024

	// DefaultReplicationFactor is the default number of replicas per chunk
	DefaultReplicationFactor = 3
)

// Network constants
const (
	// DefaultMasterPort is the default port for the master server
	DefaultMasterPort = 50051

	// DefaultChunkServerStartPort is the starting port for chunk servers
	DefaultChunkServerStartPort = 8001

	// DefaultHost is the default host address
	DefaultHost = "localhost"

	// MaxRetryAttempts is the maximum number of retry attempts for failed operations
	MaxRetryAttempts = 3
)

// Timing constants
const (
	// DefaultTimeout is the default timeout for network operations
	DefaultTimeout = 30 * time.Second

	// HeartbeatInterval is the interval between heartbeat messages
	HeartbeatInterval = 10 * time.Second

	// LeaseTimeout is the duration of a chunk lease
	LeaseTimeout = 60 * time.Second

	// MetadataCheckpointInterval is the interval for metadata checkpointing
	MetadataCheckpointInterval = 5 * time.Second

	// GarbageCollectionInterval is the interval for garbage collection
	GarbageCollectionInterval = 30 * time.Minute
)

// File and directory paths
const (
	// DefaultConfigDir is the default directory for configuration files
	DefaultConfigDir = "configs"

	// DefaultStorageDir is the default directory for chunk storage
	DefaultStorageDir = "storage"

	// DefaultLogDir is the default directory for log files
	DefaultLogDir = "logs"

	// MasterStorageSubdir is the subdirectory for master metadata
	MasterStorageSubdir = "master"

	// ChunkStorageSubdir is the subdirectory for chunk data
	ChunkStorageSubdir = "chunks"

	// TrashSubdir is the subdirectory for soft-deleted files
	TrashSubdir = ".trash"
)

// Error messages and codes
const (
	// ErrFileNotFound is returned when a file is not found
	ErrFileNotFound = "file not found"

	// ErrChunkNotFound is returned when a chunk is not found
	ErrChunkNotFound = "chunk not found"

	// ErrInvalidFilename is returned for invalid filenames
	ErrInvalidFilename = "invalid filename"

	// ErrInvalidOperation is returned for invalid operations
	ErrInvalidOperation = "invalid operation"

	// ErrNetworkFailure is returned for network failures
	ErrNetworkFailure = "network failure"
)

// Configuration file names
const (
	// GeneralConfigFile is the general configuration file name
	GeneralConfigFile = "config.yaml"

	// MasterConfigFile is the master-specific configuration file name
	MasterConfigFile = "master-config.yaml"

	// ChunkServerConfigFile is the chunkserver-specific configuration file name
	ChunkServerConfigFile = "chunkserver-config.yaml"

	// ClientConfigFile is the client-specific configuration file name
	ClientConfigFile = "client-config.yaml"
)

// Logging constants
const (
	// LogTimeFormat is the time format for log entries
	LogTimeFormat = "2006-01-02T15:04:05.000Z"

	// MaxLogFileSize is the maximum size of a log file before rotation
	MaxLogFileSize = 100 * 1024 * 1024 // 100MB

	// MaxLogFiles is the maximum number of log files to keep
	MaxLogFiles = 10
)

// Performance constants
const (
	// DefaultBufferSize is the default buffer size for I/O operations
	DefaultBufferSize = 8 * 1024 // 8KB

	// MaxConcurrentOperations is the maximum number of concurrent operations
	MaxConcurrentOperations = 100

	// DefaultQueueSize is the default size for operation queues
	DefaultQueueSize = 1000
)
