// Package config provides centralized configuration management for the GFS system.
// It consolidates all configuration files into a single, well-structured format.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/pkg/constants"
	"gopkg.in/yaml.v3"
)

// Config represents the complete configuration for the GFS system
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Storage StorageConfig `yaml:"storage"`
	Network NetworkConfig `yaml:"network"`
	Timing  TimingConfig  `yaml:"timing"`
	Logging LoggingConfig `yaml:"logging"`
}

// ServerConfig contains server-specific configuration
type ServerConfig struct {
	// Type specifies the server type (master, chunkserver, client)
	Type string `yaml:"type"`

	// ID is a unique identifier for this server instance
	ID string `yaml:"id"`

	// Environment specifies the deployment environment (development, staging, production)
	Environment string `yaml:"environment"`
}

// StorageConfig contains storage-related configuration
type StorageConfig struct {
	// ChunkSize is the size of each chunk in bytes
	ChunkSize int64 `yaml:"chunk_size"`

	// MaxChunkSize is the maximum allowed chunk size
	MaxChunkSize int64 `yaml:"max_chunk_size"`

	// ReplicationFactor is the number of replicas per chunk
	ReplicationFactor int `yaml:"replication_factor"`

	// DataDir is the directory for storing chunk data
	DataDir string `yaml:"data_dir"`

	// MetadataDir is the directory for storing metadata
	MetadataDir string `yaml:"metadata_dir"`

	// EnableChecksum enables checksum verification
	EnableChecksum bool `yaml:"enable_checksum"`
}

// NetworkConfig contains network-related configuration
type NetworkConfig struct {
	// Host is the host address to bind to
	Host string `yaml:"host"`

	// Port is the port number to listen on
	Port int `yaml:"port"`

	// MasterAddress is the address of the master server (for chunkservers and clients)
	MasterAddress string `yaml:"master_address"`

	// MaxConnections is the maximum number of concurrent connections
	MaxConnections int `yaml:"max_connections"`

	// ReadTimeout is the timeout for read operations
	ReadTimeout time.Duration `yaml:"read_timeout"`

	// WriteTimeout is the timeout for write operations
	WriteTimeout time.Duration `yaml:"write_timeout"`

	// KeepAlive enables TCP keep-alive
	KeepAlive bool `yaml:"keep_alive"`
}

// TimingConfig contains timing-related configuration
type TimingConfig struct {
	// HeartbeatInterval is the interval between heartbeat messages
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`

	// LeaseTimeout is the duration of a chunk lease
	LeaseTimeout time.Duration `yaml:"lease_timeout"`

	// RetryAttempts is the number of retry attempts for failed operations
	RetryAttempts int `yaml:"retry_attempts"`

	// RetryDelay is the delay between retry attempts
	RetryDelay time.Duration `yaml:"retry_delay"`

	// GCInterval is the garbage collection interval
	GCInterval time.Duration `yaml:"gc_interval"`

	// CheckpointInterval is the metadata checkpointing interval
	CheckpointInterval time.Duration `yaml:"checkpoint_interval"`
}

// LoggingConfig contains logging-related configuration
type LoggingConfig struct {
	// Level is the logging level (debug, info, warn, error)
	Level string `yaml:"level"`

	// Format is the log format (text, json)
	Format string `yaml:"format"`

	// OutputPath is the path to write logs to (stdout, stderr, or file path)
	OutputPath string `yaml:"output_path"`

	// MaxFileSize is the maximum size of a log file before rotation
	MaxFileSize int64 `yaml:"max_file_size"`

	// MaxFiles is the maximum number of log files to keep
	MaxFiles int `yaml:"max_files"`

	// EnableConsole enables console output even when writing to file
	EnableConsole bool `yaml:"enable_console"`
}

// LoadConfig loads configuration from the specified file path
// If no path is provided, it attempts to load from default locations
func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		configPath = findDefaultConfigPath()
	}

	if !fileExists(configPath) {
		return nil, fmt.Errorf("configuration file not found: %s", configPath)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %v", configPath, err)
	}

	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %v", configPath, err)
	}

	// Apply defaults and validate
	if err := config.setDefaults(); err != nil {
		return nil, fmt.Errorf("failed to apply defaults: %v", err)
	}

	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %v", err)
	}

	return config, nil
}

// LoadConfigFromEnvironment loads configuration based on the environment
func LoadConfigFromEnvironment(env string) (*Config, error) {
	configPath := filepath.Join(constants.DefaultConfigDir, env, "config.yaml")
	return LoadConfig(configPath)
}

// setDefaults applies default values to unset configuration options
func (c *Config) setDefaults() error {
	// Server defaults
	if c.Server.Environment == "" {
		c.Server.Environment = "development"
	}

	// Storage defaults
	if c.Storage.ChunkSize == 0 {
		c.Storage.ChunkSize = constants.DefaultChunkSize
	}
	if c.Storage.MaxChunkSize == 0 {
		c.Storage.MaxChunkSize = constants.MaxChunkSize
	}
	if c.Storage.ReplicationFactor == 0 {
		c.Storage.ReplicationFactor = constants.DefaultReplicationFactor
	}
	if c.Storage.DataDir == "" {
		c.Storage.DataDir = constants.DefaultStorageDir
	}
	if c.Storage.MetadataDir == "" {
		c.Storage.MetadataDir = filepath.Join(constants.DefaultStorageDir, constants.MasterStorageSubdir)
	}

	// Network defaults
	if c.Network.Host == "" {
		c.Network.Host = constants.DefaultHost
	}
	if c.Network.Port == 0 {
		c.Network.Port = constants.DefaultMasterPort
	}
	if c.Network.MaxConnections == 0 {
		c.Network.MaxConnections = constants.MaxConcurrentOperations
	}
	if c.Network.ReadTimeout == 0 {
		c.Network.ReadTimeout = constants.DefaultTimeout
	}
	if c.Network.WriteTimeout == 0 {
		c.Network.WriteTimeout = constants.DefaultTimeout
	}

	// Timing defaults
	if c.Timing.HeartbeatInterval == 0 {
		c.Timing.HeartbeatInterval = constants.HeartbeatInterval
	}
	if c.Timing.LeaseTimeout == 0 {
		c.Timing.LeaseTimeout = constants.LeaseTimeout
	}
	if c.Timing.RetryAttempts == 0 {
		c.Timing.RetryAttempts = constants.MaxRetryAttempts
	}
	if c.Timing.RetryDelay == 0 {
		c.Timing.RetryDelay = time.Second
	}
	if c.Timing.GCInterval == 0 {
		c.Timing.GCInterval = constants.GarbageCollectionInterval
	}
	if c.Timing.CheckpointInterval == 0 {
		c.Timing.CheckpointInterval = constants.MetadataCheckpointInterval
	}

	// Logging defaults
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "text"
	}
	if c.Logging.OutputPath == "" {
		c.Logging.OutputPath = "stdout"
	}
	if c.Logging.MaxFileSize == 0 {
		c.Logging.MaxFileSize = constants.MaxLogFileSize
	}
	if c.Logging.MaxFiles == 0 {
		c.Logging.MaxFiles = constants.MaxLogFiles
	}

	return nil
}

// validate ensures the configuration values are valid
func (c *Config) validate() error {
	// Validate server configuration
	if c.Server.Type == "" {
		return fmt.Errorf("server type must be specified")
	}

	validServerTypes := map[string]bool{
		"master":      true,
		"chunkserver": true,
		"client":      true,
	}
	if !validServerTypes[c.Server.Type] {
		return fmt.Errorf("invalid server type: %s", c.Server.Type)
	}

	// Validate storage configuration
	if c.Storage.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be positive")
	}
	if c.Storage.MaxChunkSize < c.Storage.ChunkSize {
		return fmt.Errorf("max chunk size must be >= chunk size")
	}
	if c.Storage.ReplicationFactor <= 0 {
		return fmt.Errorf("replication factor must be positive")
	}

	// Validate network configuration
	if c.Network.Port <= 0 || c.Network.Port > 65535 {
		return fmt.Errorf("invalid port number: %d", c.Network.Port)
	}
	if c.Network.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive")
	}

	// Validate timing configuration
	if c.Timing.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat interval must be positive")
	}
	if c.Timing.LeaseTimeout <= 0 {
		return fmt.Errorf("lease timeout must be positive")
	}
	if c.Timing.RetryAttempts <= 0 {
		return fmt.Errorf("retry attempts must be positive")
	}

	// Validate logging configuration
	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[c.Logging.Level] {
		return fmt.Errorf("invalid log level: %s", c.Logging.Level)
	}

	validLogFormats := map[string]bool{
		"text": true,
		"json": true,
	}
	if !validLogFormats[c.Logging.Format] {
		return fmt.Errorf("invalid log format: %s", c.Logging.Format)
	}

	return nil
}

// findDefaultConfigPath attempts to find a default configuration file
func findDefaultConfigPath() string {
	// Look for config files in order of preference
	candidates := []string{
		"config.yaml",
		"configs/config.yaml",
		"configs/development/config.yaml",
		"configs/general-config.yml", // Legacy support
	}

	for _, path := range candidates {
		if fileExists(path) {
			return path
		}
	}

	return ""
}

// fileExists checks if a file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// GetMasterAddress returns the master address in host:port format
func (c *Config) GetMasterAddress() string {
	if c.Network.MasterAddress != "" {
		return c.Network.MasterAddress
	}
	return fmt.Sprintf("%s:%d", c.Network.Host, c.Network.Port)
}

// GetServerAddress returns the server address in host:port format
func (c *Config) GetServerAddress() string {
	return fmt.Sprintf("%s:%d", c.Network.Host, c.Network.Port)
}

// IsProduction returns true if running in production environment
func (c *Config) IsProduction() bool {
	return c.Server.Environment == "production"
}

// IsDevelopment returns true if running in development environment
func (c *Config) IsDevelopment() bool {
	return c.Server.Environment == "development"
}
