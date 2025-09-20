package config

import "time"

// Config holds configuration options for Bitcask
type Config struct {
	MaxFileSize        int64         // Maximum file size before rotation
	SyncWrites         bool          // Whether to sync writes to disk immediately
	CompactionInterval time.Duration // How often to check for compaction
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		MaxFileSize:        1024 * 1024 * 1024, // 1GB
		SyncWrites:         false,
		CompactionInterval: time.Minute * 10,
	}
}
