package bitcask

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/yashagw/kvdb/internal/config"
)

// KeyDirEntry represents an entry in the in-memory key directory
type KeyDirEntry struct {
	FileID    uint32 // Which log file contains this key
	ValueSize uint32 // Size of the value
	ValuePos  uint64 // Position of the value in the file
	Timestamp uint32 // When this key was written
}

// Bitcask represents the main database instance
type Bitcask struct {
	mu            sync.RWMutex            // mutex for thread safety
	path          string                  // Directory path for data files
	keyDir        map[string]*KeyDirEntry // In-memory key directory
	activeFile    *LogFile                // Currently active log file for writes
	readOnlyFiles map[uint32]*LogFile     // Read-only log files
	config        *config.Config          // Configuration options
}

// Open opens a Bitcask database at the given path
func Open(path string, cfg *config.Config) (*Bitcask, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	// Create directory if it doesn't exist
	// 0755 sets permissions: owner has read/write/execute (7), group and others have read/execute (5)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	bc := &Bitcask{
		path:          path,
		keyDir:        make(map[string]*KeyDirEntry),
		readOnlyFiles: make(map[uint32]*LogFile),
		config:        cfg,
	}

	// Load existing files and rebuild key directory
	if err := bc.loadFiles(); err != nil {
		return nil, fmt.Errorf("failed to load existing files: %w", err)
	}

	// Create or open active file
	if err := bc.createActiveFile(); err != nil {
		return nil, fmt.Errorf("failed to create active file: %w", err)
	}

	return bc, nil
}

// Close closes the database and all open files
func (bc *Bitcask) Close() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Close active file
	if bc.activeFile != nil {
		if err := bc.activeFile.Close(); err != nil {
			return fmt.Errorf("failed to close active file: %w", err)
		}
	}

	// Close all read-only files
	for _, file := range bc.readOnlyFiles {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close read-only file: %w", err)
		}
	}

	return nil
}

// Keys returns all keys currently in the database
func (bc *Bitcask) Keys() []string {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	keys := make([]string, 0, len(bc.keyDir))
	for key := range bc.keyDir {
		keys = append(keys, key)
	}

	return keys
}

// loadFiles loads existing log files and rebuilds the key directory
func (bc *Bitcask) loadFiles() error {
	files, err := os.ReadDir(bc.path)
	if err != nil {
		return err
	}

	// Find all .bitcask files and sort by ID
	var fileIDs []uint32
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".bitcask") {
			idStr := strings.TrimSuffix(file.Name(), ".bitcask")
			id, err := strconv.ParseUint(idStr, 10, 32)
			if err != nil {
				continue // Skip invalid files
			}
			fileIDs = append(fileIDs, uint32(id))
		}
	}

	sort.Slice(fileIDs, func(i, j int) bool {
		return fileIDs[i] < fileIDs[j]
	})

	// Load files and rebuild key directory
	for _, id := range fileIDs {
		logFile, err := NewLogFile(bc.path, id, true)
		if err != nil {
			return err
		}

		bc.readOnlyFiles[id] = logFile

		// Read all entries to rebuild key directory
		if err := bc.rebuildKeyDir(logFile); err != nil {
			return err
		}
	}

	return nil
}

// createActiveFile creates a new active file for writing
func (bc *Bitcask) createActiveFile() error {
	// Find the next file ID
	var maxID uint32 = 0
	for id := range bc.readOnlyFiles {
		if id > maxID {
			maxID = id
		}
	}

	nextID := maxID + 1

	// Create new active file
	activeFile, err := NewLogFile(bc.path, nextID, false)
	if err != nil {
		return err
	}

	bc.activeFile = activeFile
	return nil
}

// rotateActiveFile moves the current active file to read-only and creates a new active file
func (bc *Bitcask) rotateActiveFile() error {
	// Sync current active file
	if err := bc.activeFile.Sync(); err != nil {
		return err
	}

	// Move to read-only files
	bc.readOnlyFiles[bc.activeFile.ID()] = bc.activeFile

	// Create new active file
	return bc.createActiveFile()
}

// rebuildKeyDir rebuilds the key directory from a log file
func (bc *Bitcask) rebuildKeyDir(logFile *LogFile) error {
	var pos int64 = 0

	for {
		entry, nextPos, err := logFile.ReadEntry(pos)
		if err != nil {
			if err.Error() == "EOF" {
				break // End of file
			}
			return err
		}

		key := string(entry.Key)

		// If value size is 0, this is a tombstone (deletion)
		if entry.ValueSize == 0 {
			delete(bc.keyDir, key)
		} else {
			// Calculate value position
			valuePos := pos + 12 + int64(entry.KeySize)

			bc.keyDir[key] = &KeyDirEntry{
				FileID:    logFile.ID(),
				ValueSize: entry.ValueSize,
				ValuePos:  uint64(valuePos),
				Timestamp: entry.Timestamp,
			}
		}

		pos = nextPos
	}

	return nil
}
