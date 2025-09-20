package bitcask

import (
	"fmt"
	"time"
)

// Put stores a key-value pair
func (bc *Bitcask) Put(key string, value []byte) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Check if we need to rotate the active file
	if bc.activeFile.Size() >= bc.config.MaxFileSize {
		if err := bc.rotateActiveFile(); err != nil {
			return fmt.Errorf("failed to rotate active file: %w", err)
		}
	}

	// Create log entry
	entry := &LogEntry{
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       []byte(key),
		Value:     value,
	}

	// Write to active file
	valuePos, err := bc.activeFile.Write(entry)
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	// Sync if configured, otherwise just flush to make data readable
	if bc.config.SyncWrites {
		if err := bc.activeFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}
	} else {
		// Flush buffer to make data immediately readable
		if err := bc.activeFile.Flush(); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
	}

	// Update key directory
	bc.keyDir[key] = &KeyDirEntry{
		FileID:    bc.activeFile.ID(),
		ValueSize: entry.ValueSize,
		ValuePos:  valuePos,
		Timestamp: entry.Timestamp,
	}

	return nil
}

// Get retrieves a value by key
func (bc *Bitcask) Get(key string) ([]byte, error) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	// Look up key in key directory
	keyDirEntry, exists := bc.keyDir[key]
	if !exists {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	var logFile *LogFile
	if keyDirEntry.FileID == bc.activeFile.ID() {
		logFile = bc.activeFile
	} else {
		logFile, exists = bc.readOnlyFiles[keyDirEntry.FileID]
		if !exists {
			return nil, fmt.Errorf("log file not found for file ID: %d", keyDirEntry.FileID)
		}
	}

	// Read value from file
	value, err := logFile.Read(keyDirEntry.ValuePos, keyDirEntry.ValueSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}

	return value, nil
}

// Delete deletes a key by writing a tombstone
func (bc *Bitcask) Delete(key string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Check if key exists
	if _, exists := bc.keyDir[key]; !exists {
		return fmt.Errorf("key not found: %s", key)
	}

	// Create tombstone entry (zero value size)
	entry := &LogEntry{
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: 0, // Tombstone
		Key:       []byte(key),
		Value:     nil,
	}

	// Write tombstone to active file
	_, err := bc.activeFile.Write(entry)
	if err != nil {
		return fmt.Errorf("failed to write tombstone: %w", err)
	}

	// Sync if configured, otherwise just flush to make data readable
	if bc.config.SyncWrites {
		if err := bc.activeFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}
	} else {
		// Flush buffer to make data immediately readable
		if err := bc.activeFile.Flush(); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
	}

	// Remove from key directory
	delete(bc.keyDir, key)

	return nil
}

// Sync forces a sync of the active file to disk
func (bc *Bitcask) Sync() error {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	if bc.activeFile != nil {
		return bc.activeFile.Sync()
	}

	return nil
}
