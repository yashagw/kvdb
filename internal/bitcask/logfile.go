package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// LogEntry represents a single entry in the log file
type LogEntry struct {
	Timestamp uint32 // Unix timestamp
	KeySize   uint32 // Size of the key in bytes
	ValueSize uint32 // Size of the value in bytes (0 for tombstone)
	Key       []byte // The key
	Value     []byte // The value (empty for tombstone)
}

// LogFile represents a single log file in the Bitcask database
type LogFile struct {
	id       uint32        // Unique identifier for this file
	file     *os.File      // The underlying file handle
	writer   *bufio.Writer // Buffered writer for better performance
	size     int64         // Current size of the file
	readOnly bool          // Whether this file is read-only
}

// NewLogFile creates a new log file
func NewLogFile(path string, id uint32, readOnly bool) (*LogFile, error) {
	filename := filepath.Join(path, fmt.Sprintf("%010d.bitcask", id))

	var file *os.File
	var err error

	if readOnly {
		file, err = os.OpenFile(filename, os.O_RDONLY, 0)
	} else {
		file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", filename, err)
	}

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}

	logFile := &LogFile{
		id:       id,
		file:     file,
		size:     stat.Size(),
		readOnly: readOnly,
	}

	if !readOnly {
		logFile.writer = bufio.NewWriter(file)
	}

	return logFile, nil
}

// Size returns the current size of the file
func (lf *LogFile) Size() int64 {
	return lf.size
}

// ID returns the file ID
func (lf *LogFile) ID() uint32 {
	return lf.id
}

// Sync flushes any buffered data to disk
func (lf *LogFile) Sync() error {
	if lf.readOnly {
		return nil
	}

	if err := lf.writer.Flush(); err != nil {
		return err
	}

	return lf.file.Sync()
}

// Flush flushes the buffer without syncing to disk
func (lf *LogFile) Flush() error {
	if lf.readOnly || lf.writer == nil {
		return nil
	}
	return lf.writer.Flush()
}

// Close closes the log file
func (lf *LogFile) Close() error {
	if !lf.readOnly && lf.writer != nil {
		if err := lf.writer.Flush(); err != nil {
			return err
		}
	}

	return lf.file.Close()
}

// Write writes a log entry to the file
// Returns the valuePos in the file
func (lf *LogFile) Write(entry *LogEntry) (uint64, error) {
	if lf.readOnly {
		return 0, fmt.Errorf("cannot write to read-only file")
	}

	// Calculate total entry size
	// timestamp + keysize + valuesize + key + value
	entrySize := 4 + 4 + 4 + len(entry.Key) + len(entry.Value)

	// Write timestamp
	if err := binary.Write(lf.writer, binary.LittleEndian, entry.Timestamp); err != nil {
		return 0, err
	}

	// Write key size
	if err := binary.Write(lf.writer, binary.LittleEndian, entry.KeySize); err != nil {
		return 0, err
	}

	// Write value size
	if err := binary.Write(lf.writer, binary.LittleEndian, entry.ValueSize); err != nil {
		return 0, err
	}

	// Write key
	if _, err := lf.writer.Write(entry.Key); err != nil {
		return 0, err
	}

	// Write value
	if _, err := lf.writer.Write(entry.Value); err != nil {
		return 0, err
	}

	// Record the position where the value starts
	// currentFileSize + 12 bytes for timestamp + keysize
	valuePos := lf.size + 12 + int64(len(entry.Key))

	lf.size += int64(entrySize)

	return uint64(valuePos), nil
}

// Read reads a value at the specified position
func (lf *LogFile) Read(valuePos uint64, valueSize uint32) ([]byte, error) {
	value := make([]byte, valueSize)

	_, err := lf.file.ReadAt(value, int64(valuePos))
	if err != nil {
		return nil, fmt.Errorf("failed to read value at position %d: %w", valuePos, err)
	}

	return value, nil
}

// ReadEntry reads a complete log entry starting at the given position
func (lf *LogFile) ReadEntry(pos int64) (*LogEntry, int64, error) {
	// Seek to position
	if _, err := lf.file.Seek(pos, 0); err != nil {
		return nil, 0, err
	}

	reader := bufio.NewReader(lf.file)

	// Read timestamp
	var timestamp uint32
	if err := binary.Read(reader, binary.LittleEndian, &timestamp); err != nil {
		return nil, 0, err
	}

	// Read key size
	var keySize uint32
	if err := binary.Read(reader, binary.LittleEndian, &keySize); err != nil {
		return nil, 0, err
	}

	// Read value size
	var valueSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &valueSize); err != nil {
		return nil, 0, err
	}

	// Read key
	key := make([]byte, keySize)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, 0, err
	}

	// Read value
	value := make([]byte, valueSize)
	if _, err := io.ReadFull(reader, value); err != nil {
		return nil, 0, err
	}

	entry := &LogEntry{
		Timestamp: timestamp,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}

	// Calculate next position
	nextPos := pos + 12 + int64(keySize) + int64(valueSize)

	return entry, nextPos, nil
}
