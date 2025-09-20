package bitcask

import (
	"fmt"
	"os"
	"testing"

	"github.com/yashagw/kvdb/internal/config"
)

// setupBenchDB creates a temporary database for benchmarking
func setupBenchDB(b *testing.B) (*Bitcask, func()) {
	b.Helper()

	tmpDir, err := os.MkdirTemp("", "bitcask_bench")
	if err != nil {
		b.Fatal(err)
	}

	cfg := config.DefaultConfig()
	cfg.SyncWrites = false              // Faster for benchmarking
	cfg.MaxFileSize = 100 * 1024 * 1024 // 100MB files

	db, err := Open(tmpDir, cfg)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

// BenchmarkPut tests write performance
func BenchmarkPut(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	value := make([]byte, 1024) // 1KB values
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGet tests read performance
func BenchmarkGet(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	// Pre-populate with data
	value := make([]byte, 1024) // 1KB values
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i%numKeys)
		_, err := db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutGet tests mixed read/write performance
func BenchmarkPutGet(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	value := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i%1000) // Cycle through 1000 keys

		if i%2 == 0 {
			// Write operation
			if err := db.Put(key, value); err != nil {
				b.Fatal(err)
			}
		} else {
			// Read operation
			db.Get(key) // Ignore error for non-existent keys
		}
	}
}

// BenchmarkDelete tests delete performance
func BenchmarkDelete(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	// Pre-populate with data
	value := []byte("test_value")
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("delete_key_%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("delete_key_%d", i)
		if err := db.Delete(key); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkKeys tests performance of listing all keys
func BenchmarkKeys(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	// Pre-populate with data
	value := []byte("test_value")
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("keys_bench_%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		keys := db.Keys()
		if len(keys) != numKeys {
			b.Fatalf("Expected %d keys, got %d", numKeys, len(keys))
		}
	}
}

// BenchmarkSync tests sync performance
func BenchmarkSync(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	// Write some data first
	value := []byte("sync_test_value")
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("sync_key_%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFileRotation tests performance when files are being rotated
func BenchmarkFileRotation(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcask_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()
	cfg.SyncWrites = false
	cfg.MaxFileSize = 1024 * 1024 // Small 1MB files to force rotation

	db, err := Open(tmpDir, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	value := make([]byte, 10*1024) // 10KB values to trigger rotation faster

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("rotation_key_%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentReads tests concurrent read performance
func BenchmarkConcurrentReads(b *testing.B) {
	db, cleanup := setupBenchDB(b)
	defer cleanup()

	// Pre-populate with data
	value := make([]byte, 1024)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("concurrent_key_%d", i)
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("concurrent_key_%d", i%numKeys)
			_, err := db.Get(key)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkReopen tests database reopen performance (persistence)
func BenchmarkReopen(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcask_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := config.DefaultConfig()

	// Pre-populate database
	{
		db, err := Open(tmpDir, cfg)
		if err != nil {
			b.Fatal(err)
		}

		value := []byte("reopen_test_value")
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("reopen_key_%d", i)
			if err := db.Put(key, value); err != nil {
				b.Fatal(err)
			}
		}

		db.Close()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db, err := Open(tmpDir, cfg)
		if err != nil {
			b.Fatal(err)
		}

		// Verify data is accessible
		keys := db.Keys()
		if len(keys) == 0 {
			b.Fatal("No keys found after reopen")
		}

		db.Close()
	}
}
