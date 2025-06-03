package datastore

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenAndClose(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close db: %v", err)
	}
}

func TestPutAndGet(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	key := "k"
	value := "v"

	if err := db.Put(key, value); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if result != value {
		t.Errorf("Expected %s, got %s", value, result)
	}
}

func TestGetNonExistentKey(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	_, err = db.Get("non-existent")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestOverwriteKey(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	key := "k"
	value1 := "v1"
	value2 := "v2"

	if err := db.Put(key, value1); err != nil {
		t.Fatalf("Failed to put first value: %v", err)
	}

	if err := db.Put(key, value2); err != nil {
		t.Fatalf("Failed to put second value: %v", err)
	}

	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if result != value2 {
		t.Errorf("Expected %s, got %s", value2, result)
	}
}

func TestPersistence(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	key := "k"
	value := "v"

	func() {
		db, err := Open(dir)
		if err != nil {
			t.Fatalf("Failed to open db: %v", err)
		}
		defer db.Close()

		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to reopen db: %v", err)
	}
	defer db.Close()

	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get after reopen: %v", err)
	}

	if result != value {
		t.Errorf("Expected %s, got %s", value, result)
	}
}

func TestRotateSegment(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	segLen := len(db.segments)
	if segLen != 0 {
		t.Fatalf("Expected 0 segment before rotation, got %d", segLen)
	}
	beforeCurSeg := db.currentSegment.Name()

	key := "k"
	value := "v"
	if err := db.Put(key, value); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateSegmentLocked(); err != nil {
		t.Fatalf("Failed to rotate segment: %v", err)
	}
	db.mu.Unlock()

	segLen = len(db.segments)
	if segLen != 1 {
		t.Errorf("Expected 1 segment after rotation, got %d", segLen)
	}

	afterCurSeg := db.currentSegment.Name()
	if beforeCurSeg == afterCurSeg {
		t.Errorf("Expected new file for a new current segment, got %s", afterCurSeg)
	}
}

func TestCompact(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	key := "k"
	value := "v"
	recordSize := recordHeaderSize + len(key) + len(value)
	db, err := open(dir, int64(recordSize), defaultCompactionThreshold)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	for i := 0; i < defaultCompactionThreshold; i++ {
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put record %d: %v", i, err)
		}
	}

	// Simulate passing the threshold
	db.mu.Lock()
	ts := time.Now().UnixNano()
	if err := db.rotateSegmentLocked(); err != nil {
		t.Fatalf("Failed to rotate segment: %v", err)
	}
	db.mu.Unlock()
	if err := db.compact(ts); err != nil {
		t.Fatalf("Failed to compact segment: %v", err)
	}

	finalSegments := len(db.segments)
	if finalSegments != 1 {
		t.Errorf("Expected 1 segment after compaction, got %d", finalSegments)
	}

	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after compaction: %v", err)
	}
	if result != value {
		t.Errorf("Value mismatch after compaction: expected %s, got %s", value, result)
	}
}

func TestSize(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	initialSize, err := db.Size()
	if err != nil {
		t.Fatalf("Failed to get initial size: %v", err)
	}

	if err := db.Put("test", "data"); err != nil {
		t.Fatalf("Failed to put data: %v", err)
	}

	finalSize, err := db.Size()
	if err != nil {
		t.Fatalf("Failed to get final size: %v", err)
	}

	if finalSize <= initialSize {
		t.Errorf("Expected size to increase after putting data")
	}
}

func TestMultipleKeys(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for key, value := range testData {
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	for key, expectedValue := range testData {
		result, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if result != expectedValue {
			t.Errorf("Value mismatch for %s: expected %s, got %s",
				key, expectedValue, result)
		}
	}
}

func createTempDir(t *testing.T) string {
	dir := filepath.Join(os.TempDir(), "db-test-"+string(rune(time.Now().UnixNano())))
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}
