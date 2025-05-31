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

	key := "test-key"
	value := "test-value"

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

	key := "test-key"
	value1 := "first-value"
	value2 := "second-value"

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

	key := "persistent-key"
	value := "persistent-value"

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

func TestSegmentRotation(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	db, err := open(dir, 100, defaultCompactionThreshold)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	largeValue := make([]byte, 150)
	for i := range largeValue {
		largeValue[i] = 'a'
	}

	if err := db.Put("key1", string(largeValue)); err != nil {
		t.Fatalf("Failed to put large value: %v", err)
	}

	if err := db.Put("key2", "small"); err != nil {
		t.Fatalf("Failed to put second value: %v", err)
	}

	result1, err := db.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if result1 != string(largeValue) {
		t.Errorf("Value mismatch for key1")
	}

	result2, err := db.Get("key2")
	if err != nil {
		t.Fatalf("Failed to get key2: %v", err)
	}
	if result2 != "small" {
		t.Errorf("Expected 'small', got %s", result2)
	}

	if len(db.segments) == 0 {
		t.Error("Expected segment rotation to create segment files")
	}
}

func TestCompaction(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	key := "k"
	value := "v"
	recordSize := 12 + len(key) + len(value)
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

	initialSegments := len(db.segments)
	if initialSegments != defaultCompactionThreshold-1 {
		t.Errorf("Expected exactly %d segments before compaction, got %d",
			defaultCompactionThreshold, initialSegments)
	}

	if err := db.Put(key, value); err != nil {
		t.Fatalf("Failed to put trigger record: %v", err)
	}

	// FIXME: This is a temporary fix, because previous version of this
	// test did not expect compact() to run concurrently
	// NOTE: We should probably rewrite the tests for compaction altogether
	// to use the compact method directly
	time.Sleep(100 * time.Millisecond)
	db.waitForCompacted()

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

func TestCompactionWithOverwrites(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	key := "k"
	oldValue := "old"
	newValue := "new"
	recordSize := 12 + len(key) + len(oldValue)
	db, err := open(dir, int64(recordSize), defaultCompactionThreshold)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	for i := 0; i < defaultCompactionThreshold; i++ {
		if err := db.Put(key, oldValue); err != nil {
			t.Fatalf("Failed to put old value: %v", err)
		}
	}

	if err := db.Put(key, newValue); err != nil {
		t.Fatalf("Failed to overwrite key: %v", err)
	}

	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after compaction: %v", err)
	}
	if result != newValue {
		t.Errorf("Expected '%s', got %s", newValue, result)
	}
}

func TestMultipleCompactions(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	key := "k"
	val := "v"
	recordSize := 12 + len(key) + len(val)
	db, err := open(dir, int64(recordSize), defaultCompactionThreshold)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	for i := 0; i < (defaultCompactionThreshold+1)*2; i++ {
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Failed to put old value: %v", err)
		}
	}

	result, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key after compaction: %v", err)
	}
	if result != val {
		t.Errorf("Expected '%s', got %s", val, result)
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
