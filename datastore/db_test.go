package datastore

import (
	"path/filepath"
	"strconv"
	"testing"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})
}

func TestSegmentCreation(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenWithSegmentSize(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		key := "key" + strconv.Itoa(i)
		value := string(make([]byte, 200))
		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	expectedSegments := 2
	actualSegments := countSegmentFiles(dir)
	if actualSegments != expectedSegments {
		t.Errorf("expected %d segments, got %d", expectedSegments, actualSegments)
	}

	for i := 0; i < 5; i++ {
		key := "key" + strconv.Itoa(i)
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
		}
		if len(value) != 200 {
			t.Errorf("expected value length 200, got %d", len(value))
		}
	}
}

func TestSegmentMerging(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenWithSegmentSize(dir, 512)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		key := "key" + strconv.Itoa(i)
		value := string(make([]byte, 100))
		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	segmentsBeforeMerge := countSegmentFiles(dir)
	if segmentsBeforeMerge < 3 {
		t.Errorf("expected at least 3 segments before merge, got %d", segmentsBeforeMerge)
	}

	err = db.mergeSegments()
	if err != nil {
		t.Fatal(err)
	}

	segmentsAfterMerge := countSegmentFiles(dir)
	if segmentsAfterMerge >= segmentsBeforeMerge {
		t.Errorf("merge should reduce segments from %d to less, got %d", segmentsBeforeMerge, segmentsAfterMerge)
	}

	for i := 0; i < 10; i++ {
		key := "key" + strconv.Itoa(i)
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s after merge: %v", key, err)
		}
		if len(value) != 100 {
			t.Errorf("expected value length 100 after merge, got %d", len(value))
		}
	}
}

func TestSegmentMergingWithUpdates(t *testing.T) {
	dir := t.TempDir()

	db, err := OpenWithSegmentSize(dir, 400)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i) + string(make([]byte, 50))
		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 3; i++ {
		key := "key" + strconv.Itoa(i)
		value := "updated" + strconv.Itoa(i) + string(make([]byte, 50))
		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.mergeSegments()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		key := "key" + strconv.Itoa(i)
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("failed to get updated key %s: %v", key, err)
		}
		if value[:7] != "updated" {
			t.Errorf("expected updated value, got %s", value)
		}
	}

	for i := 3; i < 5; i++ {
		key := "key" + strconv.Itoa(i)
		value, err := db.Get(key)
		if err != nil {
			t.Errorf("failed to get original key %s: %v", key, err)
		}
		if value[:5] != "value" {
			t.Errorf("expected original value, got %s", value)
		}
	}
}

func countSegmentFiles(dir string) int {
	files, _ := filepath.Glob(filepath.Join(dir, "segment-*"))
	return len(files)
}
