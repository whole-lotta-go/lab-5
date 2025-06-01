package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	segmentPrefix              = "segment-"
	defaultMaxSegmentSize      = 10 * 1024 * 1024
	defaultCompactionThreshold = 3
)

var ErrNotFound = fmt.Errorf("record does not exist")

type recordPosition struct {
	segment segment
	offset  int64
}

type index = map[string]recordPosition

type segment struct {
	name string
}

type Db struct {
	compacting atomic.Bool
	compacted  *sync.Cond
	mu         sync.RWMutex

	compactionThreshold int64
	maxSegmentSize      int64

	currentSegment *os.File
	currentOffset  int64
	dir            string
	segments       []segment
	index          index
}

func Open(dir string) (*Db, error) {
	return open(dir, defaultMaxSegmentSize, defaultCompactionThreshold)
}

func open(dir string, maxSegmentSize int64, compactionThreshold int64) (*Db, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db := &Db{
		index:               make(map[string]recordPosition),
		dir:                 dir,
		maxSegmentSize:      maxSegmentSize,
		compactionThreshold: compactionThreshold,
	}
	db.compacted = sync.NewCond(&db.mu)

	if err := db.loadSegments(); err != nil {
		return nil, err
	}

	if err := db.rebuildIndexLocked(); err != nil && err != io.EOF {
		return nil, err
	}

	if err := db.createCurrentSegmentLocked(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Db) loadSegments() error {
	files, err := filepath.Glob(filepath.Join(db.dir, segmentPrefix+"*"))
	if err != nil {
		return err
	}

	var segs []struct {
		segment   segment
		timestamp int64
	}

	for _, file := range files {
		base := filepath.Base(file)
		tsStr := strings.TrimPrefix(base, segmentPrefix)
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			continue // Skip files that don't match our timestamp format
		}
		seg := segment{file}
		segs = append(segs, struct {
			segment   segment
			timestamp int64
		}{seg, ts})
	}

	// Sort by timestamp (oldest first)
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].timestamp < segs[j].timestamp
	})

	db.segments = make([]segment, len(segs))
	for i, seg := range segs {
		db.segments[i] = seg.segment
	}

	return nil
}

func (db *Db) createCurrentSegmentLocked() error {
	ts := time.Now().UnixNano()
	segPath := filepath.Join(db.dir, segmentPrefix+strconv.FormatInt(ts, 10))
	file, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	db.currentSegment = file
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	db.currentOffset = stat.Size()

	return nil
}

func (db *Db) rebuildIndexLocked() error {
	db.index = make(index)

	for _, seg := range db.segments {
		if err := db.rebuildIndexFromPathLocked(seg.name); err != nil {
			return err
		}
	}
	if db.currentSegment != nil {
		if err := db.rebuildIndexFromPathLocked(db.currentSegment.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) rebuildIndexFromPathLocked(path string) error {
	index, err := getIndexFromPath(path)
	if err != nil {
		return err
	}
	for key, pos := range index {
		db.index[key] = pos
	}
	return nil
}

func getIndexFromPath(path string) (index, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	in := bufio.NewReader(file)
	index := make(map[string]recordPosition)
	var offset int64

	for {
		var rec record
		n, err := rec.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return nil, fmt.Errorf("corrupted segment file %s", path)
			}
			break
		}
		if err != nil {
			return nil, err
		}

		seg := segment{path}
		index[rec.key] = recordPosition{
			segment: seg,
			offset:  offset,
		}
		offset += int64(n)
	}
	return index, nil
}

func (db *Db) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for !db.compacting.CompareAndSwap(false, true) {
		db.compacted.Wait()
	}
	return db.currentSegment.Close()
}

func (db *Db) Get(key string) (string, error) {
	rec := NewStringRecord(key, "")
	err := db.get(rec, key)
	if err != nil {
		return "", err
	}

	value, ok := rec.value.(string)
	if !ok {
		return "", fmt.Errorf("invalid value type: expected string")
	}

	return value, nil
}

func (db *Db) GetInt64(key string) (int64, error) {
	rec := NewInt64Record(key, 0)
	err := db.get(rec, key)
	if err != nil {
		return 0, err
	}

	value, ok := rec.value.(int64)
	if !ok {
		return 0, fmt.Errorf("invalid value type: expected int64")
	}

	return value, nil
}

func (db *Db) get(rec *record, key string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	pos, ok := db.index[key]
	if !ok {
		return ErrNotFound
	}

	err := getFromPath(rec, pos.segment.name, pos.offset)
	if err != nil {
		return err
	}

	return nil
}

func getFromPath(rec *record, path string, offset int64) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if _, err := rec.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return err
	}

	return nil
}

func (db *Db) Put(key, value string) error {
	rec := NewStringRecord(key, value)
	return db.put(*rec)
}

func (db *Db) PutInt64(key string, value int64) error {
	rec := NewInt64Record(key, value)
	return db.put(*rec)
}

func (db *Db) put(rec record) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	data, err := rec.Encode()
	if err != nil {
		return err
	}

	if db.currentOffset+int64(len(data)) > db.maxSegmentSize {
		ts := time.Now().UnixNano()
		if err := db.rotateSegmentLocked(); err != nil {
			return err
		}
		if len(db.segments) >= int(db.compactionThreshold) {
			go db.compact(ts)
		}
	}

	n, err := db.currentSegment.Write(data)
	if err != nil {
		return err
	}

	if err = db.currentSegment.Sync(); err != nil {
		return err
	}

	seg := segment{db.currentSegment.Name()}
	db.index[rec.key] = recordPosition{
		segment: seg,
		offset:  db.currentOffset,
	}
	db.currentOffset += int64(n)

	return nil
}

func (db *Db) rotateSegmentLocked() error {
	if err := db.currentSegment.Close(); err != nil {
		return err
	}

	seg := segment{db.currentSegment.Name()}
	db.segments = append(db.segments, seg)

	return db.createCurrentSegmentLocked()
}

func (db *Db) compact(ts int64) error {
	if !db.compacting.CompareAndSwap(false, true) {
		return nil
	}
	defer func() {
		db.compacting.Store(false)
		db.compacted.Broadcast()
	}()

	compPath := filepath.Join(db.dir, segmentPrefix+"tmp")
	comp, err := os.Create(compPath)
	finished := false
	if err != nil {
		return err
	}
	defer func() {
		if !finished {
			comp.Close()
			os.Remove(compPath)
		}
	}()

	segsBefore, indexBefore := db.takeSnapshot()
	newPath := filepath.Join(db.dir, segmentPrefix+strconv.FormatInt(ts, 10))

	for _, seg := range segsBefore {
		segIndex, err := getIndexFromPath(seg.name)
		if err != nil {
			return err
		}

		for key, pos := range segIndex {
			posLive, ok := indexBefore[key]
			if !ok || pos != posLive {
				continue
			}

			rec := &record{}
			err := getFromPath(rec, seg.name, pos.offset)
			if err != nil {
				return err
			}

			data, err := rec.Encode()
			if err != nil {
				return err
			}

			_, err = comp.Write(data)
			if err != nil {
				return err
			}
		}
	}

	if err := comp.Close(); err != nil {
		return err
	}
	if err := os.Rename(compPath, newPath); err != nil {
		return err
	}
	compPath = newPath

	segsAfter, indexAfter := db.takeSnapshot()

	db.mu.Lock()
	defer db.mu.Unlock()

	newSegs := []segment{segment{compPath}}
	newSegs = append(newSegs, db.getNewSegments(segsBefore)...)
	db.segments = newSegs
	err = db.rebuildIndexLocked()
	if err != nil {
		db.segments = segsAfter
		db.index = indexAfter

		return fmt.Errorf("failed to rebuild index after compaction: %v", err)
	}

	for _, segBefore := range segsBefore {
		os.Remove(segBefore.name)
	}
	finished = true

	return nil
}

func (db *Db) getNewSegments(oldSegs []segment) []segment {
	mapOld := make(map[segment]struct{}, len(oldSegs))
	for _, oldSeg := range oldSegs {
		mapOld[oldSeg] = struct{}{}
	}
	var newSegs []segment
	for _, newSeg := range db.segments {
		if _, found := mapOld[newSeg]; !found {
			newSegs = append(newSegs, newSeg)
		}
	}
	return newSegs
}

func (db *Db) takeSnapshot() (segsSnap []segment, indexSnap index) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	segsSnap = make([]segment, len(db.segments))
	copy(segsSnap, db.segments)
	indexSnap = make(map[string]recordPosition)
	for key, pos := range db.index {
		indexSnap[key] = pos
	}
	return
}

func (db *Db) Size() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var totalSize int64

	for _, seg := range db.segments {
		info, err := os.Stat(seg.name)
		if err != nil {
			return 0, err
		}
		totalSize += info.Size()
	}

	if db.currentSegment != nil {
		info, err := db.currentSegment.Stat()
		if err != nil {
			return 0, err
		}
		totalSize += info.Size()
	}

	return totalSize, nil
}
