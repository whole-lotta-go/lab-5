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
	segment string
	offset  int64
}

type Index = map[string]recordPosition

type Db struct {
	compacting atomic.Bool
	compacted  *sync.Cond
	mu         sync.RWMutex

	compactionThreshold int64
	maxSegmentSize      int64

	currentSegment *os.File
	currentOffset  int64
	dir            string
	segments       []string
	index          Index
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

	var segments []struct {
		segment   string
		timestamp int64
	}

	for _, file := range files {
		base := filepath.Base(file)
		timestampStr := strings.TrimPrefix(base, segmentPrefix)
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			continue // Skip files that don't match our timestamp format
		}
		segments = append(segments, struct {
			segment   string
			timestamp int64
		}{file, timestamp})
	}

	// Sort by timestamp (oldest first)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].timestamp < segments[j].timestamp
	})

	db.segments = make([]string, len(segments))
	for i, segment := range segments {
		db.segments[i] = segment.segment
	}

	return nil
}

func (db *Db) createCurrentSegmentLocked() error {
	timestamp := time.Now().UnixNano()
	segmentPath := filepath.Join(db.dir, segmentPrefix+strconv.FormatInt(timestamp, 10))
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	db.currentSegment = f
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	db.currentOffset = stat.Size()

	return nil
}

func (db *Db) rebuildIndexLocked() error {
	db.index = make(Index)

	for _, segment := range db.segments {
		if err := db.rebuildIndexFromSegmentLocked(segment); err != nil {
			return err
		}
	}
	if db.currentSegment != nil {
		if err := db.rebuildIndexFromSegmentLocked(db.currentSegment.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) rebuildIndexFromSegmentLocked(segment string) error {
	index, err := getIndexFromSegment(segment)
	if err != nil {
		return err
	}
	for key, pos := range index {
		db.index[key] = pos
	}
	return nil
}

func getIndexFromSegment(segment string) (Index, error) {
	f, err := os.Open(segment)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	index := make(map[string]recordPosition)
	var offset int64

	for {
		var record entry
		n, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return nil, fmt.Errorf("corrupted segment file %s", segment)
			}
			break
		}
		if err != nil {
			return nil, err
		}

		index[record.key] = recordPosition{
			segment: segment,
			offset:  offset,
		}
		offset += int64(n)
	}
	return index, nil
}

func (db *Db) Close() error {
	db.waitForCompacted()
	return db.currentSegment.Close()
}

func (db *Db) waitForCompacted() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for db.compacting.Load() {
		db.compacted.Wait()
	}
}

func (db *Db) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	record, err := get(key, position.segment, position.offset)
	if err != nil {
		return "", err
	}

	return record.value, nil
}

func get(key string, segmentFile string, offset int64) (*entry, error) {
	file, err := os.Open(segmentFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	record := &entry{}
	if _, err := record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return nil, err
	}

	return record, nil
}

func (db *Db) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	e := entry{key: key, value: value}
	data := e.Encode()

	if db.currentOffset+int64(len(data)) > db.maxSegmentSize {
		if err := db.rotateSegmentLocked(); err != nil {
			return err
		}
		if len(db.segments) >= int(db.compactionThreshold) {
			go db.compact()
		}
	}

	n, err := db.currentSegment.Write(data)
	if err != nil {
		return err
	}
	err = db.currentSegment.Sync()
	if err != nil {
		return err
	}

	db.index[key] = recordPosition{
		segment: db.currentSegment.Name(),
		offset:  db.currentOffset,
	}
	db.currentOffset += int64(n)

	return nil
}

func (db *Db) rotateSegmentLocked() error {
	if err := db.currentSegment.Close(); err != nil {
		return err
	}

	db.segments = append(db.segments, db.currentSegment.Name())

	return db.createCurrentSegmentLocked()
}

func (db *Db) compact() error {
	if !db.compacting.CompareAndSwap(false, true) {
		return nil
	}
	defer func() {
		defer db.compacting.Store(false)
		defer db.compacted.Broadcast()
	}()

	ts := time.Now().UnixNano()
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
	ts = time.Now().UnixNano()
	newPath := filepath.Join(db.dir, segmentPrefix+strconv.FormatInt(ts, 10))

	for _, segPath := range segsBefore {
		segIndex, err := getIndexFromSegment(segPath)
		if err != nil {
			return err
		}

		for key, pos := range segIndex {
			posLive, ok := indexBefore[key]
			if !ok || pos != posLive {
				continue
			}

			rec, err := get(key, segPath, pos.offset)
			if err != nil {
				return err
			}

			data := rec.Encode()
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

	newSegs := []string{compPath}
	newSegs = append(newSegs, db.getNewSegments(segsBefore)...)
	db.segments = newSegs
	err = db.rebuildIndexLocked()
	if err != nil {
		db.segments = segsAfter
		db.index = indexAfter

		return fmt.Errorf("failed to rebuild index after compaction: %v", err)
	}

	for _, segBefore := range segsBefore {
		os.Remove(segBefore)
	}
	finished = true

	return nil
}

func (db *Db) getNewSegments(oldSegs []string) []string {
	mapOld := make(map[string]struct{}, len(oldSegs))
	for _, oldSeg := range oldSegs {
		mapOld[oldSeg] = struct{}{}
	}
	var newSegs []string
	for _, newSeg := range db.segments {
		if _, found := mapOld[newSeg]; !found {
			newSegs = append(newSegs, newSeg)
		}
	}
	return newSegs
}

func (db *Db) takeSnapshot() (segsSnap []string, indexSnap Index) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	segsSnap = make([]string, len(db.segments))
	copy(segsSnap, db.segments)
	indexSnap = make(map[string]recordPosition)
	for k, v := range db.index {
		indexSnap[k] = v
	}
	return
}

func (db *Db) Size() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var totalSize int64

	for _, segment := range db.segments {
		info, err := os.Stat(segment)
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
