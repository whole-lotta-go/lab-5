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
)

const (
	segmentPrefix         = "segment-"
	defaultMaxSegmentSize = 10 * 1024 * 1024
	compactionThreshold   = 3
)

var ErrNotFound = fmt.Errorf("record does not exist")

type recordPosition struct {
	segmentIndex int
	offset       int64
}

type Db struct {
	currentSegment *os.File
	currentOffset  int64
	index          map[string]recordPosition
	dir            string
	maxSegmentSize int64
	segmentFiles   []string
	nextSegmentNum int
}

func Open(dir string) (*Db, error) {
	return OpenWithSegmentSize(dir, defaultMaxSegmentSize)
}

func OpenWithSegmentSize(dir string, maxSegmentSize int64) (*Db, error) {
	db := &Db{
		index:          make(map[string]recordPosition),
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
	}

	if err := db.loadSegments(); err != nil {
		return nil, err
	}

	if err := db.recover(); err != nil && err != io.EOF {
		return nil, err
	}

	if err := db.createCurrentSegment(); err != nil {
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
		filename string
		number   int
	}

	maxNum := -1
	for _, file := range files {
		base := filepath.Base(file)
		numStr := strings.TrimPrefix(base, segmentPrefix)
		num, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		segments = append(segments, struct {
			filename string
			number   int
		}{file, num})
		if num > maxNum {
			maxNum = num
		}
	}

	db.nextSegmentNum = maxNum + 1

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].number < segments[j].number
	})

	db.segmentFiles = make([]string, len(segments))
	for i, seg := range segments {
		db.segmentFiles[i] = seg.filename
	}

	return nil
}

func (db *Db) createCurrentSegment() error {
	segmentPath := filepath.Join(db.dir, segmentPrefix+strconv.Itoa(db.nextSegmentNum))
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

func (db *Db) recover() error {
	for i, filename := range db.segmentFiles {
		if err := db.recoverFromFile(filename, i); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) recoverFromFile(filename string, segmentIndex int) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	var offset int64

	for {
		var record entry
		n, err := record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted segment file %s", filename)
			}
			break
		}
		if err != nil {
			return err
		}

		db.index[record.key] = recordPosition{
			segmentIndex: segmentIndex,
			offset:       offset,
		}
		offset += int64(n)
	}
	return nil
}

func (db *Db) Close() error {
	return db.currentSegment.Close()
}

func (db *Db) Get(key string) (string, error) {
	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	filename := db.getSegmentFilename(position.segmentIndex)
	if filename == "" {
		return "", ErrNotFound
	}

	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	if _, err = file.Seek(position.offset, 0); err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) getSegmentFilename(segmentIndex int) string {
	if segmentIndex == len(db.segmentFiles) {
		return db.currentSegment.Name()
	}
	if segmentIndex < len(db.segmentFiles) {
		return db.segmentFiles[segmentIndex]
	}
	return ""
}

func (db *Db) Put(key, value string) error {
	e := entry{key: key, value: value}
	data := e.Encode()

	if db.currentOffset+int64(len(data)) > db.maxSegmentSize {
		if err := db.rotateSegment(); err != nil {
			return err
		}
		if len(db.segmentFiles) >= compactionThreshold {
			if err := db.compact(); err != nil {
				return err
			}
		}
	}

	n, err := db.currentSegment.Write(data)
	if err == nil {
		db.index[key] = recordPosition{
			segmentIndex: len(db.segmentFiles),
			offset:       db.currentOffset,
		}
		db.currentOffset += int64(n)
	}
	return err
}

func (db *Db) rotateSegment() error {
	if err := db.currentSegment.Close(); err != nil {
		return err
	}

	db.segmentFiles = append(db.segmentFiles, db.currentSegment.Name())
	db.nextSegmentNum++

	return db.createCurrentSegment()
}

func (db *Db) compact() error {
	tempFile := filepath.Join(db.dir, "compacted-temp")
	compacted, err := os.Create(tempFile)
	if err != nil {
		return err
	}

	newIndex := make(map[string]recordPosition)
	var newOffset int64

	for key := range db.index {
		value, err := db.Get(key)
		if err != nil {
			compacted.Close()
			os.Remove(tempFile)
			return err
		}

		e := entry{key: key, value: value}
		data := e.Encode()
		n, err := compacted.Write(data)
		if err != nil {
			compacted.Close()
			os.Remove(tempFile)
			return err
		}

		newIndex[key] = recordPosition{
			segmentIndex: 0,
			offset:       newOffset,
		}
		newOffset += int64(n)
	}

	if err := compacted.Close(); err != nil {
		os.Remove(tempFile)
		return err
	}

	for _, filename := range db.segmentFiles {
		os.Remove(filename)
	}

	newSegmentName := filepath.Join(db.dir, segmentPrefix+"0")
	if err := os.Rename(tempFile, newSegmentName); err != nil {
		return err
	}

	db.segmentFiles = []string{newSegmentName}
	db.index = newIndex
	db.nextSegmentNum = 1

	return nil
}

func (db *Db) Size() (int64, error) {
	var totalSize int64

	for _, filename := range db.segmentFiles {
		info, err := os.Stat(filename)
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
