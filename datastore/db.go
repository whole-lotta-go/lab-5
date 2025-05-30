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
	segmentPrefix			= "segment-"
	defaultMaxSegmentSize	= 10 * 1024 * 1024
)

var ErrNotFound = fmt.Errorf("record does not exist")

type recordPosition struct {
	segmentIndex int
	offset       int64
}

type hashIndex map[string]recordPosition

type segmentInfo struct {
	filename string
	number   int
}

type Db struct {
	currentSegment *os.File
	currentOffset  int64
	index          hashIndex
	dir            string
	maxSegmentSize int64
	segments       []segmentInfo
	nextSegmentNum int
}

func Open(dir string) (*Db, error) {
	return OpenWithSegmentSize(dir, defaultMaxSegmentSize)
}

func OpenWithSegmentSize(dir string, maxSegmentSize int64) (*Db, error) {
	db := &Db{
		index:          make(hashIndex),
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
	}

	err := db.loadSegments()
	if err != nil {
		return nil, err
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	err = db.createCurrentSegment()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Db) loadSegments() error {
	files, err := filepath.Glob(filepath.Join(db.dir, segmentPrefix+"*"))
	if err != nil {
		return err
	}

	maxNum := -1
	for _, file := range files {
		base := filepath.Base(file)
		numStr := strings.TrimPrefix(base, segmentPrefix)
		num, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		db.segments = append(db.segments, segmentInfo{
			filename: file,
			number:   num,
		})
		if num > maxNum {
			maxNum = num
		}
	}

	db.nextSegmentNum = maxNum + 1

	sort.Slice(db.segments, func(i, j int) bool {
		return db.segments[i].number < db.segments[j].number
	})

	return nil
}

func (db *Db) createCurrentSegment() error {
	segmentPath := filepath.Join(db.dir, segmentPrefix+strconv.Itoa(db.nextSegmentNum))
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	db.currentSegment = f
	db.currentOffset = 0

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	db.currentOffset = stat.Size()

	return nil
}

func (db *Db) recover() error {
	for i, segment := range db.segments {
		err := db.recoverFromFile(segment.filename, i)
		if err != nil {
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

	var filename string
	if position.segmentIndex == len(db.segments) {
		filename = db.currentSegment.Name()
	} else if position.segmentIndex < len(db.segments) {
		filename = db.segments[position.segmentIndex].filename
	} else {
		return "", ErrNotFound
	}

	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position.offset, 0)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}

	data := e.Encode()
	shouldRotate := db.currentOffset+int64(len(data)) > db.maxSegmentSize

	if shouldRotate {
		err := db.rotateSegment()
		if err != nil {
			return err
		}
	}

	n, err := db.currentSegment.Write(data)
	if err == nil {
		db.index[key] = recordPosition{
			segmentIndex: len(db.segments),
			offset:       db.currentOffset,
		}
		db.currentOffset += int64(n)
	}
	return err
}

func (db *Db) rotateSegment() error {
	err := db.currentSegment.Close()
	if err != nil {
		return err
	}

	db.segments = append(db.segments, segmentInfo{
		filename: db.currentSegment.Name(),
		number:   db.nextSegmentNum,
	})

	db.nextSegmentNum++
	err = db.createCurrentSegment()
	return err
}

func (db *Db) mergeSegments() error {
	if len(db.segments) <= 1 {
		return nil
	}

	tempMergedFile := filepath.Join(db.dir, "merged-temp")
	mergedFile, err := os.Create(tempMergedFile)
	if err != nil {
		return err
	}

	newIndex := make(hashIndex)
	var newOffset int64

	allKeys := make(map[string]bool)
	for key := range db.index {
		allKeys[key] = true
	}

	for key := range allKeys {
		value, err := db.Get(key)
		if err != nil {
			mergedFile.Close()
			os.Remove(tempMergedFile)
			return err
		}

		e := entry{key: key, value: value}
		data := e.Encode()
		n, err := mergedFile.Write(data)
		if err != nil {
			mergedFile.Close()
			os.Remove(tempMergedFile)
			return err
		}

		newIndex[key] = recordPosition{
			segmentIndex: 0,
			offset:       newOffset,
		}
		newOffset += int64(n)
	}

	err = mergedFile.Close()
	if err != nil {
		os.Remove(tempMergedFile)
		return err
	}

	for _, segment := range db.segments {
		os.Remove(segment.filename)
	}

	newSegmentName := filepath.Join(db.dir, segmentPrefix+"0")
	err = os.Rename(tempMergedFile, newSegmentName)
	if err != nil {
		return err
	}

	db.segments = []segmentInfo{{
		filename: newSegmentName,
		number:   0,
	}}
	db.index = newIndex
	db.nextSegmentNum = 1

	return nil
}

func (db *Db) Size() (int64, error) {
	var totalSize int64
	
	for _, segment := range db.segments {
		segmentInfo, err := os.Stat(segment.filename)
		if err != nil {
			return 0, err
		}
		totalSize += segmentInfo.Size()
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