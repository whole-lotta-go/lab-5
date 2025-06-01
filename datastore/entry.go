package datastore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Entry layout:
// Header                                       Data
// recordLen    dataType    keyLen    valLen    key    val
// Offsets:
// 0            4           5         9         13     13 + keyLen

type DataType uint8

const (
	DataTypeString DataType = 1
	DataTypeInt64  DataType = 2
)

type recordHeader struct {
	RecordLen uint32
	DataType  DataType
	KeyLen    uint32
	ValLen    uint32
}

const (
	recordLenSize    = 4
	dataTypeSize     = 1
	keyLenSize       = 4
	valLenSize       = 4
	recordHeaderSize = recordLenSize + dataTypeSize + keyLenSize + valLenSize
)

type record struct {
	key      string
	value    any
	dataType DataType
}

func (r *record) Encode() ([]byte, error) {
	vb, err := r.encodeValue()
	if err != nil {
		return nil, err
	}
	kb := []byte(r.key)
	kl, vl := uint32(len(kb)), uint32(len(vb))
	rl := recordHeaderSize + vl + kl

	header := recordHeader{
		RecordLen: rl,
		DataType:  r.dataType,
		KeyLen:    kl,
		ValLen:    vl,
	}

	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		return nil, err
	}
	if _, err := buf.Write(kb); err != nil {
		return nil, err
	}
	if _, err := buf.Write(vb); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (r *record) encodeValue() ([]byte, error) {
	if r.dataType == 0 {
		return nil, fmt.Errorf("datatype field is not set")
	}
	encodeFunc, err := r.dataType.getEncodeFunc(r)
	if err != nil {
		return nil, err
	}
	return encodeFunc()
}

func (dt DataType) getEncodeFunc(r *record) (func() ([]byte, error), error) {
	switch dt {
	case DataTypeString:
		return r.encodeString, nil
	case DataTypeInt64:
		return r.encodeInt64, nil
	default:
		return nil, fmt.Errorf("unknown datatype: %v", dt)
	}
}

func (r *record) encodeString() ([]byte, error) {
	str, ok := r.value.(string)
	if !ok {
		return nil, fmt.Errorf("invalid value type: expected string")
	}
	return []byte(str), nil
}

func (r *record) encodeInt64() ([]byte, error) {
	v, ok := r.value.(int64)
	if !ok {
		return nil, fmt.Errorf("invalid value type: expected int64")
	}
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.LittleEndian, v)
	return buf.Bytes(), nil
}

func (r *record) Decode(input []byte) error {
	if len(input) < recordHeaderSize {
		return fmt.Errorf("input too short for header: got %d, expected %d", len(input), recordHeaderSize)
	}

	buf := bytes.NewReader(input[:recordHeaderSize])
	var header recordHeader
	if err := binary.Read(buf, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	r.dataType = header.DataType

	expectedLen := recordHeaderSize + int(header.KeyLen) + int(header.ValLen)
	if len(input) < expectedLen {
		return fmt.Errorf("input length mismatch: got %d, expected %d", len(input), expectedLen)
	}
	if int(header.RecordLen) != expectedLen {
		return fmt.Errorf("record length mismatch: recordLen says %d, expected %d", header.RecordLen, expectedLen)
	}

	keyStart := recordHeaderSize
	keyEnd := keyStart + int(header.KeyLen)
	r.key = string(input[keyStart:keyEnd])

	valueStart := keyEnd
	valueEnd := valueStart + int(header.ValLen)
	valueBytes := input[valueStart:valueEnd]

	return r.decodeValue(valueBytes)
}

func (r *record) decodeValue(valueBytes []byte) error {
	if r.dataType == 0 {
		return fmt.Errorf("datatype field is not set")
	}
	decodeFunc, err := r.dataType.getDecodeFunc(r)
	if err != nil {
		return err
	}
	return decodeFunc(valueBytes)
}

func (dt DataType) getDecodeFunc(r *record) (func([]byte) error, error) {
	switch dt {
	case DataTypeString:
		return r.decodeString, nil
	case DataTypeInt64:
		return r.decodeInt64, nil
	default:
		return nil, fmt.Errorf("unknown datatype: %v", dt)
	}
}

func (r *record) decodeString(valueBytes []byte) error {
	r.value = string(valueBytes)
	return nil
}

func (r *record) decodeInt64(valueBytes []byte) error {
	if len(valueBytes) != 8 {
		return fmt.Errorf("invalid int64 value length: expected 8, got %d", len(valueBytes))
	}
	r.value = int64(binary.LittleEndian.Uint64(valueBytes))
	return nil
}

func (r *record) DecodeFromReader(in *bufio.Reader) (int, error) {
	lenBuf, err := in.Peek(recordLenSize)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, err
		}
		return 0, fmt.Errorf("DecodeFromReader: cannot read recordLen: %w", err)
	}

	recordLen := binary.LittleEndian.Uint32(lenBuf)
	buf := make([]byte, recordLen)
	n, err := in.Read(buf)
	if err != nil {
		return n, fmt.Errorf("DecodeFromReader: cannot read record: %w", err)
	}

	if err := r.Decode(buf); err != nil {
		return n, fmt.Errorf("DecodeFromReader: decode error: %w", err)
	}

	return n, nil
}

func NewStringRecord(key, value string) *record {
	return &record{
		key:      key,
		value:    value,
		dataType: DataTypeString,
	}
}

func NewInt64Record(key string, value int64) *record {
	return &record{
		key:      key,
		value:    value,
		dataType: DataTypeInt64,
	}
}
