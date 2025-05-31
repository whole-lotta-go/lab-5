package datastore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var ErrWrongType = errors.New("wrong value type")

type valueType byte

const (
	stringType valueType = iota
	int64Type
)

type entry struct {
	key      string
	vtype    valueType
	rawValue []byte
}


func (e *entry) stringValue() (string, error) {
	if e.vtype != stringType {
		return "", ErrWrongType
	}
	return string(e.rawValue), nil
}

func (e *entry) int64Value() (int64, error) {
	if e.vtype != int64Type {
		return 0, ErrWrongType
	}
	if len(e.rawValue) != 8 {
		return 0, fmt.Errorf("invalid int64 value length: %d", len(e.rawValue))
	}
	return int64(binary.LittleEndian.Uint64(e.rawValue)), nil
}

func newStringEntry(key, value string) entry {
	return entry{
		key:      key,
		vtype:    stringType,
		rawValue: []byte(value),
	}
}

func newInt64Entry(key string, value int64) entry {
	rawValue := make([]byte, 8)
	binary.LittleEndian.PutUint64(rawValue, uint64(value))
	return entry{
		key:      key,
		vtype:    int64Type,
		rawValue: rawValue,
	}
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	vl := len(e.rawValue) + 1 // 1 byte for valueType

	size := kl + vl + 12
	res := make([]byte, size)

	binary.LittleEndian.PutUint32(res[0:4], uint32(size))
	binary.LittleEndian.PutUint32(res[4:8], uint32(kl))
	copy(res[8:8+kl], e.key)
	binary.LittleEndian.PutUint32(res[8+kl:12+kl], uint32(vl))
	res[12+kl] = byte(e.vtype)
	copy(res[13+kl:], e.rawValue)

	return res
}

func (e *entry) Decode(input []byte) error {
	if len(input) < 12 {
		return fmt.Errorf("input too short: expected at least 12 bytes, got %d", len(input))
	}

	size := int(binary.LittleEndian.Uint32(input[0:4]))
	if len(input) != size {
		return fmt.Errorf("size mismatch: expected %d bytes, got %d", size, len(input))
	}

	keyLen := int(binary.LittleEndian.Uint32(input[4:8]))
	if 8+keyLen+4 > len(input) {
		return fmt.Errorf("invalid key length: %d", keyLen)
	}

	e.key = string(input[8 : 8+keyLen])

	valLen := int(binary.LittleEndian.Uint32(input[8+keyLen : 12+keyLen]))
	if valLen < 1 {
		return fmt.Errorf("invalid value length: %d", valLen)
	}

	valStart := 12 + keyLen
	if valStart+valLen > len(input) {
		return fmt.Errorf("invalid value length: not enough data")
	}

	e.vtype = valueType(input[valStart])
	e.rawValue = make([]byte, valLen-1)
	copy(e.rawValue, input[valStart+1:valStart+valLen])

	return nil
}

func (e *entry) DecodeFromReader(in *bufio.Reader) (int, error) {
	sizeBuf := make([]byte, 4)
	n, err := io.ReadFull(in, sizeBuf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, err
		}
		return n, fmt.Errorf("DecodeFromReader, cannot read size: %w", err)
	}

	size := int(binary.LittleEndian.Uint32(sizeBuf))
	if size < 12 {
		return n, fmt.Errorf("DecodeFromReader, invalid size: %d", size)
	}

	buf := make([]byte, size)
	copy(buf[0:4], sizeBuf)

	remaining, err := io.ReadFull(in, buf[4:])
	n += remaining
	if err != nil {
		return n, fmt.Errorf("DecodeFromReader, cannot read record: %w", err)
	}

	err = e.Decode(buf)
	if err != nil {
		return n, fmt.Errorf("DecodeFromReader, decode error: %w", err)
	}

	return n, nil
}
