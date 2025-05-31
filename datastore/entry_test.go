// entry_test.go
package datastore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

func TestNewStringEntry(t *testing.T) {
	key := "test_key"
	value := "test_value"

	entry := newStringEntry(key, value)

	if entry.key != key {
		t.Errorf("Expected key %s, got %s", key, entry.key)
	}

	if entry.vtype != stringType {
		t.Errorf("Expected vtype %d, got %d", stringType, entry.vtype)
	}

	if string(entry.rawValue) != value {
		t.Errorf("Expected rawValue %s, got %s", value, string(entry.rawValue))
	}
}

func TestNewInt64Entry(t *testing.T) {
	key := "test_key"
	value := int64(42)

	entry := newInt64Entry(key, value)

	if entry.key != key {
		t.Errorf("Expected key %s, got %s", key, entry.key)
	}

	if entry.vtype != int64Type {
		t.Errorf("Expected vtype %d, got %d", int64Type, entry.vtype)
	}

	if len(entry.rawValue) != 8 {
		t.Errorf("Expected rawValue length 8, got %d", len(entry.rawValue))
	}

	expected := make([]byte, 8)
	binary.LittleEndian.PutUint64(expected, uint64(value))
	if !bytes.Equal(entry.rawValue, expected) {
		t.Errorf("Expected rawValue %v, got %v", expected, entry.rawValue)
	}
}

func TestEntryStringValue(t *testing.T) {
	tests := []struct {
		name      string
		entry     entry
		expected  string
		expectErr bool
	}{
		{
			name:      "valid string entry",
			entry:     newStringEntry("key", "value"),
			expected:  "value",
			expectErr: false,
		},
		{
			name:      "wrong type error",
			entry:     newInt64Entry("key", 42),
			expected:  "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.entry.stringValue()

			if tt.expectErr && err == nil {
				t.Error("Expected error but got none")
			}

			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}

			if tt.expectErr && err != ErrWrongType {
				t.Errorf("Expected ErrWrongType, got %v", err)
			}
		})
	}
}

func TestEntryInt64Value(t *testing.T) {
	tests := []struct {
		name      string
		entry     entry
		expected  int64
		expectErr bool
	}{
		{
			name:      "valid int64 entry",
			entry:     newInt64Entry("key", 42),
			expected:  42,
			expectErr: false,
		},
		{
			name:      "negative int64 entry",
			entry:     newInt64Entry("key", -100),
			expected:  -100,
			expectErr: false,
		},
		{
			name:      "wrong type error",
			entry:     newStringEntry("key", "value"),
			expected:  0,
			expectErr: true,
		},
		{
			name: "invalid length error",
			entry: entry{
				key:      "key",
				vtype:    int64Type,
				rawValue: []byte{1, 2, 3},
			},
			expected:  0,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.entry.int64Value()

			if tt.expectErr && err == nil {
				t.Error("Expected error but got none")
			}

			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestEntryEncode(t *testing.T) {
	tests := []struct {
		name     string
		entry    entry
		validate func([]byte) bool
	}{
		{
			name:  "string entry",
			entry: newStringEntry("key", "value"),
			validate: func(data []byte) bool {
				expectedSize := len("key") + len("value") + 1 + 12
				if len(data) != expectedSize {
					return false
				}

				// Перевірка заголовка розміру
				size := binary.LittleEndian.Uint32(data[0:4])
				return int(size) == expectedSize
			},
		},
		{
			name:  "int64 entry",
			entry: newInt64Entry("test", 42),
			validate: func(data []byte) bool {
				expectedSize := len("test") + 8 + 1 + 12
				if len(data) != expectedSize {
					return false
				}

				size := binary.LittleEndian.Uint32(data[0:4])
				return int(size) == expectedSize
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.entry.Encode()

			if !tt.validate(encoded) {
				t.Error("Encoded data validation failed")
			}
		})
	}
}

func TestEntryDecode(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		expectErr bool
		validate  func(entry) bool
	}{
		{
			name: "valid string entry",
			input: func() []byte {
				e := newStringEntry("key", "value")
				return e.Encode()
			}(),
			expectErr: false,
			validate: func(e entry) bool {
				val, err := e.stringValue()
				return err == nil && val == "value" && e.key == "key"
			},
		},
		{
			name: "valid int64 entry",
			input: func() []byte {
				e := newInt64Entry("test", 42)
				return e.Encode()
			}(),
			expectErr: false,
			validate: func(e entry) bool {
				val, err := e.int64Value()
				return err == nil && val == 42 && e.key == "test"
			},
		},
		{
			name:      "input too short",
			input:     []byte{1, 2, 3},
			expectErr: true,
			validate:  nil,
		},
		{
			name: "size mismatch",
			input: func() []byte {
				data := make([]byte, 20)
				binary.LittleEndian.PutUint32(data[0:4], 100)
				return data
			}(),
			expectErr: true,
			validate:  nil,
		},
		{
			name: "invalid key length",
			input: func() []byte {
				data := make([]byte, 20)
				binary.LittleEndian.PutUint32(data[0:4], 20)
				binary.LittleEndian.PutUint32(data[4:8], 100)
				return data
			}(),
			expectErr: true,
			validate:  nil,
		},
		{
			name: "invalid value length zero",
			input: func() []byte {
				data := make([]byte, 16)
				binary.LittleEndian.PutUint32(data[0:4], 16)
				binary.LittleEndian.PutUint32(data[4:8], 2)
				data[8] = 'k'
				data[9] = 'y'
				binary.LittleEndian.PutUint32(data[10:14], 0)
				return data
			}(),
			expectErr: true,
			validate:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var e entry
			err := e.Decode(tt.input)

			if tt.expectErr && err == nil {
				t.Error("Expected error but got none")
			}

			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tt.expectErr && tt.validate != nil && !tt.validate(e) {
				t.Error("Decoded entry validation failed")
			}
		})
	}
}

func TestEntryDecodeFromReader(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
		validate  func(entry, int) bool
	}{
		{
			name: "valid string entry",
			input: func() string {
				e := newStringEntry("key", "value")
				return string(e.Encode())
			}(),
			expectErr: false,
			validate: func(e entry, n int) bool {
				val, err := e.stringValue()
				expectedSize := len("key") + len("value") + 1 + 12
				return err == nil && val == "value" && e.key == "key" && n == expectedSize
			},
		},
		{
			name: "valid int64 entry",
			input: func() string {
				e := newInt64Entry("test", 42)
				return string(e.Encode())
			}(),
			expectErr: false,
			validate: func(e entry, n int) bool {
				val, err := e.int64Value()
				expectedSize := len("test") + 8 + 1 + 12
				return err == nil && val == 42 && e.key == "test" && n == expectedSize
			},
		},
		{
			name:      "empty input",
			input:     "",
			expectErr: true,
			validate:  nil,
		},
		{
			name:      "input too short for size",
			input:     "ab",
			expectErr: true,
			validate:  nil,
		},
		{
			name: "invalid size too small",
			input: func() string {
				data := make([]byte, 4)
				binary.LittleEndian.PutUint32(data, 5) // менше 12
				return string(data)
			}(),
			expectErr: true,
			validate:  nil,
		},
		{
			name: "incomplete data after size",
			input: func() string {
				e := newStringEntry("key", "value")
				encoded := e.Encode()
				// Обрізаємо дані після розміру
				return string(encoded[:10]) // менше ніж потрібно
			}(),
			expectErr: true,
			validate:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tt.input))
			var e entry

			n, err := e.DecodeFromReader(reader)

			if tt.expectErr && err == nil {
				t.Error("Expected error but got none")
			}

			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tt.expectErr && tt.validate != nil && !tt.validate(e, n) {
				t.Error("DecodeFromReader validation failed")
			}
		})
	}
}

func TestEntryDecodeFromReaderEOF(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(""))
	var e entry

	n, err := e.DecodeFromReader(reader)

	if err != io.EOF {
		t.Errorf("Expected io.EOF, got %v", err)
	}

	if n != 0 {
		t.Errorf("Expected n=0, got %d", n)
	}
}

func TestEntryRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		entry entry
	}{
		{
			name:  "string entry round trip",
			entry: newStringEntry("hello", "world"),
		},
		{
			name:  "int64 entry round trip",
			entry: newInt64Entry("number", 12345),
		},
		{
			name:  "empty string entry",
			entry: newStringEntry("empty", ""),
		},
		{
			name:  "zero int64 entry",
			entry: newInt64Entry("zero", 0),
		},
		{
			name:  "negative int64 entry",
			entry: newInt64Entry("negative", -42),
		},
		{
			name:  "unicode string entry",
			entry: newStringEntry("unicode", "привіт світ 🌍"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.entry.Encode()

			var decoded entry
			err := decoded.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode error: %v", err)
			}

			if decoded.key != tt.entry.key {
				t.Errorf("Key mismatch: expected %s, got %s", tt.entry.key, decoded.key)
			}

			if decoded.vtype != tt.entry.vtype {
				t.Errorf("Type mismatch: expected %d, got %d", tt.entry.vtype, decoded.vtype)
			}

			if !bytes.Equal(decoded.rawValue, tt.entry.rawValue) {
				t.Errorf("RawValue mismatch: expected %v, got %v", tt.entry.rawValue, decoded.rawValue)
			}

			// Test DecodeFromReader as well
			reader := bufio.NewReader(bytes.NewReader(encoded))
			var decodedFromReader entry
			n, err := decodedFromReader.DecodeFromReader(reader)
			if err != nil {
				t.Fatalf("DecodeFromReader error: %v", err)
			}

			if n != len(encoded) {
				t.Errorf("Expected to read %d bytes, got %d", len(encoded), n)
			}

			if decodedFromReader.key != tt.entry.key {
				t.Errorf("DecodeFromReader key mismatch: expected %s, got %s", tt.entry.key, decodedFromReader.key)
			}
		})
	}
}

func TestValueTypes(t *testing.T) {
	if stringType != 0 {
		t.Errorf("Expected stringType to be 0, got %d", stringType)
	}

	if int64Type != 1 {
		t.Errorf("Expected int64Type to be 1, got %d", int64Type)
	}
}

func TestErrWrongType(t *testing.T) {
	if ErrWrongType == nil {
		t.Error("ErrWrongType should not be nil")
	}

	if ErrWrongType.Error() != "wrong value type" {
		t.Errorf("Expected error message 'wrong value type', got '%s'", ErrWrongType.Error())
	}
}
