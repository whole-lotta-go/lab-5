package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestRecord_Encode(t *testing.T) {
	r := record{key: "key", value: "value", dataType: DataTypeString}
	encoded, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}

	var decoded record
	err = decoded.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.key != "key" {
		t.Error("incorrect key")
	}
	if decoded.value != "value" {
		t.Error("incorrect value")
	}
	if decoded.dataType != DataTypeString {
		t.Error("incorrect dataType")
	}
}

func TestReadValue(t *testing.T) {
	var (
		a, b record
	)
	a = record{key: "key", value: "test-value", dataType: DataTypeString}
	originalBytes, err := a.Encode()
	if err != nil {
		t.Fatal(err)
	}

	err = b.Decode(originalBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("encode/decode", a, b)
	if a.key != b.key || a.value != b.value || a.dataType != b.dataType {
		t.Error("Encode/Decode mismatch")
	}

	b = record{}
	n, err := b.DecodeFromReader(bufio.NewReader(bytes.NewReader(originalBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("encode/decodeFromReader", a, b)
	if a.key != b.key || a.value != b.value || a.dataType != b.dataType {
		t.Error("Encode/DecodeFromReader mismatch")
	}
	if n != len(originalBytes) {
		t.Errorf("DecodeFromReader() read %d bytes, expected %d", n, len(originalBytes))
	}
}

func TestRecord_DataTypes(t *testing.T) {
	t.Run("string record", func(t *testing.T) {
		r := record{key: "name", value: "John", dataType: DataTypeString}
		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		var decoded record
		err = decoded.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if decoded.key != "name" {
			t.Errorf("Expected key 'name', got '%s'", decoded.key)
		}
		if decoded.value != "John" {
			t.Errorf("Expected value 'John', got '%v'", decoded.value)
		}
		if decoded.dataType != DataTypeString {
			t.Errorf("Expected dataType %d, got %d", DataTypeString, decoded.dataType)
		}

		if str, ok := decoded.value.(string); !ok {
			t.Error("Record value should be a string")
		} else if str != "John" {
			t.Errorf("Expected string value 'John', got '%s'", str)
		}
	})

	t.Run("int64 record", func(t *testing.T) {
		r := record{key: "age", value: int64(42), dataType: DataTypeInt64}
		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		var decoded record
		err = decoded.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if decoded.key != "age" {
			t.Errorf("Expected key 'age', got '%s'", decoded.key)
		}
		if decoded.value != int64(42) {
			t.Errorf("Expected value 42, got '%v'", decoded.value)
		}
		if decoded.dataType != DataTypeInt64 {
			t.Errorf("Expected dataType %d, got %d", DataTypeInt64, decoded.dataType)
		}

		// Check that the value can be type asserted to int64
		if val, ok := decoded.value.(int64); !ok {
			t.Error("Record value should be an int64")
		} else if val != 42 {
			t.Errorf("Expected int64 value 42, got %d", val)
		}
	})
}

func TestRecord_HelperConstructors(t *testing.T) {
	t.Run("NewStringRecord", func(t *testing.T) {
		r := NewStringRecord("name", "Alice")

		if r.key != "name" {
			t.Errorf("Expected key 'name', got '%s'", r.key)
		}
		if r.value != "Alice" {
			t.Errorf("Expected value 'Alice', got '%v'", r.value)
		}
		if r.dataType != DataTypeString {
			t.Errorf("Expected dataType %d, got %d", DataTypeString, r.dataType)
		}

		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		var decoded record
		err = decoded.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if decoded.key != r.key || decoded.value != r.value || decoded.dataType != r.dataType {
			t.Error("NewStringRecord encode/decode failed")
		}
	})

	t.Run("NewInt64Record", func(t *testing.T) {
		r := NewInt64Record("score", 95)

		if r.key != "score" {
			t.Errorf("Expected key 'score', got '%s'", r.key)
		}
		if r.value != int64(95) {
			t.Errorf("Expected value 95, got '%v'", r.value)
		}
		if r.dataType != DataTypeInt64 {
			t.Errorf("Expected dataType %d, got %d", DataTypeInt64, r.dataType)
		}

		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		var decoded record
		err = decoded.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if decoded.key != r.key || decoded.value != r.value || decoded.dataType != r.dataType {
			t.Error("NewInt64Record encode/decode failed")
		}
	})

	t.Run("NewInt64Record with negative value", func(t *testing.T) {
		r := NewInt64Record("temperature", -10)

		if r.key != "temperature" {
			t.Errorf("Expected key 'temperature', got '%s'", r.key)
		}
		if r.value != int64(-10) {
			t.Errorf("Expected value -10, got '%v'", r.value)
		}
		if r.dataType != DataTypeInt64 {
			t.Errorf("Expected dataType %d, got %d", DataTypeInt64, r.dataType)
		}
	})
}

func TestRecord_EncodeDecode_WithDataTypes(t *testing.T) {
	testCases := []struct {
		name   string
		record record
	}{
		{
			name:   "string record",
			record: record{key: "city", value: "Kyiv", dataType: DataTypeString},
		},
		{
			name:   "int64 record",
			record: record{key: "population", value: int64(2800000), dataType: DataTypeInt64},
		},
		{
			name:   "empty string record",
			record: record{key: "empty", value: "", dataType: DataTypeString},
		},
		{
			name:   "zero int64 record",
			record: record{key: "zero", value: int64(0), dataType: DataTypeInt64},
		},
		{
			name:   "large int64 record",
			record: record{key: "large", value: int64(9223372036854775807), dataType: DataTypeInt64},
		},
		{
			name:   "negative int64 record",
			record: record{key: "negative", value: int64(-9223372036854775808), dataType: DataTypeInt64},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := tc.record.Encode()
			if err != nil {
				t.Fatal(err)
			}

			var decoded record
			err = decoded.Decode(encoded)
			if err != nil {
				t.Fatal(err)
			}

			if decoded.key != tc.record.key || decoded.value != tc.record.value || decoded.dataType != tc.record.dataType {
				t.Errorf("Encode/Decode mismatch: original=%+v, decoded=%+v", tc.record, decoded)
			}

			var fromReader record
			n, err := fromReader.DecodeFromReader(bufio.NewReader(bytes.NewReader(encoded)))
			if err != nil {
				t.Fatalf("DecodeFromReader failed: %v", err)
			}

			if fromReader.key != tc.record.key || fromReader.value != tc.record.value || fromReader.dataType != tc.record.dataType {
				t.Errorf("Encode/DecodeFromReader mismatch: original=%+v, decoded=%+v", tc.record, fromReader)
			}

			if n != len(encoded) {
				t.Errorf("DecodeFromReader() read %d bytes, expected %d", n, len(encoded))
			}
		})
	}
}

func TestRecord_BackwardCompatibility(t *testing.T) {
	t.Run("manual record construction with string", func(t *testing.T) {
		r := record{key: "old", value: "format", dataType: DataTypeString}

		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		var decoded record
		err = decoded.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if decoded.key != "old" {
			t.Errorf("Expected key 'old', got '%s'", decoded.key)
		}
		if decoded.value != "format" {
			t.Errorf("Expected value 'format', got '%v'", decoded.value)
		}
		if decoded.dataType != DataTypeString {
			t.Errorf("Expected dataType %d, got %d", DataTypeString, decoded.dataType)
		}
	})

	t.Run("manual record construction with int64", func(t *testing.T) {
		r := record{key: "number", value: int64(123), dataType: DataTypeInt64}

		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		var decoded record
		err = decoded.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if decoded.key != "number" {
			t.Errorf("Expected key 'number', got '%s'", decoded.key)
		}
		if decoded.value != int64(123) {
			t.Errorf("Expected value 123, got '%v'", decoded.value)
		}
		if decoded.dataType != DataTypeInt64 {
			t.Errorf("Expected dataType %d, got %d", DataTypeInt64, decoded.dataType)
		}
	})
}

func TestRecord_ErrorHandling(t *testing.T) {
	t.Run("encode with wrong type for string", func(t *testing.T) {
		r := record{key: "test", value: int64(123), dataType: DataTypeString}
		_, err := r.Encode()
		if err == nil {
			t.Error("Expected error when encoding int64 as string")
		}
	})

	t.Run("encode with wrong type for int64", func(t *testing.T) {
		r := record{key: "test", value: "string", dataType: DataTypeInt64}
		_, err := r.Encode()
		if err == nil {
			t.Error("Expected error when encoding string as int64")
		}
	})

	t.Run("decode invalid data", func(t *testing.T) {
		var decoded record
		err := decoded.Decode([]byte{1, 2, 3})
		if err == nil {
			t.Error("Expected error when decoding invalid data")
		}
	})

	t.Run("decode with unknown data type", func(t *testing.T) {
		r := record{key: "test", value: "test", dataType: DataTypeString}
		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		encoded[4] = 99 

		var decoded record
		err = decoded.Decode(encoded)
		if err == nil {
			t.Error("Expected error when decoding unknown data type")
		}
	})
}

func TestRecord_EdgeCases(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		r := NewStringRecord("", "value")
		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		var decoded record
		err = decoded.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if decoded.key != "" {
			t.Errorf("Expected empty key, got '%s'", decoded.key)
		}
		if decoded.value != "value" {
			t.Errorf("Expected value 'value', got '%v'", decoded.value)
		}
	})

	t.Run("very long key and value", func(t *testing.T) {
		longKey := string(make([]byte, 1000))
		longValue := string(make([]byte, 10000))
		for i := range longKey {
			longKey = longKey[:i] + "k" + longKey[i+1:]
		}
		for i := range longValue {
			longValue = longValue[:i] + "v" + longValue[i+1:]
		}

		r := NewStringRecord(longKey, longValue)
		encoded, err := r.Encode()
		if err != nil {
			t.Fatal(err)
		}

		var decoded record
		err = decoded.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}

		if decoded.key != longKey {
			t.Error("Long key mismatch")
		}
		if decoded.value != longValue {
			t.Error("Long value mismatch")
		}
	})

	t.Run("max and min int64 values", func(t *testing.T) {
		testValues := []int64{
			9223372036854775807,
			-9223372036854775808,
			0,
			1,
			-1,
		}

		for _, val := range testValues {
			r := NewInt64Record("test", val)
			encoded, err := r.Encode()
			if err != nil {
				t.Fatal(err)
			}

			var decoded record
			err = decoded.Decode(encoded)
			if err != nil {
				t.Fatal(err)
			}

			if decoded.value != val {
				t.Errorf("Expected value %d, got %v", val, decoded.value)
			}
		}
	})
}
