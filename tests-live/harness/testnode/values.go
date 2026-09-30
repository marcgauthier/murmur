// Command testnode is the internal live-test fixture binary. This file holds
// its JSON value codec for the test-only HTTPS API: SQL values cross the
// API as typed JSON objects so int64 precision survives and blobs travel
// base64-encoded. It is test tooling, not a product API.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// Value is one JSON-safe SQL value. Numbers keep full int64 precision and
// blobs travel base64-encoded; NULL is the zero Value.
type Value struct {
	Type string  `json:"t"`           // null, int, float, text, blob, bool
	I    int64   `json:"i,omitempty"` // int
	F    float64 `json:"f,omitempty"` // float
	S    string  `json:"s,omitempty"` // text
	B    string  `json:"b,omitempty"` // blob (base64)
	Bool bool    `json:"v,omitempty"` // bool
}

// Null is the NULL value.
var Null = Value{Type: "null"}

// ToValue converts a database/sql scan result to a Value. Supported inputs
// are nil, all int/uint widths that fit int64, float32/64, string, []byte,
// and bool; anything else fails closed.
func ToValue(v any) (Value, error) {
	switch t := v.(type) {
	case nil:
		return Null, nil
	case int64:
		return Value{Type: "int", I: t}, nil
	case int:
		return Value{Type: "int", I: int64(t)}, nil
	case int8:
		return Value{Type: "int", I: int64(t)}, nil
	case int16:
		return Value{Type: "int", I: int64(t)}, nil
	case int32:
		return Value{Type: "int", I: int64(t)}, nil
	case uint:
		return uintValue(uint64(t))
	case uint8:
		return Value{Type: "int", I: int64(t)}, nil
	case uint16:
		return Value{Type: "int", I: int64(t)}, nil
	case uint32:
		return Value{Type: "int", I: int64(t)}, nil
	case uint64:
		return uintValue(t)
	case float64:
		return Value{Type: "float", F: t}, nil
	case float32:
		return Value{Type: "float", F: float64(t)}, nil
	case string:
		return Value{Type: "text", S: t}, nil
	case []byte:
		if t == nil {
			return Null, nil
		}
		return Value{Type: "blob", B: base64.StdEncoding.EncodeToString(t)}, nil
	case bool:
		return Value{Type: "bool", Bool: t}, nil
	default:
		return Value{}, fmt.Errorf("testnode: unsupported value type %T", v)
	}
}

func uintValue(v uint64) (Value, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if v > maxInt64 {
		return Value{}, fmt.Errorf("testnode: unsigned value %d overflows int64", v)
	}
	return Value{Type: "int", I: int64(v)}, nil
}

// Any converts the Value back to a driver argument / scan-compatible value.
func (v Value) Any() (any, error) {
	switch v.Type {
	case "", "null":
		return nil, nil
	case "int":
		return v.I, nil
	case "float":
		return v.F, nil
	case "text":
		return v.S, nil
	case "blob":
		if v.B == "" {
			return []byte{}, nil
		}
		raw, err := base64.StdEncoding.DecodeString(v.B)
		if err != nil {
			return nil, fmt.Errorf("testnode: invalid blob encoding: %w", err)
		}
		return raw, nil
	case "bool":
		return v.Bool, nil
	default:
		return nil, fmt.Errorf("testnode: unknown value type %q", v.Type)
	}
}

// ValuesToAny converts argument values for query/exec calls.
func ValuesToAny(vals []Value) ([]any, error) {
	out := make([]any, len(vals))
	for i, v := range vals {
		a, err := v.Any()
		if err != nil {
			return nil, err
		}
		out[i] = a
	}
	return out, nil
}

// MarshalRows encodes scanned rows (each a []any of column values).
func MarshalRows(rows [][]any) ([][]Value, error) {
	out := make([][]Value, len(rows))
	for i, r := range rows {
		enc := make([]Value, len(r))
		for j, v := range r {
			ev, err := ToValue(v)
			if err != nil {
				return nil, err
			}
			enc[j] = ev
		}
		out[i] = enc
	}
	return out, nil
}

// DecodeJSONArgs parses the subscription endpoint's args parameter: a JSON
// array of Values.
func DecodeJSONArgs(raw string) ([]any, error) {
	if raw == "" {
		return nil, nil
	}
	var vals []Value
	dec := json.NewDecoder(bytes.NewBufferString(raw))
	if err := dec.Decode(&vals); err != nil {
		return nil, fmt.Errorf("testnode: invalid args: %w", err)
	}
	return ValuesToAny(vals)
}
