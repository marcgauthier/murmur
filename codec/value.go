// Package codec implements the compact binary encodings for replicated
// values, mutation batches, and snapshots. Normal replication traffic is
// never JSON; see FormatDebug for human-readable output.
package codec

import (
	"encoding/binary"
	"fmt"
	"math"
)

// ValueType preserves SQLite type semantics across the wire.
type ValueType byte

const (
	TypeNull ValueType = iota
	TypeInteger
	TypeReal
	TypeText
	TypeBlob
)

func (t ValueType) String() string {
	switch t {
	case TypeNull:
		return "NULL"
	case TypeInteger:
		return "INTEGER"
	case TypeReal:
		return "REAL"
	case TypeText:
		return "TEXT"
	case TypeBlob:
		return "BLOB"
	default:
		return "UNKNOWN"
	}
}

// Value is one cell value.
type Value struct {
	Type ValueType
	I    int64
	F    float64
	S    string
	B    []byte
}

// Null is the NULL value.
func Null() Value { return Value{Type: TypeNull} }

// Constructors.
func Int(v int64) Value    { return Value{Type: TypeInteger, I: v} }
func Real(v float64) Value { return Value{Type: TypeReal, F: v} }
func Text(v string) Value  { return Value{Type: TypeText, S: v} }
func Blob(v []byte) Value  { return Value{Type: TypeBlob, B: v} }

// FromAny converts driver-level values (as returned by database/sql and the
// pre-update hook) into a Value.
func FromAny(v any) (Value, error) {
	switch t := v.(type) {
	case nil:
		return Null(), nil
	case int64:
		return Int(t), nil
	case int:
		return Int(int64(t)), nil
	case int32:
		return Int(int64(t)), nil
	case float64:
		return Real(t), nil
	case float32:
		return Real(float64(t)), nil
	case string:
		return Text(t), nil
	case []byte:
		cp := make([]byte, len(t))
		copy(cp, t)
		return Blob(cp), nil
	case bool:
		if t {
			return Int(1), nil
		}
		return Int(0), nil
	default:
		return Value{}, fmt.Errorf("codec: unsupported value type %T", v)
	}
}

// ToAny converts back to a driver-level value.
func (v Value) ToAny() any {
	switch v.Type {
	case TypeNull:
		return nil
	case TypeInteger:
		return v.I
	case TypeReal:
		return v.F
	case TypeText:
		return v.S
	case TypeBlob:
		return v.B
	default:
		return nil
	}
}

// Equal reports SQLite-semantic equality (BLOB compared by bytes).
func (v Value) Equal(o Value) bool {
	if v.Type != o.Type {
		return false
	}
	switch v.Type {
	case TypeNull:
		return true
	case TypeInteger:
		return v.I == o.I
	case TypeReal:
		return v.F == o.F || (math.IsNaN(v.F) && math.IsNaN(o.F))
	case TypeText:
		return v.S == o.S
	case TypeBlob:
		if len(v.B) != len(o.B) {
			return false
		}
		for i := range v.B {
			if v.B[i] != o.B[i] {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// Size returns the encoded length of the value payload (excluding the type byte).
func (v Value) Size() int {
	switch v.Type {
	case TypeNull:
		return 0
	case TypeInteger:
		return binary.MaxVarintLen64
	case TypeReal:
		return 8
	case TypeText:
		return binary.MaxVarintLen64 + len(v.S)
	case TypeBlob:
		return binary.MaxVarintLen64 + len(v.B)
	default:
		return 0
	}
}

// EncodedSize returns the exact binary encoding size of the value, including its type tag.
func (v Value) EncodedSize() int {
	switch v.Type {
	case TypeNull:
		return 1
	case TypeInteger:
		var buf [binary.MaxVarintLen64]byte
		return 1 + binary.PutVarint(buf[:], v.I)
	case TypeReal:
		return 1 + 8
	case TypeText:
		var buf [binary.MaxVarintLen64]byte
		return 1 + binary.PutUvarint(buf[:], uint64(len(v.S))) + len(v.S)
	case TypeBlob:
		var buf [binary.MaxVarintLen64]byte
		return 1 + binary.PutUvarint(buf[:], uint64(len(v.B))) + len(v.B)
	default:
		return 1
	}
}

// AppendValue appends the binary encoding of v to b.
func AppendValue(b []byte, v Value) []byte {
	b = append(b, byte(v.Type))
	switch v.Type {
	case TypeNull:
	case TypeInteger:
		b = binary.AppendVarint(b, v.I)
	case TypeReal:
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(v.F))
	case TypeText:
		b = binary.AppendUvarint(b, uint64(len(v.S)))
		b = append(b, v.S...)
	case TypeBlob:
		b = binary.AppendUvarint(b, uint64(len(v.B)))
		b = append(b, v.B...)
	}
	return b
}

// ConsumeValue decodes one value from b, returning the remainder.
// maxLen bounds TEXT/BLOB payloads to prevent unsafe allocations from
// network-provided lengths.
func ConsumeValue(b []byte, maxLen int) (Value, []byte, error) {
	if len(b) < 1 {
		return Value{}, nil, fmt.Errorf("codec: truncated value")
	}
	t := ValueType(b[0])
	b = b[1:]
	switch t {
	case TypeNull:
		return Null(), b, nil
	case TypeInteger:
		v, n := binary.Varint(b)
		if n <= 0 {
			return Value{}, nil, fmt.Errorf("codec: truncated integer")
		}
		return Int(v), b[n:], nil
	case TypeReal:
		if len(b) < 8 {
			return Value{}, nil, fmt.Errorf("codec: truncated real")
		}
		return Real(math.Float64frombits(binary.BigEndian.Uint64(b))), b[8:], nil
	case TypeText, TypeBlob:
		l, n := binary.Uvarint(b)
		if n <= 0 {
			return Value{}, nil, fmt.Errorf("codec: truncated length")
		}
		b = b[n:]
		if l > uint64(maxLen) {
			return Value{}, nil, fmt.Errorf("codec: value length %d exceeds limit %d", l, maxLen)
		}
		if uint64(len(b)) < l {
			return Value{}, nil, fmt.Errorf("codec: truncated payload")
		}
		if t == TypeText {
			return Text(string(b[:l])), b[l:], nil
		}
		cp := make([]byte, l)
		copy(cp, b[:l])
		return Blob(cp), b[l:], nil
	default:
		return Value{}, nil, fmt.Errorf("codec: unknown value type %d", t)
	}
}
