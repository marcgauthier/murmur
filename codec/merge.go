package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/marcgauthier/murmur/crdt"
	"math"
	"sort"
	"strconv"
	"unicode/utf8"
)

// CRDTRecord is one bounded causal record. Keys are scoped to a cell.
// p/n records are PN components; a/r records are OR-set additions/removals.
type CRDTRecord struct{ Key, Data []byte }

// SetElement preserves type identity, including integer versus real.
// Fields are private to prevent constructing non-canonical values.
type SetElement struct {
	kind  byte
	value Value
}

func SetNull() SetElement { return SetElement{} }
func SetBool(v bool) SetElement {
	var i int64
	if v {
		i = 1
	}
	return SetElement{kind: 5, value: Int(i)}
}
func SetInt(v int64) SetElement     { return SetElement{kind: 1, value: Int(v)} }
func SetString(v string) SetElement { return SetElement{kind: 3, value: Text(v)} }
func SetReal(v float64) (SetElement, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return SetElement{}, fmt.Errorf("crdt: set real must be finite")
	}
	if v == 0 {
		v = 0
	}
	return SetElement{kind: 2, value: Real(v)}, nil
}
func (e SetElement) Encode() ([]byte, error) {
	if e.kind == 3 && !utf8.ValidString(e.value.S) {
		return nil, fmt.Errorf("crdt: set string is invalid UTF-8")
	}
	return AppendValue([]byte{e.kind}, e.value), nil
}
func DecodeSetElement(raw []byte, max int) (SetElement, error) {
	if len(raw) == 0 {
		return SetElement{}, fmt.Errorf("crdt: empty set element")
	}
	v, rest, err := ConsumeValue(raw[1:], max)
	if err != nil || len(rest) != 0 {
		return SetElement{}, fmt.Errorf("crdt: invalid set element: %v", err)
	}
	e := SetElement{kind: raw[0], value: v}
	valid := e.kind == 0 && v.Type == TypeNull || e.kind == 1 && v.Type == TypeInteger || e.kind == 2 && v.Type == TypeReal && !math.IsNaN(v.F) && !math.IsInf(v.F, 0) || e.kind == 3 && v.Type == TypeText && utf8.ValidString(v.S) || e.kind == 5 && v.Type == TypeInteger && (v.I == 0 || v.I == 1)
	if !valid {
		return SetElement{}, fmt.Errorf("crdt: invalid typed set element")
	}
	if e.kind == 2 && v.F == 0 {
		e.value.F = 0
	}
	canonical, _ := e.Encode()
	if !bytes.Equal(canonical, raw) {
		return SetElement{}, fmt.Errorf("crdt: non-canonical set element")
	}
	return e, nil
}
func (e SetElement) MarshalJSON() ([]byte, error) {
	switch e.kind {
	case 0:
		return []byte(`{"type":"null"}`), nil
	case 1:
		return json.Marshal(struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		}{"integer", strconv.FormatInt(e.value.I, 10)})
	case 2:
		return json.Marshal(struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		}{"real", strconv.FormatFloat(e.value.F, 'g', -1, 64)})
	case 3:
		return json.Marshal(struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		}{"string", e.value.S})
	case 5:
		return json.Marshal(struct {
			Type  string `json:"type"`
			Value bool   `json:"value"`
		}{"boolean", e.value.I == 1})
	}
	return nil, fmt.Errorf("crdt: invalid element")
}
func SetProjection(elements []SetElement) (string, error) {
	sort.Slice(elements, func(i, j int) bool {
		a, _ := elements[i].Encode()
		b, _ := elements[j].Encode()
		return bytes.Compare(a, b) < 0
	})
	if elements == nil {
		elements = []SetElement{}
	}
	raw, err := json.Marshal(elements)
	return string(raw), err
}

func EncodeCRDTRecords(dst []byte, records []CRDTRecord) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(records)))
	for _, r := range records {
		dst = AppendValue(dst, Blob(r.Key))
		dst = AppendValue(dst, Blob(r.Data))
	}
	return dst
}
func ConsumeCRDTRecords(src []byte, lim Limits) ([]CRDTRecord, []byte, error) {
	if len(src) < 4 {
		return nil, nil, fmt.Errorf("codec: truncated CRDT records")
	}
	n := binary.BigEndian.Uint32(src)
	src = src[4:]
	if int64(n) > int64(lim.MaxMutations) {
		return nil, nil, fmt.Errorf("codec: too many CRDT records")
	}
	var records []CRDTRecord
	for i := uint32(0); i < n; i++ {
		key, rest, err := ConsumeValue(src, lim.MaxValueBytes)
		if err != nil || key.Type != TypeBlob {
			return nil, nil, fmt.Errorf("codec: invalid CRDT key")
		}
		src = rest
		value, rest, err := ConsumeValue(src, lim.MaxValueBytes)
		if err != nil || value.Type != TypeBlob {
			return nil, nil, fmt.Errorf("codec: invalid CRDT value")
		}
		src = rest
		records = append(records, CRDTRecord{Key: key.B, Data: value.B})
	}
	return records, src, nil
}

// EpochRecord scopes High state to its preceding release marker.
func EpochRecord(epoch crdt.Version, key []byte) []byte {
	return append(EncodeTombstone([]byte{'e'}, epoch), key...)
}
func SplitEpochRecord(key []byte) (crdt.Version, []byte, bool) {
	if len(key) > 25 && key[0] == 'e' {
		epoch, err := DecodeTombstone(key[1:25])
		if err == nil {
			return epoch, key[25:], true
		}
	}
	return crdt.Version{}, key, false
}
func ShadowProjection(epoch crdt.Version, value Value) Value {
	return Blob(EncodeCellState([]byte{1}, CellState{Version: epoch, Value: value}))
}
func ShadowValue(value Value, lim Limits) (crdt.Version, Value, bool, error) {
	if value.Type != TypeBlob || len(value.B) == 0 {
		return crdt.Version{}, Value{}, false, fmt.Errorf("crdt: invalid shadow")
	}
	if bytes.Equal(value.B, []byte{0}) {
		return crdt.Version{}, Value{}, false, nil
	}
	if value.B[0] != 1 {
		return crdt.Version{}, Value{}, false, fmt.Errorf("crdt: invalid marker")
	}
	st, err := DecodeCellState(value.B[1:], lim)
	return st.Version, st.Value, true, err
}
