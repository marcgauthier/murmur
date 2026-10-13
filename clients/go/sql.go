package murmur

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"sync/atomic"
)

type ValueKind byte

const (
	NullKind ValueKind = iota
	IntegerKind
	RealKind
	TextKind
	BlobKind
)

type Value struct {
	Kind    ValueKind
	Integer int64
	Real    float64
	Text    string
	Blob    []byte
}

func Null() Value           { return Value{Kind: NullKind} }
func Integer(v int64) Value { return Value{Kind: IntegerKind, Integer: v} }
func Real(v float64) Value  { return Value{Kind: RealKind, Real: v} }
func Text(v string) Value   { return Value{Kind: TextKind, Text: v} }
func Blob(v []byte) Value   { return Value{Kind: BlobKind, Blob: append([]byte{}, v...)} }
func (v Value) Interface() any {
	switch v.Kind {
	case NullKind:
		return nil
	case IntegerKind:
		return v.Integer
	case RealKind:
		return v.Real
	case TextKind:
		return v.Text
	case BlobKind:
		return append([]byte{}, v.Blob...)
	}
	return nil
}
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.Kind {
	case NullKind:
		return []byte(`"Null"`), nil
	case IntegerKind:
		return json.Marshal(map[string]any{"Integer": v.Integer})
	case RealKind:
		return json.Marshal(map[string]any{"Real": v.Real})
	case TextKind:
		return json.Marshal(map[string]any{"Text": v.Text})
	case BlobKind:
		return json.Marshal(map[string]any{"Blob": Bytes(v.Blob)})
	}
	return nil, invalidError("value kind")
}
func (v *Value) UnmarshalJSON(data []byte) error {
	if string(data) == `"Null"` {
		*v = Null()
		return nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	if len(m) != 1 {
		return invalidError("value representation")
	}
	for k, b := range m {
		switch k {
		case "Integer":
			v.Kind = IntegerKind
			return json.Unmarshal(b, &v.Integer)
		case "Real":
			v.Kind = RealKind
			return json.Unmarshal(b, &v.Real)
		case "Text":
			v.Kind = TextKind
			return json.Unmarshal(b, &v.Text)
		case "Blob":
			v.Kind = BlobKind
			var bytes Bytes
			if err := json.Unmarshal(b, &bytes); err != nil {
				return err
			}
			v.Blob = bytes
			return nil
		}
	}
	return invalidError("value tag")
}
func asValue(a any) (Value, error) {
	switch v := a.(type) {
	case nil:
		return Null(), nil
	case Value:
		return v, nil
	case int:
		return Integer(int64(v)), nil
	case int8:
		return Integer(int64(v)), nil
	case int16:
		return Integer(int64(v)), nil
	case int32:
		return Integer(int64(v)), nil
	case int64:
		return Integer(v), nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return Value{}, invalidError("integer overflow")
		}
		return Integer(int64(v)), nil
	case uint64:
		if v > math.MaxInt64 {
			return Value{}, invalidError("integer overflow")
		}
		return Integer(int64(v)), nil
	case uint32:
		return Integer(int64(v)), nil
	case bool:
		if v {
			return Integer(1), nil
		}
		return Integer(0), nil
	case float32:
		return Real(float64(v)), nil
	case float64:
		return Real(v), nil
	case string:
		return Text(v), nil
	case []byte:
		return Blob(v), nil
	case Bytes:
		return Blob(v), nil
	}
	return Value{}, invalidError(fmt.Sprintf("unsupported SQL parameter %T", a))
}

type binaryWriter struct {
	data []byte
	err  error
}

func (w *binaryWriter) u32(n uint32) { w.data = binary.LittleEndian.AppendUint32(w.data, n) }
func (w *binaryWriter) u64(n uint64) { w.data = binary.LittleEndian.AppendUint64(w.data, n) }
func (w *binaryWriter) blob(b []byte) {
	if uint64(len(b)) > math.MaxUint32 {
		w.err = ErrLimit
		return
	}
	w.u32(uint32(len(b)))
	w.data = append(w.data, b...)
}
func (w *binaryWriter) text(s string) { w.blob([]byte(s)) }
func (w *binaryWriter) params(args []any) {
	w.u32(uint32(len(args)))
	for _, a := range args {
		v, e := asValue(a)
		if e != nil {
			w.err = e
			return
		}
		w.data = append(w.data, byte(v.Kind))
		switch v.Kind {
		case IntegerKind:
			w.u64(uint64(v.Integer))
		case RealKind:
			w.u64(math.Float64bits(v.Real))
		case TextKind:
			w.text(v.Text)
		case BlobKind:
			w.blob(v.Blob)
		}
	}
}

type binaryReader struct {
	data []byte
	at   int
	err  error
}

func (r *binaryReader) take(n int) []byte {
	if n < 0 || r.at > len(r.data)-n {
		r.err = io.ErrUnexpectedEOF
		return make([]byte, n)
	}
	b := r.data[r.at : r.at+n]
	r.at += n
	return b
}
func (r *binaryReader) u32() uint32 { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *binaryReader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *binaryReader) blob() []byte {
	n := r.u32()
	if uint64(n) > uint64(len(r.data)-r.at) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	return append([]byte{}, r.take(int(n))...)
}
func (r *binaryReader) value() Value {
	tag := ValueKind(r.take(1)[0])
	switch tag {
	case NullKind:
		return Null()
	case IntegerKind:
		return Integer(int64(r.u64()))
	case RealKind:
		return Real(math.Float64frombits(r.u64()))
	case TextKind:
		return Text(string(r.blob()))
	case BlobKind:
		return Blob(r.blob())
	}
	r.err = invalidError("native SQL value tag")
	return Null()
}

// Sql is usable only synchronously inside its Read or Transaction callback.
type Sql struct {
	scope uint64
	alive atomic.Bool
}

func (s *Sql) call(op uint32, w binaryWriter) ([]byte, error) {
	if !s.alive.Load() {
		return nil, invalidError("SQL callback has returned")
	}
	if w.err != nil {
		return nil, w.err
	}
	return sqlCall(s.scope, op, w.data)
}

type Result struct {
	RowsAffected    int64
	LastInsertRowID int64
}

func (s *Sql) Exec(query string, args ...any) (Result, error) {
	w := binaryWriter{}
	w.text(query)
	w.params(args)
	data, err := s.call(1, w)
	if err != nil {
		return Result{}, err
	}
	r := binaryReader{data: data}
	n := int64(r.u64())
	last, err := s.LastInsertRowID()
	return Result{RowsAffected: n, LastInsertRowID: last}, err
}
func (s *Sql) ExecuteBatch(query string) error {
	w := binaryWriter{}
	w.text(query)
	_, err := s.call(2, w)
	return err
}
func (s *Sql) LastInsertRowID() (int64, error) {
	data, err := s.call(9, binaryWriter{})
	if err != nil {
		return 0, err
	}
	r := binaryReader{data: data}
	n := int64(r.u64())
	return n, r.err
}
func (s *Sql) Prepare(query string) (*Statement, error) {
	w := binaryWriter{}
	w.text(query)
	data, err := s.call(3, w)
	if err != nil {
		return nil, err
	}
	r := binaryReader{data: data}
	id := r.u64()
	return &Statement{sql: s, id: id}, r.err
}
func (s *Sql) Query(query string, args ...any) (*Rows, error) {
	w := binaryWriter{}
	w.text(query)
	w.params(args)
	data, err := s.call(10, w)
	if err != nil {
		return nil, err
	}
	return newRows(s, data)
}
func (s *Sql) QueryRow(query string, args ...any) *Row {
	rows, err := s.Query(query, args...)
	if err != nil {
		return &Row{err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		if rows.Err() != nil {
			return &Row{err: rows.Err()}
		}
		return &Row{err: ErrNoRows}
	}
	return &Row{values: rows.Values()}
}

var ErrNoRows = fmt.Errorf("murmur: query returned no rows")

type Statement struct {
	sql    *Sql
	id     uint64
	closed bool
}

func (s *Statement) Exec(args ...any) (Result, error) {
	if s.closed {
		return Result{}, ErrClosed
	}
	w := binaryWriter{}
	w.u64(s.id)
	w.params(args)
	data, err := s.sql.call(4, w)
	if err != nil {
		return Result{}, err
	}
	r := binaryReader{data: data}
	n := int64(r.u64())
	last, err := s.sql.LastInsertRowID()
	return Result{RowsAffected: n, LastInsertRowID: last}, err
}
func (s *Statement) Query(args ...any) (*Rows, error) {
	if s.closed {
		return nil, ErrClosed
	}
	w := binaryWriter{}
	w.u64(s.id)
	w.params(args)
	data, err := s.sql.call(5, w)
	if err != nil {
		return nil, err
	}
	return newRows(s.sql, data)
}
func (s *Statement) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	w := binaryWriter{}
	w.u64(s.id)
	_, err := s.sql.call(7, w)
	return err
}

type Rows struct {
	sql     *Sql
	id      uint64
	columns []string
	values  []Value
	err     error
	closed  bool
}

func newRows(sql *Sql, data []byte) (*Rows, error) {
	r := binaryReader{data: data}
	id := r.u64()
	n := r.u32()
	if n > 32766 {
		return nil, ErrLimit
	}
	columns := make([]string, n)
	for i := range columns {
		columns[i] = string(r.blob())
	}
	if r.err != nil {
		return nil, r.err
	}
	return &Rows{sql: sql, id: id, columns: columns}, nil
}
func (r *Rows) Columns() []string { return append([]string(nil), r.columns...) }
func (r *Rows) Next() bool {
	if r.closed || r.err != nil {
		return false
	}
	w := binaryWriter{}
	w.u64(r.id)
	data, err := r.sql.call(6, w)
	if err != nil {
		r.err = err
		return false
	}
	if len(data) == 1 && data[0] == 0 {
		r.closed = true
		return false
	}
	reader := binaryReader{data: data}
	if reader.take(1)[0] != 1 {
		r.err = invalidError("native row header")
		return false
	}
	n := reader.u32()
	if int(n) != len(r.columns) {
		r.err = invalidError("native column count")
		return false
	}
	r.values = make([]Value, n)
	for i := range r.values {
		r.values[i] = reader.value()
	}
	r.err = reader.err
	return r.err == nil
}
func (r *Rows) Values() []Value {
	values := make([]Value, len(r.values))
	for i, v := range r.values {
		values[i] = v
		if v.Kind == BlobKind {
			values[i].Blob = append([]byte{}, v.Blob...)
		}
	}
	return values
}
func (r *Rows) Err() error             { return r.err }
func (r *Rows) Scan(dest ...any) error { return scan(r.values, dest) }
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	w := binaryWriter{}
	w.u64(r.id)
	_, err := r.sql.call(8, w)
	return err
}

type Row struct {
	values []Value
	err    error
}

func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scan(r.values, dest)
}
func scan(values []Value, dest []any) error {
	if len(values) != len(dest) {
		return invalidError("Scan destination count mismatch")
	}
	for i, v := range values {
		if p, ok := dest[i].(*Value); ok {
			*p = v
			continue
		}
		if p, ok := dest[i].(*any); ok {
			*p = v.Interface()
			continue
		}
		rv := reflect.ValueOf(dest[i])
		if rv.Kind() != reflect.Pointer || rv.IsNil() {
			return invalidError("Scan requires non-nil pointers")
		}
		if v.Kind == NullKind {
			return invalidError("Scan NULL into non-Value destination")
		}
		source := reflect.ValueOf(v.Interface())
		target := rv.Elem()
		switch {
		case source.Type().AssignableTo(target.Type()):
			target.Set(source)
		case v.Kind == IntegerKind && target.Kind() >= reflect.Int && target.Kind() <= reflect.Int64:
			if target.OverflowInt(v.Integer) {
				return invalidError("Scan integer overflow")
			}
			target.SetInt(v.Integer)
		case v.Kind == RealKind && (target.Kind() == reflect.Float32 || target.Kind() == reflect.Float64):
			if target.OverflowFloat(v.Real) {
				return invalidError("Scan float overflow")
			}
			target.SetFloat(v.Real)
		default:
			return invalidError(fmt.Sprintf("cannot Scan %v into %T", v.Kind, dest[i]))
		}
	}
	return nil
}
func (s *Sql) policy(operation, table string, key []Value, column string, value *Value, delta int64) error {
	if !s.alive.Load() {
		return ErrClosed
	}
	request := map[string]any{"operation": operation, "table": table, "key": key, "column": column, "delta": delta}
	if value != nil {
		request["value"] = *value
	}
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	_, err = sqlCall(s.scope, 11, data)
	return err
}
func (s *Sql) CounterAdd(table string, key []Value, column string, delta int64) error {
	return s.policy("counter_add", table, key, column, nil, delta)
}
func (s *Sql) SetAdd(table string, key []Value, column string, value Value) error {
	return s.policy("set_add", table, key, column, &value, 0)
}
func (s *Sql) SetRemove(table string, key []Value, column string, value Value) error {
	return s.policy("set_remove", table, key, column, &value, 0)
}
func (s *Sql) Min(table string, key []Value, column string, value Value) error {
	return s.policy("min", table, key, column, &value, 0)
}
func (s *Sql) Max(table string, key []Value, column string, value Value) error {
	return s.policy("max", table, key, column, &value, 0)
}

func scopedSQL(scope uint64) *Sql { s := &Sql{scope: scope}; s.alive.Store(true); return s }
