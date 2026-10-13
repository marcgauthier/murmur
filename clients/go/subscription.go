package murmur

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"
)

type RowCodec[T any] struct {
	Encode func(T) ([]byte, error)
	Decode func([]byte) (T, error)
	Equal  func(T, T) bool
}

func JSONRowCodec[T any]() RowCodec[T] {
	return RowCodec[T]{Encode: func(v T) ([]byte, error) { return json.Marshal(v) }, Decode: func(b []byte) (T, error) { var v T; err := json.Unmarshal(b, &v); return v, err }, Equal: func(a, b T) bool { return reflect.DeepEqual(a, b) }}
}

type RowChange[T any] struct {
	ChangeType ChangeType
	Key        []byte
	Row        *T
}
type SubscriptionEvent[T any] struct {
	Kind    string
	Rows    []T
	Changes []RowChange[T]
	Cursor  SubscriptionCursor
	Reason  string
}
type Subscription[T any] struct {
	object
	codec RowCodec[T]
}
type wireRow struct {
	Key  Bytes `json:"key"`
	Data Bytes `json:"data"`
}

// Subscribe is a package function because Go methods cannot declare additional type parameters.
func Subscribe[T any](db *DB, query func(*Sql) ([]T, error), key func(T) []byte, options SubscriptionOptions, codec RowCodec[T]) (*Subscription[T], error) {
	if query == nil || key == nil || codec.Encode == nil || codec.Decode == nil || codec.Equal == nil {
		return nil, invalidError("subscription callbacks and codec are required")
	}
	dbID, err := db.handle()
	if err != nil {
		return nil, err
	}
	var mu sync.Mutex
	previous := map[string]T{}
	encoded := map[string][]byte{}
	token, state := newCallback(func(op uint32, scope uint64, _ []byte) ([]byte, error) {
		if op != 5 {
			return nil, invalidError("subscription callback opcode")
		}
		sql := scopedSQL(scope)
		defer func() { sql.alive.Store(false) }()
		rows, err := query(sql)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		next := map[string]T{}
		nextBytes := map[string][]byte{}
		wire := make([]wireRow, 0, len(rows))
		for _, row := range rows {
			k := append([]byte{}, key(row)...)
			ks := string(k)
			if _, duplicate := next[ks]; duplicate {
				return nil, invalidError("duplicate subscription row key")
			}
			data, err := codec.Encode(row)
			if err != nil {
				return nil, err
			}
			if prior, ok := previous[ks]; ok && codec.Equal(prior, row) {
				data = encoded[ks]
			}
			next[ks] = row
			nextBytes[ks] = append([]byte{}, data...)
			wire = append(wire, wireRow{Key: k, Data: data})
		}
		previous = next
		encoded = nextBytes
		return json.Marshal(wire)
	})
	var id uint64
	err = invoke(dbID, "subscribe", map[string]any{"options": options, "callback": token}, &id)
	if err != nil {
		releaseCallback(token)
		if original := state.failure(); original != nil {
			return nil, original
		}
		return nil, err
	}
	s := &Subscription[T]{codec: codec}
	s.set(id)
	return s, nil
}
func (s *Subscription[T]) Close() error                           { return s.object.close("close") }
func (s *Subscription[T]) TryRecv() (SubscriptionEvent[T], error) { return s.receive("try_recv", 0) }
func (s *Subscription[T]) RecvTimeout(timeout time.Duration) (SubscriptionEvent[T], error) {
	if timeout < 0 {
		return SubscriptionEvent[T]{}, invalidError("negative timeout")
	}
	return s.receive("recv", timeout)
}
func (s *Subscription[T]) Recv(ctx context.Context) (SubscriptionEvent[T], error) {
	for {
		if err := ctx.Err(); err != nil {
			return SubscriptionEvent[T]{}, err
		}
		event, err := s.RecvTimeout(100 * time.Millisecond)
		if errors.Is(err, ErrBusy) {
			continue
		}
		return event, err
	}
}
func (s *Subscription[T]) receive(op string, timeout time.Duration) (SubscriptionEvent[T], error) {
	var raw json.RawMessage
	err := s.call(op, map[string]any{"timeout": timeout}, &raw)
	if err != nil {
		return SubscriptionEvent[T]{}, err
	}
	var tagged map[string]json.RawMessage
	if err = json.Unmarshal(raw, &tagged); err != nil {
		return SubscriptionEvent[T]{}, err
	}
	event := SubscriptionEvent[T]{}
	for kind, data := range tagged {
		event.Kind = kind
		var payload struct {
			Rows    []wireRow          `json:"rows"`
			Cursor  SubscriptionCursor `json:"cursor"`
			Reason  string             `json:"reason"`
			Changes []struct {
				ChangeType ChangeType `json:"change_type"`
				Key        Bytes      `json:"key"`
				Row        *wireRow   `json:"row"`
			} `json:"changes"`
		}
		if err = json.Unmarshal(data, &payload); err != nil {
			return event, err
		}
		event.Cursor = payload.Cursor
		event.Reason = payload.Reason
		for _, row := range payload.Rows {
			value, err := s.codec.Decode(row.Data)
			if err != nil {
				return event, err
			}
			event.Rows = append(event.Rows, value)
		}
		for _, change := range payload.Changes {
			c := RowChange[T]{ChangeType: change.ChangeType, Key: append([]byte{}, change.Key...)}
			if change.Row != nil {
				value, err := s.codec.Decode(change.Row.Data)
				if err != nil {
					return event, err
				}
				c.Row = &value
			}
			event.Changes = append(event.Changes, c)
		}
	}
	return event, nil
}

// SubscribeRows watches a bound SQL query; keyColumns identifies result columns forming its key.
func (d *DB) SubscribeRows(query string, args []any, keyColumns []int, options SubscriptionOptions) (*Subscription[[]Value], error) {
	return Subscribe(d, func(sql *Sql) ([][]Value, error) {
		r, err := sql.Query(query, args...)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		var rows [][]Value
		for r.Next() {
			rows = append(rows, r.Values())
		}
		return rows, r.Err()
	}, func(row []Value) []byte {
		values := make([]Value, len(keyColumns))
		for n, i := range keyColumns {
			if i < 0 || i >= len(row) {
				panic("subscription key column out of range")
			}
			values[n] = row[i]
		}
		data, err := encodeValueRow(values)
		if err != nil {
			panic(err)
		}
		return data
	}, options, RowCodec[[]Value]{Encode: encodeValueRow, Decode: decodeValueRow, Equal: func(a, b []Value) bool {
		x, err := encodeValueRow(a)
		if err != nil {
			return false
		}
		y, err := encodeValueRow(b)
		return err == nil && bytes.Equal(x, y)
	}})
}
func encodeValueRow(values []Value) ([]byte, error) {
	args := make([]any, len(values))
	for i, v := range values {
		args[i] = v
	}
	w := binaryWriter{}
	w.params(args)
	return w.data, w.err
}
func decodeValueRow(data []byte) ([]Value, error) {
	r := binaryReader{data: data}
	n := r.u32()
	if n > 32766 {
		return nil, ErrLimit
	}
	values := make([]Value, n)
	for i := range values {
		values[i] = r.value()
	}
	if r.err != nil {
		return nil, r.err
	}
	if r.at != len(data) {
		return nil, invalidError("trailing row bytes")
	}
	return values, nil
}

// RecvContext waits for an event until cancellation; cancellation is polled at 100 ms.
func (s *Subscription[T]) RecvContext(ctx context.Context) (SubscriptionEvent[T], error) {
	return s.Recv(ctx)
}
