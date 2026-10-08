package rime

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
)

// tShard is one partition of a table's primary-key space. The primary key
// selects the shard via hash(key) % shardCount (default 64, configurable),
// so writes against different shards proceed concurrently. Locks are held
// only for short critical sections: map lookup, chain install, chain prune.
type tShard[T any] struct {
	mu         sync.RWMutex
	rows       map[any]*chain[T]
	rowMapHint int
}

func newTShard[T any]() *tShard[T] {
	// Leave headroom for ordinary inserts; managed commits prepare a larger
	// replacement only when a transaction crosses this conservative threshold.
	// Touch and remove a private key so the initial map's backing storage is
	// allocated before the first publication (empty Go maps may allocate on
	// their first insertion even when make was given a capacity hint).
	rows := make(map[any]*chain[T], 6)
	rows[shardPrepareKey{}] = nil
	delete(rows, shardPrepareKey{})
	return &tShard[T]{rows: rows, rowMapHint: 6}
}

type shardPrepareKey struct{}

type preparedShardRows[T any] struct {
	rows map[any]*chain[T]
	hint int
}

// hashKey maps a primary key to a 64-bit shard hash. Supported key types are
// checked at registration; the default branch only guards type-erased use.
func hashKey(k any) uint64 {
	switch v := k.(type) {
	case string:
		h := fnv.New64a()
		h.Write([]byte(v))
		return h.Sum64()
	case UUID:
		return binary.LittleEndian.Uint64(v[0:8]) * 1099511628211
	case [16]byte:
		return binary.LittleEndian.Uint64(v[0:8]) * 1099511628211
	case int:
		return uint64(v) * 1099511628211
	case int64:
		return uint64(v) * 1099511628211
	case int32:
		return uint64(v) * 1099511628211
	case int16:
		return uint64(v) * 1099511628211
	case int8:
		return uint64(v) * 1099511628211
	case uint64:
		return v * 1099511628211
	case uint:
		return uint64(v) * 1099511628211
	case uint32:
		return uint64(v) * 1099511628211
	case uint16:
		return uint64(v) * 1099511628211
	case uint8:
		return uint64(v) * 1099511628211
	default:
		h := fnv.New64a()
		fmt.Fprintf(h, "%v#%T", k, k)
		return h.Sum64()
	}
}

// chains copies immutable chain pointers under the shard lock. Iterating a
// mutable map across lock releases is unsafe even when records are immutable.
func (s *tShard[T]) chains() []*chain[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*chain[T], 0, len(s.rows))
	for _, c := range s.rows {
		out = append(out, c)
	}
	return out
}
