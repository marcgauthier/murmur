// Package plumtree provides a bounded, transport-independent Plumtree
// eager/lazy dissemination state machine. The replication manager can translate
// its decisions into eager payloads and IHAVE/PRUNE/GRAFT control frames.
package plumtree

import (
	"bytes"
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/marcgauthier/spedsql/ids"
)

var (
	ErrInvalidConfig      = errors.New("plumtree: invalid configuration")
	ErrPayloadTooLarge    = errors.New("plumtree: payload exceeds cache limit")
	ErrConflictingPayload = errors.New("plumtree: conflicting payload for message identity")
)

type Config struct {
	EagerFanout     int
	MaxNeighbors    int
	MaxCacheEntries int
	MaxCacheBytes   int64
	CacheTTL        time.Duration
}

type cacheEntry struct {
	id       [32]byte
	payload  []byte
	inserted time.Time
}

// Engine serializes state transitions and bounds both payload cache size and
// duplicate suppression history. Peer sets are local to a message tree.
type Engine struct {
	mu        sync.Mutex
	cfg       Config
	cache     map[[32]byte]*list.Element
	lru       *list.List
	bytes     int64
	eager     map[ids.NodeID]struct{}
	lazy      map[ids.NodeID]struct{}
	protected map[ids.NodeID]struct{}
}

// Plan describes the actions a caller should send. Payload is copied and only
// populated when an eager send or successful GRAFT requires it.
type Plan struct {
	Duplicate bool
	Prune     bool
	Graft     bool
	Eager     []ids.NodeID
	Lazy      []ids.NodeID
	Payload   []byte
}

func New(cfg Config) (*Engine, error) {
	if cfg.EagerFanout < 2 || cfg.MaxNeighbors < cfg.EagerFanout || cfg.MaxCacheEntries <= 0 || cfg.MaxCacheBytes <= 0 || cfg.CacheTTL <= 0 {
		return nil, ErrInvalidConfig
	}
	return &Engine{cfg: cfg, cache: make(map[[32]byte]*list.Element), lru: list.New(), eager: make(map[ids.NodeID]struct{}), lazy: make(map[ids.NodeID]struct{}), protected: make(map[ids.NodeID]struct{})}, nil
}

// Start chooses a bounded eager fanout and returns remaining peers as lazy
// subscribers. The origin retains a copy until cache expiry/eviction.
func (e *Engine) Start(id [32]byte, payload []byte, peers []ids.NodeID) (Plan, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.putLocked(id, payload); err != nil {
		return Plan{}, err
	}
	e.eager = make(map[ids.NodeID]struct{})
	e.lazy = make(map[ids.NodeID]struct{})
	for _, p := range unique(peers) {
		if len(e.eager)+len(e.lazy) >= e.cfg.MaxNeighbors {
			break
		}
		if len(e.eager) < e.cfg.EagerFanout {
			e.eager[p] = struct{}{}
		} else {
			e.lazy[p] = struct{}{}
		}
	}
	return Plan{Eager: keys(e.eager), Lazy: keys(e.lazy), Payload: append([]byte(nil), payload...)}, nil
}

// Receive records a previously unseen payload and eagerly forwards it to the
// current eager set except the sender. A duplicate from an eager neighbor asks
// the caller to PRUNE that link.
func (e *Engine) Receive(id [32]byte, payload []byte, from ids.NodeID) (Plan, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.expireLocked(time.Now())
	if el := e.cache[id]; el != nil {
		e.lru.MoveToFront(el)
		if !bytes.Equal(el.Value.(cacheEntry).payload, payload) {
			return Plan{}, ErrConflictingPayload
		}
		_, wasEager := e.eager[from]
		_, protected := e.protected[from]
		if wasEager && !protected {
			delete(e.eager, from)
			e.lazy[from] = struct{}{}
		}
		return Plan{Duplicate: true, Prune: wasEager && !protected}, nil
	}
	if err := e.putLocked(id, payload); err != nil {
		return Plan{}, err
	}
	if _, ok := e.lazy[from]; ok {
		delete(e.lazy, from)
		e.eager[from] = struct{}{}
	} else if _, ok := e.eager[from]; !ok && len(e.eager)+len(e.lazy) < e.cfg.MaxNeighbors {
		e.eager[from] = struct{}{}
	}
	var out []ids.NodeID
	for peer := range e.eager {
		if peer != from {
			out = append(out, peer)
		}
	}
	return Plan{Eager: out, Lazy: keys(e.lazy), Payload: append([]byte(nil), payload...)}, nil
}

// Prune demotes a peer from eager to lazy delivery.
func (e *Engine) Prune(peer ids.NodeID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, protected := e.protected[peer]; protected {
		return
	}
	if _, ok := e.eager[peer]; ok {
		delete(e.eager, peer)
		e.lazy[peer] = struct{}{}
	}
}

// Graft promotes a lazy peer and returns the cached payload when available.
// A missing/expired cache entry is reported as Graft=true with no payload so
// the caller can request the transaction through ordinary repair.
func (e *Engine) Graft(id [32]byte, peer ids.NodeID) Plan {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.lazy[peer]; ok {
		delete(e.lazy, peer)
		e.eager[peer] = struct{}{}
	} else if _, ok := e.eager[peer]; !ok && len(e.eager)+len(e.lazy) < e.cfg.MaxNeighbors {
		e.eager[peer] = struct{}{}
	} else if _, ok := e.eager[peer]; !ok {
		return Plan{Graft: true}
	}
	e.expireLocked(time.Now())
	if el := e.cache[id]; el != nil {
		e.lru.MoveToFront(el)
		entry := el.Value.(cacheEntry)
		return Plan{Graft: true, Eager: []ids.NodeID{peer}, Payload: append([]byte(nil), entry.payload...)}
	}
	return Plan{Graft: true, Eager: []ids.NodeID{peer}}
}

// Neighbors returns copies of the current eager/lazy peer sets.
func (e *Engine) Neighbors() (eager, lazy []ids.NodeID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return keys(e.eager), keys(e.lazy)
}

// SetPeers refreshes the bounded routing neighborhood while preserving
// existing eager/lazy classifications for peers that remain connected.
// Callers should order protected ring neighbors first.
func (e *Engine) SetPeers(peers, protected []ids.NodeID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	want := make(map[ids.NodeID]struct{}, len(peers))
	for _, peer := range unique(peers) {
		want[peer] = struct{}{}
	}
	e.protected = make(map[ids.NodeID]struct{}, len(protected))
	for _, peer := range unique(protected) {
		if _, active := want[peer]; active {
			e.protected[peer] = struct{}{}
		}
	}
	for peer := range e.eager {
		if _, ok := want[peer]; !ok {
			delete(e.eager, peer)
		}
	}
	for peer := range e.lazy {
		if _, ok := want[peer]; !ok {
			delete(e.lazy, peer)
		}
	}
	for _, peer := range unique(peers) {
		if _, keepEager := e.eager[peer]; keepEager {
			continue
		}
		if _, keepLazy := e.lazy[peer]; keepLazy {
			delete(e.lazy, peer)
		}
		if _, isProtected := e.protected[peer]; !isProtected {
			continue
		}
		if len(e.eager) >= e.cfg.EagerFanout {
			for candidate := range e.eager {
				if _, candidateProtected := e.protected[candidate]; !candidateProtected {
					delete(e.eager, candidate)
					e.lazy[candidate] = struct{}{}
					break
				}
			}
		}
		e.eager[peer] = struct{}{}
	}
	for _, peer := range unique(peers) {
		if _, ok := e.eager[peer]; ok {
			continue
		}
		if _, ok := e.lazy[peer]; ok {
			continue
		}
		if len(e.eager)+len(e.lazy) >= e.cfg.MaxNeighbors {
			break
		}
		if len(e.eager) < e.cfg.EagerFanout {
			e.eager[peer] = struct{}{}
		} else {
			e.lazy[peer] = struct{}{}
		}
	}
}

// Has reports whether id is still in the bounded duplicate cache. It also
// expires old entries before answering.
func (e *Engine) Has(id [32]byte) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.expireLocked(time.Now())
	return e.cache[id] != nil
}

func (e *Engine) putLocked(id [32]byte, payload []byte) error {
	if int64(len(payload)) > e.cfg.MaxCacheBytes {
		return ErrPayloadTooLarge
	}
	e.expireLocked(time.Now())
	if el := e.cache[id]; el != nil {
		e.lru.MoveToFront(el)
		return nil
	}
	entry := cacheEntry{id: id, payload: append([]byte(nil), payload...), inserted: time.Now()}
	e.cache[id] = e.lru.PushFront(entry)
	e.bytes += int64(len(entry.payload))
	for len(e.cache) > e.cfg.MaxCacheEntries || e.bytes > e.cfg.MaxCacheBytes {
		e.removeOldestLocked()
	}
	return nil
}

func (e *Engine) expireLocked(now time.Time) {
	for el := e.lru.Back(); el != nil; {
		prev := el.Prev()
		entry := el.Value.(cacheEntry)
		if now.Sub(entry.inserted) >= e.cfg.CacheTTL {
			e.removeLocked(el)
		}
		el = prev
	}
}
func (e *Engine) removeOldestLocked() {
	if el := e.lru.Back(); el != nil {
		e.removeLocked(el)
	}
}
func (e *Engine) removeLocked(el *list.Element) {
	entry := el.Value.(cacheEntry)
	delete(e.cache, entry.id)
	e.bytes -= int64(len(entry.payload))
	e.lru.Remove(el)
}

func unique(in []ids.NodeID) []ids.NodeID {
	seen := make(map[ids.NodeID]struct{}, len(in))
	out := make([]ids.NodeID, 0, len(in))
	for _, p := range in {
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}
	return out
}
func keys(in map[ids.NodeID]struct{}) []ids.NodeID {
	out := make([]ids.NodeID, 0, len(in))
	for p := range in {
		out = append(out, p)
	}
	return out
}
