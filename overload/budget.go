// Package overload contains reusable bounded admission and rate-budget
// primitives for replication queues and transfers.
package overload

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	ErrOverloaded = errors.New("overload: budget exhausted; retry later")
	ErrPeerLimit  = errors.New("overload: tracked peer limit reached")
)

// Limit bounds both bytes and entries globally and per peer. Zero is not an
// unlimited value: callers must provide positive limits to make admission
// explicitly bounded.
type Limit struct {
	Bytes       int64
	Entries     int64
	PeerBytes   int64
	PeerEntries int64
	MaxPeers    int
}

type usage struct{ bytes, entries int64 }

// Counter is a thread-safe aggregate/per-peer resource ledger.
type Counter struct {
	mu     sync.Mutex
	limit  Limit
	global usage
	peers  map[string]usage
}

func NewCounter(limit Limit) (*Counter, error) {
	if limit.Bytes <= 0 || limit.Entries <= 0 || limit.PeerBytes <= 0 || limit.PeerEntries <= 0 || limit.MaxPeers <= 0 || limit.PeerBytes > limit.Bytes || limit.PeerEntries > limit.Entries {
		return nil, fmt.Errorf("overload: invalid counter limits")
	}
	return &Counter{limit: limit, peers: make(map[string]usage)}, nil
}

// Lease releases the reserved accounting exactly once.
type Lease struct {
	counter        *Counter
	peer           string
	bytes, entries int64
	one            sync.Once
}

func (c *Counter) Acquire(peer string, bytes, entries int64) (*Lease, error) {
	if peer == "" || bytes < 0 || entries < 0 || (bytes == 0 && entries == 0) {
		return nil, fmt.Errorf("overload: invalid reservation")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	u, exists := c.peers[peer]
	if !exists && len(c.peers) >= c.limit.MaxPeers {
		return nil, ErrPeerLimit
	}
	if exceeds(c.global.bytes, bytes, c.limit.Bytes) || exceeds(c.global.entries, entries, c.limit.Entries) || exceeds(u.bytes, bytes, c.limit.PeerBytes) || exceeds(u.entries, entries, c.limit.PeerEntries) {
		return nil, ErrOverloaded
	}
	c.global.bytes += bytes
	c.global.entries += entries
	u.bytes += bytes
	u.entries += entries
	c.peers[peer] = u
	return &Lease{counter: c, peer: peer, bytes: bytes, entries: entries}, nil
}

func (l *Lease) Release() {
	if l == nil || l.counter == nil {
		return
	}
	l.one.Do(func() {
		c := l.counter
		c.mu.Lock()
		defer c.mu.Unlock()
		c.global.bytes -= l.bytes
		c.global.entries -= l.entries
		u := c.peers[l.peer]
		u.bytes -= l.bytes
		u.entries -= l.entries
		if u.bytes == 0 && u.entries == 0 {
			delete(c.peers, l.peer)
		} else {
			c.peers[l.peer] = u
		}
	})
}

type Snapshot struct {
	Bytes, Entries int64
	Peers          int
}

func (c *Counter) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Snapshot{Bytes: c.global.bytes, Entries: c.global.entries, Peers: len(c.peers)}
}

func exceeds(current, add, max int64) bool { return add > max-current }

type bucket struct {
	tokens float64
	last   time.Time
}

// RateLimiter applies a token bucket globally and independently per peer.
// maxPeers bounds retained per-peer bucket state; call ForgetPeer on explicit
// peer retirement to release its bucket.
type RateLimiter struct {
	mu                     sync.Mutex
	globalRate, peerRate   float64
	globalBurst, peerBurst float64
	maxPeers               int
	global                 bucket
	peers                  map[string]*bucket
}

func NewRateLimiter(globalRate, peerRate, globalBurst, peerBurst int64, maxPeers int) (*RateLimiter, error) {
	if globalRate <= 0 || peerRate <= 0 || globalBurst <= 0 || peerBurst <= 0 || maxPeers <= 0 || peerBurst > globalBurst {
		return nil, fmt.Errorf("overload: invalid rate limits")
	}
	now := time.Now()
	return &RateLimiter{globalRate: float64(globalRate), peerRate: float64(peerRate), globalBurst: float64(globalBurst), peerBurst: float64(peerBurst), maxPeers: maxPeers, global: bucket{tokens: float64(globalBurst), last: now}, peers: make(map[string]*bucket)}, nil
}

func (r *RateLimiter) ForgetPeer(peer string) { r.mu.Lock(); delete(r.peers, peer); r.mu.Unlock() }

// WaitN waits until both the global and per-peer buckets have n bytes. Requests
// larger than a burst are rejected because they could never be admitted.
func (r *RateLimiter) WaitN(ctx context.Context, peer string, n int64) error {
	if peer == "" || n <= 0 {
		return fmt.Errorf("overload: invalid rate request")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if float64(n) > r.globalBurst || float64(n) > r.peerBurst {
		return ErrOverloaded
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		p, ok := r.peers[peer]
		if !ok {
			if len(r.peers) >= r.maxPeers {
				r.mu.Unlock()
				return ErrPeerLimit
			}
			p = &bucket{tokens: r.peerBurst, last: time.Now()}
			r.peers[peer] = p
		}
		now := time.Now()
		refill(&r.global, r.globalRate, r.globalBurst, now)
		refill(p, r.peerRate, r.peerBurst, now)
		need := float64(n)
		if r.global.tokens >= need && p.tokens >= need {
			r.global.tokens -= need
			p.tokens -= need
			r.mu.Unlock()
			return nil
		}
		wait := math.Max((need-r.global.tokens)/r.globalRate, (need-p.tokens)/r.peerRate)
		d := time.Duration(wait * float64(time.Second))
		if d < time.Nanosecond {
			d = time.Nanosecond
		}
		r.mu.Unlock()
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			return ctx.Err()
		case <-t.C:
		}
	}
}

func refill(b *bucket, rate, burst float64, now time.Time) {
	if now.After(b.last) {
		b.tokens = math.Min(burst, b.tokens+now.Sub(b.last).Seconds()*rate)
		b.last = now
	}
}
