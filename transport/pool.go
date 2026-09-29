package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/spedsql/ids"
)

// SessionPurpose classifies the intent of a connection or session for admission.
type SessionPurpose int

const (
	// PurposeGeneric represents an unclassified connection.
	PurposeGeneric SessionPurpose = iota
	// PurposeMembership represents SWIM/memberlist discovery or gossip.
	PurposeMembership
	// PurposeSelectedTarget represents an active outbound push replication target.
	PurposeSelectedTarget
	// PurposeRepair represents anti-entropy or ForceSync sync/repair.
	PurposeRepair
	// PurposeInboundReplication represents an inbound replication stream.
	PurposeInboundReplication
)

var (
	// ErrConnectionPoolExhausted is returned when the hard connection limit is reached.
	ErrConnectionPoolExhausted = errors.New("transport: connection pool exhausted")
	// ErrSessionCapacityExhausted is returned when replication session limits or reservations are exceeded.
	ErrSessionCapacityExhausted = errors.New("transport: replication session capacity exhausted")
	// ErrPoolClosed is returned when operating on a closed pool.
	ErrPoolClosed = errors.New("transport: connection pool closed")
)

// PoolOptions configures the shared connection and session pool.
type PoolOptions struct {
	MaxConnections         int
	ReservedMembership     int
	MaxReplicationSessions int
	Fanout                 int
	MaxConcurrentRepairs   int
	Creds                  *Credentials
}

func (o *PoolOptions) withDefaults() {
	if o.MaxConnections <= 0 {
		o.MaxConnections = 32
	}
	if o.ReservedMembership <= 0 {
		o.ReservedMembership = 8
	}
	if o.MaxReplicationSessions <= 0 {
		o.MaxReplicationSessions = 8
	}
	if o.Fanout <= 0 {
		o.Fanout = 3
	}
	if o.MaxConcurrentRepairs <= 0 {
		o.MaxConcurrentRepairs = 1
	}
}

type poolConn struct {
	sess     *Session
	refs     int
	pinned   bool
	purpose  SessionPurpose
	lastUsed time.Time
}

type dialFlight struct {
	done chan struct{}
	sess *Session
	err  error
}

// Pool manages a shared bounded QUIC connection pool with reserved capacities.
type Pool struct {
	mu    sync.Mutex
	opts  PoolOptions
	conns map[ids.NodeID]*poolConn
	dials map[ids.NodeID]*dialFlight

	// replication session counts by purpose
	targetCount  int
	repairCount  int
	inboundCount int
	genericCount int

	connDeferrals    atomic.Uint64
	sessionDeferrals atomic.Uint64
	evictions        atomic.Uint64
	dialsTotal       atomic.Uint64
	dialsCoalesced   atomic.Uint64
	dialsReused      atomic.Uint64

	closed bool
}

// NewPool creates a new shared connection pool.
func NewPool(opts PoolOptions) (*Pool, error) {
	opts.withDefaults()
	if opts.Fanout+opts.MaxConcurrentRepairs > opts.MaxReplicationSessions {
		return nil, fmt.Errorf("transport: Fanout + MaxConcurrentRepairs (%d) exceeds MaxReplicationSessions (%d)",
			opts.Fanout+opts.MaxConcurrentRepairs, opts.MaxReplicationSessions)
	}
	if opts.MaxConnections < opts.MaxReplicationSessions+opts.ReservedMembership {
		return nil, fmt.Errorf("transport: MaxConnections (%d) must be at least MaxReplicationSessions + ReservedMembership (%d)",
			opts.MaxConnections, opts.MaxReplicationSessions+opts.ReservedMembership)
	}
	return &Pool{
		opts:  opts,
		conns: make(map[ids.NodeID]*poolConn),
		dials: make(map[ids.NodeID]*dialFlight),
	}, nil
}

// PoolStats returns a point-in-time snapshot of connection and session usage.
type PoolStats struct {
	ActiveConnections  int
	SelectedTargets    int
	ActiveRepairs      int
	InboundSessions    int
	GenericSessions    int
	TotalSessions      int
	MaxConnections     int
	MaxSessions        int
	ReservedMembership int

	ConnDeferrals    uint64
	SessionDeferrals uint64
	Evictions        uint64
	Dials            uint64
	DialsCoalesced   uint64
	DialsReused      uint64
}

// Stats snapshots pool metrics.
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanupDeadLocked()
	return PoolStats{
		ActiveConnections:  len(p.conns),
		SelectedTargets:    p.targetCount,
		ActiveRepairs:      p.repairCount,
		InboundSessions:    p.inboundCount,
		GenericSessions:    p.genericCount,
		TotalSessions:      p.targetCount + p.repairCount + p.inboundCount + p.genericCount,
		MaxConnections:     p.opts.MaxConnections,
		MaxSessions:        p.opts.MaxReplicationSessions,
		ReservedMembership: p.opts.ReservedMembership,

		ConnDeferrals:    p.connDeferrals.Load(),
		SessionDeferrals: p.sessionDeferrals.Load(),
		Evictions:        p.evictions.Load(),
		Dials:            p.dialsTotal.Load(),
		DialsCoalesced:   p.dialsCoalesced.Load(),
		DialsReused:      p.dialsReused.Load(),
	}
}

func (p *Pool) cleanupDeadLocked() {
	for id, pc := range p.conns {
		if pc.sess.Context().Err() != nil {
			p.releaseSessionLocked(pc.purpose)
			delete(p.conns, id)
		}
	}
}

func (p *Pool) canAdmitSessionLocked(purpose SessionPurpose) bool {
	// Membership has its own reserved connection allowance and must remain
	// usable while all replication session slots are occupied by bulk work.
	if purpose == PurposeMembership {
		return true
	}
	totalReplication := p.targetCount + p.repairCount + p.inboundCount + p.genericCount
	if totalReplication >= p.opts.MaxReplicationSessions {
		return false
	}
	switch purpose {
	case PurposeSelectedTarget:
		return p.targetCount < p.opts.Fanout
	case PurposeRepair:
		return p.repairCount < p.opts.MaxConcurrentRepairs
	case PurposeInboundReplication:
		// Inbound cannot consume reserved outbound target or repair capacity
		unreserved := p.opts.MaxReplicationSessions - (p.opts.Fanout + p.opts.MaxConcurrentRepairs)
		if unreserved < 0 {
			unreserved = 0
		}
		return p.inboundCount < unreserved
	default:
		return totalReplication < p.opts.MaxReplicationSessions
	}
}

func (p *Pool) admitSessionLocked(purpose SessionPurpose) {
	switch purpose {
	case PurposeMembership:
		// Membership consumes reserved connections, not replication slots.
	case PurposeSelectedTarget:
		p.targetCount++
	case PurposeRepair:
		p.repairCount++
	case PurposeInboundReplication:
		p.inboundCount++
	default:
		p.genericCount++
	}
}

func (p *Pool) releaseSessionLocked(purpose SessionPurpose) {
	switch purpose {
	case PurposeMembership:
		// Membership is bounded by ReservedMembership connections.
	case PurposeSelectedTarget:
		if p.targetCount > 0 {
			p.targetCount--
		}
	case PurposeRepair:
		if p.repairCount > 0 {
			p.repairCount--
		}
	case PurposeInboundReplication:
		if p.inboundCount > 0 {
			p.inboundCount--
		}
	default:
		if p.genericCount > 0 {
			p.genericCount--
		}
	}
}

// AdmitInbound checks and reserves capacity for an inbound replication connection.
func (p *Pool) AdmitInbound(peer ids.NodeID, purpose SessionPurpose) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	p.cleanupDeadLocked()

	// If peer already has an active session, allow reuse/attachment
	if pc, ok := p.conns[peer]; ok && pc.sess.Context().Err() == nil {
		pc.refs++
		pc.lastUsed = time.Now()
		return nil
	}

	// Check total connection limit (reserving membership slots)
	if purpose != PurposeMembership {
		if len(p.conns) >= p.opts.MaxConnections-p.opts.ReservedMembership {
			// Try to evict an idle connection
			if !p.evictIdleLocked() {
				p.connDeferrals.Add(1)
				return ErrConnectionPoolExhausted
			}
		}
	} else {
		if len(p.conns) >= p.opts.MaxConnections {
			if !p.evictIdleLocked() {
				p.connDeferrals.Add(1)
				return ErrConnectionPoolExhausted
			}
		}
	}

	if !p.canAdmitSessionLocked(purpose) {
		p.sessionDeferrals.Add(1)
		return ErrSessionCapacityExhausted
	}
	p.admitSessionLocked(purpose)
	return nil
}

// RegisterSession registers an established connection with the pool.
func (p *Pool) RegisterSession(sess *Session, purpose SessionPurpose, pinned bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	p.cleanupDeadLocked()
	peer := sess.Peer
	if existing, ok := p.conns[peer]; ok && existing.sess != sess {
		if existing.sess.Context().Err() == nil {
			_ = existing.sess.Close()
		}
		p.releaseSessionLocked(existing.purpose)
	}
	p.conns[peer] = &poolConn{
		sess:     sess,
		refs:     1,
		pinned:   pinned,
		purpose:  purpose,
		lastUsed: time.Now(),
	}
	return nil
}

func (p *Pool) evictIdleLocked() bool {
	var oldestID ids.NodeID
	var oldestTime time.Time
	found := false
	for id, pc := range p.conns {
		if pc.refs == 0 && !pc.pinned {
			if !found || pc.lastUsed.Before(oldestTime) {
				oldestID = id
				oldestTime = pc.lastUsed
				found = true
			}
		}
	}
	if found {
		pc := p.conns[oldestID]
		_ = pc.sess.Close()
		p.releaseSessionLocked(pc.purpose)
		delete(p.conns, oldestID)
		p.evictions.Add(1)
		return true
	}
	return false
}

// Dial connects to expect, reusing existing connections or singleflight-coalescing concurrent dials.
func (p *Pool) Dial(ctx context.Context, addr string, creds *Credentials, expect ids.NodeID, purpose SessionPurpose) (*Session, error) {
	p.dialsTotal.Add(1)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	p.cleanupDeadLocked()

	// 1. Connection reuse: if active connection exists, reuse it
	if pc, ok := p.conns[expect]; ok && pc.sess.Context().Err() == nil {
		pc.refs++
		pc.lastUsed = time.Now()
		p.dialsReused.Add(1)
		p.mu.Unlock()
		return pc.sess, nil
	}

	// 2. Singleflight dial coalescing: if dial to expect is already in progress, wait for it
	if flight, ok := p.dials[expect]; ok {
		p.dialsCoalesced.Add(1)
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-flight.done:
			if flight.err != nil {
				return nil, flight.err
			}
			p.mu.Lock()
			if pc, ok := p.conns[expect]; ok && pc.sess.Context().Err() == nil {
				pc.refs++
				pc.lastUsed = time.Now()
				p.mu.Unlock()
				return pc.sess, nil
			}
			p.mu.Unlock()
			return flight.sess, nil
		}
	}

	// 3. Admission check
	if purpose != PurposeMembership {
		if len(p.conns) >= p.opts.MaxConnections-p.opts.ReservedMembership {
			if !p.evictIdleLocked() {
				p.connDeferrals.Add(1)
				p.mu.Unlock()
				return nil, ErrConnectionPoolExhausted
			}
		}
	} else {
		if len(p.conns) >= p.opts.MaxConnections {
			if !p.evictIdleLocked() {
				p.connDeferrals.Add(1)
				p.mu.Unlock()
				return nil, ErrConnectionPoolExhausted
			}
		}
	}

	if !p.canAdmitSessionLocked(purpose) {
		p.sessionDeferrals.Add(1)
		p.mu.Unlock()
		return nil, ErrSessionCapacityExhausted
	}

	// Start dial flight
	flight := &dialFlight{done: make(chan struct{})}
	p.dials[expect] = flight
	p.admitSessionLocked(purpose)
	p.mu.Unlock()

	// Dial outside pool lock
	sess, err := Dial(ctx, addr, creds, expect)

	p.mu.Lock()
	delete(p.dials, expect)
	flight.sess = sess
	flight.err = err
	close(flight.done)

	if err != nil {
		p.releaseSessionLocked(purpose)
		p.mu.Unlock()
		return nil, err
	}

	if p.closed {
		_ = sess.Close()
		p.releaseSessionLocked(purpose)
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}

	pinned := purpose == PurposeSelectedTarget
	p.conns[expect] = &poolConn{
		sess:     sess,
		refs:     1,
		pinned:   pinned,
		purpose:  purpose,
		lastUsed: time.Now(),
	}
	p.mu.Unlock()
	return sess, nil
}

// Release decreases the reference count and updates the last used time.
func (p *Pool) Release(peer ids.NodeID, purpose SessionPurpose) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pc, ok := p.conns[peer]; ok {
		if pc.refs > 0 {
			pc.refs--
		}
		pc.lastUsed = time.Now()
		if pc.refs == 0 && pc.sess.Context().Err() != nil {
			p.releaseSessionLocked(pc.purpose)
			delete(p.conns, peer)
		}
	}
}

// Close closes all pooled connections and rejects new operations.
func (p *Pool) Close() error {
	p.mu.Lock()
	p.closed = true
	conns := make([]*Session, 0, len(p.conns))
	for _, pc := range p.conns {
		conns = append(conns, pc.sess)
	}
	p.conns = make(map[ids.NodeID]*poolConn)
	p.mu.Unlock()

	for _, s := range conns {
		_ = s.Close()
	}
	return nil
}
