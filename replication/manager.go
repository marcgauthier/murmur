package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/quic-go/quic-go"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/overload"
	"github.com/nomadsql/replicateddb/plumtree"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/state"
	"github.com/nomadsql/replicateddb/transport"
)

var (
	// ErrPeerExcluded indicates a peer is locally retired / excluded.
	ErrPeerExcluded = errors.New("replication: peer is locally excluded")
	// ErrPeerNotFound indicates the requested peer is unknown.
	ErrPeerNotFound = errors.New("replication: peer not found")
)

const (
	queueBudgetBytes          int64 = 64 << 20
	queueBudgetEntries        int64 = 8192
	queuePeerBytes            int64 = 4 << 20
	queuePeerEntries          int64 = 128
	queueBudgetPeers                = 4096
	controlFramesPerRound           = 16
	needsPerRound                   = 16
	peerBatchQuantum                = 128
	maxApplyGroupTransactions       = 64
	maxApplyGroupBytes        int64 = 64 << 20
)

// Logger mirrors the root Logger to avoid an import cycle.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Applier durably applies received data and materializes winners into the
// query engine. Implementations must send no acknowledgement before the
// Pebble commit; acknowledgements are handled by the Manager.
type Applier interface {
	ApplyRemote(ctx context.Context, batch *codec.MutationBatch) error
	ApplySnapshotChunk(ctx context.Context, manifest *codec.SnapshotManifest, index uint64, cells []codec.SnapshotCell, last bool) (bool, error)
}

// GroupApplier optionally commits an ordered group in one durable write while
// preserving each transaction's identity and sequence position.
type GroupApplier interface {
	ApplyRemoteGroup(ctx context.Context, batches []*codec.MutationBatch) error
}

// SchemaIdentity summarizes the locally published schema revision.
type SchemaIdentity struct {
	Epoch       uint64
	Hash        [32]byte
	Author      ids.NodeID
	TimeCreated uint64
}

// SyncDecision tells a session what to do after SyncSchemas processes
// received revisions.
type SyncDecision struct {
	// Agreed reports schemas now match, so data may flow.
	Agreed bool
	// SendRevisions asks the session to send our current manifest plus
	// ancestry to the peer.
	SendRevisions bool
	// SendAck acknowledges our current identity to the peer.
	SendAck bool
	// NeedIDs lists ancestry still missing; the session requests it.
	NeedIDs [][32]byte
	// Err reports an incompatible schema: the session sends MsgError and
	// stays data-gated with nothing applied or acknowledged.
	Err error
}

// SchemaSyncer is implemented by the database to back schema
// synchronization (architecture/schema.md section 50). All methods must be
// safe for concurrent use. SyncSchemas runs on a session read loop and may
// block on database locks; it must never send on any session (the read
// loop stays send-free), it only returns what to send.
type SchemaSyncer interface {
	// CurrentSchema returns the locally published schema identity.
	CurrentSchema() SchemaIdentity
	// RevisionsForPeer returns up to maxBytes of manifests: our current
	// tip first, then ancestors covering wantIDs (plus the walk back from
	// current when wantCurrent). Unknown IDs are skipped.
	RevisionsForPeer(wantCurrent bool, wantIDs [][32]byte, maxBytes int) ([]*schema.Manifest, error)
	// SyncSchemas validates received revisions (tip first) and advances
	// local schema toward the peer: no-op, adopt, merge-publish, or
	// request-more-ancestry.
	SyncSchemas(ctx context.Context, revs []*schema.Manifest) SyncDecision
	// SchemaProvenanceKnown reports whether (epoch, hash) is the current
	// manifest or a persisted compatible ancestor, making retained
	// batches applicable.
	SchemaProvenanceKnown(epoch uint64, hash [32]byte) bool
}

// PeerInfo is a configured peer.
type PeerInfo struct {
	NodeID ids.NodeID
	Addrs  []string
}

// ManagerConfig configures replication.
type ManagerConfig struct {
	Store   *state.Store
	Applier Applier
	Creds   *transport.Credentials

	Local       ids.NodeID
	DBID        ids.DBID
	SchemaEpoch uint64
	SchemaHash  [32]byte
	// SchemaAuthor/SchemaTime identify the author and HLC creation time
	// of the current schema revision (initial values; RefreshSchema
	// updates them on local migration).
	SchemaAuthor ids.NodeID
	SchemaTime   uint64
	// AcceptRemoteSchema enables schema synchronization: mismatched peers
	// exchange manifests and converge instead of refusing. False keeps the
	// strict behavior (no session, no data) until explicit upgrade.
	AcceptRemoteSchema bool
	// SchemaSync backs schema synchronization. Nil means no adoption
	// engine: mismatches always refuse, and only current-schema batches
	// apply. The database provides the full implementation.
	SchemaSync SchemaSyncer

	ListenAddr string
	Peers      []PeerInfo

	MaxBatchBytes     int
	MaxBatchMutations int
	SendInterval      time.Duration
	DialInterval      time.Duration
	AckInterval       time.Duration
	// AckRetention is the offline log-retention window backing persisted
	// member deadlines: admission grants now+AckRetention, and each ack
	// that advances durable progress renews it. GC gates on those
	// deadlines, never on session liveness. Default 7 days.
	AckRetention time.Duration
	// SendTimeout bounds one framed write. A peer that stops reading
	// (wedged or dead without closing) must never hang a sender forever:
	// the write fails, the session is recycled, and cursors resume.
	// Default 30s.
	SendTimeout time.Duration

	SnapshotChunkCells      int
	MaxSnapshotBytes        uint64
	SnapshotTransferTimeout time.Duration
	// SnapshotRequestTimeout bounds how long a receiver waits for
	// snapshot progress (manifest or chunks) before the stall watchdog
	// re-requests. Default 30s.
	SnapshotRequestTimeout time.Duration
	// AdvertiseProtocolVersion/MinProtocolVersion override the
	// handshake's advertised versions when nonzero, for
	// interoperability testing (peers must refuse unknown versions).
	// Zero selects the protocol constants. Never set in production.
	AdvertiseProtocolVersion    uint16
	AdvertiseMinProtocolVersion uint16
	Fanout                      int
	PeerRotationInterval        time.Duration
	AntiEntropyInterval         time.Duration
	MaxConcurrentRepairs        int
	MaxReplicationSessions      int
	MaxQUICConnections          int
	Pool                        *transport.Pool
	// EnablePlumtree advertises the optional eager/lazy dissemination mode.
	// Anti-entropy and durable Need repair remain active in either mode.
	EnablePlumtree bool

	MaxTransactionBytes int64
	Limits              codec.Limits
	Logger              Logger
}

func (c *ManagerConfig) withDefaults() {
	if c.MaxBatchBytes == 0 {
		c.MaxBatchBytes = 1 << 20
	}
	if c.MaxBatchMutations == 0 {
		c.MaxBatchMutations = 10_000
	}
	if c.SendInterval == 0 {
		c.SendInterval = 20 * time.Millisecond
	}
	if c.DialInterval == 0 {
		c.DialInterval = 5 * time.Second
	}
	if c.AckInterval == 0 {
		c.AckInterval = time.Second
	}
	if c.AckRetention == 0 {
		c.AckRetention = 7 * 24 * time.Hour
	}
	if c.SendTimeout == 0 {
		c.SendTimeout = 30 * time.Second
	}
	if c.SnapshotChunkCells == 0 {
		c.SnapshotChunkCells = 2_000
	}
	if c.MaxSnapshotBytes == 0 {
		c.MaxSnapshotBytes = 512 << 20
	}
	if c.SnapshotTransferTimeout == 0 {
		c.SnapshotTransferTimeout = 10 * time.Minute
	}
	if c.SnapshotRequestTimeout == 0 {
		c.SnapshotRequestTimeout = 30 * time.Second
	}
	if c.Fanout == 0 {
		c.Fanout = 4
	}
	if c.PeerRotationInterval == 0 {
		c.PeerRotationInterval = 30 * time.Second
	}
	if c.AntiEntropyInterval == 0 {
		c.AntiEntropyInterval = 10 * time.Second
	}
	if c.MaxConcurrentRepairs == 0 {
		c.MaxConcurrentRepairs = 2
	}
	if c.MaxReplicationSessions == 0 {
		c.MaxReplicationSessions = 32
	}
	if c.MaxQUICConnections == 0 {
		c.MaxQUICConnections = 64
	}
	if c.MaxTransactionBytes == 0 {
		c.MaxTransactionBytes = 64 << 20
	}
	if c.Limits.MaxValueBytes == 0 {
		c.Limits = codec.DefaultLimits()
	}
	if c.Limits.MaxTransactionBytes == 0 {
		c.Limits.MaxTransactionBytes = c.MaxTransactionBytes
	}
}

// PeerStatus is a point-in-time peer snapshot. It never contains secrets:
// only identities, addresses, watermarks, sizes, and timestamps.
type PeerStatus struct {
	NodeID    ids.NodeID
	Addrs     []string
	Connected bool
	// Dynamic reports an inbound-discovered peer with no static
	// configuration (as opposed to a configured peer).
	Dynamic  bool
	Selected bool
	LastSeen time.Time
	RTT      time.Duration
	Have     map[ids.NodeID]uint64
	Sent     map[ids.NodeID]uint64
	// LastHandshake is the most recent session attach; LastSend/LastRecv
	// cover session traffic after the handshake.
	LastHandshake   time.Time
	LastSend        time.Time
	LastRecv        time.Time
	LastAntiEntropy time.Time
	// SchemaAgreed gates data flow; RemoteSchemaEpoch/Hash identify the
	// peer's advertised revision.
	SchemaAgreed      bool
	RemoteSchemaEpoch uint64
	RemoteSchemaHash  [32]byte
	// SnapshotRequired reports a signalled origin-log gap the peer must
	// heal with a snapshot; AwaitingSnapshot reports an outbound request
	// we are still waiting for.
	SnapshotRequired bool
	AwaitingSnapshot bool
	// SnapshotChunksReceived/Total track the in-flight inbound transfer
	// from this peer (zero when none is active).
	SnapshotChunksReceived uint64
	SnapshotChunksTotal    uint64
	// BytesSent/BytesReceived count session frame bytes (payload plus
	// frame header) in both directions.
	BytesSent     uint64
	BytesReceived uint64
	// Queued depths are the current outbound backlogs for this peer.
	QueuedNeed   int
	QueuedCtrl   int
	QueuedSchema int
}

// peerSession is one live connection with separate control, data, and snapshot streams to a peer.
type peerSession struct {
	sess        *transport.Session
	ctrlStream  *quic.Stream
	ctrlWriteMu sync.Mutex

	dataStream  *quic.Stream
	dataWriteMu sync.Mutex

	outbound  bool // true when we dialed
	done      chan struct{}
	closeOnce sync.Once
	timeout   time.Duration // per-write deadline (manager SendTimeout)
	// peer/st are installed by attach for traffic accounting; they stay
	// nil only on sessions that never attach (handshake failures).
	peer *peerState
	st   *stats
}

func (s *peerSession) isClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *peerSession) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.ctrlStream != nil {
			_ = s.ctrlStream.Close()
		}
		s.dataWriteMu.Lock()
		if s.dataStream != nil {
			_ = s.dataStream.Close()
			s.dataStream = nil
		}
		s.dataWriteMu.Unlock()
		_ = s.sess.Close()
	})
}

func (s *peerSession) sendCtrl(typ uint16, flags uint16, payload []byte) error {
	s.ctrlWriteMu.Lock()
	defer s.ctrlWriteMu.Unlock()
	select {
	case <-s.done:
		return fmt.Errorf("replication: session closed")
	default:
	}
	if s.timeout > 0 {
		_ = s.ctrlStream.SetWriteDeadline(time.Now().Add(s.timeout))
	}
	if err := WriteFrame(s.ctrlStream, typ, flags, payload); err != nil {
		return &sendError{err: err}
	}
	if n := uint64(len(payload) + frameHdrLen); n > 0 {
		if s.st != nil {
			s.st.frameBytesSent.Add(n)
		}
		if s.peer != nil {
			s.peer.mu.Lock()
			s.peer.bytesSent += n
			s.peer.lastSend = time.Now()
			s.peer.mu.Unlock()
		}
	}
	return nil
}

func (s *peerSession) sendData(flags uint16, payload []byte) error {
	return s.sendDataType(MsgBatches, flags, payload)
}

func (s *peerSession) sendDataType(typ uint16, flags uint16, payload []byte) error {
	select {
	case <-s.done:
		return fmt.Errorf("replication: session closed")
	default:
	}
	s.dataWriteMu.Lock()
	defer s.dataWriteMu.Unlock()
	select {
	case <-s.done:
		return fmt.Errorf("replication: session closed")
	default:
	}
	if s.dataStream == nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		stream, err := s.sess.OpenStream(ctx)
		cancel()
		if err != nil {
			return &sendError{err: err}
		}
		if s.st != nil {
			s.st.dataStreamsOpened.Add(1)
		}
		s.dataStream = stream
	}
	if s.timeout > 0 {
		_ = s.dataStream.SetWriteDeadline(time.Now().Add(s.timeout))
	}
	if err := WriteFrame(s.dataStream, typ, flags, payload); err != nil {
		_ = s.dataStream.Close()
		s.dataStream = nil
		return &sendError{err: err}
	}
	if n := uint64(len(payload) + frameHdrLen); n > 0 {
		if s.st != nil {
			s.st.frameBytesSent.Add(n)
		}
		if s.peer != nil {
			s.peer.mu.Lock()
			s.peer.bytesSent += n
			s.peer.lastSend = time.Now()
			s.peer.mu.Unlock()
		}
	}
	return nil
}

func (s *peerSession) openSnapshotStream(ctx context.Context) (*quic.Stream, error) {
	select {
	case <-s.done:
		return nil, fmt.Errorf("replication: session closed")
	default:
	}
	stream, err := s.sess.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	if s.st != nil {
		s.st.snapStreamsOpened.Add(1)
	}
	return stream, nil
}

func (s *peerSession) sendStream(stream *quic.Stream, typ uint16, flags uint16, payload []byte) error {
	select {
	case <-s.done:
		return fmt.Errorf("replication: session closed")
	default:
	}
	if s.timeout > 0 {
		_ = stream.SetWriteDeadline(time.Now().Add(s.timeout))
	}
	if err := WriteFrame(stream, typ, flags, payload); err != nil {
		return &sendError{err: err}
	}
	if n := uint64(len(payload) + frameHdrLen); n > 0 {
		if s.st != nil {
			s.st.frameBytesSent.Add(n)
		}
		if s.peer != nil {
			s.peer.mu.Lock()
			s.peer.bytesSent += n
			s.peer.lastSend = time.Now()
			s.peer.mu.Unlock()
		}
	}
	return nil
}

func (s *peerSession) send(typ uint16, flags uint16, payload []byte) error {
	switch typ {
	case MsgBatches, MsgPlumtreeData, MsgTransactionChunk:
		return s.sendDataType(typ, flags, payload)
	default:
		return s.sendCtrl(typ, flags, payload)
	}
}

// sendError marks transport write failures (the session is suspect and
// should be recycled). It unwraps to the underlying error, so deadline
// timeouts still match os.ErrDeadlineExceeded.
type sendError struct{ err error }

func (e *sendError) Error() string { return "replication: send: " + e.err.Error() }
func (e *sendError) Unwrap() error { return e.err }

// peerState tracks one peer (configured or dynamic inbound).
type peerState struct {
	mu      sync.Mutex
	id      ids.NodeID
	addrs   []string
	dynamic bool
	// stopped marks a peerState detached by RemovePeer/OnPeerLeft. Its
	// dialLoop must exit promptly so a later re-add does not run two
	// dialers for the same ID and flap sessions after heal.
	stopped           atomic.Bool
	selected          bool
	dialFailures      int
	lastAntiEntropy   time.Time
	session           *peerSession
	lastSeen          time.Time
	rtt               time.Duration
	have              map[ids.NodeID]uint64 // peer's watermarks
	observed          map[ids.NodeID]uint64
	retainedFrom      map[ids.NodeID]uint64
	retainedThrough   map[ids.NodeID]uint64
	chunkAvailability map[ids.TxID]ChunkAvailability
	chunkUnavailable  map[ids.TxID]bool
	progressPages     bool
	sent              map[ids.NodeID]uint64 // last seq we sent per origin
	sentErr           map[ids.NodeID]bool   // snapshot-required already signalled
	caps              uint64
	awaiting          bool // we requested a snapshot and wait for it
	// awaitingSince marks when the current wait began; lastSnapProgress
	// marks the last snapshot manifest/chunk received. The stall watchdog
	// re-requests when progress stalls past SnapshotRequestTimeout.
	// snapRetryAfter is a source-requested backoff after a busy deferral.
	awaitingSince    time.Time
	lastSnapProgress time.Time
	snapRetryAfter   time.Time
	forceAck         bool // ForceSync requested an immediate ack+pull
	wake             chan struct{}

	pingNonce uint64
	pingAt    time.Time

	// needCh queues explicit Need requests for the send loop. Needs carry
	// bulk batch payloads, so the read loop must never serve them inline:
	// two peers bulk-sending inside their read loops wedge both flow
	// windows with nobody reading (duplex deadlock). Drops are safe: the
	// peer re-requests every ack tick until its watermark advances.
	needCh chan queuedNeed

	// ctrlCh queues tiny outbound control frames (pong, Need, snapshot
	// request, error replies) for the send loop. The read loop must not
	// send at all — not even tiny frames: the session send mutex couples
	// reader progress to a bulk write that only completes once the peer
	// reads, which wedges both sides symmetrically. Drops are safe
	// (pongs are best-effort; Needs re-request; requests retry).
	ctrlCh     chan ctrlFrame
	retryCh    chan ctrlFrame
	retryAfter map[ids.NodeID]time.Time

	// Schema-sync state. Data (batches, acks/pulls, snapshots) flows only
	// while agreed; schema messages always flow so sessions can converge.
	agreed bool
	remote SchemaIdentity
	// schemaReqCh queues outbound schema requests; schemaRespCh queues
	// inbound requests for the send loop to serve. Drops are safe: both
	// sides re-request until agreed or incompatible.
	schemaReqCh  chan SchemaRequest
	schemaRespCh chan SchemaRequest
	plumtreeCh   chan ctrlFrame
	plumtree     bool

	snapSend atomic.Bool
	snapRecv *snapRecvState

	maxTxBytes uint64

	// Diagnostics, guarded by mu.
	lastHandshake time.Time
	lastSend      time.Time
	lastRecv      time.Time
	bytesSent     uint64
	bytesReceived uint64
}

// ctrlFrame is one tiny outbound control frame queued by the read loop.
type ctrlFrame struct {
	typ     uint16
	flags   uint16
	payload []byte
	due     time.Time
	lease   *overload.Lease
}

type queuedNeed struct {
	need  Need
	lease *overload.Lease
}

func newPeerState(id ids.NodeID, addrs []string, dynamic bool) *peerState {
	return &peerState{
		id: id, addrs: addrs, dynamic: dynamic,
		have:              map[ids.NodeID]uint64{},
		observed:          map[ids.NodeID]uint64{},
		retainedFrom:      map[ids.NodeID]uint64{},
		retainedThrough:   map[ids.NodeID]uint64{},
		chunkAvailability: map[ids.TxID]ChunkAvailability{},
		chunkUnavailable:  make(map[ids.TxID]bool),
		sent:              map[ids.NodeID]uint64{},
		sentErr:           map[ids.NodeID]bool{},
		wake:              make(chan struct{}, 1),
		needCh:            make(chan queuedNeed, 64),
		ctrlCh:            make(chan ctrlFrame, 64),
		retryCh:           make(chan ctrlFrame, 8),
		retryAfter:        make(map[ids.NodeID]time.Time),
		schemaReqCh:       make(chan SchemaRequest, 8),
		schemaRespCh:      make(chan SchemaRequest, 8),
		plumtreeCh:        make(chan ctrlFrame, 4),
	}
}

func (p *peerState) pokeWake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Manager owns QUIC replication: listener, dialers, senders, receivers.
type Manager struct {
	cfg ManagerConfig
	log Logger

	st stats // diagnostics counters; Manager is always used by pointer

	ln         *transport.Listener
	pool       *transport.Pool
	plumtree   *plumtree.Engine
	membership *MembershipService

	mu               sync.Mutex
	peers            map[ids.NodeID]*peerState
	selected         map[ids.NodeID]bool
	excluded         map[ids.NodeID]bool
	chunkRepairAt    map[ids.TxID]time.Time
	chunkRepairNeed  map[ids.TxID]ChunkNeed
	sendPeerCursor   string
	queueBudget      *overload.Counter
	transferLimiter  *overload.RateLimiter
	applyGroupTarget atomic.Uint32
	closed           bool // set by closeSessions under mu; attach refuses past it

	notifyCh chan struct{}
	wg       sync.WaitGroup
	// running publishes Run startup: it is stored (under mu, together
	// with the ctx/cancel/log assignment and the initial peer sweep) so
	// AddPeer can decide race-free whether to spawn a dialer. Loops and
	// sessions only exist while running is true.
	running atomic.Bool
	cancel  context.CancelFunc
	ctx     context.Context
	// schemaMu guards the advertised schema identity, refreshed on local
	// migration while handshakes and validation read it concurrently.
	schemaMu sync.RWMutex
	schemaId SchemaIdentity
}

// NewManager creates a replication manager. It does not start networking;
// call Run.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	cfg.withDefaults()
	if cfg.Store == nil || cfg.Applier == nil || cfg.Creds == nil {
		return nil, fmt.Errorf("replication: store, applier and creds are required")
	}
	local, err := cfg.Creds.LocalNodeID()
	if err != nil {
		return nil, err
	}
	if local != cfg.Local {
		return nil, fmt.Errorf("replication: certificate node %s != configured %s", local, cfg.Local)
	}
	pool := cfg.Pool
	if pool == nil {
		p, err := transport.NewPool(transport.PoolOptions{
			MaxConnections:         cfg.MaxQUICConnections,
			ReservedMembership:     8,
			MaxReplicationSessions: cfg.MaxReplicationSessions,
			Fanout:                 cfg.Fanout,
			MaxConcurrentRepairs:   cfg.MaxConcurrentRepairs,
			Creds:                  cfg.Creds,
		})
		if err != nil {
			return nil, err
		}
		pool = p
	}
	queueBudget, err := overload.NewCounter(overload.Limit{Bytes: queueBudgetBytes, Entries: queueBudgetEntries, PeerBytes: queuePeerBytes, PeerEntries: queuePeerEntries, MaxPeers: queueBudgetPeers})
	if err != nil {
		return nil, err
	}
	transferLimiter, err := overload.NewRateLimiter(64<<20, 16<<20, 16<<20, 8<<20, 4096)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		cfg:             cfg,
		pool:            pool,
		peers:           make(map[ids.NodeID]*peerState),
		selected:        make(map[ids.NodeID]bool),
		excluded:        make(map[ids.NodeID]bool),
		chunkRepairAt:   make(map[ids.TxID]time.Time),
		chunkRepairNeed: make(map[ids.TxID]ChunkNeed),
		queueBudget:     queueBudget,
		transferLimiter: transferLimiter,
		notifyCh:        make(chan struct{}, 1),
		schemaId: SchemaIdentity{
			Epoch:       cfg.SchemaEpoch,
			Hash:        cfg.SchemaHash,
			Author:      cfg.SchemaAuthor,
			TimeCreated: cfg.SchemaTime,
		},
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	if cfg.EnablePlumtree {
		fanout := cfg.Fanout
		if fanout < 2 {
			fanout = 2
		}
		engine, err := plumtree.New(plumtree.Config{EagerFanout: fanout, MaxNeighbors: 256, MaxCacheEntries: 256, MaxCacheBytes: 32 << 20, CacheTTL: time.Minute})
		if err != nil {
			return nil, err
		}
		m.plumtree = engine
	}
	if cfg.Store != nil {
		if list, err := cfg.Store.ListExcludedPeers(); err == nil {
			for _, id := range list {
				m.excluded[id] = true
			}
		}
	}
	for _, p := range cfg.Peers {
		if p.NodeID == cfg.Local || m.excluded[p.NodeID] {
			continue
		}
		m.peers[p.NodeID] = newPeerState(p.NodeID, p.Addrs, false)
	}
	m.reconcileSelectedLocked()
	return m, nil
}

func (m *Manager) logger() Logger {
	if m.cfg.Logger != nil {
		return m.cfg.Logger
	}
	return nilLogger{}
}

type nilLogger struct{}

func (nilLogger) Debug(string, ...any) {}
func (nilLogger) Info(string, ...any)  {}
func (nilLogger) Warn(string, ...any)  {}
func (nilLogger) Error(string, ...any) {}

// Run starts the listener, dialers, and send loop; it blocks until ctx ends.
func (m *Manager) Run(ctx context.Context) error {
	m.mu.Lock()
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.log = m.logger()
	m.reconcileSelectedLocked()
	initial := make([]*peerState, 0, len(m.peers))
	for _, p := range m.peers {
		initial = append(initial, p)
	}
	// Publish startup inside the same critical section AddPeer inserts
	// under: a peer inserted before the sweep is spawned here, one
	// inserted after spawns its own dialer — never zero, never two.
	m.running.Store(true)
	m.mu.Unlock()
	defer m.cancel()
	if m.cfg.ListenAddr != "" {
		ln, err := transport.Listen(m.cfg.ListenAddr, m.cfg.Creds)
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.ln = ln
		m.mu.Unlock()
		m.log.Info("replication listening", slog.String("addr", ln.Addr()))
		m.wg.Add(1)
		go m.acceptLoop()
	}
	for _, p := range initial {
		m.wg.Add(1)
		go m.dialLoop(p)
	}
	m.wg.Add(3)
	go m.sendLoop()
	go m.rotationLoop()
	go m.antiEntropyLoop()
	<-m.ctx.Done()
	m.closeSessions()
	if m.ln != nil {
		_ = m.ln.Close()
	}
	m.wg.Wait()
	return nil
}

// Close stops the manager.
func (m *Manager) Close() error {
	m.running.Store(false)
	m.mu.Lock()
	cancel := m.cancel
	mem := m.membership
	m.mu.Unlock()
	if mem != nil {
		_ = mem.Shutdown()
	}
	if cancel != nil {
		cancel()
	}
	return nil
}

// Addr returns the listener address, or "" when not listening (yet).
func (m *Manager) Addr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ln == nil {
		return ""
	}
	return m.ln.Addr()
}

// Pool returns the shared transport connection and session pool.
func (m *Manager) Pool() *transport.Pool {
	return m.pool
}

// Membership returns the active SWIM membership service if configured.
func (m *Manager) Membership() *MembershipService {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.membership
}

// SetMembership sets the active SWIM membership service.
func (m *Manager) SetMembership(svc *MembershipService) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.membership = svc
}

func (m *Manager) closeSessions() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for _, p := range m.peers {
		p.mu.Lock()
		if p.session != nil {
			p.session.close()
			p.session = nil
		}
		p.mu.Unlock()
		if m.transferLimiter != nil {
			m.transferLimiter.ForgetPeer(p.id.String())
		}
		for {
			select {
			case f := <-p.ctrlCh:
				if f.lease != nil {
					f.lease.Release()
				}
			default:
				goto controlsDrained
			}
		}
	controlsDrained:
		for {
			select {
			case n := <-p.needCh:
				if n.lease != nil {
					n.lease.Release()
				}
			default:
				goto needsDrained
			}
		}
	needsDrained:
	}
	if m.pool != nil {
		_ = m.pool.Close()
	}
}

// NotifyLocal wakes senders after a local durable commit.
func (m *Manager) NotifyLocal() {
	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

// AddPeer adds or updates a configured peer, clearing any previous local persistent exclusion.
func (m *Manager) AddPeer(id ids.NodeID, addrs []string) {
	if id == m.cfg.Local {
		return
	}
	m.mu.Lock()
	if m.excluded[id] {
		delete(m.excluded, id)
		if m.cfg.Store != nil {
			_ = m.cfg.Store.SetPeerExcluded(id, false)
		}
	}
	p, ok := m.peers[id]
	if !ok {
		p = newPeerState(id, addrs, false)
		m.peers[id] = p
		m.reconcileSelectedLocked()
		running := m.running.Load()
		if running {
			m.wg.Add(1)
		}
		m.mu.Unlock()
		if running {
			go m.dialLoop(p)
		}
		return
	}
	p.mu.Lock()
	p.addrs = addrs
	p.dynamic = false
	p.mu.Unlock()
	m.reconcileSelectedLocked()
	m.mu.Unlock()
	p.pokeWake()
}

// RemovePeer retires a peer: persists local exclusion, closes the session, and it no longer gates log GC.
func (m *Manager) RemovePeer(id ids.NodeID) {
	if id == m.cfg.Local {
		return
	}
	m.mu.Lock()
	if m.transferLimiter != nil {
		m.transferLimiter.ForgetPeer(id.String())
	}
	m.excluded[id] = true
	if m.cfg.Store != nil {
		_ = m.cfg.Store.SetPeerExcluded(id, true)
	}
	delete(m.selected, id)
	p, ok := m.peers[id]
	if ok {
		delete(m.peers, id)
	}
	m.reconcileSelectedLocked()
	m.mu.Unlock()
	if ok {
		// Stop the detached dialLoop before closing the session: a
		// later AddPeer creates a fresh peerState and dialer, and the
		// stale loop must not dial or attach sessions for this ID.
		p.stopped.Store(true)
		p.pokeWake()
		p.mu.Lock()
		p.selected = false
		if p.session != nil {
			p.session.close()
			p.session = nil
		}
		p.mu.Unlock()
	}
}

// IsPeerExcluded reports whether the node ID is locally excluded.
func (m *Manager) IsPeerExcluded(id ids.NodeID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.excluded[id]
}

// ExcludedPeers returns a slice of all currently excluded node IDs.
func (m *Manager) ExcludedPeers() []ids.NodeID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ids.NodeID, 0, len(m.excluded))
	for id := range m.excluded {
		out = append(out, id)
	}
	return out
}

// reconcileSelectedLocked adjusts the selected outbound replication targets to min(Fanout, eligible).
func (m *Manager) reconcileSelectedLocked() {
	if m.closed {
		return
	}
	var eligible []ids.NodeID
	for id, p := range m.peers {
		if id == m.cfg.Local || m.excluded[id] {
			continue
		}
		p.mu.Lock()
		isEligible := len(p.addrs) > 0 || p.session != nil
		p.mu.Unlock()
		if isEligible {
			eligible = append(eligible, id)
		}
	}

	// Prune selected peers that are no longer eligible.
	for id := range m.selected {
		isEligible := false
		for _, e := range eligible {
			if e == id {
				isEligible = true
				break
			}
		}
		if !isEligible {
			delete(m.selected, id)
			if p, ok := m.peers[id]; ok {
				p.mu.Lock()
				p.selected = false
				p.mu.Unlock()
			}
		}
	}

	targetCount := m.cfg.Fanout
	if len(eligible) < targetCount {
		targetCount = len(eligible)
	}

	if len(m.selected) < targetCount {
		var candidates []ids.NodeID
		for _, id := range eligible {
			if !m.selected[id] {
				candidates = append(candidates, id)
			}
		}
		rand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})
		for _, id := range candidates {
			if len(m.selected) >= targetCount {
				break
			}
			m.selected[id] = true
			if p, ok := m.peers[id]; ok {
				p.mu.Lock()
				p.selected = true
				p.mu.Unlock()
				p.pokeWake()
			}
		}
	}
}

// isPeerSelected reports whether a peer is currently in the active selected outbound dissemination subset.
func (m *Manager) isPeerSelected(id ids.NodeID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.selected[id]
}

// rotateOneSelection rotates one outbound selected target toward an eligible non-selected peer.
func (m *Manager) rotateOneSelection() {
	m.mu.Lock()
	if m.closed || m.ctx.Err() != nil {
		m.mu.Unlock()
		return
	}
	var eligibleNonSelected []ids.NodeID
	for id, p := range m.peers {
		if id == m.cfg.Local || m.excluded[id] {
			continue
		}
		p.mu.Lock()
		isEligible := len(p.addrs) > 0 || p.session != nil
		p.mu.Unlock()
		if isEligible && !m.selected[id] {
			eligibleNonSelected = append(eligibleNonSelected, id)
		}
	}
	if len(eligibleNonSelected) == 0 || len(m.selected) == 0 {
		m.mu.Unlock()
		return
	}

	selectedList := make([]ids.NodeID, 0, len(m.selected))
	for id := range m.selected {
		selectedList = append(selectedList, id)
	}
	oldID := selectedList[rand.Intn(len(selectedList))]
	newID := eligibleNonSelected[rand.Intn(len(eligibleNonSelected))]

	delete(m.selected, oldID)
	m.selected[newID] = true

	oldP := m.peers[oldID]
	newP := m.peers[newID]
	m.mu.Unlock()

	m.st.peerRotations.Add(1)
	m.log.Debug("rotated replication peer", slog.String("old", oldID.String()), slog.String("new", newID.String()))

	if oldP != nil {
		oldP.mu.Lock()
		oldP.selected = false
		ps := oldP.session
		oldP.mu.Unlock()
		if ps != nil && ps.outbound {
			m.recycleSession(oldP, ps, "peer rotation")
		}
	}
	if newP != nil {
		newP.mu.Lock()
		newP.selected = true
		newP.mu.Unlock()
		newP.pokeWake()
	}
	m.NotifyLocal()
}

func (m *Manager) rotationLoop() {
	defer m.wg.Done()
	interval := m.cfg.PeerRotationInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.rotateOneSelection()
		}
	}
}

func (m *Manager) antiEntropyLoop() {
	defer m.wg.Done()
	baseInterval := m.cfg.AntiEntropyInterval
	if baseInterval <= 0 {
		baseInterval = 10 * time.Second
	}
	for {
		jitter := 0.8 + 0.4*rand.Float64()
		delay := time.Duration(float64(baseInterval) * jitter)
		if delay <= 0 {
			delay = time.Second
		}
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(delay):
			m.runPeriodicAntiEntropy()
		}
	}
}

func (m *Manager) runPeriodicAntiEntropy() {
	m.mu.Lock()
	if m.closed || m.ctx.Err() != nil {
		m.mu.Unlock()
		return
	}
	// Stalled snapshot waits outrank routine anti-entropy: re-request
	// before picking a sync target.
	m.mu.Unlock()
	m.retryStalledSnapshots()
	m.mu.Lock()
	if m.closed || m.ctx.Err() != nil {
		m.mu.Unlock()
		return
	}
	var candidates []ids.NodeID
	var fallback []ids.NodeID
	for id, p := range m.peers {
		if id == m.cfg.Local || m.excluded[id] {
			continue
		}
		p.mu.Lock()
		isEligible := len(p.addrs) > 0 || p.session != nil
		p.mu.Unlock()
		if !isEligible {
			continue
		}
		fallback = append(fallback, id)
		if !m.selected[id] {
			candidates = append(candidates, id)
		}
	}
	var targetID ids.NodeID
	if len(candidates) > 0 {
		targetID = candidates[rand.Intn(len(candidates))]
	} else if len(fallback) > 0 {
		targetID = fallback[rand.Intn(len(fallback))]
	} else {
		m.mu.Unlock()
		return
	}
	targetP := m.peers[targetID]
	m.mu.Unlock()

	if targetP != nil {
		m.performAntiEntropy(targetP)
	}
}

// retryStalledSnapshots re-requests snapshots that made no progress
// (manifest or chunks) within SnapshotRequestTimeout. The original request
// may have been lost, deferred while the source was busy, or abandoned
// when the source failed mid-transfer — all previously stalled the
// receiver's one-shot wait forever. Transfers that keep moving are never
// disturbed, and source-requested busy backoff is honored.
func (m *Manager) retryStalledSnapshots() {
	if m.ctx.Err() != nil {
		return
	}
	now := time.Now()
	m.mu.Lock()
	peers := make([]*peerState, 0, len(m.peers))
	for id, p := range m.peers {
		if id == m.cfg.Local || m.excluded[id] {
			continue
		}
		peers = append(peers, p)
	}
	timeout := m.cfg.SnapshotRequestTimeout
	m.mu.Unlock()
	for _, p := range peers {
		p.mu.Lock()
		sess := p.session
		stalled := p.awaiting && sess != nil && !p.lastSnapProgress.IsZero() &&
			now.Sub(p.lastSnapProgress) >= timeout &&
			!now.Before(p.snapRetryAfter)
		if stalled {
			p.lastSnapProgress = now
		}
		p.mu.Unlock()
		if !stalled || sess.isClosed() {
			continue
		}
		m.log.Info("snapshot stalled; re-requesting", slog.String("peer", p.id.String()))
		m.queueCtrl(p, MsgSnapshotRequest, 0, nil)
		m.st.snapshotRequestsSent.Add(1)
		m.st.snapshotReRequested.Add(1)
	}
}

func (m *Manager) performAntiEntropy(p *peerState) {
	if m.IsPeerExcluded(p.id) || m.ctx.Err() != nil {
		return
	}
	p.mu.Lock()
	ps := p.session
	addrs := append([]string(nil), p.addrs...)
	p.mu.Unlock()

	if ps != nil && !ps.isClosed() {
		p.mu.Lock()
		p.lastAntiEntropy = time.Now()
		p.mu.Unlock()
		if err := m.sendAckAndPull(p); err != nil {
			m.st.antiEntropyFailures.Add(1)
		} else {
			m.st.antiEntropyRuns.Add(1)
		}
		return
	}

	if len(addrs) == 0 {
		m.st.antiEntropyFailures.Add(1)
		return
	}

	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
		m.st.dials.Add(1)
		var sess *transport.Session
		var err error
		if m.pool != nil {
			sess, err = m.pool.Dial(ctx, addr, m.cfg.Creds, p.id, transport.PurposeRepair)
		} else {
			sess, err = transport.Dial(ctx, addr, m.cfg.Creds, p.id)
		}
		cancel()
		if err != nil {
			m.st.dialFailures.Add(1)
			if errors.Is(err, transport.ErrAddressNotAllowed) {
				m.st.dialPolicyDenials.Add(1)
			}
			continue
		}
		if m.handshakeOutboundWithPurpose(p, sess, transport.PurposeRepair) {
			p.mu.Lock()
			p.lastAntiEntropy = time.Now()
			p.mu.Unlock()
			m.st.antiEntropyRuns.Add(1)
			_ = m.sendAckAndPull(p)
			return
		}
	}
	m.st.antiEntropyFailures.Add(1)
}

func (m *Manager) handleSelectedPeerFailure(failedID ids.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.selected[failedID] {
		return
	}
	delete(m.selected, failedID)
	if p, ok := m.peers[failedID]; ok {
		p.mu.Lock()
		p.selected = false
		p.dialFailures = 0
		p.mu.Unlock()
	}
	m.reconcileSelectedLocked()
}

// OnPeerDiscovered is invoked when memberlist discovers a new cluster peer.
func (m *Manager) OnPeerDiscovered(nodeID ids.NodeID, endpoint string, meta NodeMetadata) {
	if nodeID == m.cfg.Local || m.IsPeerExcluded(nodeID) {
		return
	}
	var addrs []string
	if endpoint != "" {
		addrs = []string{endpoint}
	}
	m.AddPeer(nodeID, addrs)
}

// OnPeerUpdated is invoked when a peer's endpoint or metadata changes.
func (m *Manager) OnPeerUpdated(nodeID ids.NodeID, endpoint string, meta NodeMetadata) {
	if nodeID == m.cfg.Local || m.IsPeerExcluded(nodeID) {
		return
	}
	var addrs []string
	if endpoint != "" {
		addrs = []string{endpoint}
	}
	m.AddPeer(nodeID, addrs)
}

// OnPeerLeft is invoked when a peer leaves the SWIM cluster.
func (m *Manager) OnPeerLeft(nodeID ids.NodeID) {
	m.mu.Lock()
	delete(m.selected, nodeID)
	p, ok := m.peers[nodeID]
	if ok {
		delete(m.peers, nodeID)
	}
	m.reconcileSelectedLocked()
	m.mu.Unlock()
	if ok {
		// Same dialLoop lifecycle as RemovePeer: the detached state
		// must stop dialing so a later re-add owns the only dialer.
		p.stopped.Store(true)
		p.pokeWake()
		p.mu.Lock()
		p.selected = false
		ps := p.session
		p.session = nil
		p.mu.Unlock()
		if ps != nil {
			go ps.close()
		}
	}
}

// ForceSync triggers an immediate dial, ack, and pull from the peer.
func (m *Manager) ForceSync(ctx context.Context, id ids.NodeID) error {
	if id == m.cfg.Local {
		return fmt.Errorf("replication: cannot force sync with local node")
	}
	m.mu.Lock()
	if m.excluded[id] {
		m.mu.Unlock()
		return fmt.Errorf("replication: peer %s is locally excluded: %w", id, ErrPeerExcluded)
	}
	p, ok := m.peers[id]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("replication: unknown peer %s: %w", id, ErrPeerNotFound)
	}
	p.mu.Lock()
	p.forceAck = true
	p.mu.Unlock()
	p.pokeWake()
	m.NotifyLocal()
	m.performAntiEntropy(p)
	return nil
}

// PeerStatus snapshots peer states.
func (m *Manager) PeerStatus() []PeerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]PeerStatus, 0, len(m.peers))
	for _, p := range m.peers {
		p.mu.Lock()
		var snapRecv, snapTotal uint64
		if p.snapRecv != nil {
			snapRecv = p.snapRecv.chunksReceived
			if p.snapRecv.manifest != nil {
				snapTotal = p.snapRecv.manifest.ChunkCount
			}
		}
		st := PeerStatus{
			NodeID:                 p.id,
			Addrs:                  append([]string(nil), p.addrs...),
			Connected:              p.session != nil,
			Dynamic:                p.dynamic,
			Selected:               p.selected,
			LastSeen:               p.lastSeen,
			RTT:                    p.rtt,
			Have:                   copyMap(p.have),
			Sent:                   copyMap(p.sent),
			LastHandshake:          p.lastHandshake,
			LastSend:               p.lastSend,
			LastRecv:               p.lastRecv,
			LastAntiEntropy:        p.lastAntiEntropy,
			SchemaAgreed:           p.agreed,
			RemoteSchemaEpoch:      p.remote.Epoch,
			RemoteSchemaHash:       p.remote.Hash,
			SnapshotRequired:       len(p.sentErr) > 0,
			AwaitingSnapshot:       p.awaiting,
			SnapshotChunksReceived: snapRecv,
			SnapshotChunksTotal:    snapTotal,
			BytesSent:              p.bytesSent,
			BytesReceived:          p.bytesReceived,
			QueuedNeed:             len(p.needCh),
			QueuedCtrl:             len(p.ctrlCh) + len(p.retryCh),
			QueuedSchema:           len(p.schemaReqCh) + len(p.schemaRespCh),
		}
		p.mu.Unlock()
		out = append(out, st)
	}
	return out
}

// ConfiguredPeers lists non-removed peers (for log-GC floors).
func (m *Manager) ConfiguredPeers() []PeerInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]PeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		if m.excluded[p.id] {
			continue
		}
		p.mu.Lock()
		out = append(out, PeerInfo{NodeID: p.id, Addrs: append([]string(nil), p.addrs...)})
		p.mu.Unlock()
	}
	return out
}

func copyMap(in map[ids.NodeID]uint64) map[ids.NodeID]uint64 {
	out := make(map[ids.NodeID]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// --- handshake ---

// currentSchema returns the advertised schema identity.
func (m *Manager) currentSchema() SchemaIdentity {
	m.schemaMu.RLock()
	defer m.schemaMu.RUnlock()
	return m.schemaId
}

// RefreshSchema publishes a locally migrated schema identity and recycles
// every session so peers re-handshake against it. Data sent under the old
// identity stays valid: batches carry their original provenance and apply
// as compatible ancestors after the peer converges.
func (m *Manager) RefreshSchema(id SchemaIdentity) {
	m.schemaMu.Lock()
	m.schemaId = id
	m.schemaMu.Unlock()
	m.mu.Lock()
	peers := make([]*peerState, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	m.mu.Unlock()
	for _, p := range peers {
		p.mu.Lock()
		ps := p.session
		p.session = nil
		p.agreed = false
		p.mu.Unlock()
		if ps != nil {
			ps.close()
		}
		p.pokeWake()
	}
	m.NotifyLocal()
}

func (m *Manager) ourHello() (*Hello, error) {
	wms, err := m.cfg.Store.ReceiveWatermarks()
	if err != nil {
		return nil, err
	}
	if len(wms) > ProgressPageEntries {
		wms = wms[:ProgressPageEntries]
	}
	id := m.currentSchema()
	caps := CapZstd | CapProgressPages | CapTransactionChunks
	if m.cfg.EnablePlumtree {
		caps |= CapPlumtree | CapRequiredMask
	}
	ver, minVer := m.effectiveVersions()
	return &Hello{
		ProtocolVersion:     ver,
		MinProtocolVersion:  minVer,
		NodeID:              m.cfg.Local,
		DBID:                m.cfg.DBID,
		SchemaEpoch:         id.Epoch,
		SchemaHash:          id.Hash,
		SchemaAuthorNode:    id.Author,
		SchemaTimeCreated:   id.TimeCreated,
		Capabilities:        caps,
		MaxTransactionBytes: uint64(m.cfg.MaxTransactionBytes),
		Have:                wms,
	}, nil
}

// effectiveVersions returns the protocol versions this node speaks: the
// Advertise overrides when nonzero (interoperability testing only),
// otherwise the compiled constants. Advertisement and enforcement share
// this so a node never accepts a peer its own hello would refuse.
func (m *Manager) effectiveVersions() (ver, minVer uint16) {
	ver, minVer = ProtocolVersion, MinProtocolVersion
	if m.cfg.AdvertiseProtocolVersion != 0 {
		ver = m.cfg.AdvertiseProtocolVersion
	}
	if m.cfg.AdvertiseMinProtocolVersion != 0 {
		minVer = m.cfg.AdvertiseMinProtocolVersion
	}
	return ver, minVer
}

// validateIdentity checks the fatal handshake fields: node, database, and
// protocol overlap. Schema agreement is handled separately so mismatched
// peers can synchronize instead of always refusing.
func (m *Manager) validateIdentity(h *Hello, sessPeer ids.NodeID) error {
	if h.NodeID != sessPeer {
		return fmt.Errorf("hello node %s != TLS identity %s", h.NodeID, sessPeer)
	}
	if h.DBID != m.cfg.DBID {
		return fmt.Errorf("db id mismatch")
	}
	ver, minVer := m.effectiveVersions()
	if h.ProtocolVersion < minVer || h.MinProtocolVersion > ver {
		return fmt.Errorf("protocol %d (min %d) incompatible", h.ProtocolVersion, h.MinProtocolVersion)
	}
	return nil
}

// schemaMatches reports whether a handshake advertises our exact schema.
func (m *Manager) schemaMatches(h *Hello) bool {
	id := m.currentSchema()
	return h.SchemaEpoch == id.Epoch && h.SchemaHash == id.Hash
}

// canSyncSchema reports whether schema synchronization is available:
// policy allows it and an adoption engine is configured. Without either,
// mismatches refuse (fail closed, exchange nothing).
func (m *Manager) canSyncSchema() bool {
	return m.cfg.AcceptRemoteSchema && m.cfg.SchemaSync != nil
}

// attach installs a handshaked session with deterministic dedupe: when two
// connections exist to one peer, both sides keep the one initiated by the
// lower NodeID.
func (m *Manager) attach(p *peerState, ps *peerSession, h *Hello) {
	// Never start session loops during shutdown: a session attached after
	// Run's closeSessions would never be closed and would hang Close. The
	// closed flag is checked under m.mu (same critical section as
	// closeSessions) so the check cannot race shutdown.
	m.mu.Lock()
	if m.closed || m.ctx.Err() != nil {
		m.mu.Unlock()
		go ps.close()
		return
	}
	p.mu.Lock()
	m.mu.Unlock()
	// Retired members cannot rejoin through handshake traffic; AddPeer
	// must readmit them first. (Exclusion is enforced earlier, at
	// peerFor/dial; this guards the persisted retirement record.)
	if m.isMemberRetired(p.id) {
		p.mu.Unlock()
		m.st.handshakeFailures.Add(1)
		m.st.handshakeRetiredRefusals.Add(1)
		go ps.close()
		return
	}
	// Install traffic accounting before the session can send.
	ps.peer = p
	ps.st = &m.st
	p.lastHandshake = time.Now()
	for _, w := range h.Have {
		if w.Sequence > p.have[w.Origin] {
			p.have[w.Origin] = w.Sequence
		}
	}
	// Negotiated usable capabilities (unknown-required sets were refused
	// during the handshake, so this cannot fail here).
	p.caps, _ = NegotiateCapabilities(h.Capabilities)
	p.plumtree = m.plumtree != nil && p.caps&CapPlumtree != 0
	p.progressPages = p.caps&CapProgressPages != 0
	p.chunkAvailability = make(map[ids.TxID]ChunkAvailability)
	p.chunkUnavailable = make(map[ids.TxID]bool)
	p.maxTxBytes = h.MaxTransactionBytes
	p.sentErr = make(map[ids.NodeID]bool)
	p.remote = SchemaIdentity{
		Epoch:       h.SchemaEpoch,
		Hash:        h.SchemaHash,
		Author:      h.SchemaAuthorNode,
		TimeCreated: h.SchemaTimeCreated,
	}
	p.agreed = m.schemaMatches(h)
	// New session: QUIC streams are reliable, so anything sent on the old
	// session may or may not have arrived; restart the send cursor from the
	// peer's durable acks (the receiver dedups redelivery).
	p.sent = copyMap(p.have)
	wantOutbound := m.cfg.Local.Compare(p.id) < 0
	if old := p.session; old != nil && old != ps {
		select {
		case <-old.done:
			// Dead; replace unconditionally.
		default:
			if ps.outbound != wantOutbound {
				// New session loses the tie-break; close it.
				p.mu.Unlock()
				m.st.sessionsDedupeClosed.Add(1)
				go ps.close()
				return
			}
			old.close()
		}
	}
	p.session = ps
	m.st.sessionsOpened.Add(1)
	// First successful authenticated handshake admits the member,
	// starting its persisted retention obligation. Later handshakes are
	// no-ops: only advancing acknowledgements renew the deadline.
	if m.cfg.Store != nil {
		if admitted, err := m.cfg.Store.EnsureMemberAdmitted(p.id, time.Now().UnixMilli(), m.cfg.AckRetention.Milliseconds()); err != nil {
			m.log.Warn("member admission failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
		} else if admitted {
			m.st.memberAdmissions.Add(1)
		}
	}
	agreed := p.agreed
	p.mu.Unlock()

	m.wg.Add(2)
	go m.readControlLoop(p, ps)
	go m.streamAcceptLoop(p, ps)
	if m.membership != nil && m.membership.Transport() != nil {
		m.membership.Transport().RegisterSession(ps.sess)
	}
	if !agreed {
		// Schema mismatch accepted for synchronization: request the
		// peer's manifest once attached; data stays gated until agreed.
		m.queueSchemaRequest(p, SchemaRequest{WantCurrent: true})
	}
	m.mu.Lock()
	m.reconcileSelectedLocked()
	m.mu.Unlock()
}

// isMemberRetired reports whether the persisted membership record refuses
// re-admission. Lookup errors fail open (with a warning): a transient
// store failure must not brick peering, and session refusal for excluded
// peers is enforced separately.
func (m *Manager) isMemberRetired(id ids.NodeID) bool {
	if m.cfg.Store == nil {
		return false
	}
	rec, err := m.cfg.Store.GetMember(id)
	if err != nil {
		m.log.Warn("member lookup failed", slog.String("peer", id.String()), slog.String("err", err.Error()))
		return false
	}
	return rec.Status == state.MemberRetired
}

func (m *Manager) detach(p *peerState, ps *peerSession) {
	p.mu.Lock()
	if p.session == ps {
		p.session = nil
		if m.pool != nil {
			m.pool.Release(p.id, transport.PurposeGeneric)
		}
	}
	p.mu.Unlock()

	m.mu.Lock()
	m.reconcileSelectedLocked()
	m.mu.Unlock()
}

// --- accept / dial ---

func (m *Manager) acceptLoop() {
	defer m.wg.Done()
	for {
		sess, err := m.ln.Accept(m.ctx)
		if err != nil {
			if m.ctx.Err() != nil {
				return
			}
			m.st.acceptFailures.Add(1)
			if errors.Is(err, transport.ErrAddressNotAllowed) {
				m.st.acceptPolicyDenials.Add(1)
			}
			m.log.Warn("accept failed", slog.String("err", err.Error()))
			continue
		}
		m.st.accepts.Add(1)
		go m.serveInbound(sess)
	}
}

func (m *Manager) serveInbound(sess *transport.Session) {
	if m.pool != nil {
		if err := m.pool.AdmitInbound(sess.Peer, transport.PurposeInboundReplication); err != nil {
			m.st.handshakeFailures.Add(1)
			_ = sess.Close()
			return
		}
	}
	ctx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()
	stream, err := sess.AcceptStream(ctx)
	if err != nil {
		m.st.handshakeFailures.Add(1)
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	frame, err := ReadFrame(stream)
	if err != nil {
		var memErr *ErrMembershipStream
		if errors.As(err, &memErr) {
			if m.membership != nil && m.membership.Transport() != nil {
				m.membership.Transport().HandleStream(sess, stream, memErr.Prefix)
				return
			}
		}
		m.st.handshakeFailures.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	if frame.Type != MsgHello {
		m.st.handshakeFailures.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	h, err := DecodeHello(frame.Payload)
	if err != nil {
		m.st.handshakeFailures.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	if m.IsPeerExcluded(sess.Peer) {
		m.st.handshakeFailures.Add(1)
		_ = WriteFrame(stream, MsgError, 0, EncodeError(nil, ErrPeerExcludedCode, "peer is locally excluded"))
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	if err := m.validateIdentity(h, sess.Peer); err != nil {
		m.st.handshakeFailures.Add(1)
		m.st.handshakeIdentityRefl.Add(1)
		_ = WriteFrame(stream, MsgError, 0, EncodeError(nil, ErrProtocolMismatch, err.Error()))
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	if _, err := NegotiateDisseminationCapabilities(h.Capabilities, m.cfg.EnablePlumtree); err != nil {
		m.st.handshakeFailures.Add(1)
		m.st.handshakeCapabilityRefl.Add(1)
		_ = WriteFrame(stream, MsgError, 0, EncodeError(nil, ErrProtocolMismatch, err.Error()))
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	if !m.schemaMatches(h) && !m.canSyncSchema() {
		m.st.handshakeFailures.Add(1)
		m.st.handshakeSchemaRefusals.Add(1)
		id := m.currentSchema()
		_ = WriteFrame(stream, MsgError, 0, EncodeError(nil, ErrSchemaMismatch,
			fmt.Sprintf("schema epoch %d mismatch (want %d)", h.SchemaEpoch, id.Epoch)))
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	ours, err := m.ourHello()
	if err != nil {
		m.st.handshakeFailures.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	if err := WriteFrame(stream, MsgWelcome, 0, EncodeHello(nil, ours)); err != nil {
		m.st.handshakeFailures.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}
	p := m.peerFor(sess.Peer, nil, true)
	if p == nil {
		m.st.handshakeFailures.Add(1)
		_ = WriteFrame(stream, MsgError, 0, EncodeError(nil, ErrPeerExcludedCode, "peer is locally excluded"))
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(sess.Peer, transport.PurposeInboundReplication)
		}
		return
	}

	p.mu.Lock()
	if len(p.addrs) == 0 && sess.RemoteAddr() != "" {
		p.addrs = []string{sess.RemoteAddr()}
	}
	p.mu.Unlock()

	if m.pool != nil {
		_ = m.pool.RegisterSession(sess, transport.PurposeInboundReplication, false)
	}
	m.attach(p, &peerSession{sess: sess, ctrlStream: stream, done: make(chan struct{}), timeout: m.cfg.SendTimeout}, h)
	m.NotifyLocal()
}

func (m *Manager) peerFor(id ids.NodeID, addrs []string, dynamic bool) *peerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.excluded[id] {
		return nil
	}
	if p, ok := m.peers[id]; ok {
		return p
	}
	p := newPeerState(id, addrs, dynamic)
	m.peers[id] = p
	return p
}

func (m *Manager) dialLoop(p *peerState) {
	defer m.wg.Done()
	if p.stopped.Load() {
		return
	}
	// Immediate first attempt if selected.
	if m.isPeerSelected(p.id) {
		m.tryDial(p)
	}
	t := time.NewTicker(m.cfg.DialInterval)
	defer t.Stop()
	for {
		if p.stopped.Load() {
			return
		}
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
			if m.isPeerSelected(p.id) {
				m.tryDial(p)
			}
		case <-p.wake:
			if m.isPeerSelected(p.id) {
				m.tryDial(p)
			}
		}
	}
}

func (m *Manager) tryDial(p *peerState) {
	m.tryDialWithPurpose(p, transport.PurposeSelectedTarget)
}

func (m *Manager) tryDialWithPurpose(p *peerState, purpose transport.SessionPurpose) bool {
	if m.IsPeerExcluded(p.id) {
		return false
	}
	p.mu.Lock()
	addrs := append([]string(nil), p.addrs...)
	alive := p.session != nil
	p.mu.Unlock()
	if alive || len(addrs) == 0 || m.ctx.Err() != nil {
		return false
	}

	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
		m.st.dials.Add(1)
		var sess *transport.Session
		var err error
		if m.pool != nil {
			sess, err = m.pool.Dial(ctx, addr, m.cfg.Creds, p.id, purpose)
		} else {
			sess, err = transport.Dial(ctx, addr, m.cfg.Creds, p.id)
		}
		cancel()
		if err != nil {
			m.st.dialFailures.Add(1)
			if errors.Is(err, transport.ErrAddressNotAllowed) {
				m.st.dialPolicyDenials.Add(1)
			}
			continue
		}
		if m.handshakeOutboundWithPurpose(p, sess, purpose) {
			p.mu.Lock()
			p.dialFailures = 0
			p.mu.Unlock()
			return true
		}
	}

	p.mu.Lock()
	p.dialFailures++
	failures := p.dialFailures
	p.mu.Unlock()

	if failures >= 3 && m.isPeerSelected(p.id) {
		m.handleSelectedPeerFailure(p.id)
	}
	return false
}

func (m *Manager) handshakeOutbound(p *peerState, sess *transport.Session) bool {
	return m.handshakeOutboundWithPurpose(p, sess, transport.PurposeSelectedTarget)
}

func (m *Manager) handshakeOutboundWithPurpose(p *peerState, sess *transport.Session, purpose transport.SessionPurpose) bool {
	ctx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		m.st.handshakeFailures.Add(1)
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(p.id, purpose)
		}
		return false
	}
	ours, err := m.ourHello()
	if err != nil {
		m.st.handshakeFailures.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(p.id, purpose)
		}
		return false
	}
	if err := WriteFrame(stream, MsgHello, 0, EncodeHello(nil, ours)); err != nil {
		m.st.handshakeFailures.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(p.id, purpose)
		}
		return false
	}
	frame, err := ReadFrame(stream)
	if err != nil || frame.Type != MsgWelcome {
		m.st.handshakeFailures.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(p.id, purpose)
		}
		return false
	}
	h, err := DecodeHello(frame.Payload)
	if err != nil || m.validateIdentity(h, sess.Peer) != nil {
		m.st.handshakeFailures.Add(1)
		m.st.handshakeIdentityRefl.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(p.id, purpose)
		}
		return false
	}
	if _, err := NegotiateDisseminationCapabilities(h.Capabilities, m.cfg.EnablePlumtree); err != nil {
		m.st.handshakeFailures.Add(1)
		m.st.handshakeCapabilityRefl.Add(1)
		m.log.Debug("outbound handshake refused on capabilities", slog.String("err", err.Error()))
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(p.id, purpose)
		}
		return false
	}
	if !m.schemaMatches(h) && !m.canSyncSchema() {
		// Strict: refuse without a session; the peer must upgrade first.
		m.st.handshakeFailures.Add(1)
		m.st.handshakeSchemaRefusals.Add(1)
		_ = stream.Close()
		_ = sess.Close()
		if m.pool != nil {
			m.pool.Release(p.id, purpose)
		}
		return false
	}
	m.attach(p, &peerSession{sess: sess, ctrlStream: stream, outbound: true, done: make(chan struct{}), timeout: m.cfg.SendTimeout}, h)
	m.NotifyLocal()
	return true
}

// --- stream read loops ---

func (m *Manager) readControlLoop(p *peerState, ps *peerSession) {
	defer m.wg.Done()
	defer m.detach(p, ps)
	defer ps.close()
	m.readControlFrames(p, ps, ps.ctrlStream)
}

func (m *Manager) streamAcceptLoop(p *peerState, ps *peerSession) {
	defer m.wg.Done()
	for {
		stream, err := ps.sess.AcceptStream(m.ctx)
		if err != nil {
			return
		}
		m.wg.Add(1)
		go m.handleIncomingStream(p, ps, stream)
	}
}

func (m *Manager) handleIncomingStream(p *peerState, ps *peerSession, stream *quic.Stream) {
	defer m.wg.Done()
	defer stream.Close()
	frame, err := ReadFrame(stream)
	if err != nil {
		var memErr *ErrMembershipStream
		if errors.As(err, &memErr) {
			if m.membership != nil && m.membership.Transport() != nil {
				m.membership.Transport().HandleStream(ps.sess, stream, memErr.Prefix)
			}
		}
		return
	}
	switch frame.Type {
	case MsgBatches, MsgPlumtreeData, MsgTransactionChunk:
		m.st.dataStreamsAccepted.Add(1)
		m.readDataLoop(p, ps, stream, frame)
	case MsgSnapshotManifest, MsgSnapshotChunk, MsgSnapshotDone, MsgSnapshotRequest:
		m.st.snapStreamsAccepted.Add(1)
		m.readSnapshotLoop(p, ps, stream, frame)
	default:
		m.recordFrameRecv(p, frame)
		if err := m.dispatch(p, ps, frame); err != nil {
			return
		}
		m.readControlFrames(p, ps, stream)
	}
}

func (m *Manager) readDataLoop(p *peerState, ps *peerSession, stream *quic.Stream, first *Frame) {
	if first != nil {
		m.recordFrameRecv(p, first)
		var err error
		if first.Type == MsgPlumtreeData {
			err = m.onPlumtreeData(p, first.Payload)
		} else if first.Type == MsgTransactionChunk {
			err = m.onTransactionChunk(p, first.Payload)
		} else {
			err = m.onBatches(p, ps, first.Payload)
		}
		if err != nil {
			m.log.Warn("data batch apply failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
			return
		}
	}
	for {
		frame, err := ReadFrame(stream)
		if err != nil {
			return
		}
		m.recordFrameRecv(p, frame)
		if frame.Type == MsgBatches {
			if err := m.onBatches(p, ps, frame.Payload); err != nil {
				m.log.Warn("data batch apply failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
				return
			}
		} else if frame.Type == MsgPlumtreeData {
			if err := m.onPlumtreeData(p, frame.Payload); err != nil {
				m.log.Warn("plumtree payload failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
				return
			}
		} else if frame.Type == MsgTransactionChunk {
			if err := m.onTransactionChunk(p, frame.Payload); err != nil {
				return
			}
		} else {
			if err := m.dispatch(p, ps, frame); err != nil {
				return
			}
		}
	}
}

func (m *Manager) readSnapshotLoop(p *peerState, ps *peerSession, stream *quic.Stream, first *Frame) {
	if first != nil {
		m.recordFrameRecv(p, first)
		if err := m.dispatchSnapshotFrame(p, ps, first); err != nil {
			m.log.Warn("snapshot stream frame failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
			return
		}
	}
	for {
		frame, err := ReadFrame(stream)
		if err != nil {
			return
		}
		m.recordFrameRecv(p, frame)
		if err := m.dispatchSnapshotFrame(p, ps, frame); err != nil {
			m.log.Warn("snapshot stream frame failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
			return
		}
	}
}

func (m *Manager) dispatchSnapshotFrame(p *peerState, ps *peerSession, f *Frame) error {
	switch f.Type {
	case MsgSnapshotRequest:
		go m.sendSnapshot(p, ps)
		return nil
	case MsgSnapshotManifest:
		return m.onSnapshotManifest(p, ps, f.Payload)
	case MsgSnapshotChunk:
		return m.onSnapshotChunk(p, ps, f)
	case MsgSnapshotDone:
		return m.onSnapshotDone(p, ps)
	default:
		return m.dispatch(p, ps, f)
	}
}

func (m *Manager) recordFrameRecv(p *peerState, frame *Frame) {
	m.st.framesReceived.Add(1)
	n := uint64(len(frame.Payload) + frameHdrLen)
	m.st.frameBytesReceived.Add(n)
	p.mu.Lock()
	p.lastSeen = time.Now()
	p.lastRecv = p.lastSeen
	p.bytesReceived += n
	p.mu.Unlock()
}

func (m *Manager) readControlFrames(p *peerState, ps *peerSession, stream *quic.Stream) {
	for {
		frame, err := ReadFrame(stream)
		if err != nil {
			if m.ctx.Err() == nil {
				m.log.Debug("session control read ended", slog.String("peer", p.id.String()))
			}
			return
		}
		m.recordFrameRecv(p, frame)
		if err := m.dispatch(p, ps, frame); err != nil {
			m.log.Warn("control dispatch failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
			return
		}
	}
}

func (m *Manager) dispatch(p *peerState, ps *peerSession, f *Frame) error {
	switch f.Type {
	case MsgBatches:
		return m.onBatches(p, ps, f.Payload)
	case MsgPlumtreeData:
		return m.onPlumtreeData(p, f.Payload)
	case MsgPlumtreeIHave:
		return m.onPlumtreeIHave(p, f.Payload)
	case MsgPlumtreePrune:
		return m.onPlumtreePrune(p, f.Payload)
	case MsgPlumtreeGraft:
		return m.onPlumtreeGraft(p, ps, f.Payload)
	case MsgAck:
		return m.onAck(p, f.Payload)
	case MsgProgressRequest:
		return m.onProgressRequest(p, f.Payload)
	case MsgProgressPage:
		return m.onProgressPage(p, f.Payload)
	case MsgChunkAvailabilityRequest:
		return m.onChunkAvailabilityRequest(p, f.Payload)
	case MsgChunkAvailabilityPage:
		return m.onChunkAvailabilityPage(p, f.Payload)
	case MsgTransactionChunk:
		return m.onTransactionChunk(p, f.Payload)
	case MsgChunkNeed:
		// Service bulk chunk reads in the send loop, never in the read loop.
		m.queueCtrl(p, MsgChunkNeed, 0, f.Payload)
		return nil
	case MsgNeed:
		return m.onNeed(p, ps, f.Payload)
	case MsgSnapshotRequest:
		go m.sendSnapshot(p, ps)
		return nil
	case MsgSnapshotManifest:
		return m.onSnapshotManifest(p, ps, f.Payload)
	case MsgSnapshotChunk:
		return m.onSnapshotChunk(p, ps, f)
	case MsgSnapshotDone:
		return m.onSnapshotDone(p, ps)
	case MsgSchemaRequest:
		return m.onSchemaRequest(p, f.Payload)
	case MsgSchemaManifest:
		return m.onSchemaManifest(p, f.Payload)
	case MsgSchemaAck:
		return m.onSchemaAck(p, f.Payload)
	case MsgPing:
		return m.onPing(p, ps, f.Payload)
	case MsgPong:
		m.onPong(p, f.Payload)
		return nil
	case MsgError:
		m.onError(p, ps, f.Payload)
		return nil
	default:
		m.queueCtrl(p, MsgError, 0, EncodeError(nil, ErrBadMessage, fmt.Sprintf("unknown message %d", f.Type)))
		return nil
	}
}

func (m *Manager) onBatches(p *peerState, ps *peerSession, payload []byte) error {
	batches, err := DecodeBatches(payload, m.cfg.Limits)
	if err != nil {
		return err
	}
	m.st.batchBytesReceived.Add(uint64(len(payload)))
	groupApplier, canGroup := m.cfg.Applier.(GroupApplier)
	for i := 0; i < len(batches); {
		b := batches[i]
		if err := m.validateBatch(b); err != nil {
			m.st.batchesInvalid.Add(1)
			m.log.Warn("dropping invalid batch", slog.String("err", err.Error()))
			i++
			continue
		}
		if !m.batchSchemaKnown(b) {
			// Unknown or incompatible mutation schema: reject/defer the
			// batch (no watermark advance, no acknowledgement) and ask
			// for schema synchronization. The sender's unacked tail
			// redelivers after we converge. Strict nodes refuse
			// silently until explicitly upgraded.
			m.st.batchesDeferred.Add(1)
			m.log.Debug("deferring batch with unknown schema",
				slog.Uint64("epoch", b.SchemaEpoch))
			if m.canSyncSchema() {
				m.queueSchemaRequest(p, SchemaRequest{WantCurrent: true})
			}
			i++
			continue
		}
		group := []*codec.MutationBatch{b}
		groupBytes := int64(len(codec.EncodeBatch(nil, b)))
		groupLimit := 1
		if canGroup {
			groupLimit = m.applyGroupLimit()
		}
		for j := i + 1; j < len(batches) && len(group) < groupLimit; j++ {
			next := batches[j]
			if next.OriginNode != b.OriginNode || next.Sequence != group[len(group)-1].Sequence+1 || m.validateBatch(next) != nil || !m.batchSchemaKnown(next) {
				break
			}
			nextBytes := int64(len(codec.EncodeBatch(nil, next)))
			if groupBytes+nextBytes > maxApplyGroupBytes {
				break
			}
			group = append(group, next)
			groupBytes += nextBytes
		}
		if err := m.applyRemoteGroupWithRetry(groupApplier, group); err != nil {
			if state.IsGap(err) {
				m.st.gapsDetected.Add(1)
				wm, _ := m.cfg.Store.ReceiveWatermark(group[0].OriginNode)
				m.queueCtrl(p, MsgNeed, 0, EncodeNeed(nil, Need{Origin: group[0].OriginNode, FromSeq: wm + 1}))
				m.st.needsSent.Add(1)
				i += len(group)
				continue
			}
			m.adaptApplyGroup(false)
			m.st.applyFailures.Add(uint64(len(group)))
			m.log.Warn("apply failed", slog.String("err", err.Error()))
			i += len(group)
			continue
		}
		m.adaptApplyGroup(true)
		for _, applied := range group {
			if err := m.cfg.Store.ClearStagedTransaction(applied.TxID); err != nil {
				m.log.Warn("staged transaction cleanup failed after durable apply", slog.String("err", err.Error()))
			}
			m.mu.Lock()
			delete(m.chunkRepairAt, applied.TxID)
			delete(m.chunkRepairNeed, applied.TxID)
			m.mu.Unlock()
			m.st.batchesReceived.Add(1)
			m.st.mutationsReceived.Add(uint64(len(applied.Mutations)))
			if m.plumtree != nil {
				if err := m.plumtreeReceive(p.id, applied); err != nil {
					m.log.Debug("plumtree receive ignored", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
				}
			}
		}
		i += len(group)
	}
	return nil
}

func (m *Manager) applyGroupLimit() int {
	if target := m.applyGroupTarget.Load(); target > 0 {
		return int(target)
	}
	return 1
}

func (m *Manager) adaptApplyGroup(success bool) {
	for {
		old := m.applyGroupTarget.Load()
		current := old
		if current == 0 {
			current = 1
		}
		next := current
		if success && current < maxApplyGroupTransactions {
			next = current * 2
			if next > maxApplyGroupTransactions {
				next = maxApplyGroupTransactions
			}
		} else if !success && current > 1 {
			next = current / 2
			if next == 0 {
				next = 1
			}
		}
		if m.applyGroupTarget.CompareAndSwap(old, next) {
			return
		}
	}
}

func (m *Manager) applyRemoteGroupWithRetry(groupApplier GroupApplier, group []*codec.MutationBatch) error {
	var err error
	for i := 0; i < 5; i++ {
		if len(group) > 1 && groupApplier != nil {
			err = groupApplier.ApplyRemoteGroup(m.ctx, group)
		} else {
			err = m.cfg.Applier.ApplyRemote(m.ctx, group[0])
		}
		if err == nil || !state.IsConflict(err) {
			return err
		}
		m.st.applyRetries.Add(1)
		time.Sleep(time.Duration(i+1) * 5 * time.Millisecond)
	}
	return err
}

func (m *Manager) validateBatch(b *codec.MutationBatch) error {
	if b.OriginNode.IsZero() {
		return fmt.Errorf("zero origin")
	}
	if b.ProtocolVersion < MinProtocolVersion || b.ProtocolVersion > ProtocolVersion {
		return fmt.Errorf("batch protocol %d unsupported", b.ProtocolVersion)
	}
	if b.Sequence == 0 || b.HLC == 0 {
		return fmt.Errorf("batch has zero sequence/hlc")
	}
	return nil
}

// batchSchemaKnown reports whether a batch's schema provenance is the
// current manifest or a persisted compatible ancestor (whose column IDs
// resolve unchanged under additive evolution). Without a sync engine only
// the current identity applies.
func (m *Manager) batchSchemaKnown(b *codec.MutationBatch) bool {
	id := m.currentSchema()
	if b.SchemaEpoch == id.Epoch && b.SchemaHash == id.Hash {
		return true
	}
	if m.cfg.SchemaSync == nil {
		return false
	}
	return m.cfg.SchemaSync.SchemaProvenanceKnown(b.SchemaEpoch, b.SchemaHash)
}

// queueSchemaRequest enqueues an outbound schema request. Drops under
// pressure are safe: handshake, batch deferral, and ack decisions
// re-request until the session agrees or refuses.
func (m *Manager) queueSchemaRequest(p *peerState, r SchemaRequest) {
	if len(r.WantIDs) > maxSchemaRequestIDs {
		r.WantIDs = r.WantIDs[:maxSchemaRequestIDs]
	}
	select {
	case p.schemaReqCh <- r:
		m.NotifyLocal()
	default:
		m.st.schemaReqDrops.Add(1)
	}
}

// onSchemaRequest queues an inbound request for the send loop to serve.
// The read loop never sends, not even manifests.
func (m *Manager) onSchemaRequest(p *peerState, payload []byte) error {
	r, err := DecodeSchemaRequest(payload)
	if err != nil {
		return err
	}
	m.st.schemaRequestsReceived.Add(1)
	select {
	case p.schemaRespCh <- *r:
		m.NotifyLocal()
	default:
		// Queue full; the peer re-requests.
		m.st.schemaRespDrops.Add(1)
	}
	return nil
}

// onSchemaManifest advances local schema toward the peer's revisions. It
// runs the adoption engine inline: slow but serialized per session, and
// the peer's send deadlines recycle and retry if we take too long.
func (m *Manager) onSchemaManifest(p *peerState, payload []byte) error {
	msg, err := DecodeSchemaManifest(payload)
	if err != nil {
		return err
	}
	m.st.schemaManifestsReceived.Add(1)
	if m.cfg.SchemaSync == nil {
		m.st.schemaConflicts.Add(1)
		m.queueCtrl(p, MsgError, 0, EncodeError(nil, ErrSchemaMismatch, "schema sync unavailable"))
		return nil
	}
	dec := m.cfg.SchemaSync.SyncSchemas(m.ctx, msg.Revisions)
	if dec.Err != nil {
		// Incompatible: report, stay data-gated, nothing applied or acked.
		m.st.schemaConflicts.Add(1)
		m.log.Warn("schema incompatible with peer",
			slog.String("peer", p.id.String()), slog.String("err", dec.Err.Error()))
		m.queueCtrl(p, MsgError, 0, EncodeError(nil, ErrSchemaMismatch, dec.Err.Error()))
		return nil
	}
	if dec.SendRevisions {
		m.queueSchemaResponse(p, SchemaRequest{WantCurrent: true})
	}
	if len(dec.NeedIDs) > 0 {
		m.queueSchemaRequest(p, SchemaRequest{WantIDs: dec.NeedIDs})
	}
	if dec.SendAck {
		id := m.currentSchema()
		m.queueCtrl(p, MsgSchemaAck, 0, EncodeSchemaAck(nil, &SchemaAck{
			Version: id.Epoch,
			Hash:    id.Hash,
		}))
		m.st.schemaAcksSent.Add(1)
	}
	if dec.Agreed {
		m.setAgreed(p, true)
	}
	return nil
}

// queueSchemaResponse asks the send loop to serve our revisions to the peer
// (used when SyncSchemas decides the peer should adopt from us).
func (m *Manager) queueSchemaResponse(p *peerState, r SchemaRequest) {
	select {
	case p.schemaRespCh <- r:
		m.NotifyLocal()
	default:
		m.st.schemaRespDrops.Add(1)
	}
}

// onSchemaAck marks the session agreed when the peer acknowledges our
// exact identity; anything else re-triggers synchronization.
func (m *Manager) onSchemaAck(p *peerState, payload []byte) error {
	ack, err := DecodeSchemaAck(payload)
	if err != nil {
		return err
	}
	m.st.schemaAcksReceived.Add(1)
	id := m.currentSchema()
	if ack.Version == id.Epoch && ack.Hash == id.Hash {
		m.setAgreed(p, true)
		return nil
	}
	m.queueSchemaRequest(p, SchemaRequest{WantCurrent: true})
	return nil
}

// setAgreed flips data gating on and resumes sending.
func (m *Manager) setAgreed(p *peerState, agreed bool) {
	p.mu.Lock()
	changed := p.agreed != agreed
	p.agreed = agreed
	p.mu.Unlock()
	if changed && agreed {
		m.st.schemaAgreements.Add(1)
		m.NotifyLocal()
	}
}

func (m *Manager) applyWithRetry(b *codec.MutationBatch) error {
	var err error
	for i := 0; i < 5; i++ {
		err = m.cfg.Applier.ApplyRemote(m.ctx, b)
		if err == nil || !state.IsConflict(err) {
			return err
		}
		m.st.applyRetries.Add(1)
		time.Sleep(time.Duration(i+1) * 5 * time.Millisecond)
	}
	return err
}

func (m *Manager) onAck(p *peerState, payload []byte) error {
	wms, err := DecodeWatermarks(payload)
	if err != nil {
		return err
	}
	m.st.acksReceived.Add(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range wms {
		if w.Sequence > p.have[w.Origin] {
			p.have[w.Origin] = w.Sequence
			delete(p.sentErr, w.Origin)
		}
		if w.Sequence >= p.sent[w.Origin] {
			delete(p.retryAfter, w.Origin)
		}
		// Persist peer acknowledgement (survives restart for log GC),
		// renewing the member's retention deadline only when durable
		// progress advances. Repeated unchanged acks renew nothing.
		if _, err := m.cfg.Store.AdvanceMemberAck(p.id, w.Origin, w.Sequence, time.Now().UnixMilli(), m.cfg.AckRetention.Milliseconds()); err != nil {
			m.log.Warn("AdvanceMemberAck failed", slog.String("err", err.Error()))
		}
	}
	return nil
}

func (m *Manager) onNeed(p *peerState, _ *peerSession, payload []byte) error {
	need, err := DecodeNeed(payload)
	if err != nil {
		return err
	}
	// Serve asynchronously (see needCh): the read loop never bulk-sends.
	m.st.needsReceived.Add(1)
	var lease *overload.Lease
	if m.queueBudget != nil {
		lease, err = m.queueBudget.Acquire(p.id.String(), 32, 1)
		if err != nil {
			m.st.needDrops.Add(1)
			m.queueRetryError(p, ErrOverloadedCode, requestRetryHint(need.Origin, need.FromSeq))
			return nil
		}
	}
	select {
	case p.needCh <- queuedNeed{need: need, lease: lease}:
		m.NotifyLocal()
	default:
		if lease != nil {
			lease.Release()
		}
		// Queue full; the peer's next ack tick re-requests.
		m.st.needDrops.Add(1)
		m.queueRetryError(p, ErrOverloadedCode, requestRetryHint(need.Origin, need.FromSeq))
	}
	return nil
}

// queueCtrl enqueues one tiny outbound control frame for the send loop.
// Drops under pressure are safe (see ctrlCh).
func (m *Manager) queueCtrl(p *peerState, typ uint16, flags uint16, payload []byte) {
	m.queueCtrlAt(p, typ, flags, payload, time.Time{})
}

func (m *Manager) queueCtrlAt(p *peerState, typ uint16, flags uint16, payload []byte, due time.Time) {
	var lease *overload.Lease
	if m.queueBudget != nil {
		var err error
		lease, err = m.queueBudget.Acquire(p.id.String(), int64(len(payload)+frameHdrLen), 1)
		if err != nil {
			m.st.ctrlDrops.Add(1)
			hint := ""
			if typ == MsgNeed {
				if need, e := DecodeNeed(payload); e == nil {
					hint = requestRetryHint(need.Origin, need.FromSeq)
				}
			}
			if typ == MsgChunkNeed {
				if need, e := DecodeChunkNeed(payload); e == nil {
					hint = requestRetryHint(need.Origin, need.Sequence)
				}
			}
			m.queueRetryError(p, ErrOverloadedCode, hint)
			return
		}
	}
	select {
	case p.ctrlCh <- ctrlFrame{typ: typ, flags: flags, payload: payload, lease: lease, due: due}:
		m.NotifyLocal()
	default:
		if lease != nil {
			lease.Release()
		}
		m.st.ctrlDrops.Add(1)
		hint := ""
		if typ == MsgNeed {
			if need, e := DecodeNeed(payload); e == nil {
				hint = requestRetryHint(need.Origin, need.FromSeq)
			}
		}
		if typ == MsgChunkNeed {
			if need, e := DecodeChunkNeed(payload); e == nil {
				hint = requestRetryHint(need.Origin, need.Sequence)
			}
		}
		m.queueRetryError(p, ErrOverloadedCode, hint)
	}
}

func (m *Manager) queueRetryError(p *peerState, code uint16, message string) {
	select {
	case p.retryCh <- ctrlFrame{typ: MsgError, payload: EncodeError(nil, code, message)}:
		m.NotifyLocal()
	default:
		m.st.retryDrops.Add(1)
	}
}

func retryHint(origin ids.NodeID, seq uint64) string {
	return fmt.Sprintf("send:%s/%d", origin.String(), seq)
}

func requestRetryHint(origin ids.NodeID, seq uint64) string {
	return fmt.Sprintf("request:%s/%d", origin.String(), seq)
}

func (m *Manager) waitTransfer(p *peerState, bytes int) error {
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return m.waitTransferContext(ctx, p, bytes)
}
func (m *Manager) waitTransferContext(ctx context.Context, p *peerState, bytes int) error {
	if m.transferLimiter == nil || bytes <= 0 {
		return nil
	}
	return m.transferLimiter.WaitN(ctx, p.id.String(), int64(bytes))
}

// sendQueuedCtrl flushes read-loop control frames. It runs before bulk
// sends each round so pongs and pulls stay prompt.
func (m *Manager) sendQueuedCtrl(p *peerState, ps *peerSession) error {
	sent := 0
	inspectedCtrl := 0
	for {
		if sent >= controlFramesPerRound {
			return nil
		}
		select {
		case f := <-p.retryCh:
			if err := ps.send(f.typ, f.flags, f.payload); err != nil {
				return err
			}
			sent++
			continue
		default:
		}
		select {
		case f := <-p.ctrlCh:
			inspectedCtrl++
			if !f.due.IsZero() && time.Now().Before(f.due) {
				select {
				case p.ctrlCh <- f:
				default:
					if f.lease != nil {
						f.lease.Release()
					}
				}
				if inspectedCtrl >= cap(p.ctrlCh) {
					return nil
				}
				continue
			}
			var err error
			if f.typ == MsgChunkNeed {
				err = m.serveChunkNeedCapped(p, ps, f.payload, 16)
			} else {
				err = ps.send(f.typ, f.flags, f.payload)
			}
			if f.lease != nil {
				f.lease.Release()
			}
			if err != nil {
				return err
			}
			sent++
			if f.typ == MsgChunkNeed {
				return nil // bounded repair quantum; serve controls before more bulk chunks
			}
		default:
			return nil
		}
		if inspectedCtrl >= cap(p.ctrlCh) {
			return nil
		}
	}
}

// sendQueuedNeeds serves explicit Need requests from the send loop, where
// bulk writes cannot wedge the reader. Needs from the same round coalesce
// to the lowest sequence per origin; satisfied ranges scan empty (no-op).
func (m *Manager) sendQueuedNeeds(p *peerState, ps *peerSession) error {
	from := make(map[ids.NodeID]uint64)
	for {
		if len(from) >= needsPerRound {
			break
		}
		select {
		case queued := <-p.needCh:
			need := queued.need
			if queued.lease != nil {
				queued.lease.Release()
			}
			if cur, ok := from[need.Origin]; !ok || need.FromSeq < cur {
				from[need.Origin] = need.FromSeq
			}
		default:
			goto drained
		}
	}
drained:
	for origin, seq := range from {
		if err := m.sendOrigin(p, ps, origin, seq); err != nil {
			return err
		}
	}
	return nil
}

// recycleSession drops a suspect session so its loops exit and the dialer
// redials. Writes that time out (or otherwise fail) prove the session is
// wedged or dead; cursors resume on the replacement (attach restarts the
// send cursor from the peer's durable acks; the receiver dedups). Stale
// sessions are ignored via the identity check.
func (m *Manager) recycleSession(p *peerState, ps *peerSession, reason string) {
	p.mu.Lock()
	if p.session != ps {
		p.mu.Unlock()
		return
	}
	p.session = nil
	p.mu.Unlock()
	m.st.sessionsRecycled.Add(1)
	m.log.Debug("recycling session", slog.String("peer", p.id.String()), slog.String("err", reason))
	ps.close()
	p.pokeWake() // prompt redial
}

// sendFailed recycles the session when err is a transport write failure.
// Store errors leave the session alone (reconnecting cannot fix them).
func (m *Manager) sendFailed(p *peerState, ps *peerSession, err error) {
	var se *sendError
	if errors.As(err, &se) {
		m.recycleSession(p, ps, err.Error())
	}
}

func (m *Manager) onPing(p *peerState, _ *peerSession, payload []byte) error {
	m.st.pingsReceived.Add(1)
	m.queueCtrl(p, MsgPong, 0, payload)
	return nil
}

func (m *Manager) onPong(p *peerState, payload []byte) {
	if len(payload) != 8 {
		return
	}
	m.st.pongsReceived.Add(1)
	nonce := binary.BigEndian.Uint64(payload)
	p.mu.Lock()
	defer p.mu.Unlock()
	if nonce == p.pingNonce && !p.pingAt.IsZero() {
		p.rtt = time.Since(p.pingAt)
	}
}

func binaryBigEndianPutUint64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

func (m *Manager) onError(p *peerState, ps *peerSession, payload []byte) {
	code, msg, err := DecodeError(payload)
	if err != nil {
		return
	}
	m.st.errorsReceived.Add(1)
	switch code {
	case ErrOverloadedCode:
		kind, hint, tagged := strings.Cut(msg, ":")
		if !tagged {
			kind, hint = "send", msg
		}
		parts := strings.SplitN(hint, "/", 2)
		if len(parts) == 2 {
			if origin, e := ids.ParseNodeID(parts[0]); e == nil {
				if seq, e := strconv.ParseUint(parts[1], 10, 64); e == nil && seq > 0 {
					retryAt := time.Now().Add(time.Second)
					p.mu.Lock()
					if p.retryAfter == nil {
						p.retryAfter = make(map[ids.NodeID]time.Time)
					}
					if kind == "send" && p.sent[origin] >= seq {
						p.sent[origin] = seq - 1
					}
					p.retryAfter[origin] = retryAt
					p.mu.Unlock()
					if kind == "request" {
						m.retryRejectedRequest(p, origin, seq, retryAt)
					}
				}
			}
		}
	case ErrSnapshotRequired:
		m.st.snapshotRequiredReceived.Add(1)
		if !m.markSnapshotRequested(p) {
			m.log.Info("peer requires snapshot", slog.String("peer", p.id.String()))
			m.queueCtrl(p, MsgSnapshotRequest, 0, nil)
			m.st.snapshotRequestsSent.Add(1)
		}
	case ErrSnapshotBusy:
		// The source is already sending us a snapshot; back the stall
		// watchdog off briefly instead of piling on requests.
		m.st.snapshotBusyReceived.Add(1)
		p.mu.Lock()
		p.snapRetryAfter = time.Now().Add(5 * time.Second)
		p.mu.Unlock()
		m.log.Info("snapshot source busy; backing off", slog.String("peer", p.id.String()))
	case ErrSchemaMismatch:
		// Mid-session incompatibility: gate data and idle until either
		// side migrates (migration recycles sessions and re-drives
		// sync). Closing here would spin reconnects that can never
		// converge.
		m.st.schemaConflicts.Add(1)
		m.log.Warn("peer reports schema mismatch; data gated", slog.String("peer", p.id.String()), slog.String("err", msg))
		m.setAgreed(p, false)
	case ErrRangeUnavailable:
		var tx ids.TxID
		m.mu.Lock()
		var repair ChunkNeed
		found := false
		for id, need := range m.chunkRepairNeed {
			if id.String() == msg {
				tx = id
				repair = need
				found = true
				break
			}
		}
		if found && m.chunkRepairAt != nil {
			m.chunkRepairAt[tx] = time.Time{}
		}
		m.mu.Unlock()
		if found {
			p.mu.Lock()
			if p.chunkUnavailable == nil {
				p.chunkUnavailable = make(map[ids.TxID]bool)
			}
			p.chunkUnavailable[tx] = true
			delete(p.chunkAvailability, tx)
			p.mu.Unlock()
			present, e := m.cfg.Store.StagedTransactionChunks(tx)
			if e == nil {
				missing := make([]uint32, 0, len(present))
				for i, ok := range present {
					if !ok {
						missing = append(missing, uint32(i))
					}
				}
				if len(missing) > 0 && m.requestChunkSources(p, repair, missing) == 0 {
					if !m.markSnapshotRequested(p) {
						m.queueCtrl(p, MsgSnapshotRequest, 0, nil)
					}
				}
			}
		}
	case ErrProtocolMismatch, ErrPeerNotAllowed:
		m.log.Warn("peer rejected session", slog.String("peer", p.id.String()), slog.String("err", msg))
		ps.close()
	default:
		m.log.Warn("peer error", slog.String("peer", p.id.String()), slog.String("err", msg))
	}
}

func (m *Manager) retryRejectedRequest(p *peerState, origin ids.NodeID, seq uint64, due time.Time) {
	var chunkNeed *ChunkNeed
	m.mu.Lock()
	for _, need := range m.chunkRepairNeed {
		if need.Origin == origin && need.Sequence == seq {
			copyNeed := need
			copyNeed.Missing = append([]uint32(nil), need.Missing...)
			chunkNeed = &copyNeed
			break
		}
	}
	m.mu.Unlock()
	if chunkNeed != nil {
		m.queueCtrlAt(p, MsgChunkNeed, 0, EncodeChunkNeed(nil, *chunkNeed), due)
		return
	}
	m.queueCtrlAt(p, MsgNeed, 0, EncodeNeed(nil, Need{Origin: origin, FromSeq: seq}), due)
}

// --- send loop ---

func (m *Manager) sendLoop() {
	defer m.wg.Done()
	sendTick := time.NewTicker(m.cfg.SendInterval)
	defer sendTick.Stop()
	ackTick := time.NewTicker(m.cfg.AckInterval)
	defer ackTick.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.notifyCh:
			m.sendAll(false)
		case <-sendTick.C:
			m.sendAll(false)
		case <-ackTick.C:
			m.sendAll(true)
		}
	}
}

func (m *Manager) sendAll(withAck bool) {
	m.mu.Lock()
	peers := make([]*peerState, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	peers = rotatePeers(peers, m.sendPeerCursor)
	m.mu.Unlock()
	for _, p := range peers {
		p.mu.Lock()
		ps := p.session
		force := p.forceAck
		p.forceAck = false
		agreed := p.agreed
		selected := p.selected
		p.mu.Unlock()
		if ps == nil {
			continue
		}
		select {
		case <-ps.done:
			continue
		default:
		}
		m.mu.Lock()
		m.sendPeerCursor = p.id.String()
		m.mu.Unlock()
		if err := m.sendQueuedCtrl(p, ps); err != nil {
			m.sendFailed(p, ps, err)
			continue
		}
		// Schema traffic always flows so sessions can converge; data
		// stays gated until both sides verify identical schemas.
		if err := m.sendSchemaRequests(p, ps); err != nil {
			m.sendFailed(p, ps, err)
			continue
		}
		if err := m.serveSchemaResponses(p, ps); err != nil {
			m.sendFailed(p, ps, err)
			continue
		}
		if !agreed {
			continue
		}
		if err := m.sendQueuedPlumtree(p, ps); err != nil {
			m.sendFailed(p, ps, err)
			continue
		}
		if selected || force {
			if err := m.sendDue(p, ps, peerBatchQuantum); err != nil {
				m.sendFailed(p, ps, err)
				continue
			}
		}
		if err := m.sendQueuedNeeds(p, ps); err != nil {
			m.sendFailed(p, ps, err)
			continue
		}
		if withAck || force {
			if err := m.sendAckAndPull(p); err != nil {
				m.sendFailed(p, ps, err)
				continue
			}
		}
	}
}

func rotatePeers(peers []*peerState, cursor string) []*peerState {
	sort.Slice(peers, func(i, j int) bool { return peers[i].id.String() < peers[j].id.String() })
	if len(peers) == 0 {
		return peers
	}
	start := sort.Search(len(peers), func(i int) bool { return peers[i].id.String() > cursor })
	if start == len(peers) {
		start = 0
	}
	return append(peers[start:], peers[:start]...)
}

// sendSchemaRequests flushes coalesced outbound schema requests: flags OR
// together and IDs union, capped.
func (m *Manager) sendSchemaRequests(p *peerState, ps *peerSession) error {
	var out *SchemaRequest
	for {
		select {
		case r := <-p.schemaReqCh:
			if out == nil {
				out = &SchemaRequest{}
			}
			out.WantCurrent = out.WantCurrent || r.WantCurrent
			out.WantIDs = unionSchemaIDs(out.WantIDs, r.WantIDs)
		default:
			goto drained
		}
	}
drained:
	if out == nil {
		return nil
	}
	if err := ps.send(MsgSchemaRequest, 0, EncodeSchemaRequest(nil, out)); err != nil {
		return err
	}
	m.st.schemaRequestsSent.Add(1)
	return nil
}

func unionSchemaIDs(a, b [][32]byte) [][32]byte {
	if len(a)+len(b) == 0 {
		return nil
	}
	seen := make(map[[32]byte]bool, len(a)+len(b))
	out := make([][32]byte, 0, len(a)+len(b))
	for _, id := range a {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, id := range b {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	if len(out) > maxSchemaRequestIDs {
		out = out[:maxSchemaRequestIDs]
	}
	return out
}

// serveSchemaResponses answers queued inbound schema requests with our
// revisions. Unknown IDs are skipped; an empty answer means nothing more
// to offer (the requester defers rather than guessing).
func (m *Manager) serveSchemaResponses(p *peerState, ps *peerSession) error {
	if m.cfg.SchemaSync == nil {
		// Drain without answering; strict peers refuse at handshake and
		// never get here with a live session asking.
		for {
			select {
			case <-p.schemaRespCh:
			default:
				return nil
			}
		}
	}
	for {
		select {
		case r := <-p.schemaRespCh:
			revs, err := m.cfg.SchemaSync.RevisionsForPeer(r.WantCurrent, r.WantIDs, maxSchemaPayload)
			if err != nil {
				m.log.Warn("schema revisions unavailable",
					slog.String("peer", p.id.String()), slog.String("err", err.Error()))
				continue
			}
			if len(revs) == 0 {
				continue
			}
			if err := ps.send(MsgSchemaManifest, 0, EncodeSchemaManifest(nil, &SchemaManifestMsg{Revisions: revs})); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

func (m *Manager) sessionFor(p *peerState) *peerSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.session
}

// sendDue pushes missing origin-log ranges to the peer.
func (m *Manager) sendDue(p *peerState, ps *peerSession, peerBudget int) error {
	origins, err := m.cfg.Store.KnownOrigins()
	if err != nil {
		return err
	}
	sentBatches := 0
	if peerBudget <= 0 || peerBudget > m.cfg.MaxBatchMutations {
		peerBudget = m.cfg.MaxBatchMutations
	}
	for _, origin := range origins {
		if sentBatches >= peerBudget {
			break
		}
		p.mu.Lock()
		// Suppress resends within a session: the stream is reliable, so
		// anything sent will arrive; only acks (or a new session) move the
		// cursors. Explicit Need requests bypass this via sendOrigin.
		from := p.have[origin] + 1
		if s := p.sent[origin] + 1; s > from {
			from = s
		}
		needErr := p.sentErr[origin]
		retryAfter := p.retryAfter[origin]
		p.mu.Unlock()
		if time.Now().Before(retryAfter) {
			continue
		}
		if needErr {
			continue // peer must snapshot for this origin
		}
		wm, err := m.cfg.Store.ReceiveWatermark(origin)
		if err != nil {
			return err
		}
		if from > wm {
			continue
		}
		n, err := m.sendOriginCapped(p, ps, origin, from, peerBudget-sentBatches)
		if err != nil {
			if errors.Is(err, state.ErrLogGone) {
				p.mu.Lock()
				p.sentErr[origin] = true
				p.mu.Unlock()
				m.st.snapshotRequiredSent.Add(1)
				_ = ps.send(MsgError, 0, EncodeError(nil, ErrSnapshotRequired,
					fmt.Sprintf("origin %s log before %d collected", origin, from)))
				continue
			}
			return err
		}
		sentBatches += n
	}
	return nil
}

// sendOrigin sends one origin range starting at from (Need path).
func (m *Manager) sendOrigin(p *peerState, ps *peerSession, origin ids.NodeID, from uint64) error {
	capBatches := peerBatchQuantum
	if capBatches > m.cfg.MaxBatchMutations {
		capBatches = m.cfg.MaxBatchMutations
	}
	_, err := m.sendOriginCapped(p, ps, origin, from, capBatches)
	if err != nil && errors.Is(err, state.ErrLogGone) {
		p.mu.Lock()
		p.sentErr[origin] = true
		p.mu.Unlock()
		m.st.snapshotRequiredSent.Add(1)
		_ = ps.send(MsgError, 0, EncodeError(nil, ErrSnapshotRequired,
			fmt.Sprintf("origin %s log before %d collected", origin, from)))
		return nil
	}
	return err
}

func (m *Manager) sendOriginCapped(p *peerState, ps *peerSession, origin ids.NodeID, from uint64, capBatches int) (int, error) {
	var batches []*codec.MutationBatch
	last, err := m.cfg.Store.LogScan(origin, from, capBatches, m.cfg.MaxBatchBytes,
		func(b *codec.MutationBatch) error {
			batches = append(batches, b)
			return nil
		})
	if err != nil {
		return 0, err
	}
	if len(batches) == 0 {
		return 0, nil
	}
	raw := EncodeBatches(nil, batches)
	if len(raw) <= MaxFrameBytes {
		if err := m.waitTransfer(p, len(raw)); err != nil {
			return 0, err
		}
		if err := ps.send(MsgBatches, 0, raw); err != nil {
			return 0, err
		}
	} else {
		for _, batch := range batches {
			if err := m.sendBatchOrChunks(p, ps, batch); err != nil {
				return 0, err
			}
		}
	}
	m.st.batchesSent.Add(uint64(len(batches)))
	m.st.batchBytesSent.Add(uint64(len(raw)))
	var muts uint64
	for _, b := range batches {
		muts += uint64(len(b.Mutations))
		m.plumtreeStart(p.id, b)
	}
	m.st.mutationsSent.Add(muts)
	p.mu.Lock()
	if last > p.sent[origin] {
		p.sent[origin] = last
	}
	// Optimistically treat sent bytes as in-flight (have advances on Ack).
	p.mu.Unlock()
	return len(batches), nil
}

// sendAckAndPull advertises our durable watermarks and pulls newer data.
func (m *Manager) sendAckAndPull(p *peerState) error {
	ps := m.sessionFor(p)
	if ps == nil {
		return nil
	}
	wms, err := m.cfg.Store.ReceiveWatermarks()
	if err != nil {
		return err
	}
	p.mu.Lock()
	progress := p.progressPages
	p.mu.Unlock()
	if progress {
		items, _, pageErr := m.cfg.Store.ReceiveProgressPage(ids.NodeID{}, ProgressPageEntries)
		if pageErr != nil {
			return pageErr
		}
		wms = make([]codec.OriginWatermark, 0, len(items))
		for _, item := range items {
			wms = append(wms, codec.OriginWatermark{Origin: item.Origin, Sequence: item.Applied})
		}
	}
	if err := ps.send(MsgAck, 0, EncodeWatermarks(nil, wms)); err != nil {
		return err
	}
	m.st.acksSent.Add(1)
	// Pull: request everything newer than our watermarks.
	for _, w := range wms {
		_ = ps.send(MsgNeed, 0, EncodeNeed(nil, Need{Origin: w.Origin, FromSeq: w.Sequence + 1}))
	}
	if progress {
		_ = ps.send(MsgProgressRequest, 0, EncodeProgressRequest(nil, ids.NodeID{}))
	}
	p.mu.Lock()
	chunkSync := p.caps&CapTransactionChunks != 0
	p.mu.Unlock()
	if chunkSync {
		_ = ps.send(MsgChunkAvailabilityRequest, 0, EncodeChunkAvailabilityRequest(nil, ids.TxID{}))
	}
	m.st.needsSent.Add(uint64(len(wms)))
	// Ping for RTT.
	p.mu.Lock()
	p.pingNonce++
	nonce := p.pingNonce
	p.pingAt = time.Now()
	p.mu.Unlock()
	_ = ps.send(MsgPing, 0, binaryBigEndianPutUint64(nonce))
	m.st.pingsSent.Add(1)
	return nil
}

// --- snapshots ---

var errSnapshotInFlight = errors.New("snapshot already in flight")

// markSnapshotRequested records a fresh snapshot wait and reports whether
// one was already outstanding. Repeat requests never extend the stall
// deadline. Callers must not hold p.mu.
func (m *Manager) markSnapshotRequested(p *peerState) (already bool) {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.awaiting {
		return true
	}
	p.awaiting = true
	p.awaitingSince = now
	p.lastSnapProgress = now
	return false
}

// markSnapshotProgress records received snapshot traffic so the stall
// watchdog does not re-request a transfer that is moving. Callers must
// not hold p.mu.
func (m *Manager) markSnapshotProgress(p *peerState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastSnapProgress = time.Now()
}

func (m *Manager) sendSnapshot(p *peerState, ps *peerSession) {
	if !p.snapSend.CompareAndSwap(false, true) {
		// One outbound snapshot per peer at a time. Tell the requester
		// to back off instead of dropping the request silently: a silent
		// drop used to stall the receiver's one-shot wait forever.
		m.st.snapshotsBusyDeferred.Add(1)
		m.queueCtrl(p, MsgError, 0, EncodeError(nil, ErrSnapshotBusy, "snapshot already in flight"))
		return
	}
	defer p.snapSend.Store(false)
	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.SnapshotTransferTimeout)
	defer cancel()
	snapStream, err := ps.openSnapshotStream(ctx)
	if err != nil {
		m.st.snapshotsSendFailed.Add(1)
		m.log.Warn("open snapshot stream failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
		return
	}
	defer snapStream.Close()

	p.mu.Lock()
	useZstd := p.caps&CapZstd != 0
	p.mu.Unlock()
	var enc *zstd.Encoder
	if useZstd {
		enc, _ = zstd.NewWriter(nil)
		if enc != nil {
			defer enc.Close()
		}
	}
	sentManifest := false
	chunkIndex := uint64(0)
	err = m.cfg.Store.ExportSnapshotContext(ctx, m.cfg.SnapshotChunkCells,
		func(manifest *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error {
			if manifest.EncodedBytes > m.cfg.MaxSnapshotBytes {
				return fmt.Errorf("snapshot encoded size %d exceeds configured maximum %d", manifest.EncodedBytes, m.cfg.MaxSnapshotBytes)
			}
			if !sentManifest {
				sentManifest = true
				if err := ps.sendStream(snapStream, MsgSnapshotManifest, 0, codec.EncodeManifest(nil, manifest)); err != nil {
					return err
				}
			}
			raw := EncodeSnapshotChunk(nil, &SnapshotChunk{Index: chunkIndex, Last: last, Cells: chunk})
			flags := uint16(0)
			if enc != nil && len(raw) > 4096 {
				if c := enc.EncodeAll(raw, nil); len(c) < len(raw) {
					raw = c
					flags = FlagZstd
				}
			}
			if err := m.waitTransferContext(ctx, p, len(raw)); err != nil {
				return err
			}
			if err := ps.sendStream(snapStream, MsgSnapshotChunk, flags, raw); err != nil {
				return err
			}
			m.st.snapshotChunksSent.Add(1)
			m.st.snapshotBytesSent.Add(uint64(len(raw)))
			chunkIndex++
			if last {
				if err := ps.sendStream(snapStream, MsgSnapshotDone, 0, nil); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		m.st.snapshotsSendFailed.Add(1)
		m.log.Warn("snapshot send failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
		m.sendFailed(p, ps, err)
		return
	}
	m.st.snapshotsSent.Add(1)
}

func (m *Manager) snapRecvFor(p *peerState) *snapRecvState {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snapRecv == nil {
		p.snapRecv = &snapRecvState{}
	}
	return p.snapRecv
}

type snapRecvState struct {
	manifest       *codec.SnapshotManifest
	chunksReceived uint64
}

func (m *Manager) onSnapshotManifest(p *peerState, _ *peerSession, payload []byte) error {
	manifest, rest, err := codec.DecodeManifest(payload)
	if err != nil {
		return err
	}
	m.st.snapshotManifestsReceived.Add(1)
	if len(rest) != 0 || manifest.FormatVersion != 1 || manifest.ChunkCount == 0 || manifest.ChunkCount > 65_536 || manifest.EncodedBytes > m.cfg.MaxSnapshotBytes {
		m.st.snapshotManifestsRejected.Add(1)
		return fmt.Errorf("invalid snapshot manifest bounds or format")
	}
	if manifest.DBID != m.cfg.DBID {
		m.st.snapshotManifestsRejected.Add(1)
		return fmt.Errorf("snapshot db id mismatch")
	}
	if id := m.currentSchema(); manifest.SchemaEpoch != id.Epoch || manifest.SchemaHash != id.Hash {
		m.st.snapshotManifestsRejected.Add(1)
		m.st.schemaConflicts.Add(1)
		m.queueCtrl(p, MsgError, 0, EncodeError(nil, ErrSchemaMismatch, "snapshot schema mismatch"))
		return fmt.Errorf("snapshot schema mismatch")
	}
	st := m.snapRecvFor(p)
	st.manifest = manifest
	st.chunksReceived = 0
	m.markSnapshotProgress(p)
	return nil
}

func (m *Manager) onSnapshotChunk(p *peerState, _ *peerSession, f *Frame) error {
	payload := f.Payload
	if f.Flags&FlagZstd != 0 {
		dec, err := zstd.NewReader(bytes.NewReader(payload))
		if err != nil {
			return err
		}
		defer dec.Close()
		// Bound decompressed size (chunks carry <= SnapshotChunkCells cells).
		raw, err := io.ReadAll(io.LimitReader(dec, 256<<20))
		if err != nil {
			return err
		}
		payload = raw
	}
	chunk, err := DecodeSnapshotChunk(payload, m.cfg.Limits, m.cfg.SnapshotChunkCells*4+1024)
	if err != nil {
		return err
	}
	m.st.snapshotChunksReceived.Add(1)
	m.st.snapshotBytesReceived.Add(uint64(len(payload)))
	st := m.snapRecvFor(p)
	if st.manifest == nil {
		return fmt.Errorf("snapshot chunk without manifest")
	}
	var aerr error
	complete := false
	for i := 0; i < 5; i++ {
		complete, aerr = m.cfg.Applier.ApplySnapshotChunk(m.ctx, st.manifest, chunk.Index, chunk.Cells, chunk.Last)
		if aerr == nil || !state.IsConflict(aerr) {
			break
		}
		time.Sleep(time.Duration(i+1) * 5 * time.Millisecond)
	}
	if aerr != nil {
		return aerr
	}
	st.chunksReceived++
	m.markSnapshotProgress(p)
	if complete {
		st.manifest = nil
		st.chunksReceived = 0
		m.st.snapshotsReceived.Add(1)
		p.mu.Lock()
		p.awaiting = false
		// Advertise new watermarks and pull anything newer than the
		// snapshot from the send loop (the read loop never sends).
		p.forceAck = true
		p.mu.Unlock()
		m.NotifyLocal()
	}
	return nil
}

func (m *Manager) onSnapshotDone(p *peerState, _ *peerSession) error {
	st := m.snapRecvFor(p)
	if st.manifest != nil {
		// The sender closed a transfer before all indexed chunks validated.
		// Request a fresh cut; do not advertise the manifest watermarks.
		m.queueCtrl(p, MsgSnapshotRequest, 0, nil)
		m.st.snapshotRequestsSent.Add(1)
		m.st.snapshotReRequested.Add(1)
		return nil
	}
	// A successful transfer already queued the durable watermark ack. The
	// done marker by itself is never evidence that publication completed.
	return nil
}
