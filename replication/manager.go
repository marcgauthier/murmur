package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/quic-go/quic-go"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/state"
	"github.com/nomadsql/replicateddb/transport"
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
	ApplySnapshotChunk(ctx context.Context, manifest *codec.SnapshotManifest, cells []codec.SnapshotCell, last bool) error
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

	ListenAddr string
	Peers      []PeerInfo

	MaxBatchBytes     int
	MaxBatchMutations int
	SendInterval      time.Duration
	DialInterval      time.Duration
	AckInterval       time.Duration
	// SendTimeout bounds one framed write. A peer that stops reading
	// (wedged or dead without closing) must never hang a sender forever:
	// the write fails, the session is recycled, and cursors resume.
	// Default 30s.
	SendTimeout time.Duration

	SnapshotChunkCells int

	Limits codec.Limits
	Logger Logger
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
	if c.SendTimeout == 0 {
		c.SendTimeout = 30 * time.Second
	}
	if c.SnapshotChunkCells == 0 {
		c.SnapshotChunkCells = 2_000
	}
	if c.Limits.MaxValueBytes == 0 {
		c.Limits = codec.DefaultLimits()
	}
}

// PeerStatus is a point-in-time peer snapshot.
type PeerStatus struct {
	NodeID    ids.NodeID
	Addrs     []string
	Connected bool
	LastSeen  time.Time
	RTT       time.Duration
	Have      map[ids.NodeID]uint64
	Sent      map[ids.NodeID]uint64
}

// peerSession is one live framed stream to a peer.
type peerSession struct {
	sess      *transport.Session
	stream    *quic.Stream
	writeMu   sync.Mutex
	outbound  bool // true when we dialed
	done      chan struct{}
	closeOnce sync.Once
	timeout   time.Duration // per-write deadline (manager SendTimeout)
}

func (s *peerSession) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		_ = s.stream.Close()
		_ = s.sess.Close()
	})
}

func (s *peerSession) send(typ uint16, flags uint16, payload []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	select {
	case <-s.done:
		return fmt.Errorf("replication: session closed")
	default:
	}
	// Every framed write is deadline-bounded: a peer that stops reading
	// fails the write instead of hanging the sender forever.
	if s.timeout > 0 {
		_ = s.stream.SetWriteDeadline(time.Now().Add(s.timeout))
	}
	if err := WriteFrame(s.stream, typ, flags, payload); err != nil {
		return &sendError{err: err}
	}
	return nil
}

// sendError marks transport write failures (the session is suspect and
// should be recycled). It unwraps to the underlying error, so deadline
// timeouts still match os.ErrDeadlineExceeded.
type sendError struct{ err error }

func (e *sendError) Error() string { return "replication: send: " + e.err.Error() }
func (e *sendError) Unwrap() error { return e.err }

// peerState tracks one peer (configured or dynamic inbound).
type peerState struct {
	mu       sync.Mutex
	id       ids.NodeID
	addrs    []string
	dynamic  bool
	session  *peerSession
	lastSeen time.Time
	rtt      time.Duration
	have     map[ids.NodeID]uint64 // peer's watermarks
	sent     map[ids.NodeID]uint64 // last seq we sent per origin
	sentErr  map[ids.NodeID]bool   // snapshot-required already signalled
	caps     uint64
	awaiting bool // we requested a snapshot and wait for it
	forceAck bool // ForceSync requested an immediate ack+pull
	wake     chan struct{}

	pingNonce uint64
	pingAt    time.Time

	// needCh queues explicit Need requests for the send loop. Needs carry
	// bulk batch payloads, so the read loop must never serve them inline:
	// two peers bulk-sending inside their read loops wedge both flow
	// windows with nobody reading (duplex deadlock). Drops are safe: the
	// peer re-requests every ack tick until its watermark advances.
	needCh chan Need

	// ctrlCh queues tiny outbound control frames (pong, Need, snapshot
	// request, error replies) for the send loop. The read loop must not
	// send at all — not even tiny frames: the session send mutex couples
	// reader progress to a bulk write that only completes once the peer
	// reads, which wedges both sides symmetrically. Drops are safe
	// (pongs are best-effort; Needs re-request; requests retry).
	ctrlCh chan ctrlFrame

	snapSend atomic.Bool
	snapRecv *snapRecvState
}

// ctrlFrame is one tiny outbound control frame queued by the read loop.
type ctrlFrame struct {
	typ     uint16
	flags   uint16
	payload []byte
}

func newPeerState(id ids.NodeID, addrs []string, dynamic bool) *peerState {
	return &peerState{
		id: id, addrs: addrs, dynamic: dynamic,
		have:    map[ids.NodeID]uint64{},
		sent:    map[ids.NodeID]uint64{},
		sentErr: map[ids.NodeID]bool{},
		wake:    make(chan struct{}, 1),
		needCh:  make(chan Need, 64),
		ctrlCh:  make(chan ctrlFrame, 64),
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

	ln *transport.Listener

	mu    sync.Mutex
	peers map[ids.NodeID]*peerState

	notifyCh chan struct{}
	wg       sync.WaitGroup
	// running publishes Run startup: it is stored (under mu, together
	// with the ctx/cancel/log assignment and the initial peer sweep) so
	// AddPeer can decide race-free whether to spawn a dialer. Loops and
	// sessions only exist while running is true.
	running atomic.Bool
	cancel  context.CancelFunc
	ctx     context.Context
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
	m := &Manager{
		cfg:      cfg,
		peers:    make(map[ids.NodeID]*peerState),
		notifyCh: make(chan struct{}, 1),
	}
	for _, p := range cfg.Peers {
		if p.NodeID == cfg.Local {
			continue
		}
		m.peers[p.NodeID] = newPeerState(p.NodeID, p.Addrs, false)
	}
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
	m.wg.Add(1)
	go m.sendLoop()
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
	m.mu.Unlock()
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

func (m *Manager) closeSessions() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.peers {
		p.mu.Lock()
		if p.session != nil {
			p.session.close()
			p.session = nil
		}
		p.mu.Unlock()
	}
}

// NotifyLocal wakes senders after a local durable commit.
func (m *Manager) NotifyLocal() {
	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

// AddPeer adds or updates a configured peer.
func (m *Manager) AddPeer(id ids.NodeID, addrs []string) {
	if id == m.cfg.Local {
		return
	}
	m.mu.Lock()
	p, ok := m.peers[id]
	if !ok {
		p = newPeerState(id, addrs, false)
		m.peers[id] = p
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
	m.mu.Unlock()
	p.mu.Lock()
	p.addrs = addrs
	p.dynamic = false
	p.mu.Unlock()
	p.pokeWake()
}

// RemovePeer retires a peer: the session closes and it no longer gates log GC.
func (m *Manager) RemovePeer(id ids.NodeID) {
	m.mu.Lock()
	p, ok := m.peers[id]
	if ok {
		delete(m.peers, id)
	}
	m.mu.Unlock()
	if ok {
		p.mu.Lock()
		if p.session != nil {
			p.session.close()
			p.session = nil
		}
		p.mu.Unlock()
	}
}

// ForceSync triggers an immediate dial, ack, and pull from the peer.
func (m *Manager) ForceSync(id ids.NodeID) {
	m.mu.Lock()
	p, ok := m.peers[id]
	m.mu.Unlock()
	if !ok {
		return
	}
	p.mu.Lock()
	sessAlive := p.session != nil
	p.forceAck = true
	p.mu.Unlock()
	p.pokeWake()
	m.NotifyLocal()
	if sessAlive {
		// Best-effort immediate pull; the send loop covers races.
		_ = m.sendAckAndPull(p)
	}
}

// PeerStatus snapshots peer states.
func (m *Manager) PeerStatus() []PeerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]PeerStatus, 0, len(m.peers))
	for _, p := range m.peers {
		p.mu.Lock()
		st := PeerStatus{
			NodeID:    p.id,
			Addrs:     append([]string(nil), p.addrs...),
			Connected: p.session != nil,
			LastSeen:  p.lastSeen,
			RTT:       p.rtt,
			Have:      copyMap(p.have),
			Sent:      copyMap(p.sent),
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

func (m *Manager) ourHello() (*Hello, error) {
	wms, err := m.cfg.Store.ReceiveWatermarks()
	if err != nil {
		return nil, err
	}
	return &Hello{
		ProtocolVersion:    ProtocolVersion,
		MinProtocolVersion: MinProtocolVersion,
		NodeID:             m.cfg.Local,
		DBID:               m.cfg.DBID,
		SchemaEpoch:        m.cfg.SchemaEpoch,
		SchemaHash:         m.cfg.SchemaHash,
		Capabilities:       CapZstd,
		Have:               wms,
	}, nil
}

func (m *Manager) validateHello(h *Hello, sessPeer ids.NodeID) error {
	if h.NodeID != sessPeer {
		return fmt.Errorf("hello node %s != TLS identity %s", h.NodeID, sessPeer)
	}
	if h.DBID != m.cfg.DBID {
		return fmt.Errorf("db id mismatch")
	}
	if h.ProtocolVersion < MinProtocolVersion || h.MinProtocolVersion > ProtocolVersion {
		return fmt.Errorf("protocol %d (min %d) incompatible", h.ProtocolVersion, h.MinProtocolVersion)
	}
	if h.SchemaEpoch != m.cfg.SchemaEpoch || h.SchemaHash != m.cfg.SchemaHash {
		return fmt.Errorf("schema epoch %d mismatch (want %d)", h.SchemaEpoch, m.cfg.SchemaEpoch)
	}
	return nil
}

// attach installs a handshaked session with deterministic dedupe: when two
// connections exist to one peer, both sides keep the one initiated by the
// lower NodeID.
func (m *Manager) attach(p *peerState, ps *peerSession, h *Hello) {
	// Never start session loops during shutdown: a session attached after
	// Run's closeSessions would never be closed and would hang Close.
	if m.ctx.Err() != nil {
		go ps.close()
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range h.Have {
		if w.Sequence > p.have[w.Origin] {
			p.have[w.Origin] = w.Sequence
		}
	}
	p.caps = h.Capabilities
	p.sentErr = make(map[ids.NodeID]bool)
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
				go ps.close()
				return
			}
			old.close()
		}
	}
	p.session = ps
	m.wg.Add(1)
	go m.readLoop(p, ps)
}

func (m *Manager) detach(p *peerState, ps *peerSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.session == ps {
		p.session = nil
	}
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
			m.log.Warn("accept failed", slog.String("err", err.Error()))
			continue
		}
		go m.serveInbound(sess)
	}
}

func (m *Manager) serveInbound(sess *transport.Session) {
	ctx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()
	stream, err := sess.AcceptStream(ctx)
	if err != nil {
		_ = sess.Close()
		return
	}
	frame, err := ReadFrame(stream)
	if err != nil || frame.Type != MsgHello {
		_ = stream.Close()
		_ = sess.Close()
		return
	}
	h, err := DecodeHello(frame.Payload)
	if err != nil {
		_ = stream.Close()
		_ = sess.Close()
		return
	}
	if err := m.validateHello(h, sess.Peer); err != nil {
		_ = WriteFrame(stream, MsgError, 0, EncodeError(nil, ErrSchemaMismatch, err.Error()))
		_ = stream.Close()
		_ = sess.Close()
		return
	}
	ours, err := m.ourHello()
	if err != nil {
		_ = stream.Close()
		_ = sess.Close()
		return
	}
	if err := WriteFrame(stream, MsgWelcome, 0, EncodeHello(nil, ours)); err != nil {
		_ = stream.Close()
		_ = sess.Close()
		return
	}
	p := m.peerFor(sess.Peer, nil, true)
	m.attach(p, &peerSession{sess: sess, stream: stream, done: make(chan struct{}), timeout: m.cfg.SendTimeout}, h)
	m.NotifyLocal()
}

func (m *Manager) peerFor(id ids.NodeID, addrs []string, dynamic bool) *peerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.peers[id]; ok {
		return p
	}
	p := newPeerState(id, addrs, dynamic)
	m.peers[id] = p
	return p
}

func (m *Manager) dialLoop(p *peerState) {
	defer m.wg.Done()
	// Immediate first attempt.
	m.tryDial(p)
	t := time.NewTicker(m.cfg.DialInterval)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
			m.tryDial(p)
		case <-p.wake:
			m.tryDial(p)
		}
	}
}

func (m *Manager) tryDial(p *peerState) {
	p.mu.Lock()
	addrs := append([]string(nil), p.addrs...)
	alive := p.session != nil
	p.mu.Unlock()
	if alive || len(addrs) == 0 || m.ctx.Err() != nil {
		return
	}
	for _, addr := range addrs {
		ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
		sess, err := transport.Dial(ctx, addr, m.cfg.Creds, p.id)
		cancel()
		if err != nil {
			continue
		}
		if m.handshakeOutbound(p, sess) {
			return
		}
	}
}

func (m *Manager) handshakeOutbound(p *peerState, sess *transport.Session) bool {
	ctx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		_ = sess.Close()
		return false
	}
	ours, err := m.ourHello()
	if err != nil {
		_ = stream.Close()
		_ = sess.Close()
		return false
	}
	if err := WriteFrame(stream, MsgHello, 0, EncodeHello(nil, ours)); err != nil {
		_ = stream.Close()
		_ = sess.Close()
		return false
	}
	frame, err := ReadFrame(stream)
	if err != nil || frame.Type != MsgWelcome {
		_ = stream.Close()
		_ = sess.Close()
		return false
	}
	h, err := DecodeHello(frame.Payload)
	if err != nil || m.validateHello(h, sess.Peer) != nil {
		_ = stream.Close()
		_ = sess.Close()
		return false
	}
	m.attach(p, &peerSession{sess: sess, stream: stream, outbound: true, done: make(chan struct{}), timeout: m.cfg.SendTimeout}, h)
	m.NotifyLocal()
	return true
}

// --- read loop ---

func (m *Manager) readLoop(p *peerState, ps *peerSession) {
	defer m.wg.Done()
	defer m.detach(p, ps)
	defer ps.close()
	for {
		frame, err := ReadFrame(ps.stream)
		if err != nil {
			if m.ctx.Err() == nil {
				m.log.Debug("session read ended", slog.String("peer", p.id.String()))
			}
			return
		}
		p.mu.Lock()
		p.lastSeen = time.Now()
		p.mu.Unlock()
		if err := m.dispatch(p, ps, frame); err != nil {
			m.log.Warn("dispatch failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
			return
		}
	}
}

func (m *Manager) dispatch(p *peerState, ps *peerSession, f *Frame) error {
	switch f.Type {
	case MsgBatches:
		return m.onBatches(p, ps, f.Payload)
	case MsgAck:
		return m.onAck(p, f.Payload)
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
	for _, b := range batches {
		if err := m.validateBatch(b); err != nil {
			m.log.Warn("dropping invalid batch", slog.String("err", err.Error()))
			continue
		}
		if err := m.applyWithRetry(b); err != nil {
			if state.IsGap(err) {
				wm, _ := m.cfg.Store.ReceiveWatermark(b.OriginNode)
				m.queueCtrl(p, MsgNeed, 0, EncodeNeed(nil, Need{Origin: b.OriginNode, FromSeq: wm + 1}))
				continue
			}
			m.log.Warn("apply failed", slog.String("err", err.Error()))
			continue
		}
	}
	return nil
}

func (m *Manager) validateBatch(b *codec.MutationBatch) error {
	if b.OriginNode.IsZero() {
		return fmt.Errorf("zero origin")
	}
	if b.ProtocolVersion < MinProtocolVersion || b.ProtocolVersion > ProtocolVersion {
		return fmt.Errorf("batch protocol %d unsupported", b.ProtocolVersion)
	}
	if b.SchemaEpoch != m.cfg.SchemaEpoch {
		return fmt.Errorf("batch schema epoch %d != %d", b.SchemaEpoch, m.cfg.SchemaEpoch)
	}
	if b.Sequence == 0 || b.HLC == 0 {
		return fmt.Errorf("batch has zero sequence/hlc")
	}
	return nil
}

func (m *Manager) applyWithRetry(b *codec.MutationBatch) error {
	var err error
	for i := 0; i < 5; i++ {
		err = m.cfg.Applier.ApplyRemote(m.ctx, b)
		if err == nil || !state.IsConflict(err) {
			return err
		}
		time.Sleep(time.Duration(i+1) * 5 * time.Millisecond)
	}
	return err
}

func (m *Manager) onAck(p *peerState, payload []byte) error {
	wms, err := DecodeWatermarks(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range wms {
		if w.Sequence > p.have[w.Origin] {
			p.have[w.Origin] = w.Sequence
		}
		// Persist peer acknowledgement (survives restart for log GC).
		if err := m.cfg.Store.SetPeerAck(p.id, w.Origin, w.Sequence); err != nil {
			m.log.Warn("SetPeerAck failed", slog.String("err", err.Error()))
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
	select {
	case p.needCh <- need:
		m.NotifyLocal()
	default:
		// Queue full; the peer's next ack tick re-requests.
	}
	return nil
}

// queueCtrl enqueues one tiny outbound control frame for the send loop.
// Drops under pressure are safe (see ctrlCh).
func (m *Manager) queueCtrl(p *peerState, typ uint16, flags uint16, payload []byte) {
	select {
	case p.ctrlCh <- ctrlFrame{typ: typ, flags: flags, payload: payload}:
		m.NotifyLocal()
	default:
	}
}

// sendQueuedCtrl flushes read-loop control frames. It runs before bulk
// sends each round so pongs and pulls stay prompt.
func (m *Manager) sendQueuedCtrl(p *peerState, ps *peerSession) error {
	for {
		select {
		case f := <-p.ctrlCh:
			if err := ps.send(f.typ, f.flags, f.payload); err != nil {
				return err
			}
		default:
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
		select {
		case need := <-p.needCh:
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
	m.queueCtrl(p, MsgPong, 0, payload)
	return nil
}

func (m *Manager) onPong(p *peerState, payload []byte) {
	if len(payload) != 8 {
		return
	}
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
	switch code {
	case ErrSnapshotRequired:
		p.mu.Lock()
		already := p.awaiting
		p.awaiting = true
		p.mu.Unlock()
		if !already {
			m.log.Info("peer requires snapshot", slog.String("peer", p.id.String()))
			m.queueCtrl(p, MsgSnapshotRequest, 0, nil)
		}
	case ErrSchemaMismatch, ErrProtocolMismatch, ErrPeerNotAllowed:
		m.log.Warn("peer rejected session", slog.String("peer", p.id.String()), slog.String("err", msg))
		ps.close()
	default:
		m.log.Warn("peer error", slog.String("peer", p.id.String()), slog.String("err", msg))
	}
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
	m.mu.Unlock()
	for _, p := range peers {
		p.mu.Lock()
		ps := p.session
		force := p.forceAck
		p.forceAck = false
		p.mu.Unlock()
		if ps == nil {
			continue
		}
		select {
		case <-ps.done:
			continue
		default:
		}
		if err := m.sendQueuedCtrl(p, ps); err != nil {
			m.sendFailed(p, ps, err)
			continue
		}
		if err := m.sendDue(p, ps); err != nil {
			m.sendFailed(p, ps, err)
			continue
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

func (m *Manager) sessionFor(p *peerState) *peerSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.session
}

// sendDue pushes missing origin-log ranges to the peer.
func (m *Manager) sendDue(p *peerState, ps *peerSession) error {
	origins, err := m.cfg.Store.KnownOrigins()
	if err != nil {
		return err
	}
	sentBatches := 0
	for _, origin := range origins {
		if sentBatches >= m.cfg.MaxBatchMutations {
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
		p.mu.Unlock()
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
		n, err := m.sendOriginCapped(p, ps, origin, from, m.cfg.MaxBatchMutations-sentBatches)
		if err != nil {
			if errors.Is(err, state.ErrLogGone) {
				p.mu.Lock()
				p.sentErr[origin] = true
				p.mu.Unlock()
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
	_, err := m.sendOriginCapped(p, ps, origin, from, m.cfg.MaxBatchMutations)
	if err != nil && errors.Is(err, state.ErrLogGone) {
		p.mu.Lock()
		p.sentErr[origin] = true
		p.mu.Unlock()
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
	if err := ps.send(MsgBatches, 0, EncodeBatches(nil, batches)); err != nil {
		return 0, err
	}
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
	if err := ps.send(MsgAck, 0, EncodeWatermarks(nil, wms)); err != nil {
		return err
	}
	// Pull: request everything newer than our watermarks.
	for _, w := range wms {
		_ = ps.send(MsgNeed, 0, EncodeNeed(nil, Need{Origin: w.Origin, FromSeq: w.Sequence + 1}))
	}
	// Ping for RTT.
	p.mu.Lock()
	p.pingNonce++
	nonce := p.pingNonce
	p.pingAt = time.Now()
	p.mu.Unlock()
	_ = ps.send(MsgPing, 0, binaryBigEndianPutUint64(nonce))
	return nil
}

// --- snapshots ---

var errSnapshotInFlight = errors.New("snapshot already in flight")

func (m *Manager) sendSnapshot(p *peerState, ps *peerSession) {
	if !p.snapSend.CompareAndSwap(false, true) {
		return // one outbound snapshot per peer at a time
	}
	defer p.snapSend.Store(false)
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
	err := m.cfg.Store.ExportSnapshot(m.cfg.SnapshotChunkCells,
		func(manifest *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error {
			if !sentManifest {
				sentManifest = true
				if err := ps.send(MsgSnapshotManifest, 0, codec.EncodeManifest(nil, manifest)); err != nil {
					return err
				}
			}
			raw := EncodeSnapshotChunk(nil, &SnapshotChunk{Last: last, Cells: chunk})
			flags := uint16(0)
			if enc != nil && len(raw) > 4096 {
				if c := enc.EncodeAll(raw, nil); len(c) < len(raw) {
					raw = c
					flags = FlagZstd
				}
			}
			if err := ps.send(MsgSnapshotChunk, flags, raw); err != nil {
				return err
			}
			if last {
				if err := ps.send(MsgSnapshotDone, 0, nil); err != nil {
					return err
				}
				// Snapshot covers our watermarks; clear snapshot-required flags.
				p.mu.Lock()
				p.sentErr = make(map[ids.NodeID]bool)
				p.mu.Unlock()
			}
			return nil
		})
	if err != nil {
		m.log.Warn("snapshot send failed", slog.String("peer", p.id.String()), slog.String("err", err.Error()))
		m.sendFailed(p, ps, err)
	}
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
	manifest *codec.SnapshotManifest
}

func (m *Manager) onSnapshotManifest(p *peerState, _ *peerSession, payload []byte) error {
	manifest, _, err := codec.DecodeManifest(payload)
	if err != nil {
		return err
	}
	if manifest.DBID != m.cfg.DBID {
		return fmt.Errorf("snapshot db id mismatch")
	}
	if manifest.SchemaEpoch != m.cfg.SchemaEpoch || manifest.SchemaHash != m.cfg.SchemaHash {
		m.queueCtrl(p, MsgError, 0, EncodeError(nil, ErrSchemaMismatch, "snapshot schema mismatch"))
		return fmt.Errorf("snapshot schema mismatch")
	}
	st := m.snapRecvFor(p)
	st.manifest = manifest
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
	st := m.snapRecvFor(p)
	if st.manifest == nil {
		return fmt.Errorf("snapshot chunk without manifest")
	}
	var aerr error
	for i := 0; i < 5; i++ {
		aerr = m.cfg.Applier.ApplySnapshotChunk(m.ctx, st.manifest, chunk.Cells, chunk.Last)
		if aerr == nil || !state.IsConflict(aerr) {
			break
		}
		time.Sleep(time.Duration(i+1) * 5 * time.Millisecond)
	}
	if aerr != nil {
		return aerr
	}
	if chunk.Last {
		st.manifest = nil
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
	p.mu.Lock()
	p.awaiting = false
	p.forceAck = true
	p.mu.Unlock()
	m.NotifyLocal()
	return nil
}
