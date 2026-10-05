package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/transport"
)

const (
	// MetadataMagic is the 4-byte header identifying Murmur-SQL memberlist node metadata ("SPED").
	MetadataMagic uint32 = 0x53504544
	// MetadataVersion is the current version of the node metadata format.
	MetadataVersion uint8 = 1
	// MetadataMinLen is magic(4) + version(1) + dbid(16) + capabilities(8) + endpointLen(2) = 31 bytes.
	MetadataMinLen = 31

	defaultProbeInterval  = 1000 * time.Millisecond
	defaultProbeTimeout   = 500 * time.Millisecond
	defaultGossipInterval = 200 * time.Millisecond
	defaultGossipNodes    = 3
	defaultIndirectChecks = 3

	maxBootstrapBackoff     = 30 * time.Second
	initialBootstrapBackoff = 500 * time.Millisecond
)

var (
	// ErrInvalidNodeMetadata is returned when node metadata is malformed or has invalid magic.
	ErrInvalidNodeMetadata = errors.New("replication: invalid memberlist node metadata")
	// ErrMetadataDBIDMismatch is returned when a discovered node belongs to a different DBID cluster.
	ErrMetadataDBIDMismatch = errors.New("replication: node belongs to different DBID cluster")
	// ErrServiceClosed is returned when operations are attempted on a closed membership service.
	ErrServiceClosed = errors.New("replication: membership service is closed")
)

// NodeMetadata is the compact, versioned metadata advertised in memberlist SWIM gossip.
type NodeMetadata struct {
	DBID         ids.DBID
	Capabilities uint64
	Endpoint     string // Advertised reachable host:port
	// FetchEndpoint is the reachable file-fetch host:port, empty when the
	// node does not serve object fetches. It travels as an optional
	// suffix so version-1 decoders (which ignore trailing bytes) keep
	// working: no version bump, no rolling-upgrade break.
	FetchEndpoint string
}

// EncodeNodeMetadata encodes metadata into binary format.
func EncodeNodeMetadata(m *NodeMetadata) []byte {
	endpointBytes := []byte(m.Endpoint)
	fetchBytes := []byte(m.FetchEndpoint)
	n := MetadataMinLen + len(endpointBytes)
	if len(fetchBytes) > 0 {
		n += 2 + len(fetchBytes)
	}
	buf := make([]byte, n)
	binary.BigEndian.PutUint32(buf[0:4], MetadataMagic)
	buf[4] = MetadataVersion
	copy(buf[5:21], m.DBID[:])
	binary.BigEndian.PutUint64(buf[21:29], m.Capabilities)
	binary.BigEndian.PutUint16(buf[29:31], uint16(len(endpointBytes)))
	copy(buf[31:], endpointBytes)
	if len(fetchBytes) > 0 {
		off := MetadataMinLen + len(endpointBytes)
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(fetchBytes)))
		copy(buf[off+2:], fetchBytes)
	}
	return buf
}

// DecodeNodeMetadata decodes and validates binary node metadata.
func DecodeNodeMetadata(b []byte, expectedDBID ids.DBID) (*NodeMetadata, error) {
	if len(b) < MetadataMinLen {
		return nil, ErrInvalidNodeMetadata
	}
	magic := binary.BigEndian.Uint32(b[0:4])
	version := b[4]
	if magic != MetadataMagic || version != MetadataVersion {
		return nil, ErrInvalidNodeMetadata
	}
	var dbid ids.DBID
	copy(dbid[:], b[5:21])
	if expectedDBID != (ids.DBID{}) && dbid != expectedDBID {
		return nil, fmt.Errorf("%w: remote %s != local %s", ErrMetadataDBIDMismatch, dbid, expectedDBID)
	}
	caps := binary.BigEndian.Uint64(b[21:29])
	epLen := binary.BigEndian.Uint16(b[29:31])
	if len(b) < MetadataMinLen+int(epLen) {
		return nil, ErrInvalidNodeMetadata
	}
	endpoint := string(b[31 : 31+epLen])
	// Optional fetch-endpoint suffix: fetchLen(2) + bytes. Parsed only
	// when it fits; anything beyond is ignored for forward compatibility,
	// and a truncated suffix degrades to "not serving" rather than failing
	// the whole (prefix-valid) record.
	var fetchEndpoint string
	if rest := b[MetadataMinLen+int(epLen):]; len(rest) >= 2 {
		fetchLen := int(binary.BigEndian.Uint16(rest[:2]))
		if fetchLen <= len(rest)-2 {
			fetchEndpoint = string(rest[2 : 2+fetchLen])
		}
	}
	return &NodeMetadata{
		DBID:          dbid,
		Capabilities:  caps,
		Endpoint:      endpoint,
		FetchEndpoint: fetchEndpoint,
	}, nil
}

// MembershipEventType describes a peer lifecycle transition in SWIM.
type MembershipEventType int

const (
	// EventPeerJoined is emitted when a new peer joins the SWIM cluster.
	EventPeerJoined MembershipEventType = iota
	// EventPeerUpdated is emitted when a peer's address/metadata is updated.
	EventPeerUpdated
	// EventPeerLeft is emitted when a peer leaves or is declared dead.
	EventPeerLeft
)

// MembershipEvent is emitted upon memberlist cluster state changes.
type MembershipEvent struct {
	Type     MembershipEventType
	NodeID   ids.NodeID
	Addr     string
	Endpoint string
	Meta     NodeMetadata
}

// MembershipConfig configures HashiCorp memberlist SWIM discovery and failure detection.
type MembershipConfig struct {
	// Enabled explicitly starts the SWIM membership service even if Bootstrap is empty (e.g. for seed nodes).
	Enabled        bool
	Bootstrap      []string
	AdvertiseAddr  string
	ProbeInterval  time.Duration
	ProbeTimeout   time.Duration
	GossipInterval time.Duration
	GossipNodes    int
	IndirectChecks int
}

func (c *MembershipConfig) withDefaults() {
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = defaultProbeInterval
	}
	if c.ProbeTimeout <= 0 {
		c.ProbeTimeout = defaultProbeTimeout
	}
	if c.GossipInterval <= 0 {
		c.GossipInterval = defaultGossipInterval
	}
	if c.GossipNodes <= 0 {
		c.GossipNodes = defaultGossipNodes
	}
	if c.IndirectChecks <= 0 {
		c.IndirectChecks = defaultIndirectChecks
	}
}

// MembershipHandler is called on membership changes.
type MembershipHandler interface {
	OnPeerDiscovered(nodeID ids.NodeID, endpoint string, meta NodeMetadata)
	OnPeerUpdated(nodeID ids.NodeID, endpoint string, meta NodeMetadata)
	OnPeerLeft(nodeID ids.NodeID)
}

// MembershipStats snapshots SWIM membership and transport counters/gauges.
type MembershipStats struct {
	NumMembers         int
	NumAlive           int
	NumSuspect         int
	NumDead            int
	ProbesCompleted    uint64
	ProbeFailures      uint64
	Refutations        uint64
	Suspicions         uint64
	BootstrapAttempts  uint64
	BootstrapSuccesses uint64
	BootstrapFailures  uint64
	EventDrops         uint64
	ReconciledJoins    uint64
	ReconciledLeaves   uint64
	ReconciledUpdates  uint64

	PacketsSent            uint64
	PacketsReceived        uint64
	PacketBytesSent        uint64
	PacketBytesReceived    uint64
	PacketDrops            uint64
	StreamDrops            uint64
	DatagramOversizeErrors uint64
	DatagramEnvelopeErrors uint64
	DatagramDBIDMismatches uint64
	StreamsDialed          uint64
	StreamsAccepted        uint64
	StreamDialFailures     uint64
}

// MembershipService manages HashiCorp memberlist SWIM discovery, failure detection,
// suspicion/refutation, partial seed bootstrap/retry, and event reconciliation.
type MembershipService struct {
	mu        sync.RWMutex
	localID   ids.NodeID
	dbid      ids.DBID
	cfg       MembershipConfig
	transport *transport.MemberlistTransport
	ml        *memberlist.Memberlist
	handler   MembershipHandler
	log       *slog.Logger

	aliveMembers map[ids.NodeID]NodeMetadata
	eventCh      chan MembershipEvent

	// fetchEndpoint is the locally served file-fetch host:port advertised
	// in SWIM metadata (empty when not serving). It is set after the
	// fetch server binds, so it cannot come from static config: fetch
	// endpoints commonly listen on ephemeral ports.
	fetchEndpoint atomic.Value // string

	probesCompleted    atomic.Uint64
	probeFailures      atomic.Uint64
	refutations        atomic.Uint64
	suspicions         atomic.Uint64
	bootstrapAttempts  atomic.Uint64
	bootstrapSuccesses atomic.Uint64
	bootstrapFailures  atomic.Uint64
	eventDrops         atomic.Uint64
	reconciledJoins    atomic.Uint64
	reconciledLeaves   atomic.Uint64
	reconciledUpdates  atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closed bool
}

// NewMembershipService creates and starts the SWIM membership service over QUIC.
func NewMembershipService(
	localID ids.NodeID,
	dbid ids.DBID,
	cfg MembershipConfig,
	tr *transport.MemberlistTransport,
	handler MembershipHandler,
	logger *slog.Logger,
) (*MembershipService, error) {
	if tr == nil {
		return nil, fmt.Errorf("replication: nil transport for membership service")
	}
	cfg.withDefaults()
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &MembershipService{
		localID:      localID,
		dbid:         dbid,
		cfg:          cfg,
		transport:    tr,
		handler:      handler,
		log:          logger.With(slog.String("component", "membership"), slog.String("local_node", localID.String())),
		aliveMembers: make(map[ids.NodeID]NodeMetadata),
		eventCh:      make(chan MembershipEvent, 256),
		ctx:          ctx,
		cancel:       cancel,
	}

	mlConfig := memberlist.DefaultWANConfig()
	mlConfig.Name = localID.String()
	mlConfig.Transport = tr
	mlConfig.ProbeInterval = cfg.ProbeInterval
	mlConfig.ProbeTimeout = cfg.ProbeTimeout
	mlConfig.GossipInterval = cfg.GossipInterval
	mlConfig.GossipNodes = cfg.GossipNodes
	mlConfig.IndirectChecks = cfg.IndirectChecks
	mlConfig.LogOutput = io.Discard
	mlConfig.Delegate = s
	mlConfig.Events = s
	mlConfig.Ping = s
	mlConfig.Alive = s
	mlConfig.Conflict = s

	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("replication: create memberlist: %w", err)
	}
	s.ml = ml

	s.wg.Add(2)
	go s.eventLoop()
	go s.bootstrapLoop()

	return s, nil
}

// NodeMeta implements memberlist.Delegate.
func (s *MembershipService) NodeMeta(limit int) []byte {
	advertise := s.cfg.AdvertiseAddr
	if advertise == "" {
		ip, port, err := s.transport.FinalAdvertiseAddr("", 0)
		if err == nil {
			advertise = net.JoinHostPort(ip.String(), fmt.Sprintf("%d", port))
		}
	}
	meta := &NodeMetadata{
		DBID:          s.dbid,
		Capabilities:  KnownCaps,
		Endpoint:      advertise,
		FetchEndpoint: s.advertisedFetchEndpoint(),
	}
	enc := EncodeNodeMetadata(meta)
	if len(enc) > limit {
		s.log.Warn("node metadata exceeds delegate limit", slog.Int("len", len(enc)), slog.Int("limit", limit))
		return enc[:limit]
	}
	return enc
}

// advertisedFetchEndpoint returns the locally served file-fetch endpoint,
// or "" when this node does not serve fetches.
func (s *MembershipService) advertisedFetchEndpoint() string {
	if v := s.fetchEndpoint.Load(); v != nil {
		if addr, ok := v.(string); ok {
			return addr
		}
	}
	return ""
}

// SetFetchEndpoint advertises (or clears, when empty) the locally served
// file-fetch endpoint and re-broadcasts local node state so the new
// metadata gossips to the cluster. It is called once the fetch server has
// bound its (possibly ephemeral) port. With no other members the update
// is a no-op and the next join carries the endpoint; failures only delay
// propagation and are logged.
func (s *MembershipService) SetFetchEndpoint(addr string) {
	s.fetchEndpoint.Store(addr)
	s.mu.RLock()
	closed := s.closed
	ml := s.ml
	s.mu.RUnlock()
	if closed || ml == nil {
		return
	}
	if err := ml.UpdateNode(2 * time.Second); err != nil {
		s.log.Debug("fetch endpoint re-advertisement pending", slog.Any("err", err))
	}
}

// AliveMembers returns the current alive-member view (excluding self),
// including each member's advertised fetch endpoint ("" when not serving).
func (s *MembershipService) AliveMembers() map[ids.NodeID]NodeMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[ids.NodeID]NodeMetadata, len(s.aliveMembers))
	for id, meta := range s.aliveMembers {
		out[id] = meta
	}
	return out
}

// NotifyMsg implements memberlist.Delegate.
func (s *MembershipService) NotifyMsg(b []byte) {}

// GetBroadcasts implements memberlist.Delegate.
func (s *MembershipService) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

// LocalState implements memberlist.Delegate.
func (s *MembershipService) LocalState(join bool) []byte {
	return s.NodeMeta(512)
}

// MergeRemoteState implements memberlist.Delegate.
func (s *MembershipService) MergeRemoteState(buf []byte, join bool) {
	_, _ = DecodeNodeMetadata(buf, s.dbid)
}

// AckPayload implements memberlist.PingDelegate.
func (s *MembershipService) AckPayload() []byte {
	return nil
}

// NotifyPingComplete implements memberlist.PingDelegate.
func (s *MembershipService) NotifyPingComplete(other *memberlist.Node, rtt time.Duration, payload []byte) {
	s.probesCompleted.Add(1)
}

// NotifyAlive implements memberlist.AliveDelegate.
func (s *MembershipService) NotifyAlive(peer *memberlist.Node) error {
	if peer.Name == s.localID.String() {
		s.refutations.Add(1)
	}
	return nil
}

// NotifyConflict implements memberlist.ConflictDelegate.
func (s *MembershipService) NotifyConflict(existing, other *memberlist.Node) {
	s.suspicions.Add(1)
}

// NotifyJoin implements memberlist.EventDelegate.
func (s *MembershipService) NotifyJoin(node *memberlist.Node) {
	s.enqueueNodeEvent(EventPeerJoined, node)
}

// NotifyLeave implements memberlist.EventDelegate.
func (s *MembershipService) NotifyLeave(node *memberlist.Node) {
	s.probeFailures.Add(1)
	s.enqueueNodeEvent(EventPeerLeft, node)
}

// NotifyUpdate implements memberlist.EventDelegate.
func (s *MembershipService) NotifyUpdate(node *memberlist.Node) {
	s.enqueueNodeEvent(EventPeerUpdated, node)
}

func (s *MembershipService) enqueueNodeEvent(typ MembershipEventType, node *memberlist.Node) {
	nodeID, err := ids.ParseNodeID(node.Name)
	if err != nil {
		return
	}
	if nodeID == s.localID {
		return
	}

	var meta NodeMetadata
	if len(node.Meta) > 0 {
		m, err := DecodeNodeMetadata(node.Meta, s.dbid)
		if err != nil {
			// Ignore node from different DBID
			return
		}
		meta = *m
	}

	endpoint := meta.Endpoint
	if endpoint == "" {
		endpoint = net.JoinHostPort(node.Addr.String(), fmt.Sprintf("%d", node.Port))
	}

	evt := MembershipEvent{
		Type:     typ,
		NodeID:   nodeID,
		Addr:     node.Address(),
		Endpoint: endpoint,
		Meta:     meta,
	}

	select {
	case s.eventCh <- evt:
	default:
		// Queue full: drop event and rely on periodic reconciliation
		s.eventDrops.Add(1)
		s.log.Warn("membership event queue full, event dropped", slog.String("peer", nodeID.String()))
	}
}

func (s *MembershipService) eventLoop() {
	defer s.wg.Done()
	reconcileTicker := time.NewTicker(5 * time.Second)
	defer reconcileTicker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case evt := <-s.eventCh:
			s.handleEvent(evt)
		case <-reconcileTicker.C:
			s.reconcile()
		}
	}
}

func (s *MembershipService) handleEvent(evt MembershipEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch evt.Type {
	case EventPeerJoined:
		s.aliveMembers[evt.NodeID] = evt.Meta
		if s.handler != nil {
			s.handler.OnPeerDiscovered(evt.NodeID, evt.Endpoint, evt.Meta)
		}
	case EventPeerUpdated:
		s.aliveMembers[evt.NodeID] = evt.Meta
		if s.handler != nil {
			s.handler.OnPeerUpdated(evt.NodeID, evt.Endpoint, evt.Meta)
		}
	case EventPeerLeft:
		delete(s.aliveMembers, evt.NodeID)
		if s.handler != nil {
			s.handler.OnPeerLeft(evt.NodeID)
		}
	}
}

func (s *MembershipService) reconcile() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ml == nil {
		return
	}

	current := make(map[ids.NodeID]*memberlist.Node)
	for _, m := range s.ml.Members() {
		id, err := ids.ParseNodeID(m.Name)
		if err != nil || id == s.localID {
			continue
		}
		current[id] = m
	}

	// Detect new / updated members missed due to dropped queue
	for id, m := range current {
		meta, err := DecodeNodeMetadata(m.Meta, s.dbid)
		if err != nil {
			continue
		}
		existing, ok := s.aliveMembers[id]
		if !ok {
			s.aliveMembers[id] = *meta
			s.reconciledJoins.Add(1)
			if s.handler != nil {
				endpoint := meta.Endpoint
				if endpoint == "" {
					endpoint = net.JoinHostPort(m.Addr.String(), fmt.Sprintf("%d", m.Port))
				}
				s.handler.OnPeerDiscovered(id, endpoint, *meta)
			}
		} else if existing.Endpoint != meta.Endpoint || existing.Capabilities != meta.Capabilities || existing.FetchEndpoint != meta.FetchEndpoint {
			s.aliveMembers[id] = *meta
			s.reconciledUpdates.Add(1)
			if s.handler != nil {
				s.handler.OnPeerUpdated(id, meta.Endpoint, *meta)
			}
		}
	}

	// Detect left members
	for id := range s.aliveMembers {
		if _, ok := current[id]; !ok {
			delete(s.aliveMembers, id)
			s.reconciledLeaves.Add(1)
			if s.handler != nil {
				s.handler.OnPeerLeft(id)
			}
		}
	}
}

func (s *MembershipService) bootstrapLoop() {
	defer s.wg.Done()
	if len(s.cfg.Bootstrap) == 0 {
		return
	}

	backoff := initialBootstrapBackoff
	for {
		if s.ml.NumMembers() > 1 {
			// Already peered, wait longer before re-checking
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}

		s.bootstrapAttempts.Add(1)
		joined, err := s.ml.Join(s.cfg.Bootstrap)
		if err == nil && joined > 0 {
			s.bootstrapSuccesses.Add(1)
			s.log.Debug("successfully joined bootstrap seeds", slog.Int("joined", joined))
			backoff = initialBootstrapBackoff
		} else {
			s.bootstrapFailures.Add(1)
			s.log.Debug("bootstrap join attempt failed, will retry", slog.Any("err", err))
			// Jittered exponential backoff
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			sleepDur := backoff + jitter
			backoff *= 2
			if backoff > maxBootstrapBackoff {
				backoff = maxBootstrapBackoff
			}

			select {
			case <-s.ctx.Done():
				return
			case <-time.After(sleepDur):
			}
		}
	}
}

// Stats returns a point-in-time snapshot of SWIM membership and transport counters/gauges.
func (s *MembershipService) Stats() MembershipStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var trStats transport.MemberlistTransportStats
	if s.transport != nil {
		trStats = s.transport.Stats()
	}

	numMembers := 0
	numAlive := len(s.aliveMembers)
	numSuspect := 0
	numDead := 0
	if s.ml != nil {
		numMembers = s.ml.NumMembers()
	}

	return MembershipStats{
		NumMembers:             numMembers,
		NumAlive:               numAlive,
		NumSuspect:             numSuspect,
		NumDead:                numDead,
		ProbesCompleted:        s.probesCompleted.Load(),
		ProbeFailures:          s.probeFailures.Load(),
		Refutations:            s.refutations.Load(),
		Suspicions:             s.suspicions.Load(),
		BootstrapAttempts:      s.bootstrapAttempts.Load(),
		BootstrapSuccesses:     s.bootstrapSuccesses.Load(),
		BootstrapFailures:      s.bootstrapFailures.Load(),
		EventDrops:             s.eventDrops.Load(),
		ReconciledJoins:        s.reconciledJoins.Load(),
		ReconciledLeaves:       s.reconciledLeaves.Load(),
		ReconciledUpdates:      s.reconciledUpdates.Load(),
		PacketsSent:            trStats.PacketsSent,
		PacketsReceived:        trStats.PacketsReceived,
		PacketBytesSent:        trStats.PacketBytesSent,
		PacketBytesReceived:    trStats.PacketBytesReceived,
		PacketDrops:            trStats.PacketDrops,
		StreamDrops:            trStats.StreamDrops,
		DatagramOversizeErrors: trStats.DatagramOversizeErrors,
		DatagramEnvelopeErrors: trStats.DatagramEnvelopeErrors,
		DatagramDBIDMismatches: trStats.DatagramDBIDMismatches,
		StreamsDialed:          trStats.StreamsDialed,
		StreamsAccepted:        trStats.StreamsAccepted,
		StreamDialFailures:     trStats.StreamDialFailures,
	}
}

// Join attempts to join the specified seed addresses.
func (s *MembershipService) Join(addrs []string) (int, error) {
	s.mu.RLock()
	if s.closed || s.ml == nil {
		s.mu.RUnlock()
		return 0, ErrServiceClosed
	}
	s.mu.RUnlock()
	return s.ml.Join(addrs)
}

// NumMembers returns the count of currently discovered active members.
func (s *MembershipService) NumMembers() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ml == nil {
		return 0
	}
	return s.ml.NumMembers()
}

// Members returns the list of active members in the cluster.
func (s *MembershipService) Members() map[ids.NodeID]NodeMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[ids.NodeID]NodeMetadata, len(s.aliveMembers))
	for k, v := range s.aliveMembers {
		res[k] = v
	}
	return res
}

// MemberState returns the SWIM state string ("alive", "unknown") for the given node ID.
func (s *MembershipService) MemberState(id ids.NodeID) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.aliveMembers[id]; ok {
		return "alive"
	}
	return "unknown"
}

// Leave announces graceful leave to the cluster.
func (s *MembershipService) Leave(timeout time.Duration) error {
	s.mu.Lock()
	if s.closed || s.ml == nil {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	return s.ml.Leave(timeout)
}

// Transport returns the underlying QUIC memberlist transport.
func (s *MembershipService) Transport() *transport.MemberlistTransport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.transport
}

// Shutdown stops the membership service and terminates background workers.
func (s *MembershipService) Shutdown() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	s.mu.Unlock()

	var err error
	if s.ml != nil {
		err = s.ml.Shutdown()
	}
	if s.transport != nil {
		_ = s.transport.Shutdown()
	}

	s.wg.Wait()
	return err
}
