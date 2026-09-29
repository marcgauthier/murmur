package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/spedsql/ids"
)

const (
	// DatagramMagic is the 4-byte magic header for membership datagram envelopes ("SPED").
	DatagramMagic uint32 = 0x53504544
	// DatagramVersion is the version of the datagram envelope format.
	DatagramVersion uint8 = 1
	// DatagramTypeMembership indicates a memberlist SWIM datagram.
	DatagramTypeMembership uint8 = 1
	// DatagramHeaderLen is the length of the datagram envelope header:
	// magic(4) + version(1) + type(1) + dbid(16) + sender_node(16) = 38 bytes.
	DatagramHeaderLen = 4 + 1 + 1 + 16 + 16

	// StreamMagic is the 4-byte magic header for membership stream frames ("SPED").
	StreamMagic uint32 = 0x53504544
	// StreamVersion is the version of the stream header format.
	StreamVersion uint8 = 1
	// StreamTypeMembership indicates a memberlist stream (fallback probe / push-pull sync).
	StreamTypeMembership uint8 = 1
	// StreamHeaderLen is the length of the membership stream identification header:
	// magic(4) + version(1) + type(1) + dbid(16) + sender_node(16) = 38 bytes.
	StreamHeaderLen = 4 + 1 + 1 + 16 + 16

	// DefaultMaxPacketSize is the conservative packet budget (1,000 bytes)
	// per architecture/membership-and-transport.md section 26.2.
	DefaultMaxPacketSize = 1000

	// DefaultPacketBufferSize is the bounded receive queue size for incoming packets.
	DefaultPacketBufferSize = 1024

	// DefaultStreamBufferSize is the bounded receive queue size for incoming streams.
	DefaultStreamBufferSize = 128
)

var (
	// ErrDatagramTooLarge is returned when a packet exceeds the packet size limit.
	ErrDatagramTooLarge = errors.New("transport: datagram exceeds packet size limit")
	// ErrInvalidEnvelope is returned when a datagram has an invalid magic, version, or type.
	ErrInvalidEnvelope = errors.New("transport: invalid datagram envelope")
	// ErrDBIDMismatch is returned when a packet or stream has a conflicting DBID.
	ErrDBIDMismatch = errors.New("transport: DBID mismatch")
	// ErrPeerMismatch is returned when a packet or stream claims a node identity different from the TLS peer.
	ErrPeerMismatch = errors.New("transport: peer identity mismatch")
	// ErrTransportClosed is returned when operations are attempted on a shut down transport.
	ErrTransportClosed = errors.New("transport: memberlist transport closed")
)

// MemberlistTransportConfig configures the QUIC memberlist transport.
type MemberlistTransportConfig struct {
	// LocalNodeID is the local node identity.
	LocalNodeID ids.NodeID
	// DBID is the database cluster identity.
	DBID ids.DBID
	// Creds contains TLS certificates, trust pool, and address policy.
	Creds *Credentials
	// Listener is an optional existing shared QUIC listener.
	Listener *Listener
	// BindAddr is used to start a listener if Listener is nil.
	BindAddr string
	// AdvertiseAddr is the reachable host:port advertised to the cluster.
	AdvertiseAddr string
	// MaxPacketSize is the datagram packet size limit (defaults to 1,000 bytes).
	MaxPacketSize int
	// PacketBufferSize is the capacity of the PacketCh channel.
	PacketBufferSize int
	// StreamBufferSize is the capacity of the StreamCh channel.
	StreamBufferSize int
	// Pool is an optional shared connection pool.
	Pool *Pool
}

// MemberlistTransport implements memberlist.NodeAwareTransport and memberlist.Transport
// over authenticated QUIC DATAGRAMs and deadline-aware streams on a shared endpoint.
type MemberlistTransport struct {
	mu            sync.Mutex
	localID       ids.NodeID
	dbid          ids.DBID
	creds         *Credentials
	advertiseAddr string
	maxPacketSize int

	listener    *Listener
	ownListener bool
	pool        *Pool

	packetCh chan *memberlist.Packet
	streamCh chan net.Conn

	sessions    map[ids.NodeID]*Session
	activeConns map[*Session]struct{}
	// streamLoops tracks one membership AcceptStream consumer per
	// memberlist-owned connection. Replication-owned connections must
	// never run one: two AcceptStream consumers on one connection
	// split incoming streams at random, so a data stream the
	// membership loop wins is dropped as a header mismatch while the
	// sender's writes keep succeeding into the half-closed stream
	// (silent permanent stall). replOwned marks connections whose
	// stream consumer is replication (which forwards membership
	// streams back via HandleStream).
	streamLoops map[*Session]*streamLoopState
	replOwned   map[*Session]bool
	// datagramLoops guards the datagram consumer exactly once per
	// connection across both registration paths.
	datagramLoops map[*Session]bool

	packetsSent            atomic.Uint64
	packetsReceived        atomic.Uint64
	packetBytesSent        atomic.Uint64
	packetBytesReceived    atomic.Uint64
	packetDrops            atomic.Uint64
	streamDrops            atomic.Uint64
	datagramOversizeErrors atomic.Uint64
	datagramEnvelopeErrors atomic.Uint64
	datagramDBIDMismatches atomic.Uint64
	streamsDialed          atomic.Uint64
	streamsAccepted        atomic.Uint64
	streamDialFailures     atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// streamLoopsRunning counts live membership AcceptStream loops;
	// the adoption test asserts it to prove single-acceptance.
	streamLoopsRunning atomic.Int64

	closed bool
}

// streamLoopState controls one membership AcceptStream consumer so
// replication adoption can yield it synchronously.
type streamLoopState struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Ensure MemberlistTransport satisfies memberlist.NodeAwareTransport and memberlist.Transport.
var (
	_ memberlist.Transport          = (*MemberlistTransport)(nil)
	_ memberlist.NodeAwareTransport = (*MemberlistTransport)(nil)
)

// NewMemberlistTransport creates and initializes a new MemberlistTransport.
func NewMemberlistTransport(cfg MemberlistTransportConfig) (*MemberlistTransport, error) {
	if cfg.Creds == nil {
		return nil, fmt.Errorf("transport: missing credentials for memberlist transport")
	}
	localID, err := cfg.Creds.LocalNodeID()
	if err != nil {
		return nil, fmt.Errorf("transport: credentials local node id: %w", err)
	}
	if cfg.LocalNodeID != (ids.NodeID{}) && cfg.LocalNodeID != localID {
		return nil, fmt.Errorf("transport: configured LocalNodeID %s does not match cert %s", cfg.LocalNodeID, localID)
	}

	maxPkt := cfg.MaxPacketSize
	if maxPkt <= 0 {
		maxPkt = DefaultMaxPacketSize
	}
	pktBuf := cfg.PacketBufferSize
	if pktBuf <= 0 {
		pktBuf = DefaultPacketBufferSize
	}
	streamBuf := cfg.StreamBufferSize
	if streamBuf <= 0 {
		streamBuf = DefaultStreamBufferSize
	}

	ln := cfg.Listener
	ownLn := false
	if ln == nil && cfg.BindAddr != "" {
		var err error
		ln, err = Listen(cfg.BindAddr, cfg.Creds)
		if err != nil {
			return nil, fmt.Errorf("transport: listen for memberlist: %w", err)
		}
		ownLn = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &MemberlistTransport{
		localID:       localID,
		dbid:          cfg.DBID,
		creds:         cfg.Creds,
		advertiseAddr: cfg.AdvertiseAddr,
		maxPacketSize: maxPkt,
		listener:      ln,
		ownListener:   ownLn,
		pool:          cfg.Pool,
		packetCh:      make(chan *memberlist.Packet, pktBuf),
		streamCh:      make(chan net.Conn, streamBuf),
		sessions:      make(map[ids.NodeID]*Session),
		activeConns:   make(map[*Session]struct{}),
		streamLoops:   make(map[*Session]*streamLoopState),
		replOwned:     make(map[*Session]bool),
		datagramLoops: make(map[*Session]bool),
		ctx:           ctx,
		cancel:        cancel,
	}

	if ln != nil {
		t.wg.Add(1)
		go t.acceptLoop(ln)
	}

	return t, nil
}

// FinalAdvertiseAddr returns the concrete IP and port to advertise to the cluster.
func (t *MemberlistTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	if ip != "" && ip != "0.0.0.0" && ip != "::" {
		parsedIP := net.ParseIP(ip)
		if parsedIP == nil {
			ips, err := net.LookupIP(ip)
			if err != nil || len(ips) == 0 {
				return nil, 0, fmt.Errorf("transport: cannot resolve advertise ip %q: %w", ip, err)
			}
			parsedIP = ips[0]
		}
		if port > 0 {
			return parsedIP, port, nil
		}
	}

	if t.advertiseAddr != "" {
		host, portStr, err := net.SplitHostPort(t.advertiseAddr)
		if err == nil {
			p, err := strconv.Atoi(portStr)
			if err == nil {
				parsedIP := net.ParseIP(host)
				if parsedIP == nil {
					ips, err := net.LookupIP(host)
					if err == nil && len(ips) > 0 {
						parsedIP = ips[0]
					}
				}
				if parsedIP != nil {
					return parsedIP, p, nil
				}
			}
		}
	}

	if t.listener != nil {
		addrStr := t.listener.Addr()
		host, portStr, err := net.SplitHostPort(addrStr)
		if err == nil {
			p, err := strconv.Atoi(portStr)
			if err == nil {
				parsedIP := net.ParseIP(host)
				if parsedIP != nil && !parsedIP.IsUnspecified() {
					return parsedIP, p, nil
				}
				localIP, err := findFirstNonLoopbackIP()
				if err == nil {
					return localIP, p, nil
				}
				return net.IPv4(127, 0, 0, 1), p, nil
			}
		}
	}

	return nil, 0, fmt.Errorf("transport: unable to determine concrete advertise address")
}

func findFirstNonLoopbackIP() (net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && !ip.IsLoopback() && ip.To4() != nil {
				return ip, nil
			}
		}
	}
	return nil, fmt.Errorf("no non-loopback ip found")
}

// MemberlistTransportStats snapshots transport-level packet, stream, and datagram metrics.
type MemberlistTransportStats struct {
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

// Stats snapshots transport metrics.
func (t *MemberlistTransport) Stats() MemberlistTransportStats {
	return MemberlistTransportStats{
		PacketsSent:            t.packetsSent.Load(),
		PacketsReceived:        t.packetsReceived.Load(),
		PacketBytesSent:        t.packetBytesSent.Load(),
		PacketBytesReceived:    t.packetBytesReceived.Load(),
		PacketDrops:            t.packetDrops.Load(),
		StreamDrops:            t.streamDrops.Load(),
		DatagramOversizeErrors: t.datagramOversizeErrors.Load(),
		DatagramEnvelopeErrors: t.datagramEnvelopeErrors.Load(),
		DatagramDBIDMismatches: t.datagramDBIDMismatches.Load(),
		StreamsDialed:          t.streamsDialed.Load(),
		StreamsAccepted:        t.streamsAccepted.Load(),
		StreamDialFailures:     t.streamDialFailures.Load(),
	}
}

// PacketCh returns the channel delivering received packets.
func (t *MemberlistTransport) PacketCh() <-chan *memberlist.Packet {
	return t.packetCh
}

// StreamCh returns the channel delivering incoming streams.
func (t *MemberlistTransport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

// WriteTo sends a packet payload to addr (string).
func (t *MemberlistTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	return t.WriteToAddress(b, memberlist.Address{Addr: addr})
}

// WriteToAddress sends a packet payload to addr as a QUIC DATAGRAM.
func (t *MemberlistTransport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	if len(b) > t.maxPacketSize {
		t.datagramOversizeErrors.Add(1)
		return time.Time{}, fmt.Errorf("%w: len=%d max=%d", ErrDatagramTooLarge, len(b), t.maxPacketSize)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return time.Time{}, ErrTransportClosed
	}
	t.mu.Unlock()

	sess, err := t.getOrDialSession(t.ctx, addr)
	if err != nil {
		return time.Time{}, err
	}

	buf := make([]byte, DatagramHeaderLen+len(b))
	binary.BigEndian.PutUint32(buf[0:4], DatagramMagic)
	buf[4] = DatagramVersion
	buf[5] = DatagramTypeMembership
	copy(buf[6:22], t.dbid[:])
	copy(buf[22:38], t.localID[:])
	copy(buf[38:], b)

	sendTime := time.Now()
	if err := sess.SendDatagram(buf); err != nil {
		return time.Time{}, fmt.Errorf("transport: send datagram to %s: %w", addr.String(), err)
	}
	t.packetsSent.Add(1)
	t.packetBytesSent.Add(uint64(len(buf)))
	return sendTime, nil
}

// DialTimeout establishes a two-way stream connection to addr with timeout.
func (t *MemberlistTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return t.DialAddressTimeout(memberlist.Address{Addr: addr}, timeout)
}

// DialAddressTimeout establishes a two-way stream connection to addr with timeout.
func (t *MemberlistTransport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	t.streamsDialed.Add(1)
	ctx, cancel := context.WithTimeout(t.ctx, timeout)
	defer cancel()

	sess, err := t.getOrDialSession(ctx, addr)
	if err != nil {
		t.streamDialFailures.Add(1)
		return nil, err
	}

	stream, err := sess.OpenStream(ctx)
	if err != nil {
		t.streamDialFailures.Add(1)
		return nil, fmt.Errorf("transport: open membership stream: %w", err)
	}

	var header [StreamHeaderLen]byte
	binary.BigEndian.PutUint32(header[0:4], StreamMagic)
	header[4] = StreamVersion
	header[5] = StreamTypeMembership
	copy(header[6:22], t.dbid[:])
	copy(header[22:38], t.localID[:])

	_ = stream.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := stream.Write(header[:]); err != nil {
		_ = stream.Close()
		t.streamDialFailures.Add(1)
		return nil, fmt.Errorf("transport: write stream header: %w", err)
	}
	_ = stream.SetWriteDeadline(time.Time{})

	return &StreamConn{stream: stream, sess: sess}, nil
}

// RegisterSession adopts an externally established QUIC session (a
// replication connection) for datagrams only. It marks the connection
// replication-owned and yields any running membership AcceptStream
// consumer synchronously, so from return on exactly one consumer
// accepts streams on it: replication, which forwards membership
// streams back via HandleStream. Idempotent.
func (t *MemberlistTransport) RegisterSession(sess *Session) {
	if sess == nil {
		return
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.sessions[sess.Peer] = sess
	t.activeConns[sess] = struct{}{}
	t.replOwned[sess] = true
	st, ok := t.streamLoops[sess]
	if ok {
		delete(t.streamLoops, sess)
	}
	wantDatagram := !t.datagramLoops[sess]
	if wantDatagram {
		t.datagramLoops[sess] = true
		t.wg.Add(1)
	}
	t.mu.Unlock()
	// Yield outside the lock: the exiting loop takes none, and the wait
	// is prompt (its AcceptStream fails on cancel).
	if ok {
		st.cancel()
		<-st.done
	}
	if wantDatagram {
		go t.receiveDatagramLoop(sess)
	}
}

// RegisterMemberlistSession tracks a connection that carries only
// membership traffic (accepted on a listener replication never
// attaches), ensuring a membership AcceptStream consumer runs for it.
// Unlike RegisterSession it never yields: there is no replication
// consumer to hand over to.
func (t *MemberlistTransport) RegisterMemberlistSession(sess *Session) {
	if sess == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.registerSessionLocked(sess)
}

func (t *MemberlistTransport) registerSessionLocked(sess *Session) {
	if _, exists := t.activeConns[sess]; exists {
		return
	}
	t.sessions[sess.Peer] = sess
	t.activeConns[sess] = struct{}{}
	if !t.datagramLoops[sess] {
		t.datagramLoops[sess] = true
		t.wg.Add(1)
		go t.receiveDatagramLoop(sess)
	}
	// Memberlist-owned connections run the membership accept loop;
	// replication-owned ones never do: RegisterSession yields it and
	// replication forwards membership streams back instead.
	if !t.replOwned[sess] {
		ctx, cancel := context.WithCancel(t.ctx)
		st := &streamLoopState{cancel: cancel, done: make(chan struct{})}
		t.streamLoops[sess] = st
		t.streamLoopsRunning.Add(1)
		t.wg.Add(1)
		go t.acceptStreamLoop(sess, ctx, st)
	}
}

func (t *MemberlistTransport) getOrDialSession(ctx context.Context, addr memberlist.Address) (*Session, error) {
	var targetID ids.NodeID
	if addr.Name != "" {
		if parsed, err := ids.ParseNodeID(addr.Name); err == nil {
			targetID = parsed
		}
	}

	t.mu.Lock()
	if targetID != (ids.NodeID{}) {
		if sess, ok := t.sessions[targetID]; ok && sess.Context().Err() == nil {
			t.mu.Unlock()
			return sess, nil
		}
	}
	t.mu.Unlock()

	var sess *Session
	var err error
	if t.pool != nil {
		sess, err = t.pool.Dial(ctx, addr.Addr, t.creds, targetID, PurposeMembership)
	} else {
		sess, err = Dial(ctx, addr.Addr, t.creds, targetID)
	}
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		_ = sess.Close()
		return nil, ErrTransportClosed
	}
	t.registerSessionLocked(sess)
	return sess, nil
}

func (t *MemberlistTransport) acceptLoop(ln *Listener) {
	defer t.wg.Done()
	for {
		sess, err := ln.Accept(t.ctx)
		if err != nil {
			return
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			_ = sess.Close()
			return
		}
		t.registerSessionLocked(sess)
		t.mu.Unlock()
	}
}

func (t *MemberlistTransport) receiveDatagramLoop(sess *Session) {
	defer t.wg.Done()
	for {
		data, err := sess.ReceiveDatagram(t.ctx)
		if err != nil {
			return
		}
		t.packetsReceived.Add(1)
		t.packetBytesReceived.Add(uint64(len(data)))
		recvTime := time.Now()
		if len(data) < DatagramHeaderLen {
			t.datagramEnvelopeErrors.Add(1)
			continue
		}
		magic := binary.BigEndian.Uint32(data[0:4])
		version := data[4]
		typ := data[5]
		if magic != DatagramMagic || version != DatagramVersion || typ != DatagramTypeMembership {
			t.datagramEnvelopeErrors.Add(1)
			continue
		}
		var dbid ids.DBID
		copy(dbid[:], data[6:22])
		if dbid != t.dbid {
			t.datagramDBIDMismatches.Add(1)
			continue
		}
		var senderID ids.NodeID
		copy(senderID[:], data[22:38])
		if senderID != sess.Peer {
			t.datagramEnvelopeErrors.Add(1)
			continue
		}

		payload := make([]byte, len(data)-DatagramHeaderLen)
		copy(payload, data[DatagramHeaderLen:])

		pkt := &memberlist.Packet{
			Buf:       payload,
			From:      sess.RemoteNetAddr(),
			Timestamp: recvTime,
		}
		select {
		case t.packetCh <- pkt:
		case <-t.ctx.Done():
			return
		default:
			// Packet queue full: drop to prevent unbounded buffering under overload
			t.packetDrops.Add(1)
		}
	}
}

func (t *MemberlistTransport) acceptStreamLoop(sess *Session, ctx context.Context, st *streamLoopState) {
	// Deferred LIFO: counter first so a yielded waiter observing done
	// already sees zero; then done; then map cleanup; then wg.
	defer t.wg.Done()
	defer func() {
		t.mu.Lock()
		if cur, ok := t.streamLoops[sess]; ok && cur == st {
			delete(t.streamLoops, sess)
		}
		t.mu.Unlock()
	}()
	defer close(st.done)
	defer t.streamLoopsRunning.Add(-1)
	for {
		stream, err := sess.AcceptStream(ctx)
		if err != nil {
			return
		}
		t.streamsAccepted.Add(1)
		t.wg.Add(1)
		go t.handleIncomingStream(sess, stream)
	}
}

func (t *MemberlistTransport) handleIncomingStream(sess *Session, stream *quic.Stream) {
	defer t.wg.Done()
	var header [StreamHeaderLen]byte
	_ = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		_ = stream.Close()
		return
	}
	_ = stream.SetReadDeadline(time.Time{})

	magic := binary.BigEndian.Uint32(header[0:4])
	version := header[4]
	typ := header[5]
	if magic != StreamMagic || version != StreamVersion || typ != StreamTypeMembership {
		t.datagramEnvelopeErrors.Add(1)
		_ = stream.Close()
		return
	}
	var dbid ids.DBID
	copy(dbid[:], header[6:22])
	if dbid != t.dbid {
		t.datagramDBIDMismatches.Add(1)
		_ = stream.Close()
		return
	}
	var senderID ids.NodeID
	copy(senderID[:], header[22:38])
	if senderID != sess.Peer {
		t.datagramEnvelopeErrors.Add(1)
		_ = stream.Close()
		return
	}

	conn := &StreamConn{
		stream: stream,
		sess:   sess,
	}

	select {
	case t.streamCh <- conn:
	case <-t.ctx.Done():
		_ = conn.Close()
	default:
		// Stream queue full: close
		t.streamDrops.Add(1)
		_ = conn.Close()
	}
}

// HandleStream dispatches an externally accepted stream with a known prefix to memberlist.
func (t *MemberlistTransport) HandleStream(sess *Session, stream *quic.Stream, prefix []byte) {
	var header [StreamHeaderLen]byte
	copy(header[:], prefix)
	n := len(prefix)
	if n < StreamHeaderLen {
		_ = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(stream, header[n:]); err != nil {
			_ = stream.Close()
			return
		}
		_ = stream.SetReadDeadline(time.Time{})
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	version := header[4]
	typ := header[5]
	if magic != StreamMagic || version != StreamVersion || typ != StreamTypeMembership {
		t.datagramEnvelopeErrors.Add(1)
		_ = stream.Close()
		return
	}
	var dbid ids.DBID
	copy(dbid[:], header[6:22])
	if dbid != t.dbid {
		t.datagramDBIDMismatches.Add(1)
		_ = stream.Close()
		return
	}
	var senderID ids.NodeID
	copy(senderID[:], header[22:38])
	if senderID != sess.Peer {
		t.datagramEnvelopeErrors.Add(1)
		_ = stream.Close()
		return
	}

	conn := &StreamConn{
		stream: stream,
		sess:   sess,
	}

	select {
	case t.streamCh <- conn:
	case <-t.ctx.Done():
		_ = conn.Close()
	default:
		t.streamDrops.Add(1)
		_ = conn.Close()
	}
}

// Shutdown stops membership delivery and cleans up transport workers.
func (t *MemberlistTransport) Shutdown() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	t.mu.Unlock()

	if t.ownListener && t.listener != nil {
		_ = t.listener.Close()
	}

	t.mu.Lock()
	for sess := range t.activeConns {
		_ = sess.Close()
	}
	t.mu.Unlock()

	t.wg.Wait()
	return nil
}

// StreamConn wraps a QUIC bidirectional stream as a net.Conn for memberlist.
type StreamConn struct {
	stream *quic.Stream
	sess   *Session
}

func (c *StreamConn) Read(b []byte) (n int, err error) {
	return c.stream.Read(b)
}

func (c *StreamConn) Write(b []byte) (n int, err error) {
	return c.stream.Write(b)
}

func (c *StreamConn) Close() error {
	c.stream.CancelRead(0)
	return c.stream.Close()
}

func (c *StreamConn) LocalAddr() net.Addr {
	if c.sess != nil {
		return c.sess.LocalAddr()
	}
	return nil
}

func (c *StreamConn) RemoteAddr() net.Addr {
	if c.sess != nil {
		return c.sess.RemoteNetAddr()
	}
	return nil
}

func (c *StreamConn) SetDeadline(t time.Time) error {
	return c.stream.SetDeadline(t)
}

func (c *StreamConn) SetReadDeadline(t time.Time) error {
	return c.stream.SetReadDeadline(t)
}

func (c *StreamConn) SetWriteDeadline(t time.Time) error {
	return c.stream.SetWriteDeadline(t)
}
