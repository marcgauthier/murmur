package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/spedsql/ids"
)

// Credentials carries this node's TLS identity and trust policy.
type Credentials struct {
	// Cert is the node certificate (with NodeID URI SAN).
	Cert tls.Certificate
	// CAPool trusts cluster peers.
	CAPool *x509.CertPool
	// AllowedPeers, when non-empty, restricts which NodeIDs may connect.
	AllowedPeers map[ids.NodeID]bool
	// AddressPolicy, when non-nil, additionally restricts peer network
	// addresses for inbound and outbound connections. Nil preserves the
	// certificate/NodeID-only policy.
	AddressPolicy *AddressPolicy
}

// CredentialsFromPEM builds Credentials from PEM material.
func CredentialsFromPEM(certPEM, keyPEM, caPEM []byte, allowed []ids.NodeID) (*Credentials, error) {
	cert, err := ParseNodeCert(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	pool, err := ParseCAPool(caPEM)
	if err != nil {
		return nil, err
	}
	c := &Credentials{Cert: cert, CAPool: pool}
	if len(allowed) > 0 {
		c.AllowedPeers = make(map[ids.NodeID]bool, len(allowed))
		for _, id := range allowed {
			c.AllowedPeers[id] = true
		}
	}
	return c, nil
}

// LocalNodeID returns the NodeID bound to our certificate.
func (c *Credentials) LocalNodeID() (ids.NodeID, error) {
	return NodeIDFromCert(c.Cert.Leaf)
}

// verifyPeer checks the presented chain against the cluster CA, extracts the
// NodeID SAN, enforces the allow-list and (for dialers) the expected peer.
func (c *Credentials) verifyPeer(expect ids.NodeID, hasExpect bool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("transport: peer presented no certificate")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("transport: parse peer cert: %w", err)
		}
		intermediates := x509.NewCertPool()
		for _, der := range rawCerts[1:] {
			if ic, err := x509.ParseCertificate(der); err == nil {
				intermediates.AddCert(ic)
			}
		}
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots:         c.CAPool,
			Intermediates: intermediates,
			CurrentTime:   time.Now(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		}); err != nil {
			return fmt.Errorf("transport: peer cert verification: %w", err)
		}
		id, err := NodeIDFromCert(leaf)
		if err != nil {
			return err
		}
		if len(c.AllowedPeers) > 0 && !c.AllowedPeers[id] {
			return fmt.Errorf("transport: peer %s not allowed", id)
		}
		if hasExpect && id != expect {
			return fmt.Errorf("transport: peer %s != expected %s", id, expect)
		}
		return nil
	}
}

// ServerTLSConfig returns mTLS config for the listener.
func (c *Credentials) ServerTLSConfig() *tls.Config {
	return &tls.Config{
		Certificates:          []tls.Certificate{c.Cert},
		ClientAuth:            tls.RequireAnyClientCert,
		VerifyPeerCertificate: c.verifyPeer(ids.NodeID{}, false),
		NextProtos:            []string{NextProto},
		MinVersion:            tls.VersionTLS13,
	}
}

// ClientTLSConfig returns mTLS config for dialing expect.
func (c *Credentials) ClientTLSConfig(expect ids.NodeID) *tls.Config {
	return &tls.Config{
		Certificates:          []tls.Certificate{c.Cert},
		InsecureSkipVerify:    true, // verified by VerifyPeerCertificate
		VerifyPeerCertificate: c.verifyPeer(expect, expect != ids.NodeID{}),
		NextProtos:            []string{NextProto},
		MinVersion:            tls.VersionTLS13,
		ServerName:            "replicateddb",
	}
}

// Session is one authenticated QUIC connection to a peer.
type Session struct {
	conn   *quic.Conn
	policy *AddressPolicy
	Local  ids.NodeID
	Peer   ids.NodeID
}

func newSession(conn *quic.Conn, local ids.NodeID, policy *AddressPolicy) (*Session, error) {
	st := conn.ConnectionState().TLS
	if len(st.PeerCertificates) == 0 {
		return nil, fmt.Errorf("transport: no peer certificate")
	}
	peer, err := NodeIDFromCert(st.PeerCertificates[0])
	if err != nil {
		return nil, err
	}
	return &Session{conn: conn, policy: policy, Local: local, Peer: peer}, nil
}

// VerifyRemoteAddr rechecks the session's current remote address against the
// admission policy, catching QUIC path migration (or NAT rebinding) onto a
// denied address. A nil policy always passes.
func (s *Session) VerifyRemoteAddr() error {
	if s.policy == nil || s.policy.AllowsAddr(s.conn.RemoteAddr()) {
		return nil
	}
	return fmt.Errorf("transport: remote %s: %w", redactAddrOf(s.conn.RemoteAddr()), ErrAddressNotAllowed)
}

// OpenStream opens a bidirectional stream. The remote address is rechecked
// first so a session that migrated onto a denied address is closed instead
// of serving more application traffic.
func (s *Session) OpenStream(ctx context.Context) (*quic.Stream, error) {
	if err := s.VerifyRemoteAddr(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s.conn.OpenStreamSync(ctx)
}

// AcceptStream accepts a bidirectional stream. See OpenStream for the
// remote-address recheck.
func (s *Session) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	if err := s.VerifyRemoteAddr(); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s.conn.AcceptStream(ctx)
}

// Close closes the connection.
func (s *Session) Close() error { return s.conn.CloseWithError(0, "closed") }

// Context is done when the connection closes.
func (s *Session) Context() context.Context { return s.conn.Context() }

// RemoteAddr returns the peer's network address string.
func (s *Session) RemoteAddr() string {
	if a := s.conn.RemoteAddr(); a != nil {
		return a.String()
	}
	return ""
}

// LocalAddr returns the local network address.
func (s *Session) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

// RemoteNetAddr returns the peer's net.Addr.
func (s *Session) RemoteNetAddr() net.Addr {
	return s.conn.RemoteAddr()
}

// SendDatagram transmits a single DATAGRAM frame over the session.
func (s *Session) SendDatagram(p []byte) error {
	return s.conn.SendDatagram(p)
}

// ReceiveDatagram reads the next DATAGRAM frame from the session.
func (s *Session) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return s.conn.ReceiveDatagram(ctx)
}

// Listener accepts mTLS QUIC connections.
type Listener struct {
	ln    *quic.Listener
	creds *Credentials
	local ids.NodeID
}

// Listen starts the QUIC listener.
func Listen(addr string, creds *Credentials) (*Listener, error) {
	local, err := creds.LocalNodeID()
	if err != nil {
		return nil, err
	}
	ln, err := quic.ListenAddr(addr, creds.ServerTLSConfig(), &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 10 * time.Second,
		EnableDatagrams: true,
	})
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", addr, err)
	}
	return &Listener{ln: ln, creds: creds, local: local}, nil
}

// Addr returns the listener address.
func (l *Listener) Addr() string { return l.ln.Addr().String() }

// Accept waits for the next authenticated session.
func (l *Listener) Accept(ctx context.Context) (*Session, error) {
	conn, err := l.ln.Accept(ctx)
	if err != nil {
		return nil, err
	}
	reject := func(err error) (*Session, error) {
		_ = conn.CloseWithError(1, err.Error())
		return nil, err
	}
	// Cheap address admission before certificate parsing and any
	// application work. (The QUIC/TLS handshake itself already completed
	// inside ln.Accept; quic-go offers no pre-handshake filter.)
	if pol := l.creds.AddressPolicy; pol != nil && !pol.AllowsAddr(conn.RemoteAddr()) {
		return reject(fmt.Errorf("transport: remote %s: %w", redactAddrOf(conn.RemoteAddr()), ErrAddressNotAllowed))
	}
	sess, err := newSession(conn, l.local, l.creds.AddressPolicy)
	if err != nil {
		return reject(err)
	}
	// Defense in depth: re-verify the established peer identity. Handshake
	// failures normally surface as Accept errors already.
	raw := make([][]byte, 0, len(conn.ConnectionState().TLS.PeerCertificates))
	for _, c := range conn.ConnectionState().TLS.PeerCertificates {
		raw = append(raw, c.Raw)
	}
	if err := l.creds.verifyPeer(ids.NodeID{}, false)(raw, nil); err != nil {
		return reject(err)
	}
	return sess, nil
}

// Close stops the listener.
func (l *Listener) Close() error { return l.ln.Close() }

// Dial connects to a peer and authenticates it as expect. With an address
// policy configured, the destination is resolved and filtered first: literal
// IPs are checked directly, hostnames resolve on every dial (covering DNS
// changes and reconnects) with mixed allowed/denied answers filtered to the
// allowed subset, and the dial fails before any connection work when nothing
// remains. Identity verification is unchanged: the peer certificate must
// still match expect.
func Dial(ctx context.Context, addr string, creds *Credentials, expect ids.NodeID) (*Session, error) {
	if creds.AddressPolicy != nil {
		addrs, err := creds.AddressPolicy.DialAddrs(ctx, addr)
		if err != nil {
			return nil, err
		}
		var lastErr error
		for _, a := range addrs {
			sess, err := dialOne(ctx, a, creds, expect)
			if err == nil {
				return sess, nil
			}
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("transport: dial %q: %w", addr, ErrAddressNotAllowed)
		}
		return nil, lastErr
	}
	return dialOne(ctx, addr, creds, expect)
}

func dialOne(ctx context.Context, addr string, creds *Credentials, expect ids.NodeID) (*Session, error) {
	local, err := creds.LocalNodeID()
	if err != nil {
		return nil, err
	}
	conn, err := quic.DialAddr(ctx, addr, creds.ClientTLSConfig(expect), &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 10 * time.Second,
		EnableDatagrams: true,
	})
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s: %w", addr, err)
	}
	sess, err := newSession(conn, local, creds.AddressPolicy)
	if err != nil {
		_ = conn.CloseWithError(1, err.Error())
		return nil, err
	}
	if expect != (ids.NodeID{}) && sess.Peer != expect {
		_ = sess.Close()
		return nil, fmt.Errorf("transport: peer %s != expected %s", sess.Peer, expect)
	}
	return sess, nil
}
