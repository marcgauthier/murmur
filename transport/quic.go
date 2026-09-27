package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/nomadsql/replicateddb/ids"
)

// Credentials carries this node's TLS identity and trust policy.
type Credentials struct {
	// Cert is the node certificate (with NodeID URI SAN).
	Cert tls.Certificate
	// CAPool trusts cluster peers.
	CAPool *x509.CertPool
	// AllowedPeers, when non-empty, restricts which NodeIDs may connect.
	AllowedPeers map[ids.NodeID]bool
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
		VerifyPeerCertificate: c.verifyPeer(expect, true),
		NextProtos:            []string{NextProto},
		MinVersion:            tls.VersionTLS13,
		ServerName:            "replicateddb",
	}
}

// Session is one authenticated QUIC connection to a peer.
type Session struct {
	conn  *quic.Conn
	Local ids.NodeID
	Peer  ids.NodeID
}

func newSession(conn *quic.Conn, local ids.NodeID) (*Session, error) {
	st := conn.ConnectionState().TLS
	if len(st.PeerCertificates) == 0 {
		return nil, fmt.Errorf("transport: no peer certificate")
	}
	peer, err := NodeIDFromCert(st.PeerCertificates[0])
	if err != nil {
		return nil, err
	}
	return &Session{conn: conn, Local: local, Peer: peer}, nil
}

// OpenStream opens a bidirectional stream.
func (s *Session) OpenStream(ctx context.Context) (*quic.Stream, error) {
	return s.conn.OpenStreamSync(ctx)
}

// AcceptStream accepts a bidirectional stream.
func (s *Session) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	return s.conn.AcceptStream(ctx)
}

// Close closes the connection.
func (s *Session) Close() error { return s.conn.CloseWithError(0, "closed") }

// Context is done when the connection closes.
func (s *Session) Context() context.Context { return s.conn.Context() }

// RemoteAddr returns the peer's network address.
func (s *Session) RemoteAddr() string {
	if a := s.conn.RemoteAddr(); a != nil {
		return a.String()
	}
	return ""
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
	sess, err := newSession(conn, l.local)
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

// Dial connects to a peer and authenticates it as expect.
func Dial(ctx context.Context, addr string, creds *Credentials, expect ids.NodeID) (*Session, error) {
	local, err := creds.LocalNodeID()
	if err != nil {
		return nil, err
	}
	conn, err := quic.DialAddr(ctx, addr, creds.ClientTLSConfig(expect), &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s: %w", addr, err)
	}
	sess, err := newSession(conn, local)
	if err != nil {
		_ = conn.CloseWithError(1, err.Error())
		return nil, err
	}
	if sess.Peer != expect {
		_ = sess.Close()
		return nil, fmt.Errorf("transport: peer %s != expected %s", sess.Peer, expect)
	}
	return sess, nil
}
