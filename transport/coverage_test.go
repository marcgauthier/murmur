package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur/ids"
)

type customAddr struct{ s string }

func (a customAddr) Network() string { return "custom" }
func (a customAddr) String() string  { return a.s }

// TestAddressPolicyEntries pins the allow-list accessor and its copy semantics.
func TestAddressPolicyEntries(t *testing.T) {
	var nilPolicy *AddressPolicy
	if nilPolicy.Entries() != nil {
		t.Fatal("nil policy Entries != nil")
	}
	p, err := ParseAddressPolicy([]string{"192.0.2.0/24", "2001:db8::1"})
	if err != nil {
		t.Fatal(err)
	}
	got := p.Entries()
	if len(got) != 2 || got[0] != "192.0.2.0/24" || got[1] != "2001:db8::1" {
		t.Fatalf("Entries = %v", got)
	}
	got[0] = "mutated"
	if p.Entries()[0] != "192.0.2.0/24" {
		t.Fatal("Entries does not return a copy")
	}
}

// TestAllowsAddrVariants covers every AllowsAddr input shape, including the
// fail-closed default branch for unknown address types.
func TestAllowsAddrVariants(t *testing.T) {
	p, err := ParseAddressPolicy([]string{"192.0.2.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	var nilPolicy *AddressPolicy
	if !nilPolicy.AllowsAddr(nil) {
		t.Fatal("nil policy denies")
	}
	if p.AllowsAddr(nil) {
		t.Fatal("nil addr allowed")
	}
	allowed := net.ParseIP("192.0.2.7")
	denied := net.ParseIP("198.51.100.7")
	for _, tc := range []struct {
		name  string
		addr  net.Addr
		allow bool
	}{
		{"udp-allow", &net.UDPAddr{IP: allowed}, true},
		{"udp-deny", &net.UDPAddr{IP: denied}, false},
		{"tcp-allow", &net.TCPAddr{IP: allowed}, true},
		{"tcp-deny", &net.TCPAddr{IP: denied}, false},
		{"ip-allow", &net.IPAddr{IP: allowed}, true},
		{"ip-deny", &net.IPAddr{IP: denied}, false},
		{"custom-hostport-allow", customAddr{"192.0.2.9:443"}, true},
		{"custom-hostport-deny", customAddr{"198.51.100.9:443"}, false},
		{"custom-bare-ip", customAddr{"192.0.2.9"}, true},
		{"custom-zone", customAddr{"[192.0.2.9%eth0]:443"}, true},
		{"custom-garbage", customAddr{"not-an-address"}, false},
		{"custom-hostname", customAddr{"example.com:443"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.AllowsAddr(tc.addr); got != tc.allow {
				t.Fatalf("AllowsAddr(%v) = %v, want %v", tc.addr, got, tc.allow)
			}
		})
	}
}

// TestRedactAddrOfVariants pins rejection-diagnostic rendering for every
// address shape.
func TestRedactAddrOfVariants(t *testing.T) {
	if got := redactAddrOf(nil); got != "<nil>" {
		t.Fatalf("nil = %q", got)
	}
	v4 := net.ParseIP("192.0.2.7")
	if got := redactAddrOf(&net.UDPAddr{IP: v4}); got != "192.0.2.7" {
		t.Fatalf("udp = %q", got)
	}
	if got := redactAddrOf(&net.TCPAddr{IP: v4}); got != "192.0.2.7" {
		t.Fatalf("tcp = %q", got)
	}
	if got := redactAddrOf(&net.IPAddr{IP: v4}); got != "192.0.2.7" {
		t.Fatalf("ip = %q", got)
	}
	v6 := net.ParseIP("2001:db8::1")
	wantV6 := redactIP(v6)
	if got := redactAddrOf(&net.TCPAddr{IP: v6}); got != wantV6 {
		t.Fatalf("tcp6 = %q", got)
	}
	if got := redactAddrOf(customAddr{"192.0.2.9:443"}); got != "192.0.2.9" {
		t.Fatalf("custom hostport = %q", got)
	}
	if got := redactAddrOf(customAddr{"2001:db8::9"}); got != wantV6 {
		t.Fatalf("custom bare v6 = %q", got)
	}
	if got := redactAddrOf(customAddr{"garbage"}); got != "<nil>" {
		t.Fatalf("custom garbage = %q", got)
	}
	if got := redactAddr("2001:db8::9%eth0", "443"); !strings.Contains(got, "443") {
		t.Fatalf("redactAddr zone = %q", got)
	}
}

// TestFilterDialCaps proves candidate capping with and without a policy.
func TestFilterDialCaps(t *testing.T) {
	many := make([]net.IP, 20)
	for i := range many {
		many[i] = net.ParseIP("192.0.2.1")
	}
	var nilPolicy *AddressPolicy
	if got := nilPolicy.Filter(many); len(got) != maxDialCandidates {
		t.Fatalf("nil filter = %d, want %d", len(got), maxDialCandidates)
	}
	p, err := ParseAddressPolicy([]string{"192.0.2.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Filter(many); len(got) != maxDialCandidates {
		t.Fatalf("filter = %d, want %d", len(got), maxDialCandidates)
	}
	if got := p.Filter(nil); len(got) != 0 {
		t.Fatalf("filter(nil) = %v", got)
	}
}

// TestDialAddrsErrors covers dial-address validation without touching the
// network: malformed addresses and cancelled contexts fail before DNS.
func TestDialAddrsErrors(t *testing.T) {
	var nilPolicy *AddressPolicy
	if _, err := nilPolicy.DialAddrs(context.Background(), "no-port-here"); err == nil {
		t.Fatal("malformed dial address accepted")
	}
	p, err := ParseAddressPolicy([]string{"192.0.2.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.DialAddrs(context.Background(), "also-bad"); err == nil {
		t.Fatal("malformed dial address accepted")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.DialAddrs(cancelled, "example.com:443"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled dial err = %v, want context.Canceled", err)
	}
}

// TestNodeIDFromCertShapes pins SAN selection: non-node URIs are skipped,
// and a missing SAN fails closed.
func TestNodeIDFromCertShapes(t *testing.T) {
	id := ids.NewNodeID()
	cert := &x509.Certificate{URIs: []*url.URL{
		{Scheme: "https", Host: "example.com"},
		NodeURI(id),
	}}
	if got, err := NodeIDFromCert(cert); err != nil || got != id {
		t.Fatalf("NodeIDFromCert = %v/%v", got, err)
	}
	if _, err := NodeIDFromCert(&x509.Certificate{}); err == nil {
		t.Fatal("SAN-less cert accepted")
	}
}

func plainCertPEM(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "plain"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
	}, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "plain"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
	}, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// TestParseNodeCertErrors proves malformed and SAN-less pairs are rejected.
func TestParseNodeCertErrors(t *testing.T) {
	if _, err := ParseNodeCert([]byte("junk"), []byte("junk")); err == nil {
		t.Fatal("junk PEM accepted")
	}
	certPEM, keyPEM := plainCertPEM(t)
	if _, err := ParseNodeCert(certPEM, keyPEM); err == nil {
		t.Fatal("SAN-less pair accepted")
	}
}

// TestPoolDefaults proves zero-value options select documented defaults.
func TestPoolDefaults(t *testing.T) {
	p, err := NewPool(PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	st := p.Stats()
	if st.MaxConnections != 32 || st.ReservedMembership != 8 || st.MaxSessions != 8 {
		t.Fatalf("defaults = %+v", st)
	}
}

func testPoolOpts() PoolOptions {
	return PoolOptions{
		MaxConnections:         8,
		ReservedMembership:     2,
		MaxReplicationSessions: 4,
		Fanout:                 2,
		MaxConcurrentRepairs:   1,
	}
}

// TestPoolClosedOps proves a closed pool rejects every operation.
func TestPoolClosedOps(t *testing.T) {
	p, err := NewPool(testPoolOpts())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.AdmitInbound(ids.NewNodeID(), PurposeGeneric); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("AdmitInbound = %v", err)
	}
	if err := p.RegisterSession(&Session{}, PurposeGeneric, false); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("RegisterSession = %v", err)
	}
	if _, err := p.Dial(context.Background(), "127.0.0.1:1", nil, ids.NewNodeID(), PurposeGeneric); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Dial = %v", err)
	}
	// Release on a closed/unknown pool is a silent no-op.
	p.Release(ids.NewNodeID(), PurposeGeneric)
}

// TestPoolDialSessionExhausted proves Dial enforces session caps before any
// network work (the dial address is deliberately undialable).
func TestPoolDialSessionExhausted(t *testing.T) {
	p, err := NewPool(PoolOptions{
		MaxConnections:         8,
		ReservedMembership:     2,
		MaxReplicationSessions: 2,
		Fanout:                 1,
		MaxConcurrentRepairs:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.AdmitInbound(ids.NewNodeID(), PurposeSelectedTarget); err != nil {
		t.Fatal(err)
	}
	_, err = p.Dial(context.Background(), "undialable:0", nil, ids.NewNodeID(), PurposeSelectedTarget)
	if !errors.Is(err, ErrSessionCapacityExhausted) {
		t.Fatalf("Dial = %v, want session exhaustion", err)
	}
	if st := p.Stats(); st.SessionDeferrals != 1 {
		t.Fatalf("SessionDeferrals = %d, want 1", st.SessionDeferrals)
	}
}

// TestPoolDialFlightCoalescing drives the singleflight waiter paths
// deterministically by installing manual dial flights.
func TestPoolDialFlightCoalescing(t *testing.T) {
	p, err := NewPool(testPoolOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	t.Run("flight-error", func(t *testing.T) {
		peer := ids.NewNodeID()
		flight := &dialFlight{done: make(chan struct{})}
		p.mu.Lock()
		p.dials[peer] = flight
		p.mu.Unlock()
		type result struct {
			sess *Session
			err  error
		}
		resCh := make(chan result, 1)
		go func() {
			sess, err := p.Dial(context.Background(), "unused", nil, peer, PurposeGeneric)
			resCh <- result{sess, err}
		}()
		time.Sleep(50 * time.Millisecond)
		flight.err = errors.New("boom")
		close(flight.done)
		select {
		case res := <-resCh:
			if res.err == nil || res.err.Error() != "boom" {
				t.Fatalf("Dial = %v/%v", res.sess, res.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter did not return")
		}
		if st := p.Stats(); st.DialsCoalesced != 1 {
			t.Fatalf("DialsCoalesced = %d", st.DialsCoalesced)
		}
	})

	t.Run("flight-cancelled", func(t *testing.T) {
		peer := ids.NewNodeID()
		flight := &dialFlight{done: make(chan struct{})}
		p.mu.Lock()
		p.dials[peer] = flight
		p.mu.Unlock()
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := p.Dial(cancelled, "unused", nil, peer, PurposeGeneric); !errors.Is(err, context.Canceled) {
			t.Fatalf("Dial = %v, want context.Canceled", err)
		}
		close(flight.done)
	})

	t.Run("flight-success-no-conn", func(t *testing.T) {
		peer := ids.NewNodeID()
		flight := &dialFlight{done: make(chan struct{})}
		p.mu.Lock()
		p.dials[peer] = flight
		p.mu.Unlock()
		resCh := make(chan error, 1)
		go func() {
			sess, err := p.Dial(context.Background(), "unused", nil, peer, PurposeGeneric)
			if sess != nil {
				t.Errorf("sess = %v, want nil", sess)
			}
			resCh <- err
		}()
		time.Sleep(50 * time.Millisecond)
		close(flight.done)
		select {
		case err := <-resCh:
			if err != nil {
				t.Fatalf("Dial err = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter did not return")
		}
	})
}

func loopbackListener(t *testing.T, creds *Credentials) *Listener {
	t.Helper()
	ln, err := Listen("127.0.0.1:0", creds)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// loopbackPair dials ln and returns the (accepted, dialed) session pair.
func loopbackPair(t *testing.T, ctx context.Context, ln *Listener, creds *Credentials, expect ids.NodeID) (accepted, dialed *Session) {
	t.Helper()
	type outcome struct {
		sess *Session
		err  error
	}
	accCh := make(chan outcome, 1)
	go func() {
		sess, err := ln.Accept(ctx)
		accCh <- outcome{sess, err}
	}()
	var err error
	dialed, err = Dial(ctx, ln.Addr(), creds, expect)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case out := <-accCh:
		if out.err != nil {
			t.Fatal(out.err)
		}
		return out.sess, dialed
	case <-ctx.Done():
		t.Fatal("accept timeout")
		return nil, nil
	}
}

// TestPoolSessionLifecycle exercises registration, reuse, replacement,
// release, eviction, and dead-session cleanup against real loopback sessions.
func TestPoolSessionLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ca := newTestCA(t)
	aID := ids.NewNodeID()
	ln := loopbackListener(t, ca.issueNodeCreds(t, aID))

	// Each subtest dials its own sessions: pools close registered
	// sessions on Close, so sharing them across subtests is unsafe.
	dialPeer := func(t *testing.T, peer ids.NodeID) (accepted, dialed *Session) {
		t.Helper()
		accepted, dialed = loopbackPair(t, ctx, ln, ca.issueNodeCreds(t, peer), aID)
		t.Cleanup(func() { _ = dialed.Close() })
		t.Cleanup(func() { _ = accepted.Close() })
		return accepted, dialed
	}

	t.Run("register-reuse-release", func(t *testing.T) {
		bID := ids.NewNodeID()
		acceptedB, _ := dialPeer(t, bID)
		p, err := NewPool(testPoolOpts())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.RegisterSession(acceptedB, PurposeGeneric, false); err != nil {
			t.Fatal(err)
		}
		// AdmitInbound against a live session reuses it.
		if err := p.AdmitInbound(bID, PurposeGeneric); err != nil {
			t.Fatal(err)
		}
		// Dial reuses the registered session without touching the network.
		sess, err := p.Dial(ctx, "undialable:0", nil, bID, PurposeGeneric)
		if err != nil || sess != acceptedB {
			t.Fatalf("Dial = %v/%v, want registered session", sess, err)
		}
		if st := p.Stats(); st.DialsReused != 1 {
			t.Fatalf("DialsReused = %d", st.DialsReused)
		}
		p.Release(bID, PurposeGeneric)
		p.Release(ids.NewNodeID(), PurposeGeneric) // unknown peer: no-op
		if st := p.Stats(); st.ActiveConnections != 1 {
			t.Fatalf("ActiveConnections = %d", st.ActiveConnections)
		}
	})

	t.Run("register-replaces", func(t *testing.T) {
		bID := ids.NewNodeID()
		acceptedB, _ := dialPeer(t, bID)
		acceptedB2, _ := dialPeer(t, bID)
		p, err := NewPool(testPoolOpts())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.RegisterSession(acceptedB, PurposeGeneric, false); err != nil {
			t.Fatal(err)
		}
		if err := p.RegisterSession(acceptedB2, PurposeGeneric, false); err != nil {
			t.Fatal(err)
		}
		p.mu.Lock()
		cur := p.conns[bID].sess
		p.mu.Unlock()
		if cur != acceptedB2 {
			t.Fatal("replacement session not installed")
		}
	})

	t.Run("evict-idle", func(t *testing.T) {
		bID, cID := ids.NewNodeID(), ids.NewNodeID()
		acceptedB, _ := dialPeer(t, bID)
		acceptedC, _ := dialPeer(t, cID)
		p, err := NewPool(PoolOptions{
			MaxConnections:         3,
			ReservedMembership:     1,
			MaxReplicationSessions: 2,
			Fanout:                 1,
			MaxConcurrentRepairs:   1,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.RegisterSession(acceptedB, PurposeGeneric, false); err != nil {
			t.Fatal(err)
		}
		if err := p.RegisterSession(acceptedC, PurposeGeneric, false); err != nil {
			t.Fatal(err)
		}
		p.Release(bID, PurposeGeneric)
		p.Release(cID, PurposeGeneric)
		if err := p.AdmitInbound(ids.NewNodeID(), PurposeGeneric); err != nil {
			t.Fatal(err)
		}
		// AdmitInbound reserves a session slot without inserting a conn,
		// so the map holds the one surviving registered session.
		if st := p.Stats(); st.Evictions != 1 || st.ActiveConnections != 1 || st.GenericSessions != 1 {
			t.Fatalf("after evict: %+v", st)
		}
	})

	t.Run("evict-none-idle", func(t *testing.T) {
		bID, cID := ids.NewNodeID(), ids.NewNodeID()
		acceptedB, _ := dialPeer(t, bID)
		acceptedC, _ := dialPeer(t, cID)
		p, err := NewPool(PoolOptions{
			MaxConnections:         3,
			ReservedMembership:     1,
			MaxReplicationSessions: 2,
			Fanout:                 1,
			MaxConcurrentRepairs:   1,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.RegisterSession(acceptedB, PurposeGeneric, true); err != nil {
			t.Fatal(err)
		}
		if err := p.RegisterSession(acceptedC, PurposeGeneric, true); err != nil {
			t.Fatal(err)
		}
		admitErr := p.AdmitInbound(ids.NewNodeID(), PurposeGeneric)
		if !errors.Is(admitErr, ErrConnectionPoolExhausted) {
			t.Fatalf("AdmitInbound = %v, want exhaustion", admitErr)
		}
		if st := p.Stats(); st.ConnDeferrals != 1 {
			t.Fatalf("ConnDeferrals = %d", st.ConnDeferrals)
		}
	})

	t.Run("release-dead-removes", func(t *testing.T) {
		bID := ids.NewNodeID()
		acceptedB, _ := dialPeer(t, bID)
		p, err := NewPool(testPoolOpts())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.RegisterSession(acceptedB, PurposeGeneric, false); err != nil {
			t.Fatal(err)
		}
		_ = acceptedB.Close()
		deadline := time.Now().Add(5 * time.Second)
		for acceptedB.Context().Err() == nil && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		p.Release(bID, PurposeGeneric)
		if st := p.Stats(); st.ActiveConnections != 0 {
			t.Fatalf("ActiveConnections = %d, want 0", st.ActiveConnections)
		}
	})

	t.Run("flight-then-registered", func(t *testing.T) {
		cID := ids.NewNodeID()
		acceptedC, _ := dialPeer(t, cID)
		p, err := NewPool(testPoolOpts())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if err := p.RegisterSession(acceptedC, PurposeGeneric, false); err != nil {
			t.Fatal(err)
		}
		flight := &dialFlight{done: make(chan struct{})}
		p.mu.Lock()
		p.dials[cID] = flight
		p.mu.Unlock()
		resCh := make(chan *Session, 1)
		go func() {
			sess, err := p.Dial(ctx, "unused", nil, cID, PurposeGeneric)
			if err != nil {
				t.Errorf("Dial err = %v", err)
				resCh <- nil
				return
			}
			resCh <- sess
		}()
		time.Sleep(50 * time.Millisecond)
		close(flight.done)
		select {
		case sess := <-resCh:
			if sess != acceptedC {
				t.Fatalf("Dial = %v, want registered session", sess)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter did not return")
		}
	})
}

// TestSessionAddrAccessors pins the QUIC session address surface, including
// the policy-denied AcceptStream path.
func TestSessionAddrAccessors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca := newTestCA(t)
	aID := ids.NewNodeID()
	ln := loopbackListener(t, ca.issueNodeCreds(t, aID))
	bID := ids.NewNodeID()
	accepted, dialed := loopbackPair(t, ctx, ln, ca.issueNodeCreds(t, bID), aID)
	defer dialed.Close()
	defer accepted.Close()

	if dialed.RemoteAddr() == "" {
		t.Fatal("RemoteAddr empty")
	}
	if dialed.LocalAddr() == nil || dialed.RemoteNetAddr() == nil {
		t.Fatal("nil session addrs")
	}
	if err := dialed.VerifyRemoteAddr(); err != nil {
		t.Fatalf("nil-policy VerifyRemoteAddr = %v", err)
	}

	// A session whose policy denies loopback fails AcceptStream and closes.
	deny, err := ParseAddressPolicy([]string{"192.0.2.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	denied := &Session{conn: dialed.conn, policy: deny, Local: bID, Peer: aID}
	if err := denied.VerifyRemoteAddr(); !errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("VerifyRemoteAddr = %v, want denial", err)
	}
	_, dialed2 := loopbackPair(t, ctx, ln, ca.issueNodeCreds(t, ids.NewNodeID()), aID)
	defer dialed2.Close()
	denied2 := &Session{conn: dialed2.conn, policy: deny}
	if _, err := denied2.AcceptStream(ctx); !errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("AcceptStream = %v, want denial", err)
	}
}

// TestMemberlistBasics covers transport stats, nil-safe session registration,
// and the DialTimeout wrapper.
func TestMemberlistBasics(t *testing.T) {
	ca := newTestCA(t)
	dbid := ids.NewDBID()
	nodeA := ids.NewNodeID()
	credsA := ca.issueNodeCreds(t, nodeA)
	trA, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: nodeA,
		DBID:        dbid,
		Creds:       credsA,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Shutdown()
	if st := trA.Stats(); st.PacketsSent != 0 || st.StreamsAccepted != 0 {
		t.Fatalf("fresh Stats = %+v", st)
	}

	nodeB := ids.NewNodeID()
	trB, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: nodeB,
		DBID:        dbid,
		Creds:       ca.issueNodeCreds(t, nodeB),
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Shutdown()

	connCh := make(chan interface{}, 1)
	go func() {
		select {
		case c := <-trA.StreamCh():
			connCh <- c
		case <-time.After(5 * time.Second):
			connCh <- nil
		}
	}()
	clientConn, err := trB.DialTimeout(trA.listener.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("DialTimeout: %v", err)
	}
	defer clientConn.Close()
	got := <-connCh
	serverConn, ok := got.(net.Conn)
	if !ok || serverConn == nil {
		t.Fatal("no incoming stream on A")
	}
	defer serverConn.Close()
	if clientConn.LocalAddr() == nil || clientConn.RemoteAddr() == nil {
		t.Fatal("nil stream conn addrs")
	}
	if err := clientConn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if err := clientConn.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}
	if st := trB.Stats(); st.StreamsDialed != 1 {
		t.Fatalf("StreamsDialed = %d", st.StreamsDialed)
	}
}

// TestMemberlistRegisterSessionLifecycle proves external session attachment,
// duplicate suppression, and nil/closed guards.
func TestMemberlistRegisterSessionLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca := newTestCA(t)
	aID := ids.NewNodeID()
	ln := loopbackListener(t, ca.issueNodeCreds(t, aID))
	bID := ids.NewNodeID()
	accepted, dialed := loopbackPair(t, ctx, ln, ca.issueNodeCreds(t, bID), aID)
	defer dialed.Close()
	defer accepted.Close()

	tr, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: aID,
		DBID:        ids.NewDBID(),
		Creds:       ca.issueNodeCreds(t, aID),
	})
	if err != nil {
		t.Fatal(err)
	}
	tr.RegisterSession(nil) // no-op
	tr.RegisterSession(accepted)
	tr.RegisterSession(accepted) // duplicate: no-op
	if err := tr.Shutdown(); err != nil {
		t.Fatal(err)
	}
	tr.RegisterSession(accepted) // closed: no-op
}

func streamHeader(dbid ids.DBID, sender ids.NodeID) []byte {
	h := make([]byte, StreamHeaderLen)
	binary.BigEndian.PutUint32(h[0:4], StreamMagic)
	h[4] = StreamVersion
	h[5] = StreamTypeMembership
	copy(h[6:22], dbid[:])
	copy(h[22:38], sender[:])
	return h
}

// TestHandleStreamDispatch drives externally accepted streams through
// HandleStream: full and split headers dispatch, while bad magic, DBID, and
// sender fail the envelope counters and truncated streams are dropped.
func TestHandleStreamDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ca := newTestCA(t)
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	dbid := ids.NewDBID()
	ln := loopbackListener(t, ca.issueNodeCreds(t, aID))
	accepted, dialed := loopbackPair(t, ctx, ln, ca.issueNodeCreds(t, bID), aID)
	defer dialed.Close()
	defer accepted.Close()

	tr, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: aID,
		DBID:        dbid,
		Creds:       ca.issueNodeCreds(t, aID),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Shutdown()

	// QUIC discovers a stream only when the opener sends data, so every
	// case writes initial bytes before the server accepts.
	newStreams := func(t *testing.T, initial []byte) (cli *quic.Stream, srv *quic.Stream) {
		t.Helper()
		var err error
		cli, err = dialed.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(initial) > 0 {
			if _, err := cli.Write(initial); err != nil {
				t.Fatal(err)
			}
		}
		type outcome struct {
			srv *quic.Stream
			err error
		}
		srvCh := make(chan outcome, 1)
		go func() {
			srv, err := accepted.AcceptStream(ctx)
			srvCh <- outcome{srv, err}
		}()
		select {
		case out := <-srvCh:
			if out.err != nil {
				t.Fatal(out.err)
			}
			return cli, out.srv
		case <-time.After(5 * time.Second):
			t.Fatal("accept timeout")
			return nil, nil
		}
	}

	expectConn := func(t *testing.T) net.Conn {
		t.Helper()
		select {
		case c := <-tr.StreamCh():
			return c
		case <-time.After(5 * time.Second):
			t.Fatal("no dispatched stream")
			return nil
		}
	}
	expectNone := func(t *testing.T) {
		t.Helper()
		select {
		case c := <-tr.StreamCh():
			_ = c.Close()
			t.Fatal("unexpected dispatched stream")
		case <-time.After(200 * time.Millisecond):
		}
	}

	t.Run("split-header", func(t *testing.T) {
		hdr := streamHeader(dbid, bID)
		cli, srv := newStreams(t, hdr[10:])
		defer cli.Close()
		tr.HandleStream(accepted, srv, hdr[:10])
		c := expectConn(t)
		_ = c.Close()
	})
	t.Run("full-prefix", func(t *testing.T) {
		cli, srv := newStreams(t, []byte{0})
		defer cli.Close()
		tr.HandleStream(accepted, srv, streamHeader(dbid, bID))
		c := expectConn(t)
		_ = c.Close()
	})
	t.Run("bad-magic", func(t *testing.T) {
		cli, srv := newStreams(t, []byte{0})
		defer cli.Close()
		hdr := streamHeader(dbid, bID)
		hdr[0] ^= 0xff
		before := tr.Stats().DatagramEnvelopeErrors
		tr.HandleStream(accepted, srv, hdr)
		expectNone(t)
		if tr.Stats().DatagramEnvelopeErrors != before+1 {
			t.Fatal("envelope error not counted")
		}
	})
	t.Run("dbid-mismatch", func(t *testing.T) {
		cli, srv := newStreams(t, []byte{0})
		defer cli.Close()
		before := tr.Stats().DatagramDBIDMismatches
		tr.HandleStream(accepted, srv, streamHeader(ids.NewDBID(), bID))
		expectNone(t)
		if tr.Stats().DatagramDBIDMismatches != before+1 {
			t.Fatal("DBID mismatch not counted")
		}
	})
	t.Run("sender-mismatch", func(t *testing.T) {
		cli, srv := newStreams(t, []byte{0})
		defer cli.Close()
		before := tr.Stats().DatagramEnvelopeErrors
		tr.HandleStream(accepted, srv, streamHeader(dbid, ids.NewNodeID()))
		expectNone(t)
		if tr.Stats().DatagramEnvelopeErrors != before+1 {
			t.Fatal("sender mismatch not counted")
		}
	})
	t.Run("truncated-stream", func(t *testing.T) {
		cli, srv := newStreams(t, []byte{1, 2, 3, 4, 5})
		if err := cli.Close(); err != nil {
			t.Fatal(err)
		}
		tr.HandleStream(accepted, srv, []byte{1, 2, 3})
		expectNone(t)
	})
}

// TestStreamConnNilSession pins the nil-session address guards.
func TestStreamConnNilSession(t *testing.T) {
	c := &StreamConn{}
	if c.LocalAddr() != nil || c.RemoteAddr() != nil {
		t.Fatal("nil-session addrs non-nil")
	}
}

// TestFindFirstNonLoopbackIP exercises the interface scan; the result is
// environment-dependent, so any outcome (IP or error) passes.
func TestFindFirstNonLoopbackIP(t *testing.T) {
	ip, err := findFirstNonLoopbackIP()
	t.Logf("first non-loopback = %v, err = %v", ip, err)
}
