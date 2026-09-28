package transport

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/ids"
)

func TestParseAddressPolicy(t *testing.T) {
	p, err := ParseAddressPolicy(nil)
	if err != nil || p != nil {
		t.Fatalf("empty entries = (%v, %v), want (nil, nil)", p, err)
	}
	p, err = ParseAddressPolicy([]string{})
	if err != nil || p != nil {
		t.Fatalf("empty slice = (%v, %v), want (nil, nil)", p, err)
	}

	p, err = ParseAddressPolicy([]string{
		"192.0.2.0/24",
		"2001:db8::/32",
		"198.51.100.7", // bare IP is a host route
		"2001:db8::1",
		" 10.0.0.0/8 ", // surrounding space tolerated
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		ip    string
		allow bool
	}{
		{"192.0.2.1", true},
		{"192.0.3.1", false},
		{"2001:db8::ffff", true},
		{"2001:db9::1", false},
		{"198.51.100.7", true},
		{"198.51.100.8", false},
		{"2001:db8::1", true},
		{"2001:db8::2", true}, // covered by the /32
		{"10.9.8.7", true},
		{"11.0.0.1", false},
	} {
		if got := p.Allows(net.ParseIP(tc.ip)); got != tc.allow {
			t.Errorf("Allows(%s) = %v, want %v", tc.ip, got, tc.allow)
		}
	}

	for _, bad := range []string{
		"", "   ", "not-an-ip", "192.0.2.1/33", "192.0.2.1/-1",
		"2001:db8::/129", "1.2.3.4/24/extra", "1.2.3.4/",
	} {
		if _, err := ParseAddressPolicy([]string{bad}); err == nil {
			t.Errorf("ParseAddressPolicy(%q) succeeded, want error", bad)
		}
	}
	// One bad entry poisons the whole list (fail fast at Open).
	if _, err := ParseAddressPolicy([]string{"10.0.0.0/8", "bogus"}); err == nil {
		t.Error("mixed valid/invalid entries succeeded, want error")
	}
}

func TestAddressPolicyNormalization(t *testing.T) {
	p, err := ParseAddressPolicy([]string{"127.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	// IPv4-mapped IPv6 normalizes to IPv4.
	if !p.Allows(net.ParseIP("::ffff:127.0.0.1")) {
		t.Error("mapped ::ffff:127.0.0.1 denied by 127.0.0.0/8")
	}
	if p.Allows(net.ParseIP("::ffff:10.0.0.1")) {
		t.Error("mapped ::ffff:10.0.0.1 allowed by 127.0.0.0/8")
	}
	if p.Allows(nil) {
		t.Error("nil IP allowed")
	}

	var nilPolicy *AddressPolicy
	if !nilPolicy.Allows(net.ParseIP("192.0.2.1")) {
		t.Error("nil policy denied an address")
	}
	if !nilPolicy.AllowsAddr(nil) {
		t.Error("nil policy denied a nil addr")
	}
	if p.AllowsAddr(nil) {
		t.Error("policy allowed a nil addr")
	}
	if !p.AllowsAddr(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7443}) {
		t.Error("policy denied loopback UDP addr")
	}
	// Zones do not affect matching.
	if !p.AllowsAddr(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7443, Zone: "eth0"}) {
		t.Error("zone affected matching")
	}
}

func TestAddressPolicyFilter(t *testing.T) {
	p, err := ParseAddressPolicy([]string{"10.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	in := []net.IP{net.ParseIP("192.0.2.1"), net.ParseIP("10.1.2.3"), net.ParseIP("2001:db8::1")}
	got := p.Filter(in)
	if len(got) != 1 || !got[0].Equal(net.ParseIP("10.1.2.3")) {
		t.Fatalf("Filter = %v, want [10.1.2.3]", got)
	}

	// Results are capped to bound dial work.
	many := make([]net.IP, 0, 32)
	for i := 0; i < 32; i++ {
		many = append(many, net.ParseIP("10.0.0.1"))
	}
	if got := p.Filter(many); len(got) != maxDialCandidates {
		t.Fatalf("Filter capped = %d, want %d", len(got), maxDialCandidates)
	}
}

func TestDialAddrs(t *testing.T) {
	ctx := context.Background()
	p, err := ParseAddressPolicy([]string{"10.0.0.0/8", "2001:db8::/32"})
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := p.DialAddrs(ctx, "10.1.2.3:7443")
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 1 || addrs[0] != "10.1.2.3:7443" {
		t.Fatalf("DialAddrs literal = %v", addrs)
	}
	// Zone is preserved for dialing but ignored for matching.
	addrs, err = p.DialAddrs(ctx, "[fe80::1%eth0]:7443")
	if err == nil {
		t.Fatalf("DialAddrs link-local = %v, want denial (not in policy)", addrs)
	}
	if !errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("DialAddrs link-local err = %v, want ErrAddressNotAllowed", err)
	}
	if _, err := p.DialAddrs(ctx, "192.0.2.1:7443"); !errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("DialAddrs denied literal err = %v", err)
	}
	if _, err := p.DialAddrs(ctx, "no-port-here"); errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("DialAddrs malformed err = %v, want non-policy error", err)
	}
	if _, err := p.DialAddrs(ctx, "no-port-here"); err == nil {
		t.Fatal("DialAddrs malformed succeeded")
	}
}

func TestRedactIP(t *testing.T) {
	if got := redactIP(net.ParseIP("192.0.2.1")); got != "192.0.2.1" {
		t.Fatalf("redact v4 = %q", got)
	}
	got := redactIP(net.ParseIP("2001:db8:1:2:3:4:5:6"))
	if strings.Contains(got, "3:4") || strings.Contains(got, "5:6") {
		t.Fatalf("redact v6 leaks IID: %q", got)
	}
	if !strings.HasPrefix(got, "2001:db8:1:2") {
		t.Fatalf("redact v6 lost prefix: %q", got)
	}
	if redactIP(nil) == "" {
		t.Fatal("redact nil is empty")
	}
}

func policyCreds(t *testing.T, ca *CA, id ids.NodeID, networks []string) *Credentials {
	t.Helper()
	creds := testCreds(t, ca, id, nil)
	pol, err := ParseAddressPolicy(networks)
	if err != nil {
		t.Fatal(err)
	}
	creds.AddressPolicy = pol
	return creds
}

// deniedLoopbackPolicy allows only documentation TEST-NET-1, which never
// appears on the wire: loopback peers are always denied, and nothing is
// ever dialed.
func deniedLoopbackPolicy(t *testing.T) *AddressPolicy {
	t.Helper()
	pol, err := ParseAddressPolicy([]string{"192.0.2.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	return pol
}

func TestDialDeniedOutbound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	bCreds := policyCreds(t, ca, bID, []string{"192.0.2.0/24"})

	// Literal denied IP: refused before any connection work.
	if _, err := Dial(ctx, "127.0.0.1:7443", bCreds, aID); !errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("dial denied literal err = %v", err)
	}
	// Hostname with all answers denied: refused without dialing.
	if _, err := Dial(ctx, "localhost:7443", bCreds, aID); !errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("dial denied name err = %v", err)
	}
}

func TestDialMixedDNSAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resolved, err := net.DefaultResolver.LookupIPAddr(ctx, "localhost")
	if err != nil || len(resolved) == 0 {
		t.Skipf("localhost does not resolve here: %v", err)
	}
	first := resolved[0].IP
	// Allow only the first answer as a host route: the dial must succeed
	// through it no matter how many other (denied) answers exist.
	hostRoute := first.String()
	if first.To4() != nil {
		hostRoute += "/32"
	} else {
		hostRoute += "/128"
	}

	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	aCreds := testCreds(t, ca, aID, nil)
	bCreds := policyCreds(t, ca, bID, []string{hostRoute})

	ln, err := Listen(net.JoinHostPort(first.String(), "0"), aCreds)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, err := net.SplitHostPort(ln.Addr())
	if err != nil {
		t.Fatal(err)
	}
	acceptCh := make(chan *Session, 1)
	errCh := make(chan error, 1)
	go func() {
		sess, err := ln.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		acceptCh <- sess
	}()

	dialed, err := Dial(ctx, net.JoinHostPort("localhost", port), bCreds, aID)
	if err != nil {
		t.Fatalf("dial localhost with filtered answers: %v (resolved %v)", err, resolved)
	}
	defer dialed.Close()
	select {
	case sess := <-acceptCh:
		defer sess.Close()
		if sess.Peer != bID {
			t.Fatalf("listener sees peer %s, want %s", sess.Peer, bID)
		}
	case err := <-errCh:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}
}

func TestAcceptDeniedInbound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	aCreds := policyCreds(t, ca, aID, []string{"192.0.2.0/24"})
	bCreds := testCreds(t, ca, bID, nil)

	ln, err := Listen("127.0.0.1:0", aCreds)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	errCh := make(chan error, 1)
	go func() {
		sess, err := ln.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		_ = sess.Close()
		errCh <- nil
	}()

	// The dialer has a valid identity; the server must still refuse it.
	sess, err := Dial(ctx, ln.Addr(), bCreds, aID)
	if err == nil {
		defer sess.Close()
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrAddressNotAllowed) {
			t.Fatalf("accept err = %v, want ErrAddressNotAllowed", err)
		}
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}
}

func TestAcceptAllowedAddressStillRequiresIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	// Loopback is allowed, but B's NodeID is not.
	aCreds := policyCreds(t, ca, aID, []string{"127.0.0.0/8"})
	aCreds.AllowedPeers = map[ids.NodeID]bool{aID: true}
	bCreds := testCreds(t, ca, bID, nil)

	ln, err := Listen("127.0.0.1:0", aCreds)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _, _ = ln.Accept(ctx) }()

	sess, err := Dial(ctx, ln.Addr(), bCreds, aID)
	if err != nil {
		return // rejected synchronously: fine
	}
	defer sess.Close()
	select {
	case <-sess.Context().Done():
		// Server closed the unauthorized connection.
	case <-time.After(5 * time.Second):
		t.Fatal("unauthorized identity stayed connected from an allowed address")
	}
}

func TestSessionRecheckAfterPolicyTightening(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	aCreds := testCreds(t, ca, aID, nil)
	bCreds := policyCreds(t, ca, bID, []string{"127.0.0.0/8", "::1/128"})

	ln, err := Listen("127.0.0.1:0", aCreds)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	acceptCh := make(chan *Session, 1)
	go func() {
		sess, err := ln.Accept(ctx)
		if err == nil {
			acceptCh <- sess
		}
	}()
	dialed, err := Dial(ctx, ln.Addr(), bCreds, aID)
	if err != nil {
		t.Fatal(err)
	}
	defer dialed.Close()
	var accepted *Session
	select {
	case accepted = <-acceptCh:
		defer accepted.Close()
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}

	// Allowed session serves streams.
	st, err := dialed.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	// Simulate migration onto a denied address by tightening the policy:
	// the recheck must fail and further streams must be refused.
	dialed.policy = deniedLoopbackPolicy(t)
	if err := dialed.VerifyRemoteAddr(); !errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("VerifyRemoteAddr err = %v, want ErrAddressNotAllowed", err)
	}
	if _, err := dialed.OpenStream(ctx); !errors.Is(err, ErrAddressNotAllowed) {
		t.Fatalf("OpenStream after violation err = %v", err)
	}
}
