package replication

import (
	"context"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/transport"
)

func TestNegotiateCapabilities(t *testing.T) {
	const futureOptional = uint64(1) << 20
	for _, tc := range []struct {
		name    string
		peer    uint64
		want    uint64
		wantErr bool
	}{
		{"none", 0, 0, false},
		{"known", CapCompression, CapCompression, false},
		{"unknown optional ignored", futureOptional, 0, false},
		{"known plus unknown optional", CapCompression | futureOptional, CapCompression, false},
		{"required marker alone", CapRequiredMask, 0, false},
		{"required known", CapRequiredMask | CapCompression, CapCompression, false},
		{"required unknown refused", CapRequiredMask | futureOptional, 0, true},
		{"required known plus unknown refused", CapRequiredMask | CapCompression | futureOptional, 0, true},
	} {
		got, err := NegotiateCapabilities(tc.peer)
		if tc.wantErr != (err != nil) {
			t.Fatalf("%s: err = %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: usable = %#x, want %#x", tc.name, got, tc.want)
		}
	}
}

// TestValidateIdentityHonorsVersionOverride proves enforcement matches
// advertisement: a node overridden to min 99 must refuse a stock (3,3)
// peer inbound (not just be refused outbound), so no transient session is
// ever attached. With zero overrides the constants still apply.
func TestValidateIdentityHonorsVersionOverride(t *testing.T) {
	peer := ids.NodeID{7}
	dbid := ids.DBID{9}
	stock := &Hello{ProtocolVersion: ProtocolVersion, MinProtocolVersion: MinProtocolVersion, NodeID: peer, DBID: dbid, Capabilities: CapMergePolicies | CapOriginSignatures}
	future := &Hello{ProtocolVersion: 99, MinProtocolVersion: 99, NodeID: peer, DBID: dbid, Capabilities: CapMergePolicies | CapOriginSignatures}

	overridden := &Manager{cfg: ManagerConfig{DBID: dbid, AdvertiseProtocolVersion: 99, AdvertiseMinProtocolVersion: 99}}
	if err := overridden.validateIdentity(stock, peer); err == nil {
		t.Fatalf("min-99 node accepted stock peer hello, want refusal")
	}
	if err := overridden.validateIdentity(future, peer); err != nil {
		t.Fatalf("min-99 node refused matching peer hello: %v", err)
	}
	stockMgr := &Manager{cfg: ManagerConfig{DBID: dbid}}
	if err := stockMgr.validateIdentity(future, peer); err == nil {
		t.Fatalf("stock node accepted version-99 hello, want refusal")
	}
	if err := stockMgr.validateIdentity(stock, peer); err != nil {
		t.Fatalf("stock node refused stock hello: %v", err)
	}
}

func TestDisseminationCapabilityMismatchRefuses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		peer    uint64
		local   bool
		wantErr bool
	}{
		{"gossip peers agree", CapCompression, false, false},
		{"plumtree peers agree", CapCompression | CapPlumtree | CapRequiredMask, true, false},
		{"enabled local rejects gossip peer", CapCompression, true, true},
		{"gossip local rejects enabled peer", CapCompression | CapPlumtree | CapRequiredMask, false, true},
	} {
		if _, err := NegotiateDisseminationCapabilities(tc.peer, tc.local); tc.wantErr != (err != nil) {
			t.Fatalf("%s: err=%v wantErr=%t", tc.name, err, tc.wantErr)
		}
	}
}

// TestHandshakeCapabilityNegotiation proves unknown required capabilities
// refuse the session before any peer state exists, while unknown optional
// capabilities still shake hands.
func TestHandshakeCapabilityNegotiation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := newTestCluster(t)
	server := ids.NewNodeID()
	mgr, _, addr := c.testManager(t, server, 1)

	dial := func(t *testing.T, self ids.NodeID, caps uint64) *Frame {
		t.Helper()
		sess, err := transport.Dial(ctx, addr, c.creds(t, self), server)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sess.Close() })
		stream, err := sess.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var hash [32]byte
		hash[0] = 1
		if err := WriteFrame(stream, MsgHello, 0, EncodeHello(nil, &Hello{
			ProtocolVersion: ProtocolVersion, MinProtocolVersion: MinProtocolVersion,
			NodeID: self, DBID: c.dbid, SchemaEpoch: 1, SchemaHash: hash,
			Capabilities: CapMergePolicies | CapOriginSignatures | (caps),
		})); err != nil {
			t.Fatal(err)
		}
		fr, err := ReadFrame(stream)
		if err != nil {
			return nil // close without error frame is also a rejection
		}
		return fr
	}

	// Unknown required: error (or close), no session, no peer state.
	fr := dial(t, ids.NewNodeID(), CapRequiredMask|(uint64(1)<<40))
	if fr != nil && fr.Type != MsgError {
		t.Fatalf("required-unknown handshake reply = %d, want MsgError", fr.Type)
	}
	if n := len(mgr.PeerStatus()); n != 0 {
		t.Fatalf("peers after refused handshake = %d", n)
	}
	if got := mgr.Stats().HandshakeCapabilityRefl; got != 1 {
		t.Fatalf("HandshakeCapabilityRefl = %d, want 1", got)
	}

	// Unknown optional: normal welcome and session.
	fr = dial(t, ids.NewNodeID(), CapCompression|(uint64(1)<<40))
	if fr == nil || fr.Type != MsgWelcome {
		t.Fatalf("optional-unknown handshake reply = %v, want MsgWelcome", fr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if n := len(mgr.PeerStatus()); n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("optional-unknown peer never attached")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := mgr.Stats().HandshakeCapabilityRefl; got != 1 {
		t.Fatalf("HandshakeCapabilityRefl = %d, want 1", got)
	}
}
