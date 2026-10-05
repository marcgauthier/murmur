package replication

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/transport"
)

func TestNodeMetadataFetchEndpointRoundTrip(t *testing.T) {
	dbid := fixtureDBID
	withFetch := &NodeMetadata{DBID: dbid, Capabilities: KnownCaps, Endpoint: "10.0.0.9:7443", FetchEndpoint: "10.0.0.9:7844"}
	dec, err := DecodeNodeMetadata(EncodeNodeMetadata(withFetch), dbid)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Endpoint != withFetch.Endpoint || dec.FetchEndpoint != withFetch.FetchEndpoint ||
		dec.DBID != dbid || dec.Capabilities != KnownCaps {
		t.Fatalf("round trip = %+v", dec)
	}

	// Without a fetch endpoint the encoding is byte-identical to version 1,
	// so old decoders (which ignore trailing bytes) are unaffected.
	plain := &NodeMetadata{DBID: dbid, Capabilities: KnownCaps, Endpoint: "10.0.0.9:7443"}
	enc := EncodeNodeMetadata(plain)
	if len(enc) != MetadataMinLen+len(plain.Endpoint) {
		t.Fatalf("plain encoding len = %d", len(enc))
	}
	dec, err = DecodeNodeMetadata(enc, dbid)
	if err != nil || dec.FetchEndpoint != "" {
		t.Fatalf("plain decode = %+v, err=%v", dec, err)
	}

	// A truncated suffix degrades to "not serving" instead of failing.
	truncated := append(append([]byte(nil), enc...), 0x00, 0x14)
	dec, err = DecodeNodeMetadata(truncated, dbid)
	if err != nil || dec.FetchEndpoint != "" || dec.Endpoint != plain.Endpoint {
		t.Fatalf("truncated decode = %+v, err=%v", dec, err)
	}
}

// TestFetchEndpointDiscovery proves a fetch endpoint set after startup
// gossips to peers: both services observe each other in AliveMembers with
// the advertised endpoint, and a leaving peer disappears.
func TestFetchEndpointDiscovery(t *testing.T) {
	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dbid := fixtureDBID
	newService := func(t *testing.T, node ids.NodeID) (*MembershipService, *transport.MemberlistTransport) {
		t.Helper()
		cert, key, err := ca.IssueNode(node, 24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		creds, err := transport.CredentialsFromPEM(cert, key, ca.CertPEM, nil)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := transport.NewMemberlistTransport(transport.MemberlistTransportConfig{
			LocalNodeID: node,
			DBID:        dbid,
			Creds:       creds,
			BindAddr:    "127.0.0.1:0",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tr.Shutdown() })
		svc, err := NewMembershipService(
			node,
			dbid,
			MembershipConfig{
				ProbeInterval:  50 * time.Millisecond,
				ProbeTimeout:   40 * time.Millisecond,
				GossipInterval: 20 * time.Millisecond,
			},
			tr,
			newMockMembershipHandler(),
			slog.New(slog.NewTextHandler(io.Discard, nil)),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = svc.Shutdown() })
		return svc, tr
	}

	nodeA, nodeB := ids.NewNodeID(), ids.NewNodeID()
	svcA, trA := newService(t, nodeA)
	svcB, _ := newService(t, nodeB)

	ipA, portA, err := trA.FinalAdvertiseAddr("", 0)
	if err != nil {
		t.Fatal(err)
	}
	if joined, err := svcB.Join([]string{net.JoinHostPort(ipA.String(), fmt.Sprintf("%d", portA))}); err != nil || joined == 0 {
		t.Fatalf("join: %d, %v", joined, err)
	}

	const fetchA, fetchB = "127.0.0.1:17844", "127.0.0.1:17845"
	svcA.SetFetchEndpoint(fetchA)
	svcB.SetFetchEndpoint(fetchB)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		a, b := svcA.AliveMembers(), svcB.AliveMembers()
		if a[nodeB].FetchEndpoint == fetchB && b[nodeA].FetchEndpoint == fetchA {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := svcA.AliveMembers()[nodeB].FetchEndpoint; got != fetchB {
		t.Fatalf("A sees B fetch endpoint %q, want %q", got, fetchB)
	}
	if got := svcB.AliveMembers()[nodeA].FetchEndpoint; got != fetchA {
		t.Fatalf("B sees A fetch endpoint %q, want %q", got, fetchA)
	}

	_ = svcB.Leave(500 * time.Millisecond)
	_ = svcB.Shutdown()
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := svcA.AliveMembers()[nodeB]; !ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("A still lists B after leave")
}
