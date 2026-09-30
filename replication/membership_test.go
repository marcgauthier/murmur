package replication

import (
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/transport"
)

type mockMembershipHandler struct {
	mu         sync.Mutex
	discovered map[ids.NodeID]string
	updated    map[ids.NodeID]string
	left       map[ids.NodeID]bool
}

func newMockMembershipHandler() *mockMembershipHandler {
	return &mockMembershipHandler{
		discovered: make(map[ids.NodeID]string),
		updated:    make(map[ids.NodeID]string),
		left:       make(map[ids.NodeID]bool),
	}
}

func (h *mockMembershipHandler) OnPeerDiscovered(nodeID ids.NodeID, endpoint string, meta NodeMetadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.discovered[nodeID] = endpoint
}

func (h *mockMembershipHandler) OnPeerUpdated(nodeID ids.NodeID, endpoint string, meta NodeMetadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.updated[nodeID] = endpoint
}

func (h *mockMembershipHandler) OnPeerLeft(nodeID ids.NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.left[nodeID] = true
	delete(h.discovered, nodeID)
}

func (h *mockMembershipHandler) hasDiscovered(nodeID ids.NodeID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.discovered[nodeID]
	return ok
}

func (h *mockMembershipHandler) hasLeft(nodeID ids.NodeID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.left[nodeID]
}

func TestNodeMetadataEncoding(t *testing.T) {
	dbid := ids.NewDBID()
	meta := &NodeMetadata{
		DBID:         dbid,
		Capabilities: CapZstd,
		Endpoint:     "192.0.2.1:8443",
	}

	encoded := EncodeNodeMetadata(meta)
	decoded, err := DecodeNodeMetadata(encoded, dbid)
	if err != nil {
		t.Fatalf("DecodeNodeMetadata failed: %v", err)
	}

	if decoded.DBID != dbid {
		t.Errorf("expected DBID %s, got %s", dbid, decoded.DBID)
	}
	if decoded.Capabilities != CapZstd {
		t.Errorf("expected Caps %d, got %d", CapZstd, decoded.Capabilities)
	}
	if decoded.Endpoint != "192.0.2.1:8443" {
		t.Errorf("expected endpoint %s, got %s", "192.0.2.1:8443", decoded.Endpoint)
	}

	// Test DBID mismatch rejection
	wrongDBID := ids.NewDBID()
	if _, err := DecodeNodeMetadata(encoded, wrongDBID); err == nil {
		t.Error("expected error for mismatching DBID, got nil")
	}

	// Test truncated payload
	if _, err := DecodeNodeMetadata(encoded[:10], dbid); err == nil {
		t.Error("expected error for truncated payload, got nil")
	}

	// Test invalid magic
	badMagic := make([]byte, len(encoded))
	copy(badMagic, encoded)
	badMagic[0] = 0
	if _, err := DecodeNodeMetadata(badMagic, dbid); err == nil {
		t.Error("expected error for invalid magic, got nil")
	}
}

func TestMembershipServiceDiscovery(t *testing.T) {
	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dbid := ids.NewDBID()

	nodeA := ids.NewNodeID()
	certA, keyA, err := ca.IssueNode(nodeA, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	credsA, err := transport.CredentialsFromPEM(certA, keyA, ca.CertPEM, nil)
	if err != nil {
		t.Fatal(err)
	}
	trA, err := transport.NewMemberlistTransport(transport.MemberlistTransportConfig{
		LocalNodeID: nodeA,
		DBID:        dbid,
		Creds:       credsA,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Shutdown()

	handlerA := newMockMembershipHandler()
	svcA, err := NewMembershipService(
		nodeA,
		dbid,
		MembershipConfig{
			ProbeInterval:  50 * time.Millisecond,
			ProbeTimeout:   40 * time.Millisecond,
			GossipInterval: 20 * time.Millisecond,
		},
		trA,
		handlerA,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer svcA.Shutdown()

	nodeB := ids.NewNodeID()
	certB, keyB, err := ca.IssueNode(nodeB, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	credsB, err := transport.CredentialsFromPEM(certB, keyB, ca.CertPEM, nil)
	if err != nil {
		t.Fatal(err)
	}
	trB, err := transport.NewMemberlistTransport(transport.MemberlistTransportConfig{
		LocalNodeID: nodeB,
		DBID:        dbid,
		Creds:       credsB,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Shutdown()

	handlerB := newMockMembershipHandler()
	svcB, err := NewMembershipService(
		nodeB,
		dbid,
		MembershipConfig{
			ProbeInterval:  50 * time.Millisecond,
			ProbeTimeout:   40 * time.Millisecond,
			GossipInterval: 20 * time.Millisecond,
		},
		trB,
		handlerB,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer svcB.Shutdown()

	ipA, portA, err := trA.FinalAdvertiseAddr("", 0)
	if err != nil {
		t.Fatal(err)
	}
	addrA := net.JoinHostPort(ipA.String(), fmt.Sprintf("%d", portA))

	// Node B joins cluster with Node A as seed
	joined, err := svcB.Join([]string{addrA})
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	if joined == 0 {
		t.Fatal("expected at least 1 node joined")
	}

	// Verify mutual discovery via handlers
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if handlerA.hasDiscovered(nodeB) && handlerB.hasDiscovered(nodeA) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !handlerA.hasDiscovered(nodeB) {
		t.Errorf("Node A did not discover Node B")
	}
	if !handlerB.hasDiscovered(nodeA) {
		t.Errorf("Node B did not discover Node A")
	}

	// Test graceful leave
	_ = svcB.Leave(500 * time.Millisecond)
	_ = svcB.Shutdown()

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if handlerA.hasLeft(nodeB) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !handlerA.hasLeft(nodeB) {
		t.Errorf("Node A did not observe Node B leaving")
	}
}

func TestMembershipReconciliationAndBootstrapRetry(t *testing.T) {
	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dbid := ids.NewDBID()

	nodeA := ids.NewNodeID()
	certA, keyA, err := ca.IssueNode(nodeA, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	credsA, err := transport.CredentialsFromPEM(certA, keyA, ca.CertPEM, nil)
	if err != nil {
		t.Fatal(err)
	}
	trA, err := transport.NewMemberlistTransport(transport.MemberlistTransportConfig{
		LocalNodeID: nodeA,
		DBID:        dbid,
		Creds:       credsA,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Shutdown()

	ipA, portA, err := trA.FinalAdvertiseAddr("", 0)
	if err != nil {
		t.Fatal(err)
	}
	addrA := net.JoinHostPort(ipA.String(), fmt.Sprintf("%d", portA))

	nodeB := ids.NewNodeID()
	certB, keyB, err := ca.IssueNode(nodeB, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	credsB, err := transport.CredentialsFromPEM(certB, keyB, ca.CertPEM, nil)
	if err != nil {
		t.Fatal(err)
	}
	trB, err := transport.NewMemberlistTransport(transport.MemberlistTransportConfig{
		LocalNodeID: nodeB,
		DBID:        dbid,
		Creds:       credsB,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Shutdown()

	handlerB := newMockMembershipHandler()
	// Node B starts with Bootstrap seed pointing to Node A
	svcB, err := NewMembershipService(
		nodeB,
		dbid,
		MembershipConfig{
			Bootstrap:      []string{addrA},
			ProbeInterval:  50 * time.Millisecond,
			ProbeTimeout:   40 * time.Millisecond,
			GossipInterval: 20 * time.Millisecond,
		},
		trB,
		handlerB,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer svcB.Shutdown()

	// Now start Node A
	handlerA := newMockMembershipHandler()
	svcA, err := NewMembershipService(
		nodeA,
		dbid,
		MembershipConfig{
			ProbeInterval:  50 * time.Millisecond,
			ProbeTimeout:   40 * time.Millisecond,
			GossipInterval: 20 * time.Millisecond,
		},
		trA,
		handlerA,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer svcA.Shutdown()

	// Node B's background bootstrap retry should automatically connect to Node A
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if svcA.NumMembers() >= 2 && svcB.NumMembers() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if num := svcA.NumMembers(); num < 2 {
		t.Errorf("Node A expected at least 2 members via bootstrap retry, got %d", num)
	}
	if num := svcB.NumMembers(); num < 2 {
		t.Errorf("Node B expected at least 2 members via bootstrap retry, got %d", num)
	}
}

// Silence unused import warnings if any
var _ = tls.VersionTLS13
