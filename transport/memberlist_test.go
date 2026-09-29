package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/marcgauthier/spedsql/ids"
)

type testCertAuthority struct {
	ca *CA
}

func TestMemberlistQUICRemainsResponsiveWithReplicationSlotsSaturated(t *testing.T) {
	ca := newTestCA(t)
	dbID := ids.NewDBID()
	nodeA, nodeB := ids.NewNodeID(), ids.NewNodeID()
	pool, err := NewPool(PoolOptions{MaxConnections: 16, ReservedMembership: 8, MaxReplicationSessions: 8, Fanout: 3, MaxConcurrentRepairs: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for i := 0; i < 3; i++ {
		if err := pool.AdmitInbound(ids.NewNodeID(), PurposeSelectedTarget); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.AdmitInbound(ids.NewNodeID(), PurposeRepair); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := pool.AdmitInbound(ids.NewNodeID(), PurposeInboundReplication); err != nil {
			t.Fatal(err)
		}
	}

	trA, err := NewMemberlistTransport(MemberlistTransportConfig{LocalNodeID: nodeA, DBID: dbID, Creds: ca.issueNodeCreds(t, nodeA), BindAddr: "127.0.0.1:0", Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Shutdown()
	trB, err := NewMemberlistTransport(MemberlistTransportConfig{LocalNodeID: nodeB, DBID: dbID, Creds: ca.issueNodeCreds(t, nodeB), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	address := memberlist.Address{Addr: trB.listener.Addr(), Name: nodeB.String()}
	if _, err := trA.WriteToAddress([]byte("probe"), address); err != nil {
		t.Fatalf("SWIM session dial blocked by bulk replication: %v", err)
	}
	select {
	case packet := <-trB.PacketCh():
		if string(packet.Buf) != "probe" {
			t.Fatalf("datagram = %q", packet.Buf)
		}
	case <-ctx.Done():
		t.Fatal("SWIM datagram did not make progress while replication slots were saturated")
	}
	stats := pool.Stats()
	if stats.ActiveConnections > stats.MaxConnections || stats.TotalSessions != 8 {
		t.Fatalf("membership traffic exceeded caps or consumed a replication slot: %+v", stats)
	}
}

func newTestCA(t *testing.T) *testCertAuthority {
	t.Helper()
	ca, err := GenerateCA(24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return &testCertAuthority{ca: ca}
}

func (tca *testCertAuthority) issueNodeCreds(t *testing.T, nodeID ids.NodeID) *Credentials {
	t.Helper()
	certPEM, keyPEM, err := tca.ca.IssueNode(nodeID, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := CredentialsFromPEM(certPEM, keyPEM, tca.ca.CertPEM, nil)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

// TestReplAdoptionYieldsMembershipAcceptLoop proves exactly-once stream
// acceptance per connection: a memberlist-tracked connection runs one
// membership AcceptStream consumer, and replication adoption yields it
// synchronously (replication forwards membership streams back instead).
// Two consumers split streams at random; a data stream the membership
// loop won was dropped as a header mismatch while the sender's writes
// kept succeeding into the half-closed stream — a silent permanent
// replication stall after SWIM discovery.
func TestReplAdoptionYieldsMembershipAcceptLoop(t *testing.T) {
	ca := newTestCA(t)
	dbid := ids.NewDBID()
	nodeA, nodeB := ids.NewNodeID(), ids.NewNodeID()
	trA, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: nodeA, DBID: dbid, Creds: ca.issueNodeCreds(t, nodeA),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sessB, err := Dial(ctx, trA.listener.Addr(), ca.issueNodeCreds(t, nodeB), nodeA)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sessB.Close()

	// The accept loop must have registered the connection with a
	// running membership consumer; without this the yield below would
	// pass vacuously.
	deadline := time.Now().Add(5 * time.Second)
	for trA.streamLoopsRunning.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("membership accept loop never started for dialed connection")
		}
		time.Sleep(10 * time.Millisecond)
	}
	trA.mu.Lock()
	serverSess := trA.sessions[nodeB]
	trA.mu.Unlock()
	if serverSess == nil {
		t.Fatal("accepted connection not tracked")
	}

	// Adoption (as replication attach performs) yields synchronously:
	// at return no membership consumer accepts on the connection.
	trA.RegisterSession(serverSess)
	if got := trA.streamLoopsRunning.Load(); got != 0 {
		t.Fatalf("stream loops running after adoption = %d, want 0", got)
	}
	// Idempotent: repeat adoption changes nothing.
	trA.RegisterSession(serverSess)
	if got := trA.streamLoopsRunning.Load(); got != 0 {
		t.Fatalf("stream loops running after re-adoption = %d, want 0", got)
	}
}

func TestMemberlistTransportFinalAdvertiseAddr(t *testing.T) {
	ca := newTestCA(t)
	nodeID := ids.NewNodeID()
	creds := ca.issueNodeCreds(t, nodeID)

	tr, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID:   nodeID,
		DBID:          ids.NewDBID(),
		Creds:         creds,
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:9999",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Shutdown()

	ip, port, err := tr.FinalAdvertiseAddr("127.0.0.1", 8080)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ip.Equal(net.IPv4(127, 0, 0, 1)) || port != 8080 {
		t.Fatalf("expected 127.0.0.1:8080, got %s:%d", ip, port)
	}

	ip2, port2, err := tr.FinalAdvertiseAddr("", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ip2.Equal(net.IPv4(127, 0, 0, 1)) || port2 != 9999 {
		t.Fatalf("expected 127.0.0.1:9999 from AdvertiseAddr, got %s:%d", ip2, port2)
	}
}

func TestMemberlistTransportDatagramRoundtrip(t *testing.T) {
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

	nodeB := ids.NewNodeID()
	credsB := ca.issueNodeCreds(t, nodeB)
	trB, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: nodeB,
		DBID:        dbid,
		Creds:       credsB,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Shutdown()

	addrA := trA.listener.Addr()
	addrB := trB.listener.Addr()

	// Send datagram from B to A
	payload := []byte("ping-swim-probe-message")
	sendTime, err := trB.WriteToAddress(payload, memberlist.Address{Addr: addrA, Name: nodeA.String()})
	if err != nil {
		t.Fatalf("WriteToAddress failed: %v", err)
	}
	if sendTime.IsZero() {
		t.Fatal("expected non-zero sendTime")
	}

	// Receive on A
	select {
	case pkt := <-trA.PacketCh():
		if string(pkt.Buf) != string(payload) {
			t.Fatalf("expected payload %q, got %q", payload, pkt.Buf)
		}
		if pkt.Timestamp.Before(sendTime) {
			t.Fatalf("packet receive timestamp %v before send timestamp %v", pkt.Timestamp, sendTime)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for packet on node A")
	}

	// Send datagram from A to B via WriteTo
	payload2 := []byte("ack-swim-probe-response")
	_, err = trA.WriteTo(payload2, addrB)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	select {
	case pkt := <-trB.PacketCh():
		if string(pkt.Buf) != string(payload2) {
			t.Fatalf("expected payload %q, got %q", payload2, pkt.Buf)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for packet on node B")
	}
}

func TestMemberlistTransportDatagramSizeLimits(t *testing.T) {
	ca := newTestCA(t)
	dbid := ids.NewDBID()

	nodeA := ids.NewNodeID()
	credsA := ca.issueNodeCreds(t, nodeA)
	trA, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID:   nodeA,
		DBID:          dbid,
		Creds:         credsA,
		BindAddr:      "127.0.0.1:0",
		MaxPacketSize: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Shutdown()

	oversized := make([]byte, 501)
	_, err = trA.WriteToAddress(oversized, memberlist.Address{Addr: "127.0.0.1:9999", Name: ids.NewNodeID().String()})
	if err == nil {
		t.Fatal("expected error for oversized packet, got nil")
	}
}

func TestMemberlistTransportStreamRoundtrip(t *testing.T) {
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

	nodeB := ids.NewNodeID()
	credsB := ca.issueNodeCreds(t, nodeB)
	trB, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: nodeB,
		DBID:        dbid,
		Creds:       credsB,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Shutdown()

	addrA := trA.listener.Addr()

	connCh := make(chan net.Conn, 1)
	go func() {
		select {
		case c := <-trA.StreamCh():
			connCh <- c
		case <-time.After(3 * time.Second):
			connCh <- nil
		}
	}()

	clientConn, err := trB.DialAddressTimeout(memberlist.Address{Addr: addrA, Name: nodeA.String()}, 3*time.Second)
	if err != nil {
		t.Fatalf("DialAddressTimeout failed: %v", err)
	}
	defer clientConn.Close()

	serverConn := <-connCh
	if serverConn == nil {
		t.Fatal("timed out waiting for incoming stream on node A")
	}
	defer serverConn.Close()

	// Write from client to server
	msg := []byte("hello-push-pull-sync")
	if _, err := clientConn.Write(msg); err != nil {
		t.Fatalf("client write failed: %v", err)
	}

	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(serverConn, buf); err != nil {
		t.Fatalf("server read failed: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("expected %q, got %q", msg, buf)
	}

	// Write response from server to client
	reply := []byte("push-pull-reply-data")
	if _, err := serverConn.Write(reply); err != nil {
		t.Fatalf("server write failed: %v", err)
	}

	replyBuf := make([]byte, len(reply))
	if _, err := io.ReadFull(clientConn, replyBuf); err != nil {
		t.Fatalf("client read failed: %v", err)
	}
	if string(replyBuf) != string(reply) {
		t.Fatalf("expected %q, got %q", reply, replyBuf)
	}

	// Test deadlines
	_ = clientConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	var dummy [1]byte
	_, err = clientConn.Read(dummy[:])
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestMemberlistTransportRejectsDBIDMismatch(t *testing.T) {
	ca := newTestCA(t)
	dbidA := ids.NewDBID()
	dbidB := ids.NewDBID() // different DBID

	nodeA := ids.NewNodeID()
	credsA := ca.issueNodeCreds(t, nodeA)
	trA, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: nodeA,
		DBID:        dbidA,
		Creds:       credsA,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trA.Shutdown()

	nodeB := ids.NewNodeID()
	credsB := ca.issueNodeCreds(t, nodeB)
	trB, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: nodeB,
		DBID:        dbidB,
		Creds:       credsB,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Shutdown()

	addrA := trA.listener.Addr()

	// Datagram with different DBID
	_, err = trB.WriteToAddress([]byte("wrong-dbid-packet"), memberlist.Address{Addr: addrA, Name: nodeA.String()})
	if err != nil {
		t.Fatalf("WriteToAddress failed: %v", err)
	}

	select {
	case pkt := <-trA.PacketCh():
		t.Fatalf("unexpected packet received with wrong DBID: %v", pkt)
	case <-time.After(200 * time.Millisecond):
		// Expected: packet dropped due to DBID mismatch
	}

	// Stream with different DBID
	conn, err := trB.DialAddressTimeout(memberlist.Address{Addr: addrA, Name: nodeA.String()}, 2*time.Second)
	if err == nil {
		defer conn.Close()
	}

	select {
	case c := <-trA.StreamCh():
		defer c.Close()
		t.Fatalf("unexpected stream accepted with wrong DBID: %v", c)
	case <-time.After(200 * time.Millisecond):
		// Expected: stream rejected
	}
}

func TestMemberlistTransportHashiCorpMemberlistIntegration(t *testing.T) {
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

	nodeB := ids.NewNodeID()
	credsB := ca.issueNodeCreds(t, nodeB)
	trB, err := NewMemberlistTransport(MemberlistTransportConfig{
		LocalNodeID: nodeB,
		DBID:        dbid,
		Creds:       credsB,
		BindAddr:    "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer trB.Shutdown()

	cfgA := memberlist.DefaultWANConfig()
	cfgA.Name = nodeA.String()
	cfgA.Transport = trA
	cfgA.GossipInterval = 20 * time.Millisecond
	cfgA.ProbeInterval = 50 * time.Millisecond
	cfgA.ProbeTimeout = 40 * time.Millisecond
	cfgA.LogOutput = io.Discard

	mlA, err := memberlist.Create(cfgA)
	if err != nil {
		t.Fatalf("memberlist.Create A: %v", err)
	}
	defer mlA.Shutdown()

	cfgB := memberlist.DefaultWANConfig()
	cfgB.Name = nodeB.String()
	cfgB.Transport = trB
	cfgB.GossipInterval = 20 * time.Millisecond
	cfgB.ProbeInterval = 50 * time.Millisecond
	cfgB.ProbeTimeout = 40 * time.Millisecond
	cfgB.LogOutput = io.Discard

	mlB, err := memberlist.Create(cfgB)
	if err != nil {
		t.Fatalf("memberlist.Create B: %v", err)
	}
	defer mlB.Shutdown()

	addrA := trA.listener.Addr()
	// Node B joins cluster using Node A's address
	_, err = mlB.Join([]string{addrA})
	if err != nil {
		t.Fatalf("memberlist.Join: %v", err)
	}

	// Verify both nodes see each other
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if mlA.NumMembers() >= 2 && mlB.NumMembers() >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if num := mlA.NumMembers(); num < 2 {
		t.Errorf("Node A expected at least 2 members, got %d", num)
	}
	if num := mlB.NumMembers(); num < 2 {
		t.Errorf("Node B expected at least 2 members, got %d", num)
	}
}
