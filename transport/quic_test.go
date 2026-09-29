package transport

import (
	"context"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/ids"
)

func testCreds(t *testing.T, ca *CA, id ids.NodeID, allowed []ids.NodeID) *Credentials {
	t.Helper()
	certPEM, keyPEM, err := ca.IssueNode(id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := CredentialsFromPEM(certPEM, keyPEM, ca.CertPEM, allowed)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

func TestNodeBindingLoopback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	aCreds := testCreds(t, ca, aID, nil)
	bCreds := testCreds(t, ca, bID, nil)

	ln, err := Listen("127.0.0.1:0", aCreds)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

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

	dialed, err := Dial(ctx, ln.Addr(), bCreds, aID)
	if err != nil {
		t.Fatal(err)
	}
	defer dialed.Close()
	if dialed.Peer != aID {
		t.Fatalf("dialer sees peer %s, want %s", dialed.Peer, aID)
	}
	var accepted *Session
	select {
	case accepted = <-acceptCh:
	case err := <-errCh:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("accept timeout")
	}
	defer accepted.Close()
	if accepted.Peer != bID {
		t.Fatalf("listener sees peer %s, want %s", accepted.Peer, bID)
	}

	// Bidirectional stream echo proves the connection is usable.
	srvStreamCh := make(chan streamResult, 1)
	go func() {
		st, err := accepted.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 5)
		n := 0
		for n < 5 {
			m, err := st.Read(buf[n:])
			if err != nil {
				return
			}
			n += m
		}
		_, _ = st.Write(buf)
		srvStreamCh <- streamResult{}
	}()
	cli, err := dialed.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	echo := make([]byte, 5)
	n := 0
	for n < 5 {
		m, err := cli.Read(echo[n:])
		if err != nil {
			t.Fatal(err)
		}
		n += m
	}
	if string(echo) != "hello" {
		t.Fatalf("echo = %q", echo)
	}
	select {
	case <-srvStreamCh:
	case <-ctx.Done():
		t.Fatal("server stream timeout")
	}
}

type streamResult struct{}

func TestWrongPeerRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID, cID := ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()
	aCreds := testCreds(t, ca, aID, nil)
	bCreds := testCreds(t, ca, bID, nil)

	ln, err := Listen("127.0.0.1:0", aCreds)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _, _ = ln.Accept(ctx) }()

	// B dials A but expects C: must fail.
	if _, err := Dial(ctx, ln.Addr(), bCreds, cID); err == nil {
		t.Fatal("expected dial to fail on identity mismatch")
	}
}

func TestUntrustedCARjected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca1, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca2, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	aCreds := testCreds(t, ca1, aID, nil)
	bCreds := testCreds(t, ca2, bID, nil) // different CA

	ln, err := Listen("127.0.0.1:0", aCreds)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _, _ = ln.Accept(ctx) }()

	if _, err := Dial(ctx, ln.Addr(), bCreds, aID); err == nil {
		t.Fatal("expected dial to fail across CAs")
	}
}

func TestExpiredCertRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	aCreds := testCreds(t, ca, aID, nil)
	// B's certificate is already expired (negative TTL).
	bCertPEM, bKeyPEM, err := ca.IssueNode(bID, -time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bCreds, err := CredentialsFromPEM(bCertPEM, bKeyPEM, ca.CertPEM, nil)
	if err != nil {
		t.Fatal(err)
	}

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
		// Server closed the expired-cert connection.
	case <-time.After(5 * time.Second):
		t.Fatal("expired-cert connection stayed open")
	}
}

func TestAllowList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID := ids.NewNodeID(), ids.NewNodeID()
	// A allows only itself; B is rejected at the TLS layer.
	aCreds := testCreds(t, ca, aID, []ids.NodeID{aID})
	bCreds := testCreds(t, ca, bID, nil)

	ln, err := Listen("127.0.0.1:0", aCreds)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() { _, _ = ln.Accept(ctx) }()

	// Server-side rejection is asynchronous in QUIC (1-RTT): Dial may
	// succeed while the server closes the connection.
	sess, err := Dial(ctx, ln.Addr(), bCreds, aID)
	if err != nil {
		return // rejected synchronously: also fine
	}
	defer sess.Close()
	select {
	case <-sess.Context().Done():
		// Server closed the unauthorized connection.
	case <-time.After(5 * time.Second):
		t.Fatal("unauthorized connection stayed open")
	}
}
