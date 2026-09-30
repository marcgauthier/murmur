package filefetch

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/objectstore"
	"github.com/marcgauthier/murmur/transport"
)

func fetchTestPair(t *testing.T) (srvCreds, cliCreds *transport.Credentials, nodeA, nodeB ids.NodeID, dbid ids.DBID) {
	t.Helper()
	var err error
	nodeA = ids.NewNodeID()
	nodeB = ids.NewNodeID()
	dbid = ids.NewDBID()
	ca, err := transport.GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	mkCreds := func(n ids.NodeID) *transport.Credentials {
		t.Helper()
		certPEM, keyPEM, err := ca.IssueNode(n, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		c, err := transport.CredentialsFromPEM(certPEM, keyPEM, ca.CertPEM, nil)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	return mkCreds(nodeA), mkCreds(nodeB), nodeA, nodeB, dbid
}

func fetchTestKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func fetchTestStore(t *testing.T, key, data []byte) (*objectstore.Store, objectstore.Info) {
	t.Helper()
	st, err := objectstore.New(t.TempDir(), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	info, err := st.Put(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return st, info
}

func TestFetchRoundTrip(t *testing.T) {
	ctx := context.Background()
	srvCreds, cliCreds, nodeA, _, dbid := fetchTestPair(t)
	data := make([]byte, 200_000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	key := fetchTestKey(t)
	st, info := fetchTestStore(t, key, data)

	srv, err := NewServer(ServerConfig{Objects: st, Creds: srvCreds, DBID: dbid, Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli := NewClient(cliCreds)
	stream, err := cli.Fetch(ctx, srv.Addr(), nodeA, dbid, info.Digest, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stream)
	_ = stream.Close()
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(got)) != stream.ContainerLen() {
		t.Fatalf("streamed %d bytes, declared %d", len(got), stream.ContainerLen())
	}
	// The streamed container must verify and install as the object.
	staged := filepath.Join(t.TempDir(), "obj.part")
	if err := os.WriteFile(staged, got, 0600); err != nil {
		t.Fatal(err)
	}
	dst, err := objectstore.New(t.TempDir(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	installed, err := dst.InstallVerified(ctx, staged, info.Digest, info.Length)
	if err != nil {
		t.Fatalf("streamed container failed verification: %v", err)
	}
	if installed != info {
		t.Fatalf("installed %+v, want %+v", installed, info)
	}
	stats := srv.Stats()
	if stats.Served != 1 || stats.BytesOut != uint64(len(got)) {
		t.Fatalf("server stats %+v", stats)
	}
}

func TestFetchResumeOffset(t *testing.T) {
	ctx := context.Background()
	srvCreds, cliCreds, nodeA, _, dbid := fetchTestPair(t)
	data := make([]byte, 150_000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	st, info := fetchTestStore(t, fetchTestKey(t), data)
	srv, err := NewServer(ServerConfig{Objects: st, Creds: srvCreds, DBID: dbid, Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli := NewClient(cliCreds)
	full, err := cli.Fetch(ctx, srv.Addr(), nodeA, dbid, info.Digest, 0)
	if err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(full)
	_ = full.Close()
	if err != nil {
		t.Fatal(err)
	}
	half := uint64(len(all) / 2)
	part, err := cli.Fetch(ctx, srv.Addr(), nodeA, dbid, info.Digest, half)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := io.ReadAll(part)
	_ = part.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tail, all[half:]) {
		t.Fatal("resumed tail differs from full-stream suffix")
	}
	if part.ContainerLen() != uint64(len(all)) {
		t.Fatal("resumed header must still declare the full length")
	}
	if _, err := cli.Fetch(ctx, srv.Addr(), nodeA, dbid, info.Digest, uint64(len(all))+1); !errors.Is(err, ErrBadOffset) {
		t.Fatalf("oversize offset: got %v, want ErrBadOffset", err)
	}
}

func TestFetchRefusals(t *testing.T) {
	ctx := context.Background()
	srvCreds, cliCreds, nodeA, _, dbid := fetchTestPair(t)
	st, info := fetchTestStore(t, fetchTestKey(t), []byte("refusable"))
	srv, err := NewServer(ServerConfig{Objects: st, Creds: srvCreds, DBID: dbid, Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	cli := NewClient(cliCreds)
	var missing objectstore.Digest
	missing[0] = info.Digest[0] ^ 0xff
	if _, err := cli.Fetch(ctx, srv.Addr(), nodeA, dbid, missing, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing digest: got %v, want ErrNotFound", err)
	}
	otherDB := ids.NewDBID()
	if _, err := cli.Fetch(ctx, srv.Addr(), nodeA, otherDB, info.Digest, 0); !errors.Is(err, ErrRefused) {
		t.Fatalf("wrong DBID: got %v, want ErrRefused", err)
	}
	if got := srv.Stats().Refused; got != 1 {
		t.Fatalf("refused = %d, want 1 (wrong DBID only)", got)
	}
}

func TestFetchWrongPeerIdentity(t *testing.T) {
	ctx := context.Background()
	srvCreds, cliCreds, _, nodeB, dbid := fetchTestPair(t)
	st, info := fetchTestStore(t, fetchTestKey(t), []byte("identity"))
	srv, err := NewServer(ServerConfig{Objects: st, Creds: srvCreds, DBID: dbid, Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// Expecting nodeB while the server presents nodeA must fail the mTLS
	// handshake: no bytes flow to a misidentified peer.
	cli := NewClient(cliCreds)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := cli.Fetch(ctx, srv.Addr(), nodeB, dbid, info.Digest, 0); err == nil {
		t.Fatal("fetch with wrong expected peer succeeded")
	}
}

func TestProtocolRoundTrip(t *testing.T) {
	req := &Request{Offset: 1234}
	copy(req.DBID[:], bytes.Repeat([]byte{1}, 16))
	copy(req.Digest[:], bytes.Repeat([]byte{2}, 32))
	dec, err := DecodeRequest(EncodeRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	if *dec != *req {
		t.Fatal("request round trip mismatch")
	}
	hdr, err := DecodeHeader(EncodeHeader(&Header{Status: StatusOK, ContainerLen: 999}))
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Status != StatusOK || hdr.ContainerLen != 999 {
		t.Fatal("header round trip mismatch")
	}
	if _, err := DecodeRequest([]byte("short")); err == nil {
		t.Fatal("short request decoded")
	}
	detail, err := DecodeError(EncodeError(ErrCodeRefused, "nope"))
	if err != nil {
		t.Fatal(err)
	}
	if detail.Code != ErrCodeRefused || detail.Message != "nope" {
		t.Fatal("error round trip mismatch")
	}
}
