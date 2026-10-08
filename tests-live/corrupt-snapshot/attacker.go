// Malicious snapshot-source fixture: a raw QUIC client speaking the
// replication wire protocol. It tricks a stale node into pulling a
// snapshot from it, then serves structurally valid snapshot chunks with
// bit-flipped cell bytes (the manifest digest covers the uncorrupted
// bytes, so completion must fail digest validation).
package corruptsnapshot_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/tests-live/harness"
	"github.com/marcgauthier/murmur/transport"
)

// attacker holds credentials for one malicious peer identity.
type attacker struct {
	id    ids.NodeID
	dbid  ids.DBID
	creds *transport.Credentials
}

func newAttacker(t *testing.T, cluster *harness.Cluster) *attacker {
	t.Helper()
	id := ids.NewNodeID()
	certPEM, keyPEM, err := cluster.CA.IssueNode(id, time.Hour)
	if err != nil {
		t.Fatalf("issue attacker cert: %v", err)
	}
	creds, err := transport.CredentialsFromPEM(certPEM, keyPEM, cluster.CA.CertPEM, nil)
	if err != nil {
		t.Fatalf("attacker credentials: %v", err)
	}
	return &attacker{id: id, dbid: cluster.DBID, creds: creds}
}

// evilSession is one attack QUIC session with its handshake stream open.
type evilSession struct {
	sess    *transport.Session
	stream  *quic.Stream
	welcome *replication.Hello
}

func (s *evilSession) close() {
	if s.stream != nil {
		_ = s.stream.Close()
	}
	if s.sess != nil {
		_ = s.sess.Close()
	}
}

// dialMismatch completes a handshake advertising an unknown schema and
// returns the session plus the node's Welcome (which reveals the node's
// real schema identity and watermarks for the forged snapshot).
func (a *attacker) dialMismatch(t *testing.T, addr string, expect ids.NodeID) *evilSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sess, err := transport.Dial(ctx, addr, a.creds, expect)
	if err != nil {
		t.Fatalf("attacker dial %s: %v", addr, err)
	}
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		_ = sess.Close()
		t.Fatalf("attacker open stream: %v", err)
	}
	hello := &replication.Hello{
		ProtocolVersion:     replication.ProtocolVersion,
		MinProtocolVersion:  replication.MinProtocolVersion,
		NodeID:              a.id,
		DBID:                a.dbid,
		SchemaEpoch:         9999,
		SchemaHash:          randomHash(),
		Capabilities:        replication.CapMergePolicies | replication.CapCompression | replication.CapOriginSignatures,
		MaxTransactionBytes: 64 << 20,
	}
	if err := replication.WriteFrame(stream, replication.MsgHello, 0, replication.EncodeHello(nil, hello)); err != nil {
		_ = stream.Close()
		_ = sess.Close()
		t.Fatalf("attacker write hello: %v", err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	fr, err := replication.ReadFrame(stream)
	if err != nil {
		_ = stream.Close()
		_ = sess.Close()
		t.Fatalf("attacker read welcome: %v", err)
	}
	if fr.Type != replication.MsgWelcome {
		_ = stream.Close()
		_ = sess.Close()
		t.Fatalf("attacker expected welcome, got frame type %d", fr.Type)
	}
	welcome, err := replication.DecodeHello(fr.Payload)
	if err != nil {
		_ = stream.Close()
		_ = sess.Close()
		t.Fatalf("attacker decode welcome: %v", err)
	}
	return &evilSession{sess: sess, stream: stream, welcome: welcome}
}

func (s *evilSession) send(t *testing.T, typ uint16, flags uint16, payload []byte) {
	t.Helper()
	_ = s.stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := replication.WriteFrame(s.stream, typ, flags, payload); err != nil {
		t.Fatalf("attacker send type %d: %v", typ, err)
	}
}

// collectUntil reads until want matches, total elapses, or the stream dies.
func (s *evilSession) collectUntil(total time.Duration, want func(*replication.Frame) bool) []*replication.Frame {
	var out []*replication.Frame
	deadline := time.Now().Add(total)
	for time.Now().Before(deadline) {
		_ = s.stream.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		fr, err := replication.ReadFrame(s.stream)
		if err != nil {
			return out
		}
		out = append(out, fr)
		if want(fr) {
			return out
		}
	}
	return out
}

func (s *evilSession) openStream(t *testing.T) *quic.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := s.sess.OpenStream(ctx)
	if err != nil {
		t.Fatalf("attacker open snapshot stream: %v", err)
	}
	return stream
}

// corruptSnapshot is a manifest plus chunk payloads whose cell bytes were
// bit-flipped after the manifest digest was computed.
type corruptSnapshot struct {
	manifest *codec.SnapshotManifest
	chunk0   []byte // intact first chunk payload
	chunk1   []byte // bit-flipped final chunk payload
}

// buildCorruptSnapshot forges a structurally valid two-chunk snapshot for
// the victim's schema: the manifest passes every structural check and the
// declared sizes match, but the final chunk's bytes differ from what the
// manifest digest covers.
func buildCorruptSnapshot(t *testing.T, a *attacker, welcome *replication.Hello) *corruptSnapshot {
	t.Helper()
	mkCells := func(tag string) []codec.SnapshotCell {
		var cells []codec.SnapshotCell
		for i := 0; i < 3; i++ {
			cells = append(cells, codec.SnapshotCell{
				TableID:  1,
				RowID:    ids.NewRowID(),
				ColumnID: 2,
				Version:  versionFor(a.id, uint64(1000+i)),
				Value:    codec.Text(tag + string(rune('0'+i))),
			})
		}
		return cells
	}
	cells0, cells1 := mkCells("corrupt-a"), mkCells("corrupt-b")
	raw0 := codec.EncodeSnapshotCells(nil, cells0)
	raw1 := codec.EncodeSnapshotCells(nil, cells1)

	wms := append([]codec.OriginWatermark(nil), welcome.Have...)
	if len(wms) == 0 {
		wms = []codec.OriginWatermark{{Origin: a.id, Sequence: 1}}
	}
	for i := range wms {
		wms[i].Sequence += 1000
	}
	sort.Slice(wms, func(i, j int) bool { return bytes.Compare(wms[i].Origin[:], wms[j].Origin[:]) < 0 })

	manifest := &codec.SnapshotManifest{
		FormatVersion:   3,
		SnapshotID:      ids.NewTxID(),
		DBID:            a.dbid,
		SchemaEpoch:     welcome.SchemaEpoch,
		SchemaHash:      welcome.SchemaHash,
		CreatedHLC:      (uint64(1) << 48),
		StateGeneration: 7,
		ChunkCount:      2,
		EncodedBytes:    uint64(len(raw0) + len(raw1)),
		Watermarks:      wms,
	}
	// Digest algorithm mirrors state/snapshot.go ImportSnapshotChunk: the
	// manifest with a zeroed content hash, then length-prefixed staged
	// chunk encodings.
	manifest.ContentHash = snapshotDigest(manifest, [][]byte{raw0, raw1})

	// Non-vacuous guards: the uncorrupted bytes verify against the
	// manifest, and the corrupted bytes must not.
	if got := snapshotDigest(manifest, [][]byte{raw0, raw1}); got != manifest.ContentHash {
		t.Fatalf("uncorrupted snapshot does not verify locally; fixture is broken")
	}
	wire0 := replication.EncodeSnapshotChunk(nil, &replication.SnapshotChunk{Index: 0, Last: false, Cells: cells0})
	wire1 := replication.EncodeSnapshotChunk(nil, &replication.SnapshotChunk{Index: 1, Last: true, Cells: cells1})
	// Flip a byte inside the last cell's value plaintext. The wire
	// encoding ends with framing (record key), not value bytes, so a
	// trailing flip breaks structural decoding instead of the content
	// digest. An ASCII-to-ASCII flip preserves lengths, framing, and
	// value validity; only the digest differs.
	tag := []byte("corrupt-b2")
	at := bytes.LastIndex(wire1, tag)
	if at < 0 {
		t.Fatalf("value plaintext %q not found in encoded chunk; fixture is broken", tag)
	}
	wire1[at] ^= 0x01 // 'c' -> 'b'
	bad, err := replication.DecodeSnapshotChunk(mustDecompress(t, wire1), codec.DefaultLimits(), 1<<20)
	if err != nil {
		t.Fatalf("corrupted chunk does not decode locally: %v", err)
	}
	badRaw := codec.EncodeSnapshotCells(nil, bad.Cells)
	if len(badRaw) != len(raw1) {
		t.Fatalf("corruption changed encoded length %d -> %d", len(raw1), len(badRaw))
	}
	if got := snapshotDigest(manifest, [][]byte{raw0, badRaw}); got == manifest.ContentHash {
		t.Fatalf("corrupted snapshot still verifies locally; corruption is vacuous")
	}
	return &corruptSnapshot{manifest: manifest, chunk0: wire0, chunk1: wire1}
}

// snapshotDigest recomputes the content digest the receiver verifies.
func snapshotDigest(manifest *codec.SnapshotManifest, chunks [][]byte) [32]byte {
	h := sha256.New()
	mcopy := *manifest
	mcopy.ContentHash = [32]byte{}
	h.Write(codec.EncodeManifest(nil, &mcopy))
	for _, raw := range chunks {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(raw)))
		h.Write(n[:])
		h.Write(raw)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func mustDecompress(t *testing.T, wire []byte) []byte {
	t.Helper()
	return wire // chunks are served uncompressed; helper pins the assumption
}

func versionFor(node ids.NodeID, hlc uint64) crdt.Version {
	return crdt.Version{HLC: hlc, NodeID: node}
}

func randomHash() (h [32]byte) {
	for i := range h {
		h[i] = byte(0xE0 + i)
	}
	return h
}
