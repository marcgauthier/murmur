// Malicious replication-peer fixture: a raw QUIC client speaking the
// replication wire protocol (framing via replication.WriteFrame/ReadFrame,
// no product changes). It authenticates with a CA-issued certificate like
// any peer but sends forged batches, replays handshakes, claims absurd
// frame lengths, and delivers compressed snapshot bombs.
package hostilepeer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

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

// evilSession is one attack QUIC session: the handshake stream stays open
// for further attack frames and for observing node replies.
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

// dialMismatch completes a handshake advertising a schema the node does not
// have. The session attaches (schema sync is enabled) but stays data-gated
// (agreed=false), so an honest node must send this peer no data frames.
func (a *attacker) dialMismatch(t *testing.T, addr string, expect ids.NodeID) *evilSession {
	t.Helper()
	return a.dial(t, addr, expect, 9999, randomHash())
}

// dial opens a session advertising the given schema identity and returns
// the attached session plus the node's Welcome (which reveals the node's
// real schema epoch/hash for matched-schema probes).
func (a *attacker) dial(t *testing.T, addr string, expect ids.NodeID, epoch uint64, hash [32]byte) *evilSession {
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
		SchemaEpoch:         epoch,
		SchemaHash:          hash,
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

// send writes one frame on the session control stream.
func (s *evilSession) send(t *testing.T, typ uint16, flags uint16, payload []byte) {
	t.Helper()
	_ = s.stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := replication.WriteFrame(s.stream, typ, flags, payload); err != nil {
		t.Fatalf("attacker send type %d: %v", typ, err)
	}
}

// collect reads inbound frames until total elapses or the stream dies,
// returning everything the node sent back.
func (s *evilSession) collect(total time.Duration) []*replication.Frame {
	var out []*replication.Frame
	deadline := time.Now().Add(total)
	for time.Now().Before(deadline) {
		_ = s.stream.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		fr, err := replication.ReadFrame(s.stream)
		if err != nil {
			return out
		}
		out = append(out, fr)
	}
	return out
}

// collectUntil reads until want returns true for a frame, total elapses, or
// the stream dies, returning all frames seen (including the match).
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

func isPong(nonce []byte) func(*replication.Frame) bool {
	return func(fr *replication.Frame) bool {
		if fr.Type != replication.MsgPong || len(fr.Payload) != len(nonce) {
			return false
		}
		for i := range nonce {
			if fr.Payload[i] != nonce[i] {
				return false
			}
		}
		return true
	}
}

// dataFrameTypes are frames that must never flow to a data-gated
// (schema-disagreeing) peer.
func dataFrameTypes() map[uint16]string {
	return map[uint16]string{
		replication.MsgBatches:          "Batches",
		replication.MsgSnapshotManifest: "SnapshotManifest",
		replication.MsgSnapshotChunk:    "SnapshotChunk",
		replication.MsgPlumtreeData:     "PlumtreeData",
		replication.MsgTransactionChunk: "TransactionChunk",
		replication.MsgProgressPage:     "ProgressPage",
	}
}

func frameName(typ uint16) string {
	switch typ {
	case replication.MsgHello:
		return "Hello"
	case replication.MsgWelcome:
		return "Welcome"
	case replication.MsgBatches:
		return "Batches"
	case replication.MsgAck:
		return "Ack"
	case replication.MsgNeed:
		return "Need"
	case replication.MsgSnapshotRequest:
		return "SnapshotRequest"
	case replication.MsgSnapshotManifest:
		return "SnapshotManifest"
	case replication.MsgSnapshotChunk:
		return "SnapshotChunk"
	case replication.MsgSnapshotDone:
		return "SnapshotDone"
	case replication.MsgPing:
		return "Ping"
	case replication.MsgPong:
		return "Pong"
	case replication.MsgError:
		return "Error"
	case replication.MsgSchemaRequest:
		return "SchemaRequest"
	case replication.MsgSchemaManifest:
		return "SchemaManifest"
	case replication.MsgSchemaAck:
		return "SchemaAck"
	default:
		return fmt.Sprintf("type-%d", typ)
	}
}

func randomHash() (h [32]byte) {
	for i := range h {
		h[i] = byte(0xA0 + i)
	}
	return h
}
