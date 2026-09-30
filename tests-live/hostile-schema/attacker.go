// Malicious schema-peer fixture: a raw QUIC client speaking the
// replication wire protocol. It requests the node's schema manifest, then
// answers with forged revisions (corrupted digests, incompatible versions).
package hostileschema_test

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

// dialMismatch completes a handshake advertising an unknown schema. The
// session attaches but stays data-gated until schemas agree.
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
		Capabilities:        replication.CapZstd,
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

// send writes one frame on the session control stream, failing the test on
// error. Use sendSoft after a probe that may have killed the stream.
func (s *evilSession) send(t *testing.T, typ uint16, flags uint16, payload []byte) {
	t.Helper()
	if err := s.sendSoft(typ, flags, payload); err != nil {
		t.Fatalf("attacker send type %d: %v", typ, err)
	}
}

func (s *evilSession) sendSoft(typ uint16, flags uint16, payload []byte) error {
	_ = s.stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return replication.WriteFrame(s.stream, typ, flags, payload)
}

// collect reads inbound frames until total elapses or the stream dies.
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

// fetchManifest asks the node for its current schema tip and returns the
// served manifest (schema traffic flows even on data-gated sessions).
func (s *evilSession) fetchManifest(t *testing.T) *replication.SchemaManifestMsg {
	t.Helper()
	s.send(t, replication.MsgSchemaRequest, 0,
		replication.EncodeSchemaRequest(nil, &replication.SchemaRequest{WantCurrent: true}))
	for _, fr := range s.collectUntil(15*time.Second, func(fr *replication.Frame) bool {
		return fr.Type == replication.MsgSchemaManifest
	}) {
		if fr.Type == replication.MsgSchemaManifest {
			m, err := replication.DecodeSchemaManifest(fr.Payload)
			if err != nil {
				t.Fatalf("decode served schema manifest: %v", err)
			}
			if len(m.Revisions) == 0 {
				t.Fatalf("served schema manifest has no revisions")
			}
			return m
		}
	}
	t.Fatalf("node served no schema manifest")
	return nil
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

func isSchemaMismatchError(fr *replication.Frame) bool {
	if fr.Type != replication.MsgError {
		return false
	}
	code, _, err := replication.DecodeError(fr.Payload)
	return err == nil && code == replication.ErrSchemaMismatch
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
		h[i] = byte(0xC0 + i)
	}
	return h
}
