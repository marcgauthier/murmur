package replication

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/state"
	"github.com/marcgauthier/spedsql/transport"
)

// fakeApplier records applied batches.
type fakeApplier struct {
	mu      sync.Mutex
	batches []*codec.MutationBatch
	chunks  int
}

func (f *fakeApplier) ApplyRemote(_ context.Context, b *codec.MutationBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, b)
	return nil
}

func (f *fakeApplier) ApplySnapshotChunk(_ context.Context, _ *codec.SnapshotManifest, _ uint64, _ []codec.SnapshotCell, _ bool) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chunks++
	return false, nil
}

type groupedFakeApplier struct {
	fakeApplier
	groups [][]*codec.MutationBatch
}

func (f *groupedFakeApplier) ApplyRemoteGroup(_ context.Context, batches []*codec.MutationBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	group := append([]*codec.MutationBatch(nil), batches...)
	f.groups = append(f.groups, group)
	return nil
}

func (f *fakeApplier) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func TestOnBatchesUsesBoundedAtomicApplyGroups(t *testing.T) {
	store, err := state.Open(t.TempDir(), ids.NewNodeID(), ids.NewDBID(), state.Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	applier := &groupedFakeApplier{}
	m := &Manager{
		cfg: ManagerConfig{Store: store, Applier: applier, Limits: codec.DefaultLimits()},
		ctx: context.Background(), notifyCh: make(chan struct{}, 1), peers: make(map[ids.NodeID]*peerState),
		chunkRepairAt: make(map[ids.TxID]time.Time), chunkRepairNeed: make(map[ids.TxID]ChunkNeed),
		schemaId: SchemaIdentity{Epoch: 1},
	}
	m.applyGroupTarget.Store(2)
	p := newPeerState(ids.NewNodeID(), nil, true)
	origin := ids.NewNodeID()
	batches := []*codec.MutationBatch{
		{ProtocolVersion: ProtocolVersion, TxID: ids.NewTxID(), OriginNode: origin, Sequence: 1, HLC: 10, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1)}}},
		{ProtocolVersion: ProtocolVersion, TxID: ids.NewTxID(), OriginNode: origin, Sequence: 2, HLC: 20, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(2)}}},
	}
	if err := m.onBatches(p, nil, EncodeBatches(nil, batches)); err != nil {
		t.Fatal(err)
	}
	if len(applier.groups) != 1 || len(applier.groups[0]) != 2 {
		t.Fatalf("group calls = %+v", applier.groups)
	}
	if applier.groups[0][0].TxID != batches[0].TxID || applier.groups[0][1].TxID != batches[1].TxID {
		t.Fatal("apply group changed transaction identities or order")
	}
	if got := m.applyGroupLimit(); got != 4 {
		t.Fatalf("successful group did not grow target: %d", got)
	}
}

type testCluster struct {
	ca   *transport.CA
	dbid ids.DBID
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	ca, err := transport.GenerateCA(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return &testCluster{ca: ca, dbid: ids.NewDBID()}
}

func (c *testCluster) creds(t *testing.T, id ids.NodeID) *transport.Credentials {
	t.Helper()
	certPEM, keyPEM, err := c.ca.IssueNode(id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := transport.CredentialsFromPEM(certPEM, keyPEM, c.ca.CertPEM, nil)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

// testManager starts a manager with a live listener and returns its address.
func (c *testCluster) testManager(t *testing.T, node ids.NodeID, epoch uint64) (*Manager, *fakeApplier, string) {
	t.Helper()
	st, err := state.Open(t.TempDir(), node, c.dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	applier := &fakeApplier{}
	var hash [32]byte
	hash[0] = byte(epoch)
	mgr, err := NewManager(ManagerConfig{
		Store: st, Applier: applier, Creds: c.creds(t, node),
		Local: node, DBID: c.dbid, SchemaEpoch: epoch, SchemaHash: hash,
		ListenAddr: "127.0.0.1:0",
		Limits:     codec.Limits{MaxValueBytes: 64, MaxMutations: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = mgr.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if addr := mgr.Addr(); addr != "" {
			return mgr, applier, addr
		}
		if time.Now().After(deadline) {
			t.Fatal("listener never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// rawClient dials and completes the handshake, returning the live stream.
func (c *testCluster) rawClient(t *testing.T, ctx context.Context, addr string,
	self, expect ids.NodeID, epoch uint64) (*transport.Session, *quic.Stream) {
	t.Helper()
	sess, err := transport.Dial(ctx, addr, c.creds(t, self), expect)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var hash [32]byte
	hash[0] = byte(epoch)
	if err := WriteFrame(stream, MsgHello, 0, EncodeHello(nil, &Hello{
		ProtocolVersion: ProtocolVersion, MinProtocolVersion: MinProtocolVersion,
		NodeID: self, DBID: c.dbid, SchemaEpoch: epoch, SchemaHash: hash,
		Capabilities: CapZstd,
	})); err != nil {
		t.Fatal(err)
	}
	fr, err := ReadFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Type != MsgWelcome {
		t.Fatalf("expected welcome, got type %d", fr.Type)
	}
	return sess, stream
}

func validTestBatch(node ids.NodeID, seq uint64) *codec.MutationBatch {
	var hash [32]byte
	hash[0] = 1 // test managers use hash[0] = epoch
	return &codec.MutationBatch{
		ProtocolVersion: ProtocolVersion,
		TxID:            ids.NewTxID(),
		OriginNode:      node,
		Sequence:        seq,
		HLC:             seq * 100,
		SchemaEpoch:     1,
		SchemaHash:      hash,
		Mutations: []codec.Mutation{
			{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("ok")},
		},
	}
}

func TestAdversarialUnknownMessageSurvived(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := newTestCluster(t)
	server, peer := ids.NewNodeID(), ids.NewNodeID()
	_, applier, addr := c.testManager(t, server, 1)
	_, stream := c.rawClient(t, ctx, addr, peer, server, 1)

	// Unknown message type: server answers MsgError, session stays open.
	if err := WriteFrame(stream, 999, 0, []byte{1}); err != nil {
		t.Fatal(err)
	}
	fr, err := ReadFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Type != MsgError {
		t.Fatalf("expected error frame, got %d", fr.Type)
	}
	// Session still live: a valid batch applies.
	if err := WriteFrame(stream, MsgBatches, 0,
		EncodeBatches(nil, []*codec.MutationBatch{validTestBatch(peer, 1)})); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for applier.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("valid batch never applied after unknown message")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAdversarialBadBatchEncodingClosesSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := newTestCluster(t)
	server, peer := ids.NewNodeID(), ids.NewNodeID()
	_, applier, addr := c.testManager(t, server, 1)
	sess, stream := c.rawClient(t, ctx, addr, peer, server, 1)

	// Structurally invalid batch payload: fail closed (session ends), nothing applied.
	if err := WriteFrame(stream, MsgBatches, 0, []byte{0, 0, 0, 1, 0xFF}); err != nil {
		t.Fatal(err)
	}
	// The server closes the stream; reads must fail.
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := ReadFrame(stream); err == nil {
		t.Fatal("expected session close after corrupt batch")
	}
	_ = sess
	if n := applier.count(); n != 0 {
		t.Fatalf("applied %d corrupt batches", n)
	}
	// The manager itself survives: a fresh session replicates fine.
	_, stream2 := c.rawClient(t, ctx, addr, peer, server, 1)
	if err := WriteFrame(stream2, MsgBatches, 0,
		EncodeBatches(nil, []*codec.MutationBatch{validTestBatch(peer, 1)})); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for applier.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("manager did not survive corrupt batch")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAdversarialOversizeValueSkipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := newTestCluster(t)
	server, peer := ids.NewNodeID(), ids.NewNodeID()
	// Manager limits values to 64 bytes (see testManager).
	_, applier, addr := c.testManager(t, server, 1)
	_, stream := c.rawClient(t, ctx, addr, peer, server, 1)

	big := validTestBatch(peer, 1)
	big.Mutations[0].Value = codec.Text(string(make([]byte, 1024)))
	if err := WriteFrame(stream, MsgBatches, 0, EncodeBatches(nil, []*codec.MutationBatch{big})); err != nil {
		t.Fatal(err)
	}
	// Oversize decode fails the frame: session ends, nothing applied.
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := ReadFrame(stream); err == nil {
		t.Fatal("expected session close after oversize batch")
	}
	if n := applier.count(); n != 0 {
		t.Fatalf("applied %d oversize batches", n)
	}
}

func TestAdversarialBadMagicClosesSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := newTestCluster(t)
	server, peer := ids.NewNodeID(), ids.NewNodeID()
	_, _, addr := c.testManager(t, server, 1)
	sess, err := transport.Dial(ctx, addr, c.creds(t, peer), server)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Garbage instead of a handshake: the server must drop the session
	// without affecting the listener.
	if _, err := stream.Write([]byte("garbage-garbage-garbage!")); err != nil {
		t.Fatal(err)
	}
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	if _, err := stream.Read(buf); err == nil {
		// A response is acceptable only if it is an error frame; anything
		// else means the garbage was misparsed.
		t.Fatal("server answered garbage with data")
	}
	// Listener still healthy: a proper handshake works.
	_, stream2 := c.rawClient(t, ctx, addr, peer, server, 1)
	_ = stream2
}

func TestAdversarialOversizeFrameRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := newTestCluster(t)
	server, peer := ids.NewNodeID(), ids.NewNodeID()
	_, _, addr := c.testManager(t, server, 1)
	sess, err := transport.Dial(ctx, addr, c.creds(t, peer), server)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Valid magic but a 4GB length prefix: must fail fast without a huge alloc.
	hdr := []byte{0x52, 0x44, 0, 1, 0, 3, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF}
	if _, err := stream.Write(hdr); err != nil {
		t.Fatal(err)
	}
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := ReadFrame(stream); err == nil {
		t.Fatal("expected rejection of oversize frame")
	}
}

func TestAdversarialInvalidBatchesSkippedSessionLives(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := newTestCluster(t)
	server, peer := ids.NewNodeID(), ids.NewNodeID()
	_, applier, addr := c.testManager(t, server, 1)
	_, stream := c.rawClient(t, ctx, addr, peer, server, 1)

	mkbatch := func(mut func(*codec.MutationBatch)) *codec.MutationBatch {
		b := validTestBatch(peer, 1)
		mut(b)
		return b
	}
	// Each batch is well-framed but invalid: skipped, session survives.
	cases := []*codec.MutationBatch{
		mkbatch(func(b *codec.MutationBatch) { b.SchemaEpoch = 999 }),
		mkbatch(func(b *codec.MutationBatch) { b.Sequence = 0 }),
		mkbatch(func(b *codec.MutationBatch) { b.HLC = 0 }),
		mkbatch(func(b *codec.MutationBatch) { b.ProtocolVersion = 99 }),
		mkbatch(func(b *codec.MutationBatch) { b.OriginNode = ids.NodeID{} }),
	}
	if err := WriteFrame(stream, MsgBatches, 0, EncodeBatches(nil, cases)); err != nil {
		t.Fatal(err)
	}
	// Session alive: ping/pong round-trips.
	if err := WriteFrame(stream, MsgPing, 0, []byte{0, 0, 0, 0, 0, 0, 0, 7}); err != nil {
		t.Fatal(err)
	}
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	fr, err := ReadFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Type != MsgPong || !bytes.Equal(fr.Payload, []byte{0, 0, 0, 0, 0, 0, 0, 7}) {
		t.Fatalf("bad pong: %+v", fr)
	}
	// Give the batch frame time to be (not) applied, then verify nothing was.
	time.Sleep(200 * time.Millisecond)
	if n := applier.count(); n != 0 {
		t.Fatalf("applied %d invalid batches", n)
	}
}

func TestHandshakeWrongDBIDRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := newTestCluster(t)
	server, peer := ids.NewNodeID(), ids.NewNodeID()
	_, _, addr := c.testManager(t, server, 1)

	sess, err := transport.Dial(ctx, addr, c.creds(t, peer), server)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var hash [32]byte
	hash[0] = 1
	if err := WriteFrame(stream, MsgHello, 0, EncodeHello(nil, &Hello{
		ProtocolVersion: ProtocolVersion, MinProtocolVersion: MinProtocolVersion,
		NodeID: peer, DBID: ids.NewDBID(), SchemaEpoch: 1, SchemaHash: hash,
	})); err != nil {
		t.Fatal(err)
	}
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	fr, err := ReadFrame(stream)
	if err != nil {
		return // close without error frame is also a rejection
	}
	if fr.Type != MsgError {
		t.Fatalf("expected error frame, got %d", fr.Type)
	}
}
