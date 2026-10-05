package state

import (
	"context"
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/origin"
)

func signedSecurityStore(t *testing.T, node ids.NodeID, dbid ids.DBID) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), node, dbid, Options{OriginSigning: testidentity.Config(node), Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func securityBatch(origin ids.NodeID, seq uint64) *codec.MutationBatch {
	return &codec.MutationBatch{ProtocolVersion: 4, TxID: ids.NewTxID(), OriginNode: origin, Sequence: seq, HLC: 100 + seq, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("honest")}}}
}

func TestOriginRemoteRejectsBeforeAnyProgress(t *testing.T) {
	ctx := context.Background()
	dbid := ids.NewDBID()
	origin := ids.NewNodeID()
	s := signedSecurityStore(t, ids.NewNodeID(), dbid)
	valid := testidentity.Sign(securityBatch(origin, 1), dbid)
	cases := map[string]func(*codec.MutationBatch){
		"unsigned":       func(b *codec.MutationBatch) { b.SignatureVersion = 0 },
		"modified value": func(b *codec.MutationBatch) { b.Mutations[0].Value = codec.Text("forged") },
		"impersonated":   func(b *codec.MutationBatch) { b.OriginNode = ids.NewNodeID() },
		"cross database": func(b *codec.MutationBatch) { testidentity.Sign(b, ids.NewDBID()) },
		"modified HLC":   func(b *codec.MutationBatch) { b.HLC = 1 << 63 },
	}
	for name, alter := range cases {
		t.Run(name, func(t *testing.T) {
			b := *valid
			b.Mutations = append([]codec.Mutation(nil), valid.Mutations...)
			alter(&b)
			clock := s.ClockMax()
			gen, _ := s.StateGeneration()
			if _, err := s.CommitRemote(ctx, &b); err == nil {
				t.Fatal("forgery committed")
			}
			if s.ClockMax() != clock {
				t.Fatal("forgery changed clock")
			}
			after, _ := s.StateGeneration()
			if after != gen {
				t.Fatal("forgery changed generation")
			}
			if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
				t.Fatal("forgery changed watermark")
			}
			if _, err := s.getDirect(ReceiptKey(valid.TxID)); !isNotFound(err) {
				t.Fatal("forgery created receipt")
			}
			if _, err := s.getDirect(SysKey(sysRemotePrepare)); !isNotFound(err) {
				t.Fatal("forgery created prepare")
			}
		})
	}
	if _, err := s.CommitRemote(ctx, valid); err != nil {
		t.Fatal(err)
	}
	// Signature verification must precede the duplicate receipt fast path.
	bad := *valid
	bad.OriginSignature[0]++
	if _, err := s.CommitRemote(ctx, &bad); !errors.Is(err, codec.ErrOriginSignature) {
		t.Fatalf("tampered duplicate: %v", err)
	}
	if res, err := s.CommitRemote(ctx, valid); err != nil || res.Applied {
		t.Fatalf("identical duplicate: %+v %v", res, err)
	}
	conflict := *valid
	conflict.TxID = ids.NewTxID()
	testidentity.Sign(&conflict, dbid)
	if _, err := s.CommitRemote(ctx, &conflict); !errors.Is(err, codec.ErrOriginConflict) {
		t.Fatalf("retained sequence conflict: %v", err)
	}
}

func TestOriginRemoteGroupInvalidMemberIsAtomic(t *testing.T) {
	s := signedSecurityStore(t, ids.NewNodeID(), ids.NewDBID())
	origin := ids.NewNodeID()
	first := testidentity.Sign(securityBatch(origin, 1), s.DBID())
	second := testidentity.Sign(securityBatch(origin, 2), s.DBID())
	second.OriginSignature[0]++
	before := s.ClockMax()
	if _, err := s.CommitRemoteGroup(context.Background(), []*codec.MutationBatch{first, second}); !errors.Is(err, codec.ErrOriginSignature) {
		t.Fatal(err)
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatal("partial group progress")
	}
	if s.ClockMax() != before {
		t.Fatal("partial group clock")
	}
	if _, err := s.getDirect(ReceiptKey(first.TxID)); !isNotFound(err) {
		t.Fatal("partial group receipt")
	}
}

func TestOriginChunkSignatureBeforeStaging(t *testing.T) {
	s := signedSecurityStore(t, ids.NewNodeID(), ids.NewDBID())
	b := testidentity.Sign(securityBatch(ids.NewNodeID(), 1), s.DBID())
	b.Mutations[0].Value = codec.Text(strings.Repeat("x", 150<<10))
	testidentity.Sign(b, s.DBID())
	frames, err := codec.EncodeTransactionChunks(b, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	c, err := codec.DecodeTransactionChunk(frames[0], 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	c.HLC++
	raw, err := codec.EncodeTransactionChunk(nil, c, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StageTransactionChunk(context.Background(), raw, 1<<20); !errors.Is(err, codec.ErrOriginSignature) {
		t.Fatal(err)
	}
	if bits, err := s.StagedTransactionChunks(b.TxID); err != nil || len(bits) != 0 {
		t.Fatalf("invalid chunk persisted: %v %v", bits, err)
	}
	// A valid signature over the announced digest cannot authenticate fragment
	// data before assembly. Tampered content must fail the complete digest.
	frames[0][len(frames[0])-1] ^= 1
	for i, frame := range frames {
		_, _, err = s.StageTransactionChunk(context.Background(), frame, 1<<20)
		if i < len(frames)-1 && err != nil {
			t.Fatal(err)
		}
	}
	if err == nil {
		t.Fatal("tampered assembled content accepted")
	}
	if wm, _ := s.ReceiveWatermark(b.OriginNode); wm != 0 {
		t.Fatal("tampered content advanced watermark")
	}
}

func TestOriginLocalGroupSignsEveryMember(t *testing.T) {
	s := signedSecurityStore(t, ids.NewNodeID(), ids.NewDBID())
	bs := []*codec.MutationBatch{securityBatch(s.NodeID(), 0), securityBatch(s.NodeID(), 0)}
	bs[0].HLC = s.ClockNow()
	bs[1].HLC = s.ClockNow()
	if _, err := s.CommitLocalGroup(context.Background(), bs); err != nil {
		t.Fatal(err)
	}
	for _, b := range bs {
		if err := s.VerifyOrigin(b); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOriginKeyChangeRefusedAcrossRestart(t *testing.T) {
	path := t.TempDir()
	node := ids.NewNodeID()
	cfg := testidentity.Config(node)
	s, err := Open(path, node, ids.NewDBID(), Options{OriginSigning: cfg})
	if err != nil {
		t.Fatal(err)
	}
	dbid := s.DBID()
	_ = s.Close()
	pub, key, _ := ed25519.GenerateKey(nil)
	registry, _ := origin.NewKeyRegistry(map[ids.NodeID]ed25519.PublicKey{node: pub})
	cfg.PrivateKey = key
	cfg.TrustedKeys = registry
	if s, err = Open(path, node, dbid, Options{OriginSigning: cfg}); err == nil {
		_ = s.Close()
		t.Fatal("same NodeID key replacement accepted")
	}
}

func TestOriginLegacyMigrationPreservesStateAndRecoversPrepare(t *testing.T) {
	path := t.TempDir()
	node := ids.NewNodeID()
	opt := Options{OriginSigning: testidentity.Config(node), Limits: codec.DefaultLimits()}
	s, err := Open(path, node, ids.NewDBID(), opt)
	if err != nil {
		t.Fatal(err)
	}
	dbid := s.DBID()
	local := securityBatch(node, 0)
	local.HLC = s.ClockNow()
	if _, err = s.CommitLocal(context.Background(), local); err != nil {
		t.Fatal(err)
	}
	pending := securityBatch(ids.NewNodeID(), 1)
	raw := codec.EncodeBatch(nil, pending)
	legacy := append(append([]byte(nil), raw[:94]...), raw[codec.BatchHeaderSize:]...)
	// Build a legacy fixture with a durable, uncompleted remote prepare.
	b := s.db.NewBatch()
	for _, name := range []string{sysFormat, sysMinReader, sysMinWriter} {
		_ = b.Set(SysKey(name), encodeU64(3), nil)
	}
	_ = b.Set(SysKey(sysRemotePrepare), legacy, nil)
	_ = b.Delete(SysKey("origin_signing_key"), nil)
	if err = b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	_ = s.Close()
	if s, err = Open(path, node, dbid, opt); err == nil {
		_ = s.Close()
		t.Fatal("legacy ordinary open succeeded")
	}
	opt.MigrateUnsignedBaseline = true
	s, err = Open(path, node, dbid, opt)
	if err != nil {
		t.Fatal(err)
	}
	if st, ok, err := s.GetCell(1, local.Mutations[0].RowID, 2); err != nil || !ok || st.Value.S != "honest" {
		t.Fatalf("lost local baseline: %v %v", ok, err)
	}
	if st, ok, err := s.GetCell(1, pending.Mutations[0].RowID, 2); err != nil || !ok || st.Value.S != "honest" {
		t.Fatalf("lost prepared baseline: %v %v", ok, err)
	}
	if seq, _ := s.LocalSeq(); seq != 1 {
		t.Fatal("local sequence changed")
	}
	if wm, _ := s.ReceiveWatermark(pending.OriginNode); wm != 1 {
		t.Fatal("prepare not recovered")
	}
	format, r, w, err := s.FormatInfo()
	if err != nil || format != FormatVersion || r != MinReaderVersion || w != MinWriterVersion {
		t.Fatalf("markers %d/%d/%d: %v", format, r, w, err)
	}
	called := false
	_, err = s.LogScan(node, 1, 10, 1<<20, func(*codec.MutationBatch) error { called = true; return nil })
	if called {
		t.Fatal("unsigned logs survived")
	}
	if !errors.Is(err, ErrLogGone) {
		t.Fatalf("baseline log gap: %v", err)
	}
	_ = s.Close()
	opt.MigrateUnsignedBaseline = false
	s, err = Open(path, node, dbid, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	next := securityBatch(node, 0)
	next.HLC = s.ClockNow()
	if _, err = s.CommitLocal(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if next.Sequence != 2 {
		t.Fatal("sequence reused")
	}
	if err = s.VerifyOrigin(next); err != nil {
		t.Fatal(err)
	}
}

func TestOriginRemoteGroupRejectsSignedEquivocationWithinGroup(t *testing.T) {
	s := signedSecurityStore(t, ids.NewNodeID(), ids.NewDBID())
	writer := ids.NewNodeID()
	first := testidentity.Sign(securityBatch(writer, 1), s.DBID())
	second := testidentity.Sign(securityBatch(writer, 1), s.DBID())
	before := s.ClockMax()
	if _, err := s.CommitRemoteGroup(context.Background(), []*codec.MutationBatch{first, second}); !errors.Is(err, codec.ErrOriginConflict) {
		t.Fatal(err)
	}
	if wm, _ := s.ReceiveWatermark(writer); wm != 0 {
		t.Fatal("equivocation advanced progress")
	}
	if s.ClockMax() != before {
		t.Fatal("equivocation changed clock")
	}
}
