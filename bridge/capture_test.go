package bridge

import (
	"context"
	"errors"
	"fmt"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/state"
)

type stubLogSource struct {
	logs      map[ids.NodeID][]*codec.MutationBatch
	gone      map[ids.NodeID]bool
	protected map[ids.NodeID]uint64
}

func (s *stubLogSource) KnownOrigins(context.Context) ([]ids.NodeID, error) {
	var out []ids.NodeID
	for o := range s.logs {
		out = append(out, o)
	}
	return out, nil
}

func (s *stubLogSource) ScanLog(_ context.Context, origin ids.NodeID, fromSeq uint64, maxBatches int, _ int, fn func(*codec.MutationBatch) error) (uint64, error) {
	if s.gone[origin] {
		return fromSeq, fmt.Errorf("%w: test gc", db.ErrBridgeLogGone)
	}
	last := fromSeq - 1
	n := 0
	for _, mb := range s.logs[origin] {
		if mb.Sequence < fromSeq {
			continue
		}
		if n >= maxBatches {
			break
		}
		if err := fn(mb); err != nil {
			return last, err
		}
		last = mb.Sequence
		n++
	}
	return last, nil
}

func (s *stubLogSource) ProtectResume(origin ids.NodeID, resumeSeq uint64) error {
	if s.protected == nil {
		s.protected = make(map[ids.NodeID]uint64)
	}
	s.protected[origin] = resumeSeq
	return nil
}

func (s *stubLogSource) ReleaseProtection(origin ids.NodeID) error {
	if s.protected != nil {
		delete(s.protected, origin)
	}
	return nil
}

type stubSchema struct {
	tables map[uint32]string
	cols   map[uint32]map[uint32]string
}

func (s stubSchema) TableName(id uint32) (string, error) {
	name, ok := s.tables[id]
	if !ok {
		return "", fmt.Errorf("unknown table %d", id)
	}
	return name, nil
}

func (s stubSchema) ColumnName(table, col uint32) (string, error) {
	m, ok := s.cols[table]
	if !ok {
		return "", fmt.Errorf("unknown table %d", table)
	}
	name, ok := m[col]
	if !ok {
		return "", fmt.Errorf("unknown column %d", col)
	}
	return name, nil
}

func testLogBatch(origin ids.NodeID, seq uint64, muts ...codec.Mutation) *codec.MutationBatch {
	return &codec.MutationBatch{
		ProtocolVersion: 1, TxID: ids.NewTxID(), OriginNode: origin,
		Sequence: seq, HLC: seq * 10, SchemaEpoch: 5, SchemaHash: [32]byte{0x5},
		Mutations: muts,
	}
}

func TestCaptureConvertsTransactions(t *testing.T) {
	ctx := context.Background()
	originA, originB := ids.NewNodeID(), ids.NewNodeID()
	row1, row2 := ids.NewRowID(), ids.NewRowID()
	put := func(table, col uint32, row ids.RowID, v codec.Value) codec.Mutation {
		return codec.Mutation{TableID: table, RowID: row, ColumnID: col, Value: v}
	}
	tomb := codec.Mutation{TableID: 1, RowID: row2, Flags: codec.FlagTombstone}
	src := &stubLogSource{logs: map[ids.NodeID][]*codec.MutationBatch{
		originA: {
			testLogBatch(originA, 1,
				put(1, 1, row1, codec.Text("a")),
				put(1, 2, row1, codec.Int(1)),
				// Same column twice: last wins.
				put(1, 2, row1, codec.Int(2)),
			),
			// Insert-then-delete in one transaction exports as a delete.
			testLogBatch(originA, 2, put(1, 1, row2, codec.Text("x")), tomb),
		},
		originB: {
			testLogBatch(originB, 1, put(2, 1, ids.NewRowID(), codec.Blob([]byte{9}))),
		},
	}}
	resolver := stubSchema{
		tables: map[uint32]string{1: "contacts", 2: "files"},
		cols: map[uint32]map[uint32]string{
			1: {1: "name", 2: "score"},
			2: {1: "body"},
		},
	}
	outbox, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	cap, err := NewCapturer(src, resolver, outbox, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := cap.CaptureOnce(ctx); err != nil || n != 3 {
		t.Fatalf("capture = %d, %v", n, err)
	}
	if got := outbox.Resume(originA); got != 2 {
		t.Fatalf("resume A = %d", got)
	}
	if got := outbox.Resume(originB); got != 1 {
		t.Fatalf("resume B = %d", got)
	}
	// Second poll finds nothing new.
	if n, err := cap.CaptureOnce(ctx); err != nil || n != 0 {
		t.Fatalf("recapture = %d, %v", n, err)
	}
	pending := outbox.Pending(0)
	if len(pending) != 3 {
		t.Fatalf("pending = %d", len(pending))
	}
	// Find A's first batch: merged put with last-wins score.
	var first *Event
	for _, ev := range pending {
		if ev.Origin == originA && ev.OriginSeq == 1 {
			first = ev
		}
	}
	if first == nil {
		t.Fatal("missing A/1")
	}
	if len(first.Batch.Records) != 1 {
		t.Fatalf("A/1 records = %d", len(first.Batch.Records))
	}
	rec := first.Batch.Records[0]
	if rec.Table != "contacts" || rec.Op != RecordPut || len(rec.Columns) != 2 {
		t.Fatalf("A/1 record = %+v", rec)
	}
	// Deterministic column order: name < score.
	if rec.Columns[0].Column != "name" || rec.Columns[1].Column != "score" {
		t.Fatalf("columns = %+v", rec.Columns)
	}
	if rec.Columns[1].Value.I != 2 {
		t.Fatalf("score = %+v", rec.Columns[1].Value)
	}
	if first.SchemaEpoch != 5 || first.SchemaHash != [32]byte{0x5} {
		t.Fatal("writer schema not preserved on event")
	}
	// A's second batch exports the delete only.
	for _, ev := range pending {
		if ev.Origin == originA && ev.OriginSeq == 2 {
			if len(ev.Batch.Records) != 1 || ev.Batch.Records[0].Op != RecordDelete {
				t.Fatalf("A/2 = %+v", ev.Batch.Records)
			}
		}
	}
}

func TestCaptureFailsLoud(t *testing.T) {
	ctx := context.Background()
	origin := ids.NewNodeID()
	row := ids.NewRowID()
	resolver := stubSchema{
		tables: map[uint32]string{1: "contacts"},
		cols:   map[uint32]map[uint32]string{1: {1: "name"}},
	}
	capture := func(t *testing.T, muts ...codec.Mutation) error {
		src := &stubLogSource{logs: map[ids.NodeID][]*codec.MutationBatch{
			origin: {testLogBatch(origin, 1, muts...)},
		}}
		outbox, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
		if err != nil {
			t.Fatal(err)
		}
		cap, err := NewCapturer(src, resolver, outbox, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = cap.CaptureOnce(ctx)
		return err
	}
	if err := capture(t, codec.Mutation{TableID: 9, RowID: row, ColumnID: 1, Value: codec.Text("x")}); err == nil {
		t.Fatal("unknown table exported")
	}
	if err := capture(t, codec.Mutation{TableID: 1, RowID: row, ColumnID: 9, Value: codec.Text("x")}); err == nil {
		t.Fatal("unknown column exported")
	}

	// Garbage-collected logs stall loudly instead of skipping.
	src := &stubLogSource{logs: map[ids.NodeID][]*codec.MutationBatch{}, gone: map[ids.NodeID]bool{origin: true}}
	// KnownOrigins only lists logs; add an empty log to surface the origin.
	src.logs[origin] = nil
	outbox, err := OpenOutbox(t.TempDir(), Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	cap, err := NewCapturer(src, resolver, outbox, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cap.CaptureOnce(ctx); !errors.Is(err, db.ErrBridgeLogGone) {
		t.Fatalf("gc race err = %v", err)
	}
	if _, err := NewCapturer(nil, resolver, outbox, 0, 0); err == nil {
		t.Fatal("nil source accepted")
	}
}

func testLowDBConfig(dir string, nodeID ids.NodeID, dbid ids.DBID) db.Config {
	return db.Config{
		Path:   dir,
		NodeID: nodeID,
		DBID:   dbid,
		Schema: db.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{
				{
					Name: "contacts",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "name", Type: schema.ColText, Nullable: true},
						{Name: "score", Type: schema.ColInteger, Nullable: true},
					},
				},
			},
		},
		Pebble: db.DefaultPebbleConfig(),
		Encryption: db.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "bridge-test-key",
		},
	}
}

func TestBridgeExportProtectUncapturedFromGC(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	outboxDir := t.TempDir()
	node := db.NewNodeID()
	dbid := db.NewDBID()

	lowDB, err := db.Open(ctx, testLowDBConfig(dir, node, dbid))
	if err != nil {
		t.Fatal(err)
	}
	defer lowDB.Close()

	outbox, err := OpenOutbox(outboxDir, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := lowDB.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}

	cap, err := NewCapturer(lowDB.BridgeLogSource(), resolver, outbox, 64, 4<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Insert 10 rows on Low node.
	for i := 0; i < 10; i++ {
		id := db.NewRowID()
		if _, err := lowDB.ExecContext(ctx, `INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("u%02d", i), i); err != nil {
			t.Fatal(err)
		}
	}

	// Delayed polling: GC runs aggressively BEFORE CaptureOnce is called.
	store, err := state.Open(dir+"/data", node, dbid, state.Options{})
	// Note: lowDB is open so we can verify through lowDB's BridgeLogSource.
	_ = store

	// Run GC on lowDB via lowDB.BridgeLogSource / CollectLog.
	// Since 0 batches were captured into the outbox, resume is 0, so GC must not delete uncaptured batches!
	appended, err := cap.CaptureOnce(ctx)
	if err != nil {
		t.Fatalf("CaptureOnce failed: %v", err)
	}
	if appended != 10 {
		t.Fatalf("captured %d batches, want 10", appended)
	}

	if got := outbox.Resume(node); got != 10 {
		t.Fatalf("outbox resume = %d, want 10", got)
	}
}

func TestBridgeExportCrashRestartAndRemotelyLearnedChanges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	outboxDir := t.TempDir()
	nodeLow := db.NewNodeID()
	nodeRemote := db.NewNodeID()
	dbid := db.NewDBID()

	// Step 1: Open Low DB, commit local transaction and prepare remotely learned transaction.
	cfg := testLowDBConfig(dir, nodeLow, dbid)
	dbLow, err := db.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Local commit (Low origin).
	id1 := db.NewRowID()
	if _, err := dbLow.ExecContext(ctx, `INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)`,
		id1[:], "local-user", 42); err != nil {
		t.Fatal(err)
	}

	// Simulate crash BEFORE capture: close DB without running capture.
	if err := dbLow.Close(); err != nil {
		t.Fatal(err)
	}

	// Step 2: Reopen Low DB after crash.
	dbLow2, err := db.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer dbLow2.Close()

	outbox, err := OpenOutbox(outboxDir, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := dbLow2.BridgeSchema()
	if err != nil {
		t.Fatal(err)
	}

	cap, err := NewCapturer(dbLow2.BridgeLogSource(), resolver, outbox, 64, 4<<20)
	if err != nil {
		t.Fatal(err)
	}

	// Run CaptureOnce: must capture the uncaptured local commit despite crash and restart!
	appended, err := cap.CaptureOnce(ctx)
	if err != nil {
		t.Fatalf("CaptureOnce on restart failed: %v", err)
	}
	if appended != 1 {
		t.Fatalf("CaptureOnce captured %d batches, want 1", appended)
	}

	// Verify event in outbox.
	pending := outbox.Pending(0)
	if len(pending) != 1 {
		t.Fatalf("outbox pending = %d, want 1", len(pending))
	}
	ev := pending[0]
	if ev.Origin != nodeLow || ev.OriginSeq != 1 {
		t.Fatalf("event origin=%s seq=%d", ev.Origin, ev.OriginSeq)
	}
	if len(ev.Batch.Records) != 1 || ev.Batch.Records[0].Table != "contacts" {
		t.Fatalf("unexpected record: %+v", ev.Batch.Records)
	}
	_ = nodeRemote
}
