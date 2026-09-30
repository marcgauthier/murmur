package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

func openHighDB(t *testing.T) *db.DB {
	t.Helper()
	return openHighDBAt(t, t.TempDir(), db.NewNodeID(), nil)
}

// openHighDBAt opens the test High database at an explicit path, node, and
// base filesystem so failure tests can arm faults and reopen.
func openHighDBAt(t *testing.T, dir string, node db.NodeID, fs vfs.FS) *db.DB {
	t.Helper()
	pebbleCfg := db.DefaultPebbleConfig()
	if fs != nil {
		pebbleCfg.BaseFS = fs
	}
	database, err := db.Open(context.Background(), db.Config{
		Path:   dir,
		NodeID: node,
		Schema: db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "contacts",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
				{Name: "score", Type: schema.ColInteger, Nullable: true},
			},
		}}},
		Pebble:     pebbleCfg,
		Encryption: db.EncryptionConfig{Key: bytes.Repeat([]byte{0x44}, 32), KeyID: "test-key"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func inboxKeys(t *testing.T, stream string) (*SignerKey, *RecipientKey, *TrustStore) {
	t.Helper()
	signer, err := GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	recip, err := GenerateRecipientKey()
	if err != nil {
		t.Fatal(err)
	}
	trust := NewTrustStore()
	if err := trust.AddSigner(signer.ID, stream); err != nil {
		t.Fatal(err)
	}
	if err := trust.AddRecipient(recip); err != nil {
		t.Fatal(err)
	}
	return signer, recip, trust
}

func putBatch(seq uint64, row ids.RowID, cols ...ColumnValue) Batch {
	return Batch{TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: seq, HLC: seq,
		Records: []Record{{Table: "contacts", Row: row, Op: RecordPut, Columns: cols}}}
}

func delBatch(seq uint64, row ids.RowID) Batch {
	return Batch{TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: seq, HLC: seq,
		Records: []Record{{Table: "contacts", Row: row, Op: RecordDelete}}}
}

func sealForInbox(t *testing.T, signer *SignerKey, recip *RecipientKey, stream string, first uint64, batches []Batch) Artifact {
	t.Helper()
	for i := range batches {
		batches[i].Sequence = first + uint64(i)
	}
	txIDs := make([]ids.TxID, len(batches))
	for i, b := range batches {
		txIDs[i] = b.TxID
	}
	sum := sha256.Sum256([]byte("bridge-test-source:" + stream))
	var source ids.DBID
	copy(source[:], sum[:len(source)])
	m := Manifest{
		SourceDomain: source, Stream: stream,
		SeqFirst: first, SeqLast: first + uint64(len(batches)) - 1,
		TxIDs: txIDs, SchemaEpoch: 1,
	}
	sealed, err := SealBatches(signer, recip.Public(), m, batches, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	return Artifact{Name: "test.spb", Data: sealed}
}

func queryNames(t *testing.T, database *db.DB) map[string]int64 {
	t.Helper()
	rows, err := database.QueryContext(context.Background(), `SELECT name, score FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var name string
		var score sqlNullInt64
		if err := rows.Scan(&name, &score); err != nil {
			t.Fatal(err)
		}
		if score.Valid {
			out[name] = score.Int64
		} else {
			out[name] = -1
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// sqlNullInt64 mirrors sql.NullInt64 without importing database/sql.
type sqlNullInt64 struct {
	Int64 int64
	Valid bool
}

// Scan implements sql.Scanner for INTEGER columns.
func (n *sqlNullInt64) Scan(value any) error {
	switch v := value.(type) {
	case nil:
		n.Int64, n.Valid = 0, false
		return nil
	case int64:
		n.Int64, n.Valid = v, true
		return nil
	default:
		return errScanType
	}
}

type scanError string

func (e scanError) Error() string { return string(e) }

const errScanType = scanError("bridge: unexpected scan type")

func TestInboxDrainInOrder(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	rowAnn, rowBob := ids.NewRowID(), ids.NewRowID()
	b1 := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, rowAnn, ColumnValue{Column: "name", Value: codec.Text("ann")}, ColumnValue{Column: "score", Value: codec.Int(1)}),
	})
	b23 := sealForInbox(t, signer, recip, "s", 2, []Batch{
		putBatch(2, rowBob, ColumnValue{Column: "name", Value: codec.Text("bob")}),
		delBatch(3, rowAnn),
	})

	// Out-of-order arrival stages but cannot import past the gap.
	if err := inbox.Receive(b23); err != nil {
		t.Fatal(err)
	}
	if next, _, err := inbox.NextImport(); err != nil || next != nil {
		t.Fatalf("import past gap: %v, %v", next, err)
	}
	prog := inbox.Progress()
	if len(prog) != 1 || prog[0].Applied != 0 || prog[0].Observed != 3 || len(prog[0].Missing) != 1 || prog[0].Missing[0] != 1 {
		t.Fatalf("progress = %+v", prog)
	}
	if err := inbox.Receive(b1); err != nil {
		t.Fatal(err)
	}

	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	n, err := im.Drain(ctx, inbox)
	if err != nil || n != 2 {
		t.Fatalf("drain = %d, %v", n, err)
	}
	got := queryNames(t, high)
	if len(got) != 1 {
		t.Fatalf("high state = %v", got)
	}
	if _, ok := got["bob"]; !ok {
		t.Fatalf("high state = %v", got)
	}
	prog = inbox.Progress()
	if prog[0].Applied != 3 || len(prog[0].Missing) != 0 {
		t.Fatalf("progress = %+v", prog)
	}

	// Identical replay is idempotent: no dup, no movement.
	if err := inbox.Receive(b1); err != nil {
		t.Fatal(err)
	}
	n, err = im.Drain(ctx, inbox)
	if err != nil || n != 0 {
		t.Fatalf("redrain = %d, %v", n, err)
	}
	if got := queryNames(t, high); len(got) != 1 {
		t.Fatalf("replay changed state: %v", got)
	}
}

func TestInboxConflictQuarantine(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	// A second trusted signer lets us craft conflicting same-seq content.
	signer2, err := GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := trust.AddSigner(signer2.ID, "s"); err != nil {
		t.Fatal(err)
	}
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	a1 := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, row, ColumnValue{Column: "name", Value: codec.Text("a")}),
	})
	a1conflict := sealForInbox(t, signer2, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("b")}),
	})

	if err := inbox.Receive(a1); err != nil {
		t.Fatal(err)
	}
	// Same sequence, different bytes: quarantined, original staging kept.
	if err := inbox.Receive(a1conflict); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("conflict err = %v", err)
	}
	prog := inbox.Progress()
	if len(prog[0].Quarantine) != 1 {
		t.Fatalf("progress = %+v", prog)
	}
	// Terminal hold: retry of the conflicting bytes is refused (operator
	// resolution only).
	if err := inbox.RetryQuarantined("s", 1); err == nil {
		t.Fatal("conflict retry accepted")
	}
	// The known original is unaffected and still imports.
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	n, err := im.Drain(ctx, inbox)
	if err != nil || n != 1 {
		t.Fatalf("drain = %d, %v", n, err)
	}
	got := queryNames(t, high)
	if len(got) != 1 {
		t.Fatalf("high state = %v", got)
	}
	if _, ok := got["a"]; !ok {
		t.Fatalf("high state = %v", got)
	}
}

func TestInboxApplyFailureQuarantine(t *testing.T) {
	ctx := context.Background()
	highDir := t.TempDir()
	node := db.NewNodeID()
	fsys := &failFS{FS: vfs.Default}
	high := openHighDBAt(t, highDir, node, fsys)
	signer, recip, trust := inboxKeys(t, "s")
	dir := t.TempDir()
	inbox, err := OpenInbox(dir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// Both bundles are schema-valid; the failure is a full disk mid-apply.
	// Bundle 2 is fine but held behind bundle 1's quarantine.
	row1 := ids.NewRowID()
	bad := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, row1, ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	row := ids.NewRowID()
	good := sealForInbox(t, signer, recip, "s", 2, []Batch{
		putBatch(2, row, ColumnValue{Column: "name", Value: codec.Text("bob")}),
	})
	if err := inbox.Receive(bad); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(good); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	fsys.arm()
	n, err := im.Drain(ctx, inbox)
	if err == nil || n != 0 {
		t.Fatalf("drain = %d, %v", n, err)
	}
	prog := inbox.Progress()
	if len(prog[0].Quarantine) != 1 || prog[0].Applied != 0 {
		t.Fatalf("progress = %+v", prog)
	}
	// Later sequences stay held behind the quarantine.
	if next, _, err := inbox.NextImport(); err != nil || next != nil {
		t.Fatalf("import past hold: %v, %v", next, err)
	}
	// Restart preserves the hold and the staged bytes.
	inbox2, err := OpenInbox(dir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	prog = inbox2.Progress()
	if len(prog[0].Quarantine) != 1 || len(prog[0].Staged) != 1 {
		t.Fatalf("replayed progress = %+v", prog)
	}
	// Retry restores the bytes, but the failed node cannot even run the
	// schema gate: the drain errors and the bundle stays staged.
	if err := inbox2.RetryQuarantined("s", 1); err != nil {
		t.Fatal(err)
	}
	n, err = im.Drain(ctx, inbox2)
	var held *HoldError
	if err == nil || n != 0 || errors.As(err, &held) {
		t.Fatalf("redrain = %d, %v", n, err)
	}
	if prog := inbox2.Progress(); len(prog[0].Staged) != 2 {
		t.Fatalf("restaged progress = %+v", prog)
	}
	// Space returns and the node restarts: replay applies both bundles.
	fsys.disarm()
	_ = high.Close() // a failed node may report its fatal cause on close
	high = openHighDBAt(t, highDir, node, fsys)
	im2, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := im2.Drain(ctx, inbox2); err != nil || n != 2 {
		t.Fatalf("final drain = %d, %v", n, err)
	}
	if got := queryNames(t, high); len(got) != 2 || got["ann"] != -1 || got["bob"] != -1 {
		t.Fatalf("high state = %v", got)
	}
}

func TestImportAtomicity(t *testing.T) {
	ctx := context.Background()
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// One bundle, two valid batches: a full disk fails the commit, so the
	// whole bundle rolls back and quarantines with nothing applied.
	highDir := t.TempDir()
	node := db.NewNodeID()
	fsys := &failFS{FS: vfs.Default}
	high := openHighDBAt(t, highDir, node, fsys)
	row, row2 := ids.NewRowID(), ids.NewRowID()
	b := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, row, ColumnValue{Column: "name", Value: codec.Text("ann")}),
		putBatch(2, row2, ColumnValue{Column: "name", Value: codec.Text("bob")}),
	})
	if err := inbox.Receive(b); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	fsys.arm()
	if n, err := im.Drain(ctx, inbox); err == nil || n != 0 {
		t.Fatalf("drain = %d, %v", n, err)
	}
	// The whole bundle rolled back: not even the valid first batch landed.
	// (The failed node cannot serve reads; reopen to inspect and replay.)
	prog := inbox.Progress()
	if len(prog[0].Quarantine) != 1 {
		t.Fatalf("progress = %+v", prog)
	}
	fsys.disarm()
	_ = high.Close() // a failed node may report its fatal cause on close
	high = openHighDBAt(t, highDir, node, fsys)
	// The failed commit may have reached the memtable and flushed at
	// close, but never partially: the bundle is one storage batch, so
	// the reopened state holds all or nothing of it.
	if got := queryNames(t, high); len(got) != 0 && len(got) != 2 {
		t.Fatalf("partial import: %v", got)
	} else {
		for name := range got {
			if name != "ann" && name != "bob" {
				t.Fatalf("foreign row %q in %+v", name, got)
			}
		}
	}
	im2, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.RetryQuarantined("s", 1); err != nil {
		t.Fatal(err)
	}
	if n, err := im2.Drain(ctx, inbox); err != nil || n != 1 {
		t.Fatalf("redrain = %d, %v", n, err)
	}
	if got := queryNames(t, high); len(got) != 2 {
		t.Fatalf("replayed state = %v", got)
	}
}

func TestCrossReceiverConvergence(t *testing.T) {
	ctx := context.Background()
	highA, highB := openHighDB(t), openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	row := ids.NewRowID()
	a := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, row,
			ColumnValue{Column: "name", Value: codec.Text("ann")},
			ColumnValue{Column: "score", Value: codec.Int(7)}),
	})
	// Two receivers import the same bundle independently.
	for _, high := range []*db.DB{highA, highB} {
		inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
		if err != nil {
			t.Fatal(err)
		}
		if err := inbox.Receive(a); err != nil {
			t.Fatal(err)
		}
		im, err := NewImporter(high)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := im.Drain(ctx, inbox); err != nil || n != 1 {
			t.Fatalf("drain = %d, %v", n, err)
		}
	}
	stA, stB := queryNames(t, highA), queryNames(t, highB)
	if len(stA) != 1 || stA["ann"] != 7 || len(stB) != 1 || stB["ann"] != 7 {
		t.Fatalf("receivers diverged: %v vs %v", stA, stB)
	}
}

func TestInboxRestartRecovery(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	dir := t.TempDir()
	inbox, err := OpenInbox(dir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	a1 := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, row, ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	a2 := sealForInbox(t, signer, recip, "s", 2, []Batch{
		putBatch(2, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("bob")}),
	})
	if err := inbox.Receive(a1); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(a2); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	// Import one, "crash" (drop the handle), reopen, finish.
	bundle, _, err := inbox.NextImport()
	if err != nil || bundle == nil {
		t.Fatalf("next = %v, %v", bundle, err)
	}
	if err := im.ApplyBundle(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	m := bundle.Manifest
	if err := inbox.MarkApplied(m.Stream, m.SeqFirst, m.SeqLast); err != nil {
		t.Fatal(err)
	}
	inbox2, err := OpenInbox(dir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	n, err := im.Drain(ctx, inbox2)
	if err != nil || n != 1 {
		t.Fatalf("drain = %d, %v", n, err)
	}
	if got := queryNames(t, high); len(got) != 2 {
		t.Fatalf("high state = %v", got)
	}
	if p := inbox2.Progress(); p[0].Applied != 2 {
		t.Fatalf("progress = %+v", p)
	}
}
