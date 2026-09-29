package bridge

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/schema"
)

// holdTestContacts mirrors openHighDB's declaration for Migrate calls.
func holdTestContacts(extra ...schema.ColumnSchema) schema.TableSchema {
	return schema.TableSchema{
		Name: "contacts",
		Columns: append([]schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		}, extra...),
	}
}

func TestSchemaHoldOrderedRetry(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	row1, row2 := ids.NewRowID(), ids.NewRowID()
	head := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, row1,
			ColumnValue{Column: "name", Value: codec.Text("ann")},
			ColumnValue{Column: "email", Value: codec.Text("a@x")}),
	})
	tail := sealForInbox(t, signer, recip, "s", 2, []Batch{
		putBatch(2, row2, ColumnValue{Column: "name", Value: codec.Text("bob")}),
	})
	if err := inbox.Receive(head); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(tail); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	// The head waits on the missing column; nothing applies, not even the
	// ready tail.
	n, err := im.Drain(ctx, inbox)
	var held *HoldError
	if n != 0 || !errors.As(err, &held) {
		t.Fatalf("drain = %d, %v", n, err)
	}
	if len(held.Missing) != 1 || held.Missing[0] != `table "contacts" column "email"` {
		t.Fatalf("missing = %q", held.Missing)
	}
	prog := inbox.Progress()
	if len(prog) != 1 || prog[0].Applied != 0 || len(prog[0].Holds) != 1 {
		t.Fatalf("progress = %+v", prog)
	}
	h := prog[0].Holds[0]
	if h.First != 1 || h.Last != 1 || h.RequiredEpoch != 1 || len(h.Missing) != 1 {
		t.Fatalf("hold = %+v", h)
	}
	if next, _, err := inbox.NextImport(); err != nil || next != nil {
		t.Fatalf("import past hold: %v, %v", next, err)
	}
	if got := queryNames(t, high); len(got) != 0 {
		t.Fatalf("partial import: %v", got)
	}
	// The administrator's migration releases both bundles in order on the
	// next drain, with no manual replay step.
	if err := high.Migrate(ctx, []schema.TableSchema{holdTestContacts(
		schema.ColumnSchema{Name: "email", Type: schema.ColText, Nullable: true},
	)}); err != nil {
		t.Fatal(err)
	}
	if n, err := im.Drain(ctx, inbox); err != nil || n != 2 {
		t.Fatalf("redrain = %d, %v", n, err)
	}
	prog = inbox.Progress()
	if prog[0].Applied != 2 || len(prog[0].Holds) != 0 {
		t.Fatalf("progress = %+v", prog)
	}
	if got := queryNames(t, high); len(got) != 2 || got["ann"] != -1 || got["bob"] != -1 {
		t.Fatalf("high state = %v", got)
	}
}

func TestSchemaHoldMissingTable(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	put := sealForInbox(t, signer, recip, "s", 1, []Batch{{
		TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 1, HLC: 1,
		Records: []Record{{Table: "nope", Row: row, Op: RecordPut,
			Columns: []ColumnValue{{Column: "c", Value: codec.Text("x")}}}},
	}})
	del := sealForInbox(t, signer, recip, "s", 2, []Batch{{
		TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 2, HLC: 2,
		Records: []Record{{Table: "nope", Row: row, Op: RecordDelete}},
	}})
	if err := inbox.Receive(put); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(del); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	n, err := im.Drain(ctx, inbox)
	var held *HoldError
	if n != 0 || !errors.As(err, &held) {
		t.Fatalf("drain = %d, %v", n, err)
	}
	if len(held.Missing) != 1 || held.Missing[0] != `table "nope"` {
		t.Fatalf("missing = %q", held.Missing)
	}
	// Deletes gate on the table too, so the delete cannot run first.
	if err := high.Migrate(ctx, []schema.TableSchema{holdTestContacts(), {
		Name: "nope",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "c", Type: schema.ColText, Nullable: true},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	if n, err := im.Drain(ctx, inbox); err != nil || n != 2 {
		t.Fatalf("redrain = %d, %v", n, err)
	}
	// Put-then-delete in order leaves the table empty.
	rows, err := high.QueryContext(ctx, `SELECT c FROM nope`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("delete did not follow put in order")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaHoldDurable(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	dir := t.TempDir()
	inbox, err := OpenInbox(dir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	bad := sealForInbox(t, signer, recip, "s", 1, []Batch{{
		TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 1, HLC: 1,
		Records: []Record{{Table: "nope", Row: ids.NewRowID(), Op: RecordPut,
			Columns: []ColumnValue{{Column: "c", Value: codec.Int(1)}}}},
	}})
	if err := inbox.Receive(bad); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := im.Drain(ctx, inbox); n != 0 || err == nil {
		t.Fatalf("drain = %d, %v", n, err)
	}
	inbox2, err := OpenInbox(dir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	prog := inbox2.Progress()
	if len(prog) != 1 || len(prog[0].Holds) != 1 || len(prog[0].Staged) != 1 {
		t.Fatalf("replayed progress = %+v", prog)
	}
	h := prog[0].Holds[0]
	if h.First != 1 || h.RequiredEpoch != 1 || h.RequiredHash != strings.Repeat("0", 64) ||
		len(h.Missing) != 1 || h.Missing[0] != `table "nope"` {
		t.Fatalf("replayed hold = %+v", h)
	}
	if next, _, err := inbox2.NextImport(); err != nil || next != nil {
		t.Fatalf("import past hold: %v, %v", next, err)
	}
}

func TestHoldValidation(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Hold("ghost", 1, "b", 1, 1, [32]byte{}, nil); err == nil {
		t.Fatal("hold on unknown stream succeeded")
	}
	a := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	if err := inbox.Receive(a); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Hold("s", 7, "b", 7, 1, [32]byte{}, nil); err == nil {
		t.Fatal("hold on unstaged sequence succeeded")
	}
	if n, err := inbox.RecheckHolds(func(*Bundle) error { return nil }); err != nil || n != 0 {
		t.Fatalf("recheck without holds = %d, %v", n, err)
	}
}

func TestRecheckHoldsOrdered(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	a1 := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	a2 := sealForInbox(t, signer, recip, "s", 2, []Batch{
		putBatch(2, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("bob")}),
	})
	b1, err := OpenBundle(a1.Data, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	b2, err := OpenBundle(a2.Data, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(a1); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(a2); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Hold("s", 1, b1.Manifest.BundleID.String(), 1, 1, [32]byte{}, []string{`table "nope"`}); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Hold("s", 2, b2.Manifest.BundleID.String(), 2, 1, [32]byte{}, []string{`table "alsono"`}); err != nil {
		t.Fatal(err)
	}
	// A failing head stops the retry: later holds are never even checked.
	var checked []uint64
	failAll := func(b *Bundle) error {
		checked = append(checked, b.Batches[0].Sequence)
		return errors.New("still incompatible")
	}
	if n, err := inbox.RecheckHolds(failAll); err != nil || n != 0 {
		t.Fatalf("recheck = %d, %v", n, err)
	}
	if len(checked) != 1 || checked[0] != 1 {
		t.Fatalf("checked = %v", checked)
	}
	// Release is head-only per pass: the head frees while the tail stays
	// held until progress advances past the head.
	if n, err := inbox.RecheckHolds(func(*Bundle) error { return nil }); err != nil || n != 1 {
		t.Fatalf("recheck = %d, %v", n, err)
	}
	prog := inbox.Progress()
	if len(prog[0].Holds) != 1 || prog[0].Holds[0].First != 2 {
		t.Fatalf("progress = %+v", prog)
	}
	next, _, err := inbox.NextImport()
	if err != nil || next == nil || next.Manifest.SeqFirst != 1 {
		t.Fatalf("next = %+v, %v", next, err)
	}
	if err := inbox.MarkApplied("s", 1, 1); err != nil {
		t.Fatal(err)
	}
	if n, err := inbox.RecheckHolds(func(*Bundle) error { return nil }); err != nil || n != 1 {
		t.Fatalf("recheck = %d, %v", n, err)
	}
	if prog := inbox.Progress(); len(prog[0].Holds) != 0 || prog[0].Applied != 1 {
		t.Fatalf("progress = %+v", prog)
	}
	next, _, err = inbox.NextImport()
	if err != nil || next == nil || next.Manifest.SeqFirst != 2 {
		t.Fatalf("next = %+v, %v", next, err)
	}
}

func TestSchemaHoldTypeMismatch(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// One bundle, two batches: compatible INTEGER alongside an
	// incompatible TEXT for the same INTEGER column. Checking is
	// per value, and the whole bundle waits with no partial apply.
	b := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(),
			ColumnValue{Column: "name", Value: codec.Text("ann")},
			ColumnValue{Column: "score", Value: codec.Int(7)}),
		putBatch(2, ids.NewRowID(),
			ColumnValue{Column: "name", Value: codec.Text("bob")},
			ColumnValue{Column: "score", Value: codec.Text("seven")}),
	})
	if err := inbox.Receive(b); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	n, err := im.Drain(ctx, inbox)
	var held *HoldError
	if n != 0 || !errors.As(err, &held) {
		t.Fatalf("drain = %d, %v", n, err)
	}
	want := `table "contacts" column "score" expects INTEGER, got TEXT`
	if len(held.Missing) != 1 || held.Missing[0] != want {
		t.Fatalf("missing = %q", held.Missing)
	}
	if got := queryNames(t, high); len(got) != 0 {
		t.Fatalf("partial import: %v", got)
	}
}

func TestSchemaHoldNullability(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// NULL satisfies the nullable name column but violates the NOT NULL
	// primary key: the first bundle applies, the second waits.
	ok := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(),
			ColumnValue{Column: "name", Value: codec.Null()},
			ColumnValue{Column: "score", Value: codec.Int(1)}),
	})
	row := ids.NewRowID()
	bad := sealForInbox(t, signer, recip, "s", 2, []Batch{{
		TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 2, HLC: 2,
		Records: []Record{{Table: "contacts", Row: row, Op: RecordPut,
			Columns: []ColumnValue{{Column: "id", Value: codec.Null()}}}},
	}})
	if err := inbox.Receive(ok); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(bad); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	n, err := im.Drain(ctx, inbox)
	var held *HoldError
	if n != 1 || !errors.As(err, &held) {
		t.Fatalf("drain = %d, %v", n, err)
	}
	want := `table "contacts" column "id" must not be NULL`
	if len(held.Missing) != 1 || held.Missing[0] != want {
		t.Fatalf("missing = %q", held.Missing)
	}
	prog := inbox.Progress()
	if prog[0].Applied != 1 || len(prog[0].Holds) != 1 {
		t.Fatalf("progress = %+v", prog)
	}
}

func TestSchemaHoldIntegerWidening(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	if err := high.Migrate(ctx, []schema.TableSchema{holdTestContacts(
		schema.ColumnSchema{Name: "ratio", Type: schema.ColReal, Nullable: true},
	)}); err != nil {
		t.Fatal(err)
	}
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// Lossless INTEGER into REAL applies; REAL into INTEGER waits.
	wide := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(),
			ColumnValue{Column: "name", Value: codec.Text("ann")},
			ColumnValue{Column: "ratio", Value: codec.Int(5)}),
	})
	narrow := sealForInbox(t, signer, recip, "s", 2, []Batch{
		putBatch(2, ids.NewRowID(),
			ColumnValue{Column: "name", Value: codec.Text("bob")},
			ColumnValue{Column: "score", Value: codec.Real(1.5)}),
	})
	if err := inbox.Receive(wide); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(narrow); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	n, err := im.Drain(ctx, inbox)
	var held *HoldError
	if n != 1 || !errors.As(err, &held) {
		t.Fatalf("drain = %d, %v", n, err)
	}
	want := `table "contacts" column "score" expects INTEGER, got REAL`
	if len(held.Missing) != 1 || held.Missing[0] != want {
		t.Fatalf("missing = %q", held.Missing)
	}
	if got := queryNames(t, high); len(got) != 1 || got["ann"] != -1 {
		t.Fatalf("high state = %v", got)
	}
}

func TestHeldStreamDoesNotBlockOthers(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recip, trust := inboxKeys(t, "a")
	if err := trust.AddSigner(signer.ID, "a", "b"); err != nil {
		t.Fatal(err)
	}
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	stuck := sealForInbox(t, signer, recip, "a", 1, []Batch{{
		TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 1, HLC: 1,
		Records: []Record{{Table: "nope", Row: ids.NewRowID(), Op: RecordPut,
			Columns: []ColumnValue{{Column: "c", Value: codec.Int(1)}}}},
	}})
	row := ids.NewRowID()
	free := sealForInbox(t, signer, recip, "b", 1, []Batch{
		putBatch(1, row, ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	if err := inbox.Receive(stuck); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(free); err != nil {
		t.Fatal(err)
	}
	im, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	// First drain stops holding stream a...
	var held *HoldError
	if n, err := im.Drain(ctx, inbox); n != 0 || !errors.As(err, &held) {
		t.Fatalf("drain = %d, %v", n, err)
	}
	// ...but the held stream never blocks stream b afterwards.
	if n, err := im.Drain(ctx, inbox); err != nil || n != 1 {
		t.Fatalf("redrain = %d, %v", n, err)
	}
	if got := queryNames(t, high); len(got) != 1 || got["ann"] != -1 {
		t.Fatalf("high state = %v", got)
	}
	prog := inbox.Progress()
	for _, sp := range prog {
		if sp.Stream == "a" && len(sp.Holds) != 1 {
			t.Fatalf("stream a progress = %+v", sp)
		}
		if sp.Stream == "b" && (sp.Applied != 1 || len(sp.Holds) != 0) {
			t.Fatalf("stream b progress = %+v", sp)
		}
	}
}
