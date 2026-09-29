package bridge

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
)

func testBatch() Batch {
	return Batch{
		TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 1, HLC: 2,
		Records: []Record{{Table: "t", Row: ids.NewRowID(), Op: RecordPut,
			Columns: []ColumnValue{{Column: "c", Value: codec.Int(1)}}}},
	}
}

func TestOutboxRoundTrip(t *testing.T) {
	dir := t.TempDir()
	limits := Limits{}.withDefaults()
	o, err := OpenOutbox(dir, limits)
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	b1, b2 := testBatch(), testBatch()
	s1, err := o.Append(b1, origin, 41, 3, [32]byte{0x1})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := o.Append(b2, origin, 42, 3, [32]byte{0x1})
	if err != nil {
		t.Fatal(err)
	}
	if s1 != 1 || s2 != 2 {
		t.Fatalf("seqs = %d,%d", s1, s2)
	}
	if got := o.Resume(origin); got != 42 {
		t.Fatalf("resume = %d", got)
	}
	if o.Len() != 2 {
		t.Fatalf("len = %d", o.Len())
	}

	pending := o.Pending(0)
	if len(pending) != 2 || pending[0].Seq != 1 || pending[1].Seq != 2 {
		t.Fatalf("pending = %d events", len(pending))
	}
	if pending[0].Batch.TxID != b1.TxID || pending[0].OriginSeq != 41 {
		t.Fatal("event identity drift")
	}
	// Invalid batches are rejected before journaling.
	bad := testBatch()
	bad.Records = nil
	if _, err := o.Append(bad, origin, 43, 3, [32]byte{0x1}); err == nil {
		t.Fatal("invalid batch appended")
	}
	if got := o.Resume(origin); got != 42 {
		t.Fatalf("resume moved on failed append: %d", got)
	}

	if err := o.MarkPublished(1); err != nil {
		t.Fatal(err)
	}
	if err := o.MarkFailed(2, errTest); err != nil {
		t.Fatal(err)
	}
	if got := o.Pending(0); len(got) != 1 || got[0].Seq != 2 {
		t.Fatalf("pending after marks = %d", len(got))
	}

	// Reopen replays journal + resume exactly.
	o2, err := OpenOutbox(dir, limits)
	if err != nil {
		t.Fatal(err)
	}
	if got := o2.Resume(origin); got != 42 {
		t.Fatalf("replayed resume = %d", got)
	}
	pending = o2.Pending(0)
	if len(pending) != 1 || pending[0].Seq != 2 || pending[0].Attempts != 1 {
		t.Fatalf("replayed pending = %+v", pending)
	}
	s3, err := o2.Append(testBatch(), origin, 43, 3, [32]byte{0x1})
	if err != nil {
		t.Fatal(err)
	}
	if s3 != 3 {
		t.Fatalf("seq after reopen = %d", s3)
	}

	// Retention keeps the newest published plus all unpublished.
	if removed, err := o2.GC(0); err != nil || removed != 1 {
		t.Fatalf("gc = %d, %v", removed, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "events", eventName(1))); !os.IsNotExist(err) {
		t.Fatal("published event not reclaimed")
	}
	if o2.Len() != 2 {
		t.Fatalf("len after gc = %d", o2.Len())
	}
	if err := o2.MarkPublished(99); err == nil {
		t.Fatal("mark unknown succeeded")
	}
}

var errTest = errTestType{}

type errTestType struct{}

func (errTestType) Error() string { return "test failure" }

func TestOutboxCorruptFailsClosed(t *testing.T) {
	dir := t.TempDir()
	limits := Limits{}.withDefaults()
	o, err := OpenOutbox(dir, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Append(testBatch(), ids.NewNodeID(), 1, 1, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events", eventName(1)), []byte("{nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenOutbox(dir, limits); err == nil {
		t.Fatal("corrupt event accepted")
	}
}
