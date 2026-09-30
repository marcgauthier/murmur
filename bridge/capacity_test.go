package bridge

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func appendOne(t *testing.T, o *Outbox, origin ids.NodeID, originSeq uint64) (uint64, error) {
	t.Helper()
	return o.Append(putBatch(originSeq, ids.NewRowID(),
		ColumnValue{Column: "name", Value: codec.Text("ann")}), origin, originSeq, 1, [32]byte{0x1})
}

func TestOutboxByteBackpressure(t *testing.T) {
	o, err := OpenOutbox(t.TempDir(), Limits{MaxStagingBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	var admitted []uint64
	for i := uint64(1); i <= 20; i++ {
		seq, err := appendOne(t, o, origin, i)
		if err != nil {
			var bp *BackpressureError
			if !errors.As(err, &bp) || !errors.Is(err, ErrBackpressure) {
				t.Fatalf("append %d err = %v", i, err)
			}
			if bp.Journal != "outbox" || bp.UsedBytes <= 0 || bp.MaxBytes != 2048 {
				t.Fatalf("backpressure = %+v", bp)
			}
			break
		}
		admitted = append(admitted, seq)
	}
	if len(admitted) == 0 || len(admitted) >= 20 {
		t.Fatalf("admitted %d events without backpressure", len(admitted))
	}
	// Queued work is intact and usage is consistent.
	if got := o.Pending(0); len(got) != len(admitted) {
		t.Fatalf("pending = %d, admitted %d", len(got), len(admitted))
	}
	u := o.Usage()
	if u.BytesUsed <= 0 || u.BytesUsed > 2048 || u.EntriesUsed != len(admitted) {
		t.Fatalf("usage = %+v", u)
	}
	// Draining published history through GC reopens admission.
	for _, seq := range admitted {
		if err := o.MarkPublished(seq); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o.GC(0); err != nil {
		t.Fatal(err)
	}
	if u := o.Usage(); u.BytesUsed != 0 || u.EntriesUsed != 0 {
		t.Fatalf("usage after gc = %+v", u)
	}
	if _, err := appendOne(t, o, origin, 100); err != nil {
		t.Fatalf("append after gc: %v", err)
	}
}

func TestOutboxEntryBackpressure(t *testing.T) {
	o, err := OpenOutbox(t.TempDir(), Limits{MaxStagingEntries: 3})
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	for i := uint64(1); i <= 3; i++ {
		if _, err := appendOne(t, o, origin, i); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	_, err = appendOne(t, o, origin, 4)
	var bp *BackpressureError
	if !errors.As(err, &bp) || bp.UsedEntries != 3 || bp.MaxEntries != 3 {
		t.Fatalf("err = %v", err)
	}
	if u := o.Usage(); u.EntriesUsed != 3 || u.EntriesMax != 3 {
		t.Fatalf("usage = %+v", u)
	}
}

func TestOutboxRestartAccounting(t *testing.T) {
	dir := t.TempDir()
	o, err := OpenOutbox(dir, Limits{MaxStagingBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	if _, err := appendOne(t, o, origin, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := appendOne(t, o, origin, 2); err != nil {
		t.Fatal(err)
	}
	before := o.Usage()
	// Crash-orphaned temp files are never referenced; reopen sweeps them.
	tmp := filepath.Join(dir, "events", "00000000000000000099.json.tmp-1")
	if err := os.WriteFile(tmp, make([]byte, 1024), 0o644); err != nil {
		t.Fatal(err)
	}
	o2, err := OpenOutbox(dir, Limits{MaxStagingBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("orphaned temp file survived reopen")
	}
	after := o2.Usage()
	if after != before {
		t.Fatalf("usage before=%+v after=%+v", before, after)
	}
	// The recomputed budget still refuses over-cap appends.
	full := false
	for i := uint64(3); i <= 30; i++ {
		if _, err := appendOne(t, o2, origin, i); err != nil {
			if !errors.Is(err, ErrBackpressure) {
				t.Fatalf("append %d err = %v", i, err)
			}
			full = true
			break
		}
	}
	if !full {
		t.Fatal("reopened outbox lost its budget")
	}
}

func TestOutboxConcurrentAppends(t *testing.T) {
	const maxBytes = 16384
	o, err := OpenOutbox(t.TempDir(), Limits{MaxStagingBytes: maxBytes})
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	var seqGen atomic.Uint64
	var admitted atomic.Int64
	var refused atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				n := seqGen.Add(1)
				_, err := appendOne(t, o, origin, n)
				switch {
				case err == nil:
					admitted.Add(1)
				case errors.Is(err, ErrBackpressure):
					refused.Add(1)
				default:
					t.Errorf("append: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if admitted.Load() == 0 || refused.Load() == 0 {
		t.Fatalf("admitted=%d refused=%d", admitted.Load(), refused.Load())
	}
	u := o.Usage()
	if u.BytesUsed > maxBytes {
		t.Fatalf("usage = %+v over cap", u)
	}
	if int64(u.EntriesUsed) != admitted.Load() {
		t.Fatalf("usage = %+v, admitted %d", u, admitted.Load())
	}
	if got := o.Pending(0); len(got) != int(admitted.Load()) {
		t.Fatalf("pending = %d, admitted %d", len(got), admitted.Load())
	}
}

func TestInboxByteBackpressure(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{MaxStagingBytes: 3072})
	if err != nil {
		t.Fatal(err)
	}
	var arts []Artifact
	first := uint64(1)
	for ; first <= 10; first++ {
		a := sealForInbox(t, signer, recip, "s", first, []Batch{
			putBatch(first, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
		})
		if err := inbox.Receive(a); err != nil {
			if !errors.Is(err, ErrBackpressure) {
				t.Fatalf("receive %d err = %v", first, err)
			}
			break
		}
		arts = append(arts, a)
	}
	if len(arts) == 0 || first > 10 {
		t.Fatalf("staged %d bundles without backpressure", len(arts))
	}
	u := inbox.Usage()
	if u.BytesUsed <= 0 || u.BytesUsed > 3072 || u.EntriesUsed != len(arts) {
		t.Fatalf("usage = %+v", u)
	}
	// An identical replay of staged bytes is idempotent, not an admission,
	// so it succeeds even when the journal is full.
	if err := inbox.Receive(arts[0]); err != nil {
		t.Fatalf("replay under pressure: %v", err)
	}
	// Applying (and thus freeing) the head reopens admission.
	if err := inbox.MarkApplied("s", 1, 1); err != nil {
		t.Fatal(err)
	}
	a := sealForInbox(t, signer, recip, "s", first, []Batch{
		putBatch(first, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	if err := inbox.Receive(a); err != nil {
		t.Fatalf("receive after apply: %v", err)
	}
}

func TestInboxQuarantineCountsCapacity(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	a1 := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	a1conflict := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("bob")}),
	})
	fits := int64(len(a1.Data) + len(a1conflict.Data))

	// Forensics fit: conflict quarantines and counts.
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{MaxStagingBytes: fits})
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(a1); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Receive(a1conflict); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("conflict err = %v", err)
	}
	if u := inbox.Usage(); u.BytesUsed != fits || u.EntriesUsed != 2 {
		t.Fatalf("usage = %+v, want %d bytes 2 entries", u, fits)
	}

	// Forensics do not fit: backpressure, no record, original intact.
	tight, err := OpenInbox(t.TempDir(), trust, Limits{MaxStagingBytes: fits - 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := tight.Receive(a1); err != nil {
		t.Fatal(err)
	}
	err = tight.Receive(a1conflict)
	if !errors.Is(err, ErrBackpressure) || errors.Is(err, ErrQuarantined) {
		t.Fatalf("conflict err = %v", err)
	}
	prog := tight.Progress()
	if prog[0].QuarantineTotal != 0 || len(prog[0].Staged) != 1 {
		t.Fatalf("progress = %+v", prog)
	}
}

func TestInboxRestartAccounting(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	dir := t.TempDir()
	inbox, err := OpenInbox(dir, trust, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	a1 := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
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
	b1, err := OpenBundle(a1.Data, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// Held bytes stay staged, so they stay counted.
	if err := inbox.Hold("s", 1, b1.Manifest.BundleID.String(), 1, 1, [32]byte{}, []string{`table "nope"`}); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Quarantine("s", 2, "boom"); err != nil {
		t.Fatal(err)
	}
	before := inbox.Usage()
	if before.EntriesUsed != 2 {
		t.Fatalf("usage = %+v", before)
	}
	inbox2, err := OpenInbox(dir, trust, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if after := inbox2.Usage(); after != before {
		t.Fatalf("usage before=%+v after=%+v", before, after)
	}
	prog := inbox2.Progress()
	if len(prog[0].Holds) != 1 || prog[0].QuarantineTotal != 1 {
		t.Fatalf("progress = %+v", prog)
	}
}

func TestBackpressureErrorShape(t *testing.T) {
	byEntries := &BackpressureError{Journal: "inbox", UsedEntries: 64, MaxEntries: 64}
	if !errors.Is(byEntries, ErrBackpressure) {
		t.Fatal("entries error is not backpressure")
	}
	byBytes := &BackpressureError{Journal: "outbox", NeedBytes: 10, UsedBytes: 100, MaxBytes: 105}
	if !errors.Is(byBytes, ErrBackpressure) {
		t.Fatal("bytes error is not backpressure")
	}
	for i, e := range []*BackpressureError{byEntries, byBytes} {
		var bp *BackpressureError
		if !errors.As(fmt.Errorf("wrap: %w", e), &bp) || bp != e {
			t.Fatalf("case %d does not unwrap", i)
		}
	}
}

func TestCapacityInStatus(t *testing.T) {
	signer, recip, trust := inboxKeys(t, "s")
	o, err := OpenOutbox(t.TempDir(), Limits{MaxStagingBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := appendOne(t, o, ids.NewNodeID(), 1); err != nil {
		t.Fatal(err)
	}
	es := o.ExportStatus(StatusOptions{})
	if es.Capacity != o.Usage() {
		t.Fatalf("export capacity = %+v, usage = %+v", es.Capacity, o.Usage())
	}
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{MaxStagingBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	a := sealForInbox(t, signer, recip, "s", 1, []Batch{
		putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("ann")}),
	})
	if err := inbox.Receive(a); err != nil {
		t.Fatal(err)
	}
	is := inbox.ImportStatus(StatusOptions{})
	if is.Capacity != inbox.Usage() {
		t.Fatalf("import capacity = %+v, usage = %+v", is.Capacity, inbox.Usage())
	}
	if !is.HasBacklog || is.Capacity.BytesUsed <= 0 {
		t.Fatalf("import status = %+v", is)
	}
}
