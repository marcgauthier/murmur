package state

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

func openTestStore(t *testing.T, node ids.NodeID) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func localBatch(s *Store, hlc uint64, muts ...codec.Mutation) *codec.MutationBatch {
	return &codec.MutationBatch{
		ProtocolVersion: 1,
		TxID:            ids.NewTxID(),
		OriginNode:      s.NodeID(),
		HLC:             hlc,
		SchemaEpoch:     1,
		Mutations:       muts,
	}
}

func TestCommitLocalAndGet(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	res, err := s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: row, ColumnID: 2, Value: codec.Text("hi")}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied || len(res.Winners) != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	st, ok, err := s.GetCell(7, row, 2)
	if err != nil || !ok {
		t.Fatalf("GetCell: %v %v", st, err)
	}
	if st.Value.S != "hi" {
		t.Fatalf("got %q", st.Value.S)
	}
	if seq, _ := s.LocalSeq(); seq != 1 {
		t.Fatalf("seq = %d", seq)
	}
	if wm, _ := s.ReceiveWatermark(s.NodeID()); wm != 1 {
		t.Fatalf("watermark = %d", wm)
	}
}

func TestCommitLocalIdempotentTxID(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	b := localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: row, ColumnID: 2, Value: codec.Text("v1")})
	if _, err := s.CommitLocal(ctx, b); err != nil {
		t.Fatal(err)
	}
	// Retry with the same TxID: no new sequence, no winners.
	res, err := s.CommitLocal(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied || len(res.Winners) != 0 {
		t.Fatalf("retry should be a no-op: %+v", res)
	}
	if seq, _ := s.LocalSeq(); seq != 1 {
		t.Fatalf("seq = %d, want 1", seq)
	}
}

func TestRemoteLWWAndResurrect(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	b := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()

	// A writes phone=v1 at HLC 100 (simulated clock values for determinism).
	ab, err := a.CommitLocal(ctx, localBatch(a, 100<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Text("v1")}))
	if err != nil {
		t.Fatal(err)
	}
	_ = ab
	// B writes phone=v2 at HLC 200.
	if _, err := b.CommitLocal(ctx, localBatch(b, 200<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Text("v2")})); err != nil {
		t.Fatal(err)
	}
	// Exchange logs both ways.
	forward := func(from, to *Store, origin ids.NodeID) {
		t.Helper()
		wm, _ := to.ReceiveWatermark(origin)
		_, err := from.LogScan(origin, wm+1, 100, 1<<20, func(batch *codec.MutationBatch) error {
			_, err := to.CommitRemote(ctx, batch)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	forward(a, b, a.NodeID())
	forward(b, a, b.NodeID())
	// Both converge to v2 (higher HLC).
	for i, s := range []*Store{a, b} {
		st, ok, err := s.GetCell(1, row, 2)
		if err != nil || !ok || st.Value.S != "v2" {
			t.Fatalf("store %d: %+v %v %v", i, st, ok, err)
		}
	}
	// A deletes the row at HLC 300; B's older update must not resurrect it,
	// but a newer update must.
	if _, err := a.CommitLocal(ctx, localBatch(a, 300<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone})); err != nil {
		t.Fatal(err)
	}
	forward(a, b, a.NodeID())
	if _, present, _ := b.GetTombstone(1, row); !present {
		t.Fatal("tombstone missing on B")
	}
	// Stale update at HLC 250 loses to the tombstone for visibility...
	if _, err := b.CommitLocal(ctx, localBatch(b, 250<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Text("stale")})); err != nil {
		t.Fatal(err)
	}
	// ...but the cell itself is still stored (LWW per cell).
	st, _, _ := b.GetCell(1, row, 2)
	if st.Value.S != "stale" {
		t.Fatalf("stale cell write lost: %+v", st)
	}
	// Newer update resurrects.
	if _, err := b.CommitLocal(ctx, localBatch(b, 400<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Text("new")})); err != nil {
		t.Fatal(err)
	}
	forward(b, a, b.NodeID())
	for i, s := range []*Store{a, b} {
		if err := s.IterateTable(1, func(r *Row) error {
			if r.ID != row {
				return nil
			}
			if !r.Visible() {
				t.Fatalf("store %d: row should be resurrected", i)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRemoteGapAndDuplicate(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	b := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	mk := func(hlc uint64) *codec.MutationBatch {
		return localBatch(a, hlc,
			codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Int(int64(hlc))})
	}
	b1 := mk(a.ClockNow())
	if _, err := a.CommitLocal(ctx, b1); err != nil {
		t.Fatal(err)
	}
	b2 := mk(a.ClockNow())
	if _, err := a.CommitLocal(ctx, b2); err != nil {
		t.Fatal(err)
	}
	// Deliver b2 first: gap.
	if _, err := b.CommitRemote(ctx, b2); !errors.Is(err, ErrGap) {
		t.Fatalf("expected gap, got %v", err)
	}
	// Deliver b1, then b2 again: both apply.
	if _, err := b.CommitRemote(ctx, b1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CommitRemote(ctx, b2); err != nil {
		t.Fatal(err)
	}
	// Redeliver b1: duplicate, no new winners.
	res, err := b.CommitRemote(ctx, b1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied || len(res.Winners) != 0 {
		t.Fatalf("duplicate should be a no-op: %+v", res)
	}
}

func TestSnapshotExportImport(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	b := openTestStore(t, ids.NewNodeID())
	row1, row2 := ids.NewRowID(), ids.NewRowID()
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 1, RowID: row1, ColumnID: 1, Value: codec.Text("a")},
		codec.Mutation{TableID: 1, RowID: row1, ColumnID: 2, Value: codec.Int(1)},
		codec.Mutation{TableID: 2, RowID: row2, ColumnID: 1, Value: codec.Blob([]byte{9})},
	)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 1, RowID: row2, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone})); err != nil {
		t.Fatal(err)
	}
	if err := a.ExportSnapshot(2, func(m *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error {
		if m.DBID != a.DBID() {
			t.Fatalf("manifest db mismatch")
		}
		_, err := b.ImportSnapshotChunk(ctx, m, chunk)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// B converged without any log batches.
	st, ok, _ := b.GetCell(1, row1, 2)
	if !ok || st.Value.I != 1 {
		t.Fatalf("cell missing: %+v %v", st, ok)
	}
	if _, present, _ := b.GetTombstone(1, row2); !present {
		t.Fatal("tombstone missing")
	}
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 2 {
		t.Fatalf("watermark = %d, want 2", wm)
	}
	// Re-importing the same snapshot is idempotent (no new winners).
	if err := a.ExportSnapshot(100, func(m *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error {
		res, err := b.ImportSnapshotChunk(ctx, m, chunk)
		if err != nil {
			return err
		}
		if len(res.Winners) != 0 {
			t.Fatalf("re-import produced %d winners", len(res.Winners))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLogGoneAfterGC(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	for i := 0; i < 5; i++ {
		if _, err := s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
			codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Int(int64(i))})); err != nil {
			t.Fatal(err)
		}
	}
	// GC everything (retention in the far future => wall cutoff MaxInt64
	// deletes all, minRetain 0).
	if n, err := s.CollectLog(s.NodeID(), ^uint64(0), 1<<62, 0); err != nil || n != 5 {
		t.Fatalf("CollectLog = %d, %v", n, err)
	}
	if _, err := s.LogScan(s.NodeID(), 1, 10, 1<<20, func(*codec.MutationBatch) error {
		return nil
	}); !errors.Is(err, ErrLogGone) {
		t.Fatalf("expected ErrLogGone, got %v", err)
	}
	// Current state is untouched by log GC.
	st, ok, _ := s.GetCell(1, row, 2)
	if !ok || st.Value.I != 4 {
		t.Fatalf("current state damaged: %+v %v", st, ok)
	}
}

// TestConvergenceProperty applies the same logical mutation set to three
// stores in different delivery orders (including duplicates and restarts of
// delivery) and requires identical final state.
func TestConvergenceProperty(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(20260926))
	const origins = 3
	const batchesPerOrigin = 60

	type originLog struct {
		node    ids.NodeID
		batches []*codec.MutationBatch
	}
	logs := make([]originLog, origins)
	var allRows []ids.RowID
	for i := 0; i < 12; i++ {
		allRows = append(allRows, ids.NewRowID())
	}
	for o := 0; o < origins; o++ {
		node := ids.NewNodeID()
		// Simulated per-origin HLCs (interleaved wall clocks).
		var hlc uint64 = uint64(rng.Int63n(50)+1) << 16
		for b := 0; b < batchesPerOrigin; b++ {
			hlc += uint64(rng.Int63n(5) + 1)
			nm := 1 + rng.Intn(4)
			batch := &codec.MutationBatch{
				ProtocolVersion: 1,
				TxID:            ids.NewTxID(),
				OriginNode:      node,
				Sequence:        uint64(b + 1),
				HLC:             hlc,
				SchemaEpoch:     1,
			}
			for k := 0; k < nm; k++ {
				row := allRows[rng.Intn(len(allRows))]
				if rng.Intn(20) == 0 {
					batch.Mutations = append(batch.Mutations, codec.Mutation{
						TableID: 1, RowID: row, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone,
					})
				} else {
					batch.Mutations = append(batch.Mutations, codec.Mutation{
						TableID: 1, RowID: row, ColumnID: uint32(1 + rng.Intn(3)),
						Value: codec.Int(rng.Int63n(1000)),
					})
				}
			}
			logs[o].node = node
			logs[o].batches = append(logs[o].batches, batch)
		}
	}

	deliver := func(order []int) map[string]string {
		s := openTestStore(t, ids.NewNodeID())
		// Pending buffers one out-of-order batch per origin (simple retry).
		pending := make(map[ids.NodeID][]*codec.MutationBatch)
		apply := func(b *codec.MutationBatch) {
			_, err := s.CommitRemote(ctx, b)
			if errors.Is(err, ErrGap) {
				pending[b.OriginNode] = append(pending[b.OriginNode], b)
				return
			}
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			// Drain newly-unblocked pending batches.
			for {
				progress := false
				for origin, list := range pending {
					rest := list[:0]
					for _, pb := range list {
						_, err := s.CommitRemote(ctx, pb)
						if errors.Is(err, ErrGap) {
							rest = append(rest, pb)
							continue
						}
						if err != nil {
							t.Fatalf("apply pending: %v", err)
						}
						progress = true
					}
					pending[origin] = rest
				}
				if !progress {
					break
				}
			}
		}
		// Flatten in the requested origin-interleaving with duplicates.
		var flat []*codec.MutationBatch
		for _, o := range order {
			flat = append(flat, logs[o].batches...)
		}
		// Add duplicates of 10% of batches at random positions.
		dups := append([]*codec.MutationBatch{}, flat...)
		for i := range dups {
			j := rng.Intn(len(dups))
			dups[i], dups[j] = dups[j], dups[i]
		}
		flat = append(flat, dups[:len(dups)/10]...)
		for _, b := range flat {
			apply(b)
		}
		// Drain stragglers in origin order.
		for o := 0; o < origins; o++ {
			for _, b := range logs[o].batches {
				_, err := s.CommitRemote(ctx, b)
				if err != nil && !errors.Is(err, ErrGap) {
					t.Fatalf("drain: %v", err)
				}
			}
		}
		return fingerprint(t, s)
	}

	orders := [][]int{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}, {0, 2, 1}}
	var want map[string]string
	for i, order := range orders {
		got := deliver(order)
		if i == 0 {
			want = got
			continue
		}
		if len(got) != len(want) {
			t.Fatalf("order %v: %d cells != %d", order, len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("order %v: cell %s = %q != %q", order, k, got[k], v)
			}
		}
	}
}

// TestFormatVersionTamperFailsOpen proves a store with an unknown persistent
// format version fails closed instead of misreading data.
func TestFormatVersionTamperFailsOpen(t *testing.T) {
	dir := t.TempDir()
	node := ids.NewNodeID()
	s, err := Open(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	// Tamper the format version directly in Pebble.
	if err := s.db.Set(SysKey(sysFormat), encodeU64(999), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if _, err := Open(dir, node, ids.DBID{}, Options{}); err == nil {
		t.Fatal("expected open with format 999 to fail")
	}
}

// TestMaintenanceCloseReopen cycles Pebble underneath the same Store handle:
// operations block across the window and data is intact afterwards.
func TestMaintenanceCloseReopen(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	if _, err := s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: row, ColumnID: 2, Value: codec.Text("hi")})); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseForMaintenance(); err != nil {
		t.Fatal(err)
	}
	// Operations block while closed: run one and assert it waits.
	done := make(chan error, 1)
	go func() {
		_, _, err := s.GetCell(7, row, 2)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("op did not block: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := s.ReopenAfterMaintenance(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("blocked op: %v", err)
	}
	st, ok, err := s.GetCell(7, row, 2)
	if err != nil || !ok || st.Value.S != "hi" {
		t.Fatalf("after reopen: %v %v", st, err)
	}
	if _, err := s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("yo")})); err != nil {
		t.Fatalf("commit after reopen: %v", err)
	}
}

// fingerprint renders the full current state (cells + tombstones) as a map.
func fingerprint(t *testing.T, s *Store) map[string]string {
	t.Helper()
	out := make(map[string]string)
	var cells []codec.SnapshotCell
	if err := s.IterateCells(func(c codec.SnapshotCell) error {
		cells = append(cells, c)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	sort.Slice(cells, func(i, j int) bool {
		if cells[i].TableID != cells[j].TableID {
			return cells[i].TableID < cells[j].TableID
		}
		if cells[i].RowID != cells[j].RowID {
			return cells[i].RowID.String() < cells[j].RowID.String()
		}
		return cells[i].ColumnID < cells[j].ColumnID
	})
	for _, c := range cells {
		key := fmt.Sprintf("%d/%s/%d", c.TableID, c.RowID, c.ColumnID)
		out[key] = fmt.Sprintf("%s/%d/%s", c.Version.NodeID, c.Version.HLC, valueString(c.Value))
	}
	return out
}

func valueString(v codec.Value) string {
	switch v.Type {
	case codec.TypeNull:
		return "null"
	case codec.TypeInteger:
		return fmt.Sprintf("i%d", v.I)
	case codec.TypeReal:
		return fmt.Sprintf("f%v", v.F)
	case codec.TypeText:
		return "t" + v.S
	case codec.TypeBlob:
		return fmt.Sprintf("b%x", v.B)
	}
	return "?"
}
