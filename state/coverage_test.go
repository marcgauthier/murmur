package state

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

// TestStoreMaintenanceAccessors covers the small status hooks: a fresh store
// is not maintenance-closed, reports no sticky failure, has disk usage, and
// supports the manual Flush/Compact/Checkpoint operations.
func TestStoreMaintenanceAccessors(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	if s.MaintenanceClosed() {
		t.Fatal("fresh store reports maintenance-closed")
	}
	if err := s.Failed(); err != nil {
		t.Fatalf("Failed = %v, want nil", err)
	}
	size, err := s.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size == 0 {
		t.Fatal("Size = 0, want > 0")
	}
	m := s.Metrics()
	if m.DiskBytes == 0 {
		t.Fatal("Metrics DiskBytes = 0, want > 0")
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := s.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Checkpoint(filepath.Join(t.TempDir(), "ckpt")); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

// TestReceiveWatermarksListsOrigins proves ReceiveWatermarks reports every
// origin with received log entries, including remote origins.
func TestReceiveWatermarksListsOrigins(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	b := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	if _, err := a.CommitLocal(ctx, localBatch(a, 100<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Text("v1")})); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CommitLocal(ctx, localBatch(b, 200<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 3, Value: codec.Text("v2")})); err != nil {
		t.Fatal(err)
	}
	// Ship a's log to b so b tracks two origins.
	_, err := a.LogScan(a.NodeID(), 1, 100, 1<<20, func(batch *codec.MutationBatch) error {
		_, err := commitRemoteFixture(b, ctx, batch)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.ReceiveWatermarks()
	if err != nil {
		t.Fatal(err)
	}
	byOrigin := make(map[ids.NodeID]uint64, len(got))
	for _, wm := range got {
		byOrigin[wm.Origin] = wm.Sequence
	}
	if byOrigin[a.NodeID()] != 1 || byOrigin[b.NodeID()] != 1 {
		t.Fatalf("ReceiveWatermarks = %v, want both origins at 1", byOrigin)
	}
}

// TestClearAllPeerExclusions proves exclusions can be listed and cleared.
func TestClearAllPeerExclusions(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	p1, p2 := ids.NewNodeID(), ids.NewNodeID()
	if err := s.SetPeerExcluded(p1, true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPeerExcluded(p2, true); err != nil {
		t.Fatal(err)
	}
	listed, err := s.ListExcludedPeers()
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 {
		t.Fatalf("ListExcludedPeers = %d peers, want 2", len(listed))
	}
	if err := s.ClearAllPeerExclusions(); err != nil {
		t.Fatal(err)
	}
	if listed, err := s.ListExcludedPeers(); err != nil || len(listed) != 0 {
		t.Fatalf("after clear: peers = %v, err = %v", listed, err)
	}
	if excluded, err := s.IsPeerExcluded(p1); err != nil || excluded {
		t.Fatalf("after clear: excluded = %v, err = %v", excluded, err)
	}
}

// TestErrorClassifiers pins the IsConflict/IsGap compatibility helpers.
func TestErrorClassifiers(t *testing.T) {
	if IsConflict(ErrGap) || IsConflict(errors.New("boom")) || IsConflict(nil) {
		t.Fatal("IsConflict must always report false")
	}
	if !IsGap(ErrGap) {
		t.Fatal("IsGap(ErrGap) = false, want true")
	}
	if IsGap(errors.New("boom")) || IsGap(nil) {
		t.Fatal("IsGap(non-gap) = true, want false")
	}
}

// TestBridgeExportResumeLifecycle proves the in-memory bridge resume floor
// can be set, read, and cleared.
func TestBridgeExportResumeLifecycle(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	origin := ids.NewNodeID()
	if _, ok := s.BridgeExportFloor(origin); ok {
		t.Fatal("fresh BridgeExportFloor reports ok")
	}
	s.SetBridgeExportResume(origin, 42)
	if seq, ok := s.BridgeExportFloor(origin); !ok || seq != 42 {
		t.Fatalf("BridgeExportFloor = %d/%v, want 42/true", seq, ok)
	}
	s.ClearBridgeExportResume(origin)
	if _, ok := s.BridgeExportFloor(origin); ok {
		t.Fatal("cleared BridgeExportFloor still reports ok")
	}
	// Clearing an absent origin (and a nil map) must not panic.
	s.ClearBridgeExportResume(ids.NewNodeID())
	fresh := openTestStore(t, ids.NewNodeID())
	fresh.ClearBridgeExportResume(origin)
}

// TestReceiptRecordHasCollect proves receipts can be recorded, probed, and
// garbage-collected by per-origin floors.
func TestReceiptRecordHasCollect(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	tx := ids.NewTxID()
	if has, err := s.HasReceipt(tx); err != nil || has {
		t.Fatalf("fresh HasReceipt = %v/%v, want false/nil", has, err)
	}
	if err := s.RecordReceipt(tx); err != nil {
		t.Fatal(err)
	}
	if has, err := s.HasReceipt(tx); err != nil || !has {
		t.Fatalf("recorded HasReceipt = %v/%v, want true/nil", has, err)
	}
	// No floors: nothing collectable.
	if n, err := s.CollectReceipts(nil); err != nil || n != 0 {
		t.Fatalf("CollectReceipts(nil) = %d/%v, want 0/nil", n, err)
	}
	// RecordReceipt stamps seq 0, so floor 0 collects it.
	if n, err := s.CollectReceipts(map[ids.NodeID]uint64{s.NodeID(): 0}); err != nil || n != 1 {
		t.Fatalf("CollectReceipts = %d/%v, want 1/nil", n, err)
	}
	if has, err := s.HasReceipt(tx); err != nil || has {
		t.Fatalf("collected HasReceipt = %v/%v, want false/nil", has, err)
	}
}

// TestCollectReceiptsRespectsFloors proves retention leases and bridge
// resume points bound receipt collection floors.
func TestCollectReceiptsRespectsFloors(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	tx := ids.NewTxID()
	if err := s.RecordReceipt(tx); err != nil {
		t.Fatal(err)
	}
	// A retention lease for another origin must not protect this receipt,
	// but the bound path still runs through RetentionFloor.
	release, err := s.AcquireRetentionLease("test", time.Now().Add(time.Hour),
		map[ids.NodeID]uint64{ids.NewNodeID(): 99})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	s.SetBridgeExportResume(ids.NewNodeID(), 7)
	if n, err := s.CollectReceipts(map[ids.NodeID]uint64{s.NodeID(): 0}); err != nil || n != 1 {
		t.Fatalf("CollectReceipts = %d/%v, want 1/nil", n, err)
	}
}

// TestBridgeStreamProgress proves stream progress round-trips and reports
// absent streams.
func TestBridgeStreamProgress(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	if _, ok, err := s.BridgeStreamProgress("missing"); err != nil || ok {
		t.Fatalf("missing stream = %v/%v, want false/nil", ok, err)
	}
	if err := s.SetBridgeStreamProgress("alpha", 17); err != nil {
		t.Fatal(err)
	}
	if seq, ok, err := s.BridgeStreamProgress("alpha"); err != nil || !ok || seq != 17 {
		t.Fatalf("alpha = %d/%v/%v, want 17/true/nil", seq, ok, err)
	}
}

// TestBridgeProgressKeys pins the bridge progress key codec, including the
// invalid inputs ParseBridgeProgressKey must reject.
func TestBridgeProgressKeys(t *testing.T) {
	k := BridgeProgressKey("alpha")
	if got, ok := ParseBridgeProgressKey(k); !ok || got != "alpha" {
		t.Fatalf("round trip = %q/%v", got, ok)
	}
	if prefix := BridgeProgressPrefix(); len(prefix) != 1 || k[0] != prefix[0] {
		t.Fatalf("prefix %x does not match key %x", prefix, k)
	}
	for _, bad := range [][]byte{nil, {}, {0x00}, BridgeProgressKey("")[:1]} {
		if got, ok := ParseBridgeProgressKey(bad); ok || got != "" {
			t.Fatalf("ParseBridgeProgressKey(%x) = %q/%v, want rejected", bad, got, ok)
		}
	}
	peer := ids.NewNodeID()
	pp := PeerAckPeerPrefix(peer)
	if len(pp) != 17 || string(pp[1:]) != string(peer[:]) {
		t.Fatalf("PeerAckPeerPrefix = %x, want 0x05+peer", pp)
	}
	row := ids.NewRowID()
	rp := CellRowPrefix(9, row)
	if len(rp) != 21 || string(rp[5:]) != string(row[:]) {
		t.Fatalf("CellRowPrefix = %x, want prefix+table+row", rp)
	}
}

// TestGetRowAndFirstRetainedSeq proves GetRow returns stored cells and
// FirstRetainedSeq tracks log retention per origin.
func TestGetRowAndFirstRetainedSeq(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	if _, err := s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: row, ColumnID: 2, Value: codec.Text("hi")},
		codec.Mutation{TableID: 7, RowID: row, ColumnID: 3, Value: codec.Text("yo")},
	)); err != nil {
		t.Fatal(err)
	}
	cells, err := s.GetRow(7, row)
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 2 || cells[2].Value.S != "hi" || cells[3].Value.S != "yo" {
		t.Fatalf("GetRow = %v, want both cells", cells)
	}
	empty, err := s.GetRow(7, ids.NewRowID())
	if err != nil || len(empty) != 0 {
		t.Fatalf("GetRow(missing) = %v/%v, want empty/nil", empty, err)
	}
	first, err := s.FirstRetainedSeq(s.NodeID())
	if err != nil || first != 1 {
		t.Fatalf("FirstRetainedSeq(local) = %d/%v, want 1/nil", first, err)
	}
	// Unknown origin: empty log, so watermark+1 = 1.
	first, err = s.FirstRetainedSeq(ids.NewNodeID())
	if err != nil || first != 1 {
		t.Fatalf("FirstRetainedSeq(unknown) = %d/%v, want 1/nil", first, err)
	}
}

// TestSnapshotMetadataPersists writes a metadata entry and reads it back
// through the raw snapshot key.
func TestSnapshotMetadataPersists(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	if err := s.SnapshotMetadata("last-applied", []byte("snap-9")); err != nil {
		t.Fatal(err)
	}
	raw, err := s.getDirect(SnapshotKey("last-applied"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "snap-9" {
		t.Fatalf("metadata = %q, want snap-9", raw)
	}
}

// TestFatalCaptureTerminal proves the fail-closed capture trips once with
// the first message and cause, and stays nil-safe.
func TestFatalCaptureTerminal(t *testing.T) {
	var fatal fatalCapture
	fatal.noteTerminal("boom", errTestCause)
	fatal.noteTerminal("second", errTestCause)
	err := fatal.err()
	if !IsStorageFailure(err) {
		t.Fatalf("err = %v, want storage failure", err)
	}
	if got := err.Error(); !strings.Contains(got, "boom") || strings.Contains(got, "second") {
		t.Fatalf("err = %q, want first message to win", got)
	}
	if !errors.Is(err, errTestCause) {
		t.Fatalf("err = %v, want test cause", err)
	}
	var nilCap *fatalCapture
	nilCap.noteTerminal("x", errTestCause)
	if err := nilCap.err(); err != nil {
		t.Fatalf("nil capture err = %v, want nil", err)
	}
}
