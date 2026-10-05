package murmur

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/state"
)

// scanOriginLog counts live origin-log batches; fully collected logs
// report ErrLogGone and count as zero remaining.
func scanOriginLog(t *testing.T, db *DB, origin NodeID) int {
	t.Helper()
	var n int
	_, err := db.store.LogScan(origin, 1, 1<<20, 1<<30, func(*codec.MutationBatch) error {
		n++
		return nil
	})
	if err != nil {
		if errors.Is(err, state.ErrLogGone) {
			return 0
		}
		t.Fatal(err)
	}
	return n
}

// gcTestConfig returns a single-node config with GC floors that collect
// everything eligible: near-zero retention and no minimum retention.
func gcTestConfig(path string) Config {
	cfg := testConfig(path)
	cfg.Replication.MinLogRetention = time.Nanosecond
	cfg.Replication.MinRetainedBatches = 0
	return cfg
}

// TestGCDrainsPastUnitCap proves one gcOnce collects past the per-call
// unit cap: with 6000 eligible batches and the 1000-batch minimum
// retention (zero MinRetainedBatches defaults to 1000), a single pass
// must collect 5000. Without the drain loop only the first 4096 collect
// and the retained log grows without bound under any sustained workload.
func TestGCDrainsPastUnitCap(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, gcTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const batches = 6000
	for i := 0; i < batches; i++ {
		id := NewRowID()
		if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
			id[:], fmt.Sprintf("d%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-time.Hour).UnixMilli()
	if _, err := db.store.EnsureMemberAdmitted(NewNodeID(), past, 60_000); err != nil {
		t.Fatal(err)
	}
	db.gcOnce(false)
	const want = batches - 1000 // minimum retention keeps the newest 1000
	if got := db.metrics.gcLogCollected.Load(); got != want {
		t.Fatalf("one gcOnce collected %d batches, want %d (drain loop)", got, want)
	}
	first, err := db.store.FirstRetainedSeq(db.cfg.NodeID)
	if err != nil || first != want+1 {
		t.Fatalf("first retained seq = %d, %v; want %d", first, err, want+1)
	}
	if got := queryAll(t, db, `SELECT id FROM contacts`); len(got) != batches {
		t.Fatalf("rows after GC = %d, want %d", len(got), batches)
	}
}

// TestGCExpiredObligationReleasesHistory proves retention deadlines gate
// log collection: an expired member pins nothing (its history collects
// while SQL keeps serving), and a live member without acks pins everything.
func TestGCExpiredObligationReleasesHistory(t *testing.T) {
	ctx := context.Background()

	t.Run("expired", func(t *testing.T) {
		db, err := openSignedFixture(ctx, gcTestConfig(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		nodeA := db.cfg.NodeID
		for i := 0; i < 10; i++ {
			id := NewRowID()
			if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
				id[:], fmt.Sprintf("g%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		// Obligation already expired an hour ago: deterministic, no waiting.
		past := time.Now().Add(-time.Hour).UnixMilli()
		if _, err := db.store.EnsureMemberAdmitted(NewNodeID(), past, 60_000); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for db.metrics.gcLogCollected.Load() < 10 {
			if time.Now().After(deadline) {
				t.Fatal("expired member still pins history after 5s of GC")
			}
			db.gcOnce(false)
			time.Sleep(5 * time.Millisecond)
		}
		// The collected prefix reports gone; data and membership survive.
		if _, err := db.store.LogScan(nodeA, 1, 1<<20, 1<<30,
			func(*codec.MutationBatch) error { return nil }); !errors.Is(err, state.ErrLogGone) {
			t.Fatalf("collected prefix scan err = %v, want ErrLogGone", err)
		}
		if got := queryAll(t, db, `SELECT id FROM contacts`); len(got) != 10 {
			t.Fatalf("rows after GC = %d, want 10", len(got))
		}
		members, err := db.store.ListMembers()
		if err != nil || len(members) != 1 {
			t.Fatalf("members = %v, %v", members, err)
		}
		if members[0].Gating(time.Now().UnixMilli()) {
			t.Fatal("expired member still gating")
		}
	})

	t.Run("live-pins", func(t *testing.T) {
		db, err := openSignedFixture(ctx, gcTestConfig(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		nodeA := db.cfg.NodeID
		for i := 0; i < 10; i++ {
			id := NewRowID()
			if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`,
				id[:], fmt.Sprintf("h%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		// Live obligation, no acks: floor stays zero, nothing collects.
		if _, err := db.store.EnsureMemberAdmitted(NewNodeID(), time.Now().UnixMilli(), 3_600_000); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			db.gcOnce(false)
			time.Sleep(50 * time.Millisecond)
		}
		if got := db.metrics.gcLogCollected.Load(); got != 0 {
			t.Fatalf("live member pinned history but %d batches collected", got)
		}
		if got := scanOriginLog(t, db, nodeA); got != 10 {
			t.Fatalf("live member pinned %d batches, want 10", got)
		}
	})
}
