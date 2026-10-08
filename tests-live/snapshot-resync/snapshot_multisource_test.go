// Multi-source snapshot resync: the stale node rejoins with three
// survivors serving snapshots concurrently. Concurrent transfers used
// to preempt each other in staging (every new SnapshotID wipes staged
// chunks) and livelock; the receiver now accepts one source and
// suppresses the rest until the active transfer completes or stalls.
package snapshotresync_test

import (
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestStaleNodeRejoinsViaSingleSnapshotSource(t *testing.T) {
	const victim = 3
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "snapshot-multisource",
		NumNodes:        4,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 20000,
			MinRetainedBatches:       10,
		},
	})

	for i := 0; i < 5; i++ {
		if err := insertRows(cluster, victim, 1000+i, 1, "stale"); err != nil {
			t.Fatalf("victim pre-stop write: %v", err)
		}
	}
	waitConverged(t, cluster, 5, 30*time.Second)

	// Victim stops; survivors write thousands of rows so the resync
	// snapshot spans many chunks (a 1-2 chunk snapshot rarely
	// interleaves badly enough to expose source contention).
	cluster.StopNode(victim)
	// Batched multi-row INSERTs: the test needs snapshot volume (many
	// chunks), not round trips. One commit per row is ~100x slower on
	// fsync-heavy disks (NTFS/USB) and blew the 10m scenario timeout
	// without ever hanging.
	const freshRows = 6000
	const batchSize = 100
	start := time.Now()
	for base := 0; base < freshRows; base += batchSize {
		if err := insertRows(cluster, 0, 2000+base, batchSize, "fresh"); err != nil {
			t.Fatalf("survivor write batch %d: %v", base/batchSize, err)
		}
	}
	t.Logf("wrote %d survivor rows in %s", freshRows, time.Since(start).Round(time.Second))
	want := 5 + freshRows
	for i := 0; i < 3; i++ {
		if _, err := waitRowCount(t, cluster, i, want, 60*time.Second); err != nil {
			t.Fatal(err)
		}
	}

	// Same expiry + GC-pass wait as the single-source test: the
	// victim's needed ranges must be genuinely unrecoverable from logs.
	t.Logf("waiting for member-deadline expiry and a GC pass")
	time.Sleep(55 * time.Second)

	cluster.StartNode(victim)
	cluster.UnlockNode(victim, cluster.Nodes[victim].KeyHex)
	cluster.WaitNodeReady(victim)
	waitConverged(t, cluster, want, 120*time.Second)

	// The rejoin used the snapshot path (not log catch-up) under real
	// multi-source contention: at least one rival source's frames were
	// suppressed while the active transfer completed.
	if got := snapshotCounter(t, cluster.Nodes[victim].APIAddr, "spedsql_repl_snapshots_received_total"); got < 1 {
		t.Fatalf("victim snapshots received = %v, want >= 1", got)
	}
	if got := snapshotCounter(t, cluster.Nodes[victim].APIAddr, "spedsql_repl_snapshot_frames_suppressed_total"); got < 1 {
		t.Fatalf("victim suppressed frames = %v, want >= 1 (no multi-source contention observed)", got)
	}

	rows, err := cluster.TypedContentionRows(victim)
	if err != nil {
		t.Fatal(err)
	}
	if countPrefix(rows, "stale-") != 5 {
		t.Fatalf("victim stale rows = %d, want 5", countPrefix(rows, "stale-"))
	}
}
