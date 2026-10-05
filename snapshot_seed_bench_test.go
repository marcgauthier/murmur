package murmur

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkSnapshotSeed joins a fresh node via snapshot after wiping the
// source origin log (as if long-retained GC ran), reporting rows/sec to
// convergence plus snapshot bytes received.
func BenchmarkSnapshotSeed(b *testing.B) {
	ctx := context.Background()
	const n = 1000
	nodeA, nodeC := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(b, nodeA, nodeC)

	dbA, err := openSignedFixture(ctx, replConfig(b.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		b.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(b, dbA, 5*time.Second)

	for i := 0; i < n; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("seed%04d", i), fmt.Sprintf("p%04d", i)); err != nil {
			b.Fatal(err)
		}
	}
	if got, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || got != n {
		b.Fatalf("CollectLog = %d, %v", got, err)
	}
	cfgC := replConfig(b.TempDir(), nodeC, dbid, creds[nodeC],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	b.ResetTimer()
	start := time.Now()
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		b.Fatal(err)
	}
	defer dbC.Close()
	waitForRows(b, dbC, n, 120*time.Second)
	elapsed := time.Since(start)
	snapBytes := dbC.Status().Replication.SnapshotBytesReceived
	b.ReportMetric(float64(n)/elapsed.Seconds(), "rows/sec")
	b.ReportMetric(float64(snapBytes)/1e6, "snapshot-MB")
	b.ReportMetric(float64(elapsed.Nanoseconds()), "seed-converge-ns")
}
