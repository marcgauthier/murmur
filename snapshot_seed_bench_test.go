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

	cfgA := replConfig(b.TempDir(), nodeA, dbid, creds[nodeA], nil)
	cfgA.Schema.Tables = nil
	definition, err := Define[facadeRecord]("records", 71, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
	})
	if err != nil {
		b.Fatal(err)
	}
	cfgA.Tables = []TableDefinition{definition}
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		b.Fatal(err)
	}
	defer dbA.Close()
	tableA, err := TableOf[facadeRecord](dbA, "records")
	if err != nil {
		b.Fatal(err)
	}
	addrA := waitForAddr(b, dbA, 5*time.Second)

	for i := 0; i < n; i++ {
		if err := insertRecord(ctx, dbA, tableA, &facadeRecord{ID: NewRowID(), Name: fmt.Sprintf("seed%04d", i)}); err != nil {
			b.Fatal(err)
		}
	}
	if got, err := dbA.store.CollectLog(nodeA, ^uint64(0), 1<<62, 0); err != nil || got != n {
		b.Fatalf("CollectLog = %d, %v", got, err)
	}
	cfgC := replConfig(b.TempDir(), nodeC, dbid, creds[nodeC],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgC.Schema.Tables = nil
	cfgC.Tables = []TableDefinition{definition}
	b.ResetTimer()
	start := time.Now()
	dbC, err := openSignedFixture(ctx, cfgC)
	if err != nil {
		b.Fatal(err)
	}
	defer dbC.Close()
	tableC, err := TableOf[facadeRecord](dbC, "records")
	if err != nil {
		b.Fatal(err)
	}
	deadline := time.Now().Add(120 * time.Second)
	for {
		count, err := tableC.Where().Count()
		if err == nil && count == n {
			break
		}
		if time.Now().After(deadline) {
			b.Fatalf("timed out waiting for %d rows", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	snapBytes := dbC.Status().Replication.SnapshotBytesReceived
	b.ReportMetric(float64(n)/elapsed.Seconds(), "rows/sec")
	b.ReportMetric(float64(snapBytes)/1e6, "snapshot-MB")
	b.ReportMetric(float64(elapsed.Nanoseconds()), "seed-converge-ns")
}
