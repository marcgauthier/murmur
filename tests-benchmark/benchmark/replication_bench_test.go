package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"github.com/marcgauthier/murmur/internal/testdb"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/transport"
)

const benchPortA = "127.0.0.1:17443"

// benchMesh builds a CA plus a config factory for star-topology meshes:
// node A listens on a fixed loopback port, others dial it.
type benchMesh struct {
	dbid  murmur.DBID
	ca    *transport.CA
	creds map[murmur.NodeID]*murmur.TLSCredential
}

func newBenchMesh(b *testing.B) *benchMesh {
	b.Helper()
	ca, err := transport.GenerateCA(time.Hour)
	if err != nil {
		b.Fatal(err)
	}
	return &benchMesh{dbid: murmur.NewDBID(), ca: ca, creds: map[murmur.NodeID]*murmur.TLSCredential{}}
}

func (m *benchMesh) tlsFor(b *testing.B, n murmur.NodeID) *murmur.TLSCredential {
	b.Helper()
	if c, ok := m.creds[n]; ok {
		return c
	}
	certPEM, keyPEM, err := m.ca.IssueNode(n, time.Hour)
	if err != nil {
		b.Fatal(err)
	}
	c := &murmur.TLSCredential{CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: m.ca.CertPEM}
	m.creds[n] = c
	return c
}

func (m *benchMesh) config(b *testing.B, path string, node murmur.NodeID, listen string, peers []murmur.Peer) murmur.Config {
	b.Helper()
	return testdb.Configure(murmur.Config{
		Path:   path,
		NodeID: node,
		DBID:   m.dbid,
		Tables: mustBenchTables(),
		Spool:  murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{
			Key: bytes.Clone(benchKey), KeyID: "bench",
		},
		Replication: murmur.ReplicationConfig{
			ListenAddr: listen, TLS: m.tlsFor(b, node), Peers: peers,
		},
	})
}

func waitConnected(b *testing.B, db *murmur.DB, secs int) {
	b.Helper()
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	for db.Status().ConnectedPeers == 0 {
		if time.Now().After(deadline) {
			b.Fatal("peer never connected")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitContacts(b *testing.B, db *murmur.DB, n int, secs int) {
	b.Helper()
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	for {
		if countContacts(b, db) == n {
			return
		}
		if time.Now().After(deadline) {
			b.Fatalf("converged to %d rows, want %d", countContacts(b, db), n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// BenchmarkReplicationThroughput writes single-row batches on A and
// reports batch/mutation/byte throughput observed on B.
func BenchmarkReplicationThroughput(b *testing.B) {
	ctx := context.Background()
	const n = 1000
	mesh := newBenchMesh(b)
	nodeA, nodeB := murmur.NewNodeID(), murmur.NewNodeID()
	dbA, err := murmur.Open(ctx, mesh.config(b, b.TempDir(), nodeA, benchPortA, nil))
	if err != nil {
		b.Fatal(err)
	}
	defer dbA.Close()
	dbB, err := murmur.Open(ctx, mesh.config(b, b.TempDir(), nodeB, "127.0.0.1:0",
		[]murmur.Peer{{NodeID: nodeA, Addrs: []string{benchPortA}}}))
	if err != nil {
		b.Fatal(err)
	}
	defer dbB.Close()
	waitConnected(b, dbB, 10)
	before := dbB.Status().Replication
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < n; i++ {
		tx, err := dbA.BeginTx(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.InsertItem(&benchContact{
			ID: murmur.NewRowID(), Name: fmt.Sprintf("t %d", i),
			Phone: fmt.Sprintf("555-%04d", i), Score: int64(i % 1000),
		}); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	waitContacts(b, dbB, n, 120)
	elapsed := time.Since(start)
	after := dbB.Status().Replication
	batches := float64(after.BatchesReceived - before.BatchesReceived)
	muts := float64(after.MutationsReceived - before.MutationsReceived)
	payloadMB := float64(after.BatchBytesReceived-before.BatchBytesReceived) / 1e6
	wireMB := float64(after.FrameBytesReceived-before.FrameBytesReceived) / 1e6
	b.ReportMetric(batches/elapsed.Seconds(), "batches/sec")
	b.ReportMetric(muts/elapsed.Seconds(), "mutations/sec")
	b.ReportMetric(payloadMB/elapsed.Seconds(), "payload-MB/sec")
	b.ReportMetric(wireMB/elapsed.Seconds(), "wire-MB/sec")
}

// BenchmarkFiveNodeSync converges a five-node star (A writes, B-E catch
// up) and reports rows/sec to last convergence. Capped at 10K rows:
// five 100K stores exceed a routine benchmark budget.
func BenchmarkFiveNodeSync(b *testing.B) {
	for _, n := range datasetSizes(b) {
		if n > 10_000 {
			continue
		}
		b.Run(sizeName(n), func(b *testing.B) {
			ctx := context.Background()
			mesh := newBenchMesh(b)
			nodeA := murmur.NewNodeID()
			dbA, err := murmur.Open(ctx, mesh.config(b, b.TempDir(), nodeA, benchPortA, nil))
			if err != nil {
				b.Fatal(err)
			}
			defer dbA.Close()
			var followers []*murmur.DB
			for i := 0; i < 4; i++ {
				node := murmur.NewNodeID()
				db, err := murmur.Open(ctx, mesh.config(b, b.TempDir(), node, "127.0.0.1:0",
					[]murmur.Peer{{NodeID: nodeA, Addrs: []string{benchPortA}}}))
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				followers = append(followers, db)
				waitConnected(b, db, 10)
			}
			b.ResetTimer()
			start := time.Now()
			populate(b, dbA, n)
			for _, db := range followers {
				waitContacts(b, db, n, 180)
			}
			elapsed := time.Since(start)
			b.ReportMetric(float64(n)/elapsed.Seconds(), "rows/sec")
			b.ReportMetric(float64(elapsed.Nanoseconds()), "last-converge-ns")
		})
	}
}

// BenchmarkReconnectBacklog closes B, writes a backlog on A, reopens B,
// and reports catch-up rows/sec.
func BenchmarkReconnectBacklog(b *testing.B) {
	for _, n := range datasetSizes(b) {
		if n > 100_000 {
			continue
		}
		b.Run(sizeName(n), func(b *testing.B) {
			ctx := context.Background()
			mesh := newBenchMesh(b)
			nodeA, nodeB := murmur.NewNodeID(), murmur.NewNodeID()
			dbA, err := murmur.Open(ctx, mesh.config(b, b.TempDir(), nodeA, benchPortA, nil))
			if err != nil {
				b.Fatal(err)
			}
			defer dbA.Close()
			pathB := b.TempDir()
			cfgB := mesh.config(b, pathB, nodeB, "127.0.0.1:0",
				[]murmur.Peer{{NodeID: nodeA, Addrs: []string{benchPortA}}})
			dbB, err := murmur.Open(ctx, cfgB)
			if err != nil {
				b.Fatal(err)
			}
			waitConnected(b, dbB, 10)
			if err := dbB.Close(); err != nil {
				b.Fatal(err)
			}
			populate(b, dbA, n)
			b.ResetTimer()
			start := time.Now()
			dbB, err = murmur.Open(ctx, cfgB)
			if err != nil {
				b.Fatal(err)
			}
			defer dbB.Close()
			waitContacts(b, dbB, n, 180)
			elapsed := time.Since(start)
			b.ReportMetric(float64(n)/elapsed.Seconds(), "rows/sec")
			b.ReportMetric(float64(elapsed.Nanoseconds()), "catchup-ns")
		})
	}
}
