package benchmark

import (
	"bytes"
	"context"
	"github.com/marcgauthier/murmur/internal/testdb"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/transport"
)

// BenchmarkRebuild times Open (Pebble open + full SQL rebuild from current
// state) on a pre-populated store. Single-shot per size with explicit metrics.
func BenchmarkRebuild(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			dir := b.TempDir()
			node := murmur.NewNodeID()
			mkcfg := func() murmur.Config {
				return testdb.Configure(murmur.Config{
					Path:   dir,
					NodeID: node,
					Schema: murmur.SchemaConfig{
						Version: 1, Tables: benchSchema(), LocalDDL: benchLocalDDL(),
					},
					Pebble: murmur.DefaultPebbleConfig(),
					Encryption: murmur.EncryptionConfig{
						Key: bytes.Repeat([]byte{0x62}, 32), KeyID: "bench",
					},
				})
			}
			seed, err := murmur.Open(context.Background(), mkcfg())
			if err != nil {
				b.Fatal(err)
			}
			// Populate without benchmark-Cleanup Close (we close manually).
			populate(b, seed, n)
			if err := seed.Close(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			start := time.Now()
			db, err := murmur.Open(context.Background(), mkcfg())
			elapsed := time.Since(start)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			// Time to first query.
			qstart := time.Now()
			rows, err := db.QueryContext(context.Background(), `SELECT COUNT(*) FROM contacts`)
			if err != nil {
				b.Fatal(err)
			}
			var count int
			for rows.Next() {
				_ = rows.Scan(&count)
			}
			rows.Close()
			if count != n {
				b.Fatalf("rebuilt %d rows, want %d", count, n)
			}
			b.ReportMetric(float64(elapsed.Nanoseconds()), "open-ns")
			b.ReportMetric(float64(time.Since(qstart).Nanoseconds()), "first-query-ns")
			b.ReportMetric(float64(n)/elapsed.Seconds(), "rows/sec")
		})
	}
}

// BenchmarkReplicationSync writes n rows on node A and measures wall time
// until node B converges. Single-shot per size with explicit metrics.
// It uses a fixed loopback port (the public API exposes no listener
// address) and caps datasets: sync is timing-sensitive, not size-sensitive.
func BenchmarkReplicationSync(b *testing.B) {
	for _, n := range datasetSizes(b) {
		if n > 100_000 {
			continue
		}
		b.Run(sizeName(n), func(b *testing.B) {
			ctx := context.Background()
			nodeA, nodeB := murmur.NewNodeID(), murmur.NewNodeID()
			dbid := murmur.NewDBID()
			ca, err := transport.GenerateCA(time.Hour)
			if err != nil {
				b.Fatal(err)
			}
			creds := func(n murmur.NodeID) *murmur.TLSCredential {
				certPEM, keyPEM, err := ca.IssueNode(n, time.Hour)
				if err != nil {
					b.Fatal(err)
				}
				return &murmur.TLSCredential{CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM}
			}
			mkcfg := func(path string, node murmur.NodeID, tls *murmur.TLSCredential, listen string, peers []murmur.Peer) murmur.Config {
				return testdb.Configure(murmur.Config{
					Path:   path,
					NodeID: node,
					DBID:   dbid,
					Schema: murmur.SchemaConfig{
						Version: 1, Tables: benchSchema(), LocalDDL: benchLocalDDL(),
					},
					Pebble: murmur.DefaultPebbleConfig(),
					Encryption: murmur.EncryptionConfig{
						Key: bytes.Repeat([]byte{0x62}, 32), KeyID: "bench",
					},
					Replication: murmur.ReplicationConfig{
						ListenAddr: listen, TLS: tls, Peers: peers,
					},
				})
			}
			const portA = "127.0.0.1:17443"
			dbA, err := murmur.Open(ctx, mkcfg(b.TempDir(), nodeA, creds(nodeA), portA, nil))
			if err != nil {
				b.Fatal(err)
			}
			defer dbA.Close()
			dbB, err := murmur.Open(ctx, mkcfg(b.TempDir(), nodeB, creds(nodeB), "127.0.0.1:0",
				[]murmur.Peer{{NodeID: nodeA, Addrs: []string{portA}}}))
			if err != nil {
				b.Fatal(err)
			}
			defer dbB.Close()

			// Wait for the session.
			deadline := time.Now().Add(10 * time.Second)
			for dbB.Status().ConnectedPeers == 0 {
				if time.Now().After(deadline) {
					b.Fatal("B never connected")
				}
				time.Sleep(20 * time.Millisecond)
			}
			b.ResetTimer()
			start := time.Now()
			populate(b, dbA, n)
			populateDone := time.Since(start)
			// Wait for B convergence.
			deadline = time.Now().Add(120 * time.Second)
			for {
				if countContacts(b, dbB) == n {
					break
				}
				if time.Now().After(deadline) {
					b.Fatalf("B converged to %d rows, want %d", countContacts(b, dbB), n)
				}
				time.Sleep(20 * time.Millisecond)
			}
			elapsed := time.Since(start)
			b.ReportMetric(float64(elapsed.Nanoseconds()), "sync-ns")
			b.ReportMetric(float64(populateDone.Nanoseconds()), "populate-ns")
			b.ReportMetric(float64((elapsed - populateDone).Nanoseconds()), "converge-ns")
			b.ReportMetric(float64(n)/elapsed.Seconds(), "rows/sec")
		})
	}
}

func countContacts(b *testing.B, db *murmur.DB) int {
	b.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT COUNT(*) FROM contacts`)
	if err != nil {
		b.Fatal(err)
	}
	defer rows.Close()
	var n int
	for rows.Next() {
		_ = rows.Scan(&n)
	}
	return n
}
