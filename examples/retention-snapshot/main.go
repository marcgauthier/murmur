// Command retention-snapshot shows log retention and snapshot resync:
// with tiny retention, a node offline past one GC pass cannot catch up
// from origin logs, so its rejoin completes via a snapshot transfer,
// proven by the receiver's snapshot counter.
//
// The demo takes about a minute: the 30s log-GC tick must fire once
// while the node is offline.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/retention-snapshot
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	replicateddb "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/transport"
)

func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitCount(ctx context.Context, db *replicateddb.DB, want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&n); err == nil && n == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatalf("timed out waiting for %d rows", want)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "spedsql-snapshot-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)

	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		log.Fatal(err)
	}
	dbid := replicateddb.NewDBID()
	ids := []replicateddb.NodeID{replicateddb.NewNodeID(), replicateddb.NewNodeID()}
	addrs := []string{
		fmt.Sprintf("127.0.0.1:%d", freePort()),
		fmt.Sprintf("127.0.0.1:%d", freePort()),
	}
	dirs := []string{
		mustTempDir(base, "node1-"),
		mustTempDir(base, "node2-"),
	}

	open := func(i int) *replicateddb.DB {
		certPEM, keyPEM, err := ca.IssueNode(ids[i], 24*time.Hour)
		if err != nil {
			log.Fatal(err)
		}
		peer := 1 - i
		db, err := replicateddb.Open(ctx, replicateddb.Config{
			Path:   dirs[i],
			NodeID: ids[i],
			DBID:   dbid,
			Schema: replicateddb.SchemaConfig{
				Version: 1,
				Tables: []schema.TableSchema{{
					Name: "notes",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "body", Type: schema.ColText, Nullable: true},
					},
				}},
			},
			Pebble: replicateddb.DefaultPebbleConfig(),
			Encryption: replicateddb.EncryptionConfig{
				Key:   []byte("0123456789abcdef0123456789abcdef"),
				KeyID: "snapshot-key",
			},
			Replication: replicateddb.ReplicationConfig{
				ListenAddr: addrs[i],
				TLS: &replicateddb.TLSCredential{
					CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM,
				},
				Peers: []replicateddb.Peer{{NodeID: ids[peer], Addrs: []string{addrs[peer]}}},
				// Aggressive retention: one GC pass collects
				// anything the offline peer still needs.
				MinLogRetention:        time.Second,
				MaxOfflineLogRetention: 2 * time.Second,
				MinRetainedBatches:     2,
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		return db
	}

	node1 := open(0)
	node2 := open(1)
	defer node1.Close()

	id := replicateddb.NewRowID()
	if _, err := node1.ExecContext(ctx,
		`INSERT INTO notes (id, body) VALUES (?, ?)`, id[:], "seed"); err != nil {
		log.Fatal(err)
	}
	waitCount(ctx, node2, 1, 30*time.Second)
	fmt.Println("seed converged; stopping node 2")
	if err := node2.Close(); err != nil {
		log.Fatal(err)
	}

	for r := 0; r < 30; r++ {
		id := replicateddb.NewRowID()
		if _, err := node1.ExecContext(ctx,
			`INSERT INTO notes (id, body) VALUES (?, ?)`,
			id[:], fmt.Sprintf("fresh-%d", r)); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("waiting for the retention window plus one GC pass...")
	time.Sleep(45 * time.Second)

	node2 = open(1)
	defer node2.Close()
	waitCount(ctx, node2, 31, 90*time.Second)
	got := node2.Metrics().SnapshotAppliesCompleted
	fmt.Printf("node 2 rejoined with %d rows; snapshot applies completed: %d\n", 31, got)
	if got < 1 {
		log.Fatal("rejoin converged without the snapshot path")
	}
}

func mustTempDir(base, pattern string) string {
	dir, err := os.MkdirTemp(base, pattern)
	if err != nil {
		log.Fatal(err)
	}
	return dir
}
