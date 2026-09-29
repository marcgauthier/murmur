// Command plumtree shows opt-in Plumtree dissemination: all three
// members select DisseminationPlumtree (mixed modes are rejected by
// capability negotiation) and concurrent writes converge over
// eager/lazy gossip with the usual anti-entropy repair underneath.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/plumtree
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	replicateddb "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/transport"
)

func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitCounts(ctx context.Context, dbs []*replicateddb.DB, want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, db := range dbs {
			var n int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&n); err != nil || n != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatalf("timed out waiting for %d rows on every node", want)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "spedsql-plumtree-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)

	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		log.Fatal(err)
	}
	dbid := replicateddb.NewDBID()

	const nodes = 3
	ids := make([]replicateddb.NodeID, nodes)
	addrs := make([]string, nodes)
	dbs := make([]*replicateddb.DB, nodes)
	for i := range ids {
		ids[i] = replicateddb.NewNodeID()
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort())
	}
	for i := range dbs {
		var peers []replicateddb.Peer
		for j := range ids {
			if i != j {
				peers = append(peers, replicateddb.Peer{NodeID: ids[j], Addrs: []string{addrs[j]}})
			}
		}
		certPEM, keyPEM, err := ca.IssueNode(ids[i], 24*time.Hour)
		if err != nil {
			log.Fatal(err)
		}
		dir, err := os.MkdirTemp(base, fmt.Sprintf("node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
		dbs[i], err = replicateddb.Open(ctx, replicateddb.Config{
			Path:   dir,
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
				KeyID: "plumtree-key",
			},
			Replication: replicateddb.ReplicationConfig{
				ListenAddr: addrs[i],
				TLS: &replicateddb.TLSCredential{
					CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM,
				},
				Peers:         peers,
				Dissemination: replicateddb.DisseminationPlumtree,
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		defer dbs[i].Close()
	}

	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func(i int, db *replicateddb.DB) {
			defer wg.Done()
			for r := 0; r < 5; r++ {
				id := replicateddb.NewRowID()
				if _, err := db.ExecContext(ctx,
					`INSERT INTO notes (id, body) VALUES (?, ?)`,
					id[:], fmt.Sprintf("node%d-note%d", i+1, r)); err != nil {
					log.Fatal(err)
				}
			}
		}(i, db)
	}
	wg.Wait()
	waitCounts(ctx, dbs, 15, 90*time.Second)
	fmt.Println("15 rows converged on all 3 Plumtree nodes")
}
