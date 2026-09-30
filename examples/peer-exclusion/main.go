// Command peer-exclusion shows persistent peer retirement: RemovePeer
// excludes a node across restarts (reopening does not reconnect),
// and AddPeer readmits it with a fresh obligation.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/peer-exclusion
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

func count(ctx context.Context, db *replicateddb.DB) int {
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	return n
}

func waitCount(ctx context.Context, db *replicateddb.DB, want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if count(ctx, db) == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatalf("timed out waiting for %d rows", want)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "spedsql-exclusion-*")
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
	dirs := make([]string, 2)
	for i := range dirs {
		dirs[i], err = os.MkdirTemp(base, fmt.Sprintf("node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
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
				KeyID: "exclusion-key",
			},
			Replication: replicateddb.ReplicationConfig{
				ListenAddr: addrs[i],
				TLS: &replicateddb.TLSCredential{
					CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM,
				},
				Peers: []replicateddb.Peer{{NodeID: ids[peer], Addrs: []string{addrs[peer]}}},
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		return db
	}

	nodeA, nodeB := open(0), open(1)
	defer nodeB.Close()
	for r := 0; r < 2; r++ {
		id := replicateddb.NewRowID()
		if _, err := nodeA.ExecContext(ctx,
			`INSERT INTO notes (id, body) VALUES (?, ?)`, id[:], fmt.Sprintf("base-%d", r)); err != nil {
			log.Fatal(err)
		}
	}
	waitCount(ctx, nodeB, 2, 30*time.Second)
	fmt.Println("baseline converged; node A retires node B")

	if err := nodeA.RemovePeer(ctx, ids[1]); err != nil {
		log.Fatal(err)
	}
	if err := nodeA.Close(); err != nil {
		log.Fatal(err)
	}

	// Reopen: the exclusion is durable, so no session reforms.
	nodeA = open(0)
	defer nodeA.Close()
	time.Sleep(5 * time.Second)
	if got := nodeA.Status().ConnectedPeers; got != 0 {
		log.Fatalf("node A reconnected to retired peer (%d sessions)", got)
	}
	fmt.Println("exclusion survived the restart: still 0 sessions")

	// Readmit with a fresh obligation and prove replication resumes.
	if err := nodeA.AddPeer(ctx, replicateddb.Peer{NodeID: ids[1], Addrs: []string{addrs[1]}}); err != nil {
		log.Fatal(err)
	}
	id := replicateddb.NewRowID()
	if _, err := nodeB.ExecContext(ctx,
		`INSERT INTO notes (id, body) VALUES (?, ?)`, id[:], "reunion"); err != nil {
		log.Fatal(err)
	}
	waitCount(ctx, nodeA, 3, 30*time.Second)
	fmt.Println("readmitted: reunion row replicated to node A")
}
