// Command advanced runs a 3-node replicated mesh inside one process:
// a shared cluster DBID, a generated mTLS CA with per-node certificates,
// static peer wiring, concurrent writes on every node, and a conflicting
// concurrent update that resolves to one deterministic winner on all nodes.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/advanced
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
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

func openMeshNode(ctx context.Context, dir string, id replicateddb.NodeID, dbid replicateddb.DBID,
	ca *transport.CA, addr string, peers []replicateddb.Peer) *replicateddb.DB {
	certPEM, keyPEM, err := ca.IssueNode(id, 24*time.Hour)
	if err != nil {
		log.Fatal(err)
	}
	db, err := replicateddb.Open(ctx, replicateddb.Config{
		Path:   dir,
		NodeID: id,
		DBID:   dbid, // all members of one cluster share the DBID
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
			KeyID: "advanced-key",
		},
		Replication: replicateddb.ReplicationConfig{
			ListenAddr: addr,
			TLS: &replicateddb.TLSCredential{
				CertPEM: certPEM,
				KeyPEM:  keyPEM,
				CAPEM:   ca.CertPEM,
			},
			Peers: peers,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func rowCount(ctx context.Context, db *replicateddb.DB) int {
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	return n
}

// waitConverged polls until every node holds want rows with the same
// ordered bodies, or fails after timeout.
func waitConverged(ctx context.Context, dbs []*replicateddb.DB, want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i, db := range dbs {
			if rowCount(ctx, db) != want {
				ok = false
				break
			}
			rows, err := db.QueryContext(ctx, `SELECT body FROM notes ORDER BY body`)
			if err != nil {
				log.Fatal(err)
			}
			var bodies string
			for rows.Next() {
				var b string
				if err := rows.Scan(&b); err != nil {
					rows.Close()
					log.Fatal(err)
				}
				bodies += b + "\n"
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				log.Fatal(err)
			}
			if i == 0 {
				first = bodies
			} else if bodies != first {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i, db := range dbs {
		fmt.Printf("node %d at timeout: count=%d\n", i+1, rowCount(ctx, db))
	}
	log.Fatalf("nodes did not converge on %d identical rows within %v", want, timeout)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "spedsql-advanced-*")
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
	for i := range ids {
		ids[i] = replicateddb.NewNodeID()
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort())
	}
	dbs := make([]*replicateddb.DB, nodes)
	for i := range dbs {
		var peers []replicateddb.Peer
		for j := range ids {
			if i != j {
				peers = append(peers, replicateddb.Peer{NodeID: ids[j], Addrs: []string{addrs[j]}})
			}
		}
		dir, err := os.MkdirTemp(base, fmt.Sprintf("node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
		dbs[i] = openMeshNode(ctx, dir, ids[i], dbid, ca, addrs[i], peers)
		defer dbs[i].Close()
	}

	// Every node writes concurrently; all commits are local and offline-safe.
	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func(i int, db *replicateddb.DB) {
			defer wg.Done()
			for r := 0; r < 10; r++ {
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
	waitConverged(ctx, dbs, 30, 60*time.Second)
	fmt.Println("30 concurrent rows converged on all 3 nodes")

	// Conflicting update: all nodes overwrite the same cell at once.
	// Last-writer-wins picks one deterministic winner everywhere.
	var targetID []byte
	if err := dbs[0].QueryRowContext(ctx,
		`SELECT id FROM notes LIMIT 1`).Scan(&targetID); err != nil {
		log.Fatal(err)
	}
	for i, db := range dbs {
		wg.Add(1)
		go func(i int, db *replicateddb.DB) {
			defer wg.Done()
			if _, err := db.ExecContext(ctx,
				`UPDATE notes SET body = ? WHERE id = ?`,
				fmt.Sprintf("winner-node%d", i+1), targetID); err != nil {
				log.Fatal(err)
			}
		}(i, db)
	}
	wg.Wait()

	deadline := time.Now().Add(30 * time.Second)
	for {
		bodies := make([]string, nodes)
		for i, db := range dbs {
			if err := db.QueryRowContext(ctx,
				`SELECT body FROM notes WHERE id = ?`, targetID).Scan(&bodies[i]); err != nil {
				log.Fatal(err)
			}
		}
		if bodies[0] == bodies[1] && bodies[1] == bodies[2] {
			fmt.Printf("conflict resolved identically everywhere: %q\n", bodies[0])
			break
		}
		if time.Now().After(deadline) {
			log.Fatalf("conflict did not converge: %q", bodies)
		}
		time.Sleep(100 * time.Millisecond)
	}

	for i, db := range dbs {
		st := db.Status()
		fmt.Printf("node %d: state=%s connectedPeers=%d\n",
			i+1, st.State, st.ConnectedPeers)
	}
}
