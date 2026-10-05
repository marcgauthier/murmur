// Command allowed-peers shows NodeID admission: nodes 1 and 2
// allow-list each other, so node 3's dials are refused and it stays
// isolated — its rows never arrive, and mesh rows never reach it.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/allowed-peers
package main

import (
	"context"
	"fmt"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"log"
	"net"
	"os"
	"time"

	"github.com/marcgauthier/murmur"
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

func count(ctx context.Context, db *murmur.DB) int {
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM notes`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	return n
}

func waitCount(ctx context.Context, db *murmur.DB, want int, timeout time.Duration) {
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
	base, err := os.MkdirTemp("", "murmur-allowlist-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)

	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		log.Fatal(err)
	}
	dbid := murmur.NewDBID()

	const nodes = 3
	ids := make([]murmur.NodeID, nodes)
	addrs := make([]string, nodes)
	dbs := make([]*murmur.DB, nodes)
	for i := range ids {
		ids[i] = murmur.NewNodeID()
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort())
	}
	for i := range dbs {
		var peers []murmur.Peer
		for j := range ids {
			if i != j {
				peers = append(peers, murmur.Peer{NodeID: ids[j], Addrs: []string{addrs[j]}})
			}
		}
		certPEM, keyPEM, err := ca.IssueNode(ids[i], 24*time.Hour)
		if err != nil {
			log.Fatal(err)
		}
		// Nodes 1 and 2 admit only each other; node 3 admits
		// everyone but nobody admits node 3.
		var allowed []murmur.NodeID
		if i < 2 {
			allowed = []murmur.NodeID{ids[1-i]}
		}
		dir, err := os.MkdirTemp(base, fmt.Sprintf("node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
		dbs[i], err = murmur.Open(ctx, demoidentity.Configure(murmur.Config{
			Path:   dir,
			NodeID: ids[i],
			DBID:   dbid,
			Schema: murmur.SchemaConfig{
				Version: 1,
				Tables: []schema.TableSchema{{
					Name: "notes",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "body", Type: schema.ColText, Nullable: true},
					},
				}},
			},
			Pebble: murmur.DefaultPebbleConfig(),
			Encryption: murmur.EncryptionConfig{
				Key:   []byte("0123456789abcdef0123456789abcdef"),
				KeyID: "allowlist-key",
			},
			Replication: murmur.ReplicationConfig{
				ListenAddr: addrs[i],
				TLS: &murmur.TLSCredential{
					CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM,
				},
				Peers:        peers,
				AllowedPeers: allowed,
			},
		}))
		if err != nil {
			log.Fatal(err)
		}
		defer dbs[i].Close()
	}

	for r := 0; r < 2; r++ {
		id := murmur.NewRowID()
		if _, err := dbs[0].ExecContext(ctx,
			`INSERT INTO notes (id, body) VALUES (?, ?)`, id[:], fmt.Sprintf("mesh-%d", r)); err != nil {
			log.Fatal(err)
		}
	}
	waitCount(ctx, dbs[1], 2, 30*time.Second)

	// Negative checks need a settle margin: isolation means node 3
	// holds nothing after the mesh has long converged.
	time.Sleep(3 * time.Second)
	if got := count(ctx, dbs[2]); got != 0 {
		log.Fatalf("node 3 received %d mesh rows despite the allow-list", got)
	}
	fmt.Println("mesh converged on nodes 1-2; node 3 received nothing")

	id := murmur.NewRowID()
	if _, err := dbs[2].ExecContext(ctx,
		`INSERT INTO notes (id, body) VALUES (?, ?)`, id[:], "stranded"); err != nil {
		log.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	if got := count(ctx, dbs[0]); got != 2 {
		log.Fatalf("node 1 holds %d rows, want 2 (node 3 leaked in)", got)
	}
	fmt.Println("node 3's write stayed stranded; allow-list holds both directions")
	for i, db := range dbs {
		fmt.Printf("node %d connectedPeers=%d\n", i+1, db.Status().ConnectedPeers)
	}
}
