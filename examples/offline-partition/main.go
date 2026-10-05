// Command offline-partition shows partition tolerance: a 3-node mesh
// splits one node off with RemovePeer, both sides keep committing
// offline writes, then AddPeer rejoins and everything converges.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/offline-partition
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

func waitCounts(ctx context.Context, dbs []*murmur.DB, want int, timeout time.Duration, what string) {
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
	log.Fatalf("timed out waiting for %s (want %d rows everywhere)", what, want)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "murmur-partition-*")
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
				KeyID: "partition-key",
			},
			Replication: murmur.ReplicationConfig{
				ListenAddr: addrs[i],
				TLS: &murmur.TLSCredential{
					CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM,
				},
				Peers: peers,
			},
		}))
		if err != nil {
			log.Fatal(err)
		}
		defer dbs[i].Close()
	}

	// Baseline converges, then node 3 is cut off in both directions.
	for r := 0; r < 6; r++ {
		id := murmur.NewRowID()
		if _, err := dbs[0].ExecContext(ctx,
			`INSERT INTO notes (id, body) VALUES (?, ?)`, id[:], fmt.Sprintf("base-%d", r)); err != nil {
			log.Fatal(err)
		}
	}
	waitCounts(ctx, dbs, 6, 60*time.Second, "baseline")
	for _, db := range []*murmur.DB{dbs[0], dbs[1]} {
		if err := db.RemovePeer(ctx, ids[2]); err != nil {
			log.Fatal(err)
		}
	}
	for _, id := range []murmur.NodeID{ids[0], ids[1]} {
		if err := dbs[2].RemovePeer(ctx, id); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("node 3 partitioned off; both sides keep writing")

	// Offline writes on both sides: neither fails, neither crosses yet.
	for r := 0; r < 4; r++ {
		id := murmur.NewRowID()
		if _, err := dbs[0].ExecContext(ctx,
			`INSERT INTO notes (id, body) VALUES (?, ?)`, id[:], fmt.Sprintf("online-%d", r)); err != nil {
			log.Fatal(err)
		}
		id = murmur.NewRowID()
		if _, err := dbs[2].ExecContext(ctx,
			`INSERT INTO notes (id, body) VALUES (?, ?)`, id[:], fmt.Sprintf("offline-%d", r)); err != nil {
			log.Fatal(err)
		}
	}
	waitCounts(ctx, dbs[:2], 10, 30*time.Second, "survivor side")
	waitCounts(ctx, dbs[2:], 10, 30*time.Second, "isolated side")

	// Rejoin: AddPeer clears the exclusion and the mesh heals to 14 rows.
	for i, db := range dbs {
		for j, id := range ids {
			if i != j {
				if err := db.AddPeer(ctx, murmur.Peer{NodeID: id, Addrs: []string{addrs[j]}}); err != nil {
					log.Fatal(err)
				}
			}
		}
	}
	waitCounts(ctx, dbs, 14, 60*time.Second, "healed mesh")
	fmt.Println("healed: all 14 rows on all 3 nodes")
}
