// Command offline-partition shows partition tolerance: a 3-node mesh
// splits one node off with RemovePeer, both sides keep committing
// offline writes, then AddPeer rejoins and everything converges.
//
// Run it:
//
//	go run ./examples/offline-partition
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
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/transport"
)

type note struct {
	ID   ids.RowID `rime:"primary"`
	Body string
}

func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func waitCounts(dbs []*murmur.DB, want int, timeout time.Duration, what string) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, db := range dbs {
			n, err := db.Count(context.Background(), note{})
			if err != nil || n != want {
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

func insertNote(ctx context.Context, db *murmur.DB, body string) error {
	return db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return tx.InsertItem(&note{ID: murmur.NewRowID(), Body: body})
	})
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
	definition, err := murmur.Model[note](murmur.ModelOptions{
		Name: "notes", TableID: 8,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
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
			Schema: murmur.SchemaConfig{Version: 1},
			Tables: []murmur.TableDefinition{definition},
			Spool:  murmur.DefaultSpoolConfig(),
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
		if err := insertNote(ctx, dbs[0], fmt.Sprintf("base-%d", r)); err != nil {
			log.Fatal(err)
		}
	}
	waitCounts(dbs, 6, 60*time.Second, "baseline")
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
		if err := insertNote(ctx, dbs[0], fmt.Sprintf("online-%d", r)); err != nil {
			log.Fatal(err)
		}
		if err := insertNote(ctx, dbs[2], fmt.Sprintf("offline-%d", r)); err != nil {
			log.Fatal(err)
		}
	}
	waitCounts(dbs[:2], 10, 30*time.Second, "survivor side")
	waitCounts(dbs[2:], 10, 30*time.Second, "isolated side")

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
	waitCounts(dbs, 14, 60*time.Second, "healed mesh")
	fmt.Println("healed: all 14 rows on all 3 nodes")
}
