// Command plumtree shows opt-in Plumtree dissemination: all three
// members select DisseminationPlumtree (mixed modes are rejected by
// capability negotiation) and concurrent writes converge over
// eager/lazy gossip with the usual anti-entropy repair underneath.
//
// Run it:
//
//	go run ./examples/plumtree
package main

import (
	"context"
	"fmt"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"log"
	"net"
	"os"
	"sync"
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

func waitCounts(dbs []*murmur.DB, want int, timeout time.Duration) {
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
	log.Fatalf("timed out waiting for %d rows on every node", want)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "murmur-plumtree-*")
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
	definition, err := murmur.Model[note](murmur.ModelOptions{
		Name: "notes", TableID: 11,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
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
			Schema: murmur.SchemaConfig{Version: 1},
			Tables: []murmur.TableDefinition{definition},
			Spool:  murmur.DefaultSpoolConfig(),
			Encryption: murmur.EncryptionConfig{
				Key:   []byte("0123456789abcdef0123456789abcdef"),
				KeyID: "plumtree-key",
			},
			Replication: murmur.ReplicationConfig{
				ListenAddr: addrs[i],
				TLS: &murmur.TLSCredential{
					CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM,
				},
				Peers:         peers,
				Dissemination: murmur.DisseminationPlumtree,
			},
		}))
		if err != nil {
			log.Fatal(err)
		}
		defer dbs[i].Close()
	}

	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func(i int, db *murmur.DB) {
			defer wg.Done()
			if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
				batch := make([]*note, 5)
				for r := range batch {
					batch[r] = &note{ID: murmur.NewRowID(), Body: fmt.Sprintf("node%d-note%d", i+1, r)}
				}
				return tx.InsertMany(batch)
			}); err != nil {
				log.Fatal(err)
			}
		}(i, db)
	}
	wg.Wait()
	waitCounts(dbs, 15, 90*time.Second)
	fmt.Println("15 rows converged on all 3 Plumtree nodes")
}
