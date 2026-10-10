// Command peer-exclusion shows persistent peer retirement: RemovePeer
// excludes a node across restarts (reopening does not reconnect),
// and AddPeer readmits it with a fresh obligation.
//
// Run it:
//
//	go run ./examples/peer-exclusion
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
	rowids "github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/transport"
)

type note struct {
	ID   rowids.RowID `rime:"primary"`
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

func count(db *murmur.DB) int {
	n, err := db.Count(context.Background(), note{})
	if err != nil {
		log.Fatal(err)
	}
	return n
}

func waitCount(db *murmur.DB, want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if count(db) == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatalf("timed out waiting for %d rows", want)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "murmur-exclusion-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)
	definition, err := murmur.Model[note](murmur.ModelOptions{
		Name: "notes", TableID: 10,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ca, err := transport.GenerateCA(24 * time.Hour)
	if err != nil {
		log.Fatal(err)
	}
	dbid := murmur.NewDBID()
	ids := []murmur.NodeID{murmur.NewNodeID(), murmur.NewNodeID()}
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

	open := func(i int) *murmur.DB {
		certPEM, keyPEM, err := ca.IssueNode(ids[i], 24*time.Hour)
		if err != nil {
			log.Fatal(err)
		}
		peer := 1 - i
		db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
			Path:   dirs[i],
			NodeID: ids[i],
			DBID:   dbid,
			Schema: murmur.SchemaConfig{Version: 1},
			Tables: []murmur.TableDefinition{definition},
			Spool:  murmur.DefaultSpoolConfig(),
			Encryption: murmur.EncryptionConfig{
				Key:   []byte("0123456789abcdef0123456789abcdef"),
				KeyID: "exclusion-key",
			},
			Replication: murmur.ReplicationConfig{
				ListenAddr: addrs[i],
				TLS: &murmur.TLSCredential{
					CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM,
				},
				Peers: []murmur.Peer{{NodeID: ids[peer], Addrs: []string{addrs[peer]}}},
			},
		}))
		if err != nil {
			log.Fatal(err)
		}
		return db
	}

	nodeA, nodeB := open(0), open(1)
	defer nodeB.Close()
	for r := 0; r < 2; r++ {
		if err := nodeA.WriteTxContext(ctx, func(tx *murmur.Tx) error {
			return tx.InsertItem(&note{ID: murmur.NewRowID(), Body: fmt.Sprintf("base-%d", r)})
		}); err != nil {
			log.Fatal(err)
		}
	}
	waitCount(nodeB, 2, 30*time.Second)
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
	if err := nodeA.AddPeer(ctx, murmur.Peer{NodeID: ids[1], Addrs: []string{addrs[1]}}); err != nil {
		log.Fatal(err)
	}
	id := murmur.NewRowID()
	if err := nodeB.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return tx.InsertItem(&note{ID: id, Body: "reunion"})
	}); err != nil {
		log.Fatal(err)
	}
	waitCount(nodeA, 3, 30*time.Second)
	fmt.Println("readmitted: reunion row replicated to node A")
}
