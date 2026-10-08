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
//	go run ./examples/retention-snapshot
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

func waitCount(table *murmur.RecordTable[note], want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, err := table.Where().Count(); err == nil && n == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatalf("timed out waiting for %d rows", want)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "murmur-snapshot-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)
	definition, err := murmur.Define[note]("notes", 12, murmur.RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
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
	dirs := []string{
		mustTempDir(base, "node1-"),
		mustTempDir(base, "node2-"),
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
				KeyID: "snapshot-key",
			},
			Replication: murmur.ReplicationConfig{
				ListenAddr: addrs[i],
				TLS: &murmur.TLSCredential{
					CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM,
				},
				Peers: []murmur.Peer{{NodeID: ids[peer], Addrs: []string{addrs[peer]}}},
				// Aggressive retention: one GC pass collects
				// anything the offline peer still needs.
				MinLogRetention:        time.Second,
				MaxOfflineLogRetention: 2 * time.Second,
				MinRetainedBatches:     2,
			},
		}))
		if err != nil {
			log.Fatal(err)
		}
		return db
	}

	node1 := open(0)
	node2 := open(1)
	defer node1.Close()
	table1, err := murmur.TableOf[note](node1, "notes")
	if err != nil {
		log.Fatal(err)
	}
	table2, err := murmur.TableOf[note](node2, "notes")
	if err != nil {
		log.Fatal(err)
	}

	id := murmur.NewRowID()
	if err := node1.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return table1.Insert(tx, &note{ID: id, Body: "seed"})
	}); err != nil {
		log.Fatal(err)
	}
	waitCount(table2, 1, 30*time.Second)
	fmt.Println("seed converged; stopping node 2")
	if err := node2.Close(); err != nil {
		log.Fatal(err)
	}

	for r := 0; r < 30; r++ {
		id := murmur.NewRowID()
		if err := node1.WriteTxContext(ctx, func(tx *murmur.Tx) error {
			return table1.Insert(tx, &note{ID: id, Body: fmt.Sprintf("fresh-%d", r)})
		}); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("waiting for the retention window plus one GC pass...")
	time.Sleep(45 * time.Second)

	node2 = open(1)
	defer node2.Close()
	table2, err = murmur.TableOf[note](node2, "notes")
	if err != nil {
		log.Fatal(err)
	}
	waitCount(table2, 31, 90*time.Second)
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
