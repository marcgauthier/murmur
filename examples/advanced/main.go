// Command advanced runs a three-node typed-record mesh, concurrent writes,
// and a conflicting update that converges to one deterministic winner.
//
// Run it with:
//
//	go run ./examples/advanced
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
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

func openMeshNode(ctx context.Context, dir string, id murmur.NodeID, dbid murmur.DBID,
	definition murmur.TableDefinition, ca *transport.CA, addr string, peers []murmur.Peer) *murmur.DB {
	certPEM, keyPEM, err := ca.IssueNode(id, 24*time.Hour)
	if err != nil {
		log.Fatal(err)
	}
	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path: dir, NodeID: id, DBID: dbid,
		Schema: murmur.SchemaConfig{Version: 1}, Tables: []murmur.TableDefinition{definition},
		Spool:      murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "advanced-key"},
		Replication: murmur.ReplicationConfig{
			ListenAddr: addr,
			TLS:        &murmur.TLSCredential{CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM},
			Peers:      peers,
		},
	}))
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func rowCount(table *murmur.RecordTable[note]) int {
	n, err := table.Where().Count()
	if err != nil {
		log.Fatal(err)
	}
	return n
}

func orderedBodies(table *murmur.RecordTable[note]) string {
	rows, err := table.Where().Find()
	if err != nil {
		log.Fatal(err)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Body < rows[j].Body })
	bodies := ""
	for _, row := range rows {
		bodies += row.Body + "\n"
	}
	return bodies
}

func waitConverged(tables []*murmur.RecordTable[note], want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		first := ""
		for i, table := range tables {
			if rowCount(table) != want {
				ok = false
				break
			}
			bodies := orderedBodies(table)
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
	for i, table := range tables {
		fmt.Printf("node %d at timeout: count=%d\n", i+1, rowCount(table))
	}
	log.Fatalf("nodes did not converge on %d identical rows within %v", want, timeout)
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "murmur-advanced-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)
	definition, err := murmur.Define[note]("notes", 7, murmur.RecordOptions{
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
	const nodes = 3
	idsByNode := make([]murmur.NodeID, nodes)
	addrs := make([]string, nodes)
	for i := range idsByNode {
		idsByNode[i] = murmur.NewNodeID()
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", freePort())
	}
	dbs := make([]*murmur.DB, nodes)
	tables := make([]*murmur.RecordTable[note], nodes)
	for i := range idsByNode {
		var peers []murmur.Peer
		for j := range idsByNode {
			if i != j {
				peers = append(peers, murmur.Peer{NodeID: idsByNode[j], Addrs: []string{addrs[j]}})
			}
		}
		dir, err := os.MkdirTemp(base, fmt.Sprintf("node%d-", i+1))
		if err != nil {
			log.Fatal(err)
		}
		dbs[i] = openMeshNode(ctx, dir, idsByNode[i], dbid, definition, ca, addrs[i], peers)
		defer dbs[i].Close()
		tables[i], err = murmur.TableOf[note](dbs[i], "notes")
		if err != nil {
			log.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for i, db := range dbs {
		table := tables[i]
		wg.Add(1)
		go func(i int, db *murmur.DB, table *murmur.RecordTable[note]) {
			defer wg.Done()
			if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
				batch := make([]*note, 10)
				for r := range batch {
					batch[r] = &note{ID: murmur.NewRowID(), Body: fmt.Sprintf("node%d-note%d", i+1, r)}
				}
				return table.InsertMany(tx, batch)
			}); err != nil {
				log.Fatal(err)
			}
		}(i, db, table)
	}
	wg.Wait()
	waitConverged(tables, 30, 60*time.Second)
	fmt.Println("30 concurrent typed rows converged on all 3 nodes")

	rows, err := tables[0].Where().Find()
	if err != nil || len(rows) == 0 {
		log.Fatalf("select conflict target: rows=%d err=%v", len(rows), err)
	}
	targetID := rows[0].ID
	for i, db := range dbs {
		table := tables[i]
		wg.Add(1)
		go func(i int, db *murmur.DB, table *murmur.RecordTable[note]) {
			defer wg.Done()
			if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
				return table.Update(tx, targetID, func(value *note) error {
					value.Body = fmt.Sprintf("winner-node%d", i+1)
					return nil
				})
			}); err != nil {
				log.Fatal(err)
			}
		}(i, db, table)
	}
	wg.Wait()

	deadline := time.Now().Add(30 * time.Second)
	for {
		bodies := make([]string, nodes)
		converged := true
		for i, table := range tables {
			value, err := table.Get(targetID)
			if err != nil {
				log.Fatal(err)
			}
			bodies[i] = value.Body
			if i > 0 && bodies[i] != bodies[0] {
				converged = false
			}
		}
		if converged {
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
		fmt.Printf("node %d: state=%s connectedPeers=%d\n", i+1, st.State, st.ConnectedPeers)
	}
}
