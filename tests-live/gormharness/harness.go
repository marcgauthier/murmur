// Package gormharness spins in-process Murmur engines replicating over
// real QUIC, each wrapped in the GORM dialect. GORM requires an
// embedded *replicateddb.DB handle, so unlike the multi-process
// harness these nodes share the test process; replication,
// encryption, and disk durability are fully real. Each app instance
// embedding engine+GORM maps to one Node here.
package gormharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"testing"
	"time"

	replicateddb "github.com/marcgauthier/murmur"
	murmur "github.com/marcgauthier/murmur/gormmurmur"
	murmurSchema "github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/transport"
	"gorm.io/gorm"
)

// TestKey is the shared wrapping key for suite engines.
var TestKey = []byte("0123456789abcdef0123456789abcdef")

// Node is one replicated engine plus its GORM handle.
type Node struct {
	Label  string
	Dir    string
	NodeID replicateddb.NodeID
	Addr   string
	DB     *replicateddb.DB
	GDB    *gorm.DB
}

// Cluster is a set of meshed Nodes.
type Cluster struct {
	t     *testing.T
	Nodes []*Node
	dbID  replicateddb.DBID
	creds []*replicateddb.TLSCredential
}

// NewCluster opens numNodes meshed engines with genesis derived from
// models, wraps each in GORM, migrates on node 0, and waits for peer
// connections plus schema convergence.
func NewCluster(t *testing.T, name string, numNodes int, models ...interface{}) *Cluster {
	t.Helper()
	genesis, err := murmur.GenesisTables(models...)
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	ca, err := transport.GenerateCA(2 * time.Hour)
	if err != nil {
		t.Fatalf("ca: %v", err)
	}
	dbid := replicateddb.NewDBID()
	root := t.TempDir()

	var c *Cluster
	// Explicit ports can collide with a parallel suite's grab; retry
	// bring-up a few times before failing.
	for attempt := 0; attempt < 3; attempt++ {
		c, err = tryCluster(t, name, root, ca, dbid, genesis, numNodes)
		if err == nil {
			break
		}
		t.Logf("cluster bring-up attempt %d: %v", attempt+1, err)
	}
	if err != nil {
		t.Fatalf("cluster bring-up: %v", err)
	}
	for _, n := range c.Nodes {
		gdb, err := gorm.Open(murmur.Open(n.DB), &gorm.Config{})
		if err != nil {
			t.Fatalf("gorm open %s: %v", n.Label, err)
		}
		n.GDB = gdb
	}
	// Migrate once; peers adopt the schema over replication.
	if err := c.Nodes[0].GDB.AutoMigrate(models...); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	c.WaitConnected(20 * time.Second)
	c.WaitSchemaEpoch(20 * time.Second)
	t.Cleanup(c.Close)
	return c
}

func tryCluster(t *testing.T, name, root string, ca *transport.CA, dbid replicateddb.DBID, genesis []murmurSchema.TableSchema, numNodes int) (*Cluster, error) {
	t.Helper()
	c := &Cluster{t: t}
	ports := make([]int, numNodes)
	for i := range ports {
		ports[i] = grabPort(t)
	}
	nodeIDs := make([]replicateddb.NodeID, numNodes)
	creds := make([]*replicateddb.TLSCredential, numNodes)
	for i := range nodeIDs {
		nodeIDs[i] = replicateddb.NewNodeID()
		certPEM, keyPEM, err := ca.IssueNode(nodeIDs[i], 2*time.Hour)
		if err != nil {
			return nil, fmt.Errorf("issue node %d: %w", i, err)
		}
		creds[i] = &replicateddb.TLSCredential{CertPEM: certPEM, KeyPEM: keyPEM, CAPEM: ca.CertPEM}
	}
	cleanupOnErr := true
	defer func() {
		if cleanupOnErr {
			c.Close()
		}
	}()
	for i := 0; i < numNodes; i++ {
		var peers []replicateddb.Peer
		for j := 0; j < numNodes; j++ {
			if j == i {
				continue
			}
			peers = append(peers, replicateddb.Peer{
				NodeID: nodeIDs[j],
				Addrs:  []string{fmt.Sprintf("127.0.0.1:%d", ports[j])},
			})
		}
		label := fmt.Sprintf("%s-node%d", name, i)
		dir := filepath.Join(root, label)
		db, err := replicateddb.Open(context.Background(), replicateddb.Config{
			Path:       filepath.Join(dir, "data"),
			NodeID:     nodeIDs[i],
			DBID:       dbid,
			Encryption: replicateddb.EncryptionConfig{Key: append([]byte(nil), TestKey...), KeyID: "test"},
			Schema:     replicateddb.SchemaConfig{Version: 1, Tables: genesis},
			Replication: replicateddb.ReplicationConfig{
				ListenAddr:      fmt.Sprintf("127.0.0.1:%d", ports[i]),
				TLS:             creds[i],
				Peers:           peers,
				SendInterval:    20 * time.Millisecond,
				DialInterval:    200 * time.Millisecond,
				AckInterval:     100 * time.Millisecond,
				MinLogRetention: time.Hour,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", label, err)
		}
		c.Nodes = append(c.Nodes, &Node{
			Label:  label,
			Dir:    dir,
			NodeID: nodeIDs[i],
			Addr:   fmt.Sprintf("127.0.0.1:%d", ports[i]),
			DB:     db,
		})
	}
	cleanupOnErr = false
	c.dbID = dbid
	c.creds = creds
	return c, nil
}

// grabPort reserves an ephemeral port by binding and releasing it.
// The reuse race is tiny and bring-up retries on collision.
func grabPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("grab port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Close shuts every node down: GORM/sql first, then the engine.
func (c *Cluster) Close() {
	for _, n := range c.Nodes {
		if n.GDB != nil {
			if sqldb, err := n.GDB.DB(); err == nil && sqldb != nil {
				sqldb.Close()
			}
			n.GDB = nil
		}
		if n.DB != nil {
			n.DB.Close()
			n.DB = nil
		}
	}
}

// WaitConnected waits until every node reports at least one peer.
func (c *Cluster) WaitConnected(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range c.Nodes {
			if n.DB.Status().ConnectedPeers < 1 {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("nodes never connected")
}

// WaitSchemaEpoch waits until every node shares node 0's schema epoch.
func (c *Cluster) WaitSchemaEpoch(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		want := c.Nodes[0].DB.Status().SchemaEpoch
		ok := true
		for _, n := range c.Nodes[1:] {
			if n.DB.Status().SchemaEpoch != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("schema epochs never converged")
}

// Count returns the GORM row count for model on node idx,
// retrying through transient materializer rebuilds.
func (c *Cluster) Count(idx int, model interface{}) int64 {
	c.t.Helper()
	n := c.Nodes[idx]
	var count int64
	for attempt := 0; attempt < 10; attempt++ {
		err := n.GDB.Model(model).Count(&count).Error
		if err == nil {
			return count
		}
		if !errors.Is(err, replicateddb.ErrMaterializerDirty) {
			c.t.Fatalf("count %s: %v", n.Label, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("count %s: materializer never settled", n.Label)
	return 0
}

// WaitForCount polls until node idx holds want rows for model.
func (c *Cluster) WaitForCount(idx int, model interface{}, want int64, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Count(idx, model) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("%s never reached %d rows", c.Nodes[idx].Label, want)
}

// Digest returns a canonical SHA-256 over all rows of model ordered
// by orderBy, read through GORM. Equal digests on two nodes prove
// identical replicated content.
func (c *Cluster) Digest(idx int, model interface{}, orderBy string) string {
	c.t.Helper()
	var rows []map[string]any
	q := c.Nodes[idx].GDB.Model(model)
	if orderBy != "" {
		q = q.Order(orderBy)
	}
	if err := q.Find(&rows).Error; err != nil {
		c.t.Fatalf("digest read %s: %v", c.Nodes[idx].Label, err)
	}
	h := sha256.New()
	for _, row := range rows {
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(h, "%s=", k)
			switch v := row[k].(type) {
			case []byte:
				h.Write([]byte(hex.EncodeToString(v)))
			case nil:
				h.Write([]byte("<nil>"))
			default:
				fmt.Fprintf(h, "%v", v)
			}
			h.Write([]byte(";"))
		}
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// WaitConverged waits until every node holds want rows for model
// with identical digests.
func (c *Cluster) WaitConverged(model interface{}, want int64, orderBy string, timeout time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for i := range c.Nodes {
			if c.Count(i, model) != want {
				ok = false
				break
			}
		}
		if !ok {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		base := c.Digest(0, model, orderBy)
		match := true
		for i := range c.Nodes[1:] {
			if c.Digest(i+1, model, orderBy) != base {
				match = false
				break
			}
		}
		if match {
			return base
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("nodes never converged on %d rows", want)
	return ""
}

// RestartNode closes node idx and reopens it from its live schema
// export, rewrapping GORM. Data and schema must survive.
func (c *Cluster) RestartNode(idx int) {
	c.t.Helper()
	n := c.Nodes[idx]
	epoch, live, err := n.DB.LiveSchema()
	if err != nil {
		c.t.Fatalf("liveschema %s: %v", n.Label, err)
	}
	if sqldb, err := n.GDB.DB(); err == nil && sqldb != nil {
		sqldb.Close()
	}
	if err := n.DB.Close(); err != nil {
		c.t.Fatalf("close %s: %v", n.Label, err)
	}
	// Rebind the same address (peers dial it statically); retry
	// briefly while the socket releases.
	var db *replicateddb.DB
	for attempt := 0; attempt < 25; attempt++ {
		db, err = replicateddb.Open(context.Background(), c.reopenConfig(idx, epoch, live))
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatalf("reopen %s: %v", n.Label, err)
	}
	n.DB = db
	gdb, err := gorm.Open(murmur.Open(db), &gorm.Config{})
	if err != nil {
		c.t.Fatalf("gorm reopen %s: %v", n.Label, err)
	}
	n.GDB = gdb
}

func (c *Cluster) reopenConfig(idx int, epoch uint64, live []murmurSchema.TableSchema) replicateddb.Config {
	n := c.Nodes[idx]
	var peers []replicateddb.Peer
	for j, other := range c.Nodes {
		if j == idx {
			continue
		}
		peers = append(peers, replicateddb.Peer{NodeID: other.NodeID, Addrs: []string{other.Addr}})
	}
	return replicateddb.Config{
		Path:       filepath.Join(n.Dir, "data"),
		NodeID:     n.NodeID,
		DBID:       c.dbID,
		Encryption: replicateddb.EncryptionConfig{Key: append([]byte(nil), TestKey...), KeyID: "test"},
		Schema:     replicateddb.SchemaConfig{Version: epoch, Tables: live},
		Replication: replicateddb.ReplicationConfig{
			ListenAddr:      n.Addr,
			TLS:             c.creds[idx],
			Peers:           peers,
			SendInterval:    20 * time.Millisecond,
			DialInterval:    200 * time.Millisecond,
			AckInterval:     100 * time.Millisecond,
			MinLogRetention: time.Hour,
		},
	}
}
