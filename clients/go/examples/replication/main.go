package main

import (
	"context"
	"fmt"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	for _, mode := range []murmur.Dissemination{murmur.Gossip, murmur.Plumtree} {
		if err := replicate(ctx, e, mode); err != nil {
			return err
		}
	}
	return nil
}
func replicate(ctx context.Context, e *demo.Env, mode murmur.Dissemination) error {
	cluster, err := demo.NewCluster(2)
	if err != nil {
		return err
	}
	for i := range cluster.Configs {
		cluster.Configs[i].Dissemination = mode
		cluster.Configs[i].BatchTransactions = 32
		cluster.Configs[i].BatchBytes = 2 << 20
		cluster.Configs[i].AdaptiveSync = true
		cluster.Configs[i].AllowedNetworks = []string{"127.0.0.0/8"}
		cluster.Configs[i].SchemaMaster = &cluster.Nodes[0]
		cluster.Configs[i].Master = i == 0
	}
	configure := func(o *murmur.Options) {
		o.SearchIndexes = []murmur.SearchIndex{murmur.FTS5Index("docs_fts", "docs", "title"), murmur.VectorIndex("docs_vec", "docs", "embedding", 2)}
	}
	a, err := cluster.Open(e, string(mode)+"-a", 0, configure)
	if err != nil {
		return err
	}
	b, err := cluster.Open(e, string(mode)+"-b", 1, configure)
	if err != nil {
		return err
	}
	// Only the schema master creates the table. The second peer adopts it.
	if err = a.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,embedding BLOB); CREATE TABLE private(id INTEGER PRIMARY KEY)")); err != nil {
		return err
	}
	if err = a.SetReplication("private", false); err != nil {
		return err
	}
	if _, err = a.Exec("INSERT INTO private VALUES(1)"); err != nil {
		return err
	}
	if _, err = a.Exec("INSERT INTO docs(id,title,embedding) VALUES(?,?,?)", 1, "from peer A", demo.Embedding(1, 0)); err != nil {
		return err
	}
	if err = a.Flush(); err != nil {
		return err
	}
	ar, br, err := demo.Connect(e, a, b)
	if err != nil {
		return err
	}
	if err = demo.Wait(ctx, "replicated row and local search", func() (bool, error) {
		rows, err := b.Query("SELECT id FROM docs")
		if err != nil {
			return false, nil
		}
		if len(rows.Rows) != 1 {
			return false, nil
		}
		if err = b.RefreshIndexes(); err != nil {
			return false, err
		}
		n, err := demo.Int(b, "SELECT count(*) FROM docs_fts WHERE docs_fts MATCH 'peer'")
		if err != nil {
			return false, err
		}
		id, err := demo.Int(b, "SELECT rowid FROM docs_vec WHERE embedding MATCH ? AND k=1", demo.Embedding(1, 0))
		return n == 1 && id == 1, err
	}); err != nil {
		return err
	}
	n, err := demo.Int(b, "SELECT count(*) FROM sqlite_schema WHERE name='private'")
	if err != nil {
		return err
	}
	if err = demo.Require(n == 0, "local-only table propagated"); err != nil {
		return err
	}
	if _, err = b.Exec("INSERT INTO docs(id,title,embedding) VALUES(?,?,?)", 2, "from peer B", demo.Embedding(0, 1)); err != nil {
		return err
	}
	if err = b.Flush(); err != nil {
		return err
	}
	if err = demo.Wait(ctx, "bidirectional rows", func() (bool, error) { n, err := demo.Int(a, "SELECT count(*) FROM docs"); return n == 2, err }); err != nil {
		return err
	}
	if err = ar.ForceSync(); err != nil {
		return err
	}
	status, err := br.Status()
	if err != nil {
		return err
	}
	if err = ar.RemovePeer(cluster.Nodes[1]); err != nil {
		return err
	}
	e.Print("%s: bidirectional rows, authoritative schema, private table filtering, FTS/vector projections; peer status=%+v", mode, status)
	if err = ar.Release(); err != nil {
		return err
	}
	if err = br.Release(); err != nil {
		return err
	}
	if err = a.Close(); err != nil {
		return fmt.Errorf("close A: %w", err)
	}
	return b.Close()
}
func main() { demo.Main("replication", run) }
