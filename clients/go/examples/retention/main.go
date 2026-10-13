package main

import (
	"context"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
	"time"
)

func run(ctx context.Context, e *demo.Env) error {
	c, err := demo.NewCluster(3)
	if err != nil {
		return err
	}
	for i := range c.Configs {
		c.Configs[i].MinimumRetained = 2
		c.Configs[i].Retention = time.Hour
		c.Configs[i].RetentionInterval = time.Hour
		c.Configs[i].MergePolicies = []murmur.PolicyEntry{{Table: "items", Column: "n", Policy: murmur.CounterPolicy}, {Table: "items", Column: "tags", Policy: murmur.SetPolicy}}
	}
	a, err := c.Open(e, "a", 0, nil)
	if err != nil {
		return err
	}
	b, err := c.Open(e, "b", 1, nil)
	if err != nil {
		return err
	}
	for _, db := range []*murmur.DB{a, b} {
		if err = db.Migrate(murmur.NewMigration("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL,tags BLOB NOT NULL,payload BLOB)")); err != nil {
			return err
		}
		if err = db.Flush(); err != nil {
			return err
		}
	}
	if _, err = a.Exec("INSERT INTO items VALUES(1,0,x'00',zeroblob(180000)),(2,0,x'00',x'')"); err != nil {
		return err
	}
	if err = a.Flush(); err != nil {
		return err
	}
	baseline, err := a.ExportState()
	if err != nil {
		return err
	}
	if err = b.MergeState(baseline); err != nil {
		return err
	}
	if err = b.Transaction(func(s *murmur.Sql) error {
		key := []murmur.Value{murmur.Integer(1)}
		if err := s.CounterAdd("items", key, "n", 3); err != nil {
			return err
		}
		return s.SetAdd("items", key, "tags", murmur.Text("offline"))
	}); err != nil {
		return err
	}
	if err = b.Flush(); err != nil {
		return err
	}
	for i := 0; i < 8; i++ {
		if err = a.Transaction(func(s *murmur.Sql) error {
			if _, err := s.Exec("DELETE FROM items WHERE id=2"); err != nil {
				return err
			}
			return s.CounterAdd("items", []murmur.Value{murmur.Integer(1)}, "n", 2)
		}); err != nil {
			return err
		}
		if err = a.Flush(); err != nil {
			return err
		}
	}
	// First show that young logs are protected. Then reopen with demo age=0,
	// retain at least two, and publish a durable snapshot before pruning. No known
	// peer ACK holds these logs. Never prune directly from application SQL.
	young, err := a.CollectRetention()
	if err != nil {
		return err
	}
	if err = demo.Require(young == 0, "minimum age failed to protect young logs"); err != nil {
		return err
	}
	e.Print("Minimum age=1h protected all newly persisted logs")
	if err = a.Close(); err != nil {
		return err
	}
	c.Configs[0].Retention = 0 // Reopen with a shortened age for the next phase.
	reopen := murmur.DefaultOptions()
	reopen.Replication = &c.Configs[0]
	a, err = e.Open("a", reopen)
	if err != nil {
		return err
	}
	collected, err := a.CollectRetention()
	if err != nil {
		return err
	}
	if err = demo.Require(collected > 0, "history was not pruned"); err != nil {
		return err
	}
	ar, _, err := demo.Connect(e, a, b)
	if err != nil {
		return err
	}
	if err = demo.Wait(ctx, "managed snapshot catch-up preserving offline edits", func() (bool, error) {
		for _, db := range []*murmur.DB{a, b} {
			n, err := demo.Int(db, "SELECT n FROM items WHERE id=1")
			if err != nil {
				return false, err
			}
			count, err := demo.Int(db, "SELECT count(*) FROM items WHERE id=2")
			if err != nil {
				return false, err
			}
			if n != 19 || count != 0 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err = ar.RetirePeer(c.Nodes[2]); err != nil {
		return err
	}
	status, err := ar.RetentionStatus()
	if err != nil {
		return err
	}
	retired := false
	for _, peer := range status.Peers {
		if peer.Node == c.Nodes[2] && peer.Retired {
			retired = true
		}
	}
	e.Print("Pruned %d transactions with minimum count=2 and age=0 (demo only); snapshot preserved offline counter=19 and tombstone", collected)
	e.Print("Peer ACK/retirement status: %+v", status)
	return demo.Require(retired, "retirement was not persisted")
}
func main() { demo.Main("retention", run) }
