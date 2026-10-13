package main

import (
	"context"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	cluster, err := demo.NewCluster(2)
	if err != nil {
		return err
	}
	for i := range cluster.Configs {
		cluster.Configs[i].MergePolicies = []murmur.PolicyEntry{{Table: "stats", Column: "count", Policy: murmur.CounterPolicy}, {Table: "stats", Column: "tags", Policy: murmur.SetPolicy}, {Table: "stats", Column: "low", Policy: murmur.MinPolicy}, {Table: "stats", Column: "high", Policy: murmur.MaxPolicy}}
	}
	a, err := cluster.Open(e, "a", 0, nil)
	if err != nil {
		return err
	}
	b, err := cluster.Open(e, "b", 1, nil)
	if err != nil {
		return err
	}
	for _, db := range []*murmur.DB{a, b} {
		if err = db.Migrate(murmur.NewMigration("CREATE TABLE stats(id INTEGER PRIMARY KEY,count INTEGER NOT NULL,tags BLOB,low INTEGER,high INTEGER,title TEXT)")); err != nil {
			return err
		}
	}
	if _, err = a.Exec("INSERT INTO stats VALUES(1,0,NULL,100,0,'initial')"); err != nil {
		return err
	}
	initial, err := a.ExportState()
	if err != nil {
		return err
	}
	if err = b.MergeState(initial); err != nil {
		return err
	}
	// Both peers update the same generation while disconnected.
	for i, db := range []*murmur.DB{a, b} {
		err = db.Transaction(func(s *murmur.Sql) error {
			key := []murmur.Value{murmur.Integer(1)}
			if err := s.CounterAdd("stats", key, "count", int64(3+i)); err != nil {
				return err
			}
			if err := s.SetAdd("stats", key, "tags", murmur.Text([]string{"alpha", "beta"}[i])); err != nil {
				return err
			}
			if err := s.Min("stats", key, "low", murmur.Integer(int64(10-i))); err != nil {
				return err
			}
			if err := s.Max("stats", key, "high", murmur.Integer(int64(90+i))); err != nil {
				return err
			}
			_, err := s.Exec("UPDATE stats SET title=? WHERE id=1", []string{"A", "B"}[i])
			return err
		})
		if err != nil {
			return err
		}
		if err = db.Flush(); err != nil {
			return err
		}
	}
	if _, _, err = demo.Connect(e, a, b); err != nil {
		return err
	}
	if err = demo.Wait(ctx, "causal convergence", func() (bool, error) {
		for _, db := range []*murmur.DB{a, b} {
			r, err := db.Query("SELECT count,low,high FROM stats WHERE id=1")
			if err != nil {
				return false, err
			}
			if len(r.Rows) != 1 || r.Rows[0][0].Integer != 7 || r.Rows[0][1].Integer != 9 || r.Rows[0][2].Integer != 91 {
				return false, nil
			}
		}
		return true, nil
	}); err != nil {
		return err
	}
	if err = a.Transaction(func(s *murmur.Sql) error {
		return s.SetRemove("stats", []murmur.Value{murmur.Integer(1)}, "tags", murmur.Text("alpha"))
	}); err != nil {
		return err
	}
	if err = a.Flush(); err != nil {
		return err
	}
	if err = demo.Wait(ctx, "observed-remove set and LWW", func() (bool, error) {
		x, err := a.Query("SELECT tags,title FROM stats")
		if err != nil {
			return false, err
		}
		y, err := b.Query("SELECT tags,title FROM stats")
		if err != nil {
			return false, err
		}
		return string(x.Rows[0][0].Blob) == string(y.Rows[0][0].Blob) && x.Rows[0][1].Text == y.Rows[0][1].Text, nil
	}); err != nil {
		return err
	}
	var x, y murmur.Counter
	if err = x.Add(cluster.Database, cluster.Nodes[0], 3); err != nil {
		return err
	}
	if err = y.Add(cluster.Database, cluster.Nodes[1], 4); err != nil {
		return err
	}
	cx, cy := murmur.Causal{Counter: &x}, murmur.Causal{Counter: &y}
	if err = cx.Join(cy); err != nil {
		return err
	}
	n, err := x.Value()
	if err != nil {
		return err
	}
	var set murmur.Set
	if err = set.Add([]byte("tag"), murmur.Tag{Database: cluster.Database, Node: cluster.Nodes[0], Transaction: cluster.Nodes[1]}); err != nil {
		return err
	}
	if err = set.Remove([]byte("tag")); err != nil {
		return err
	}
	values, err := set.Values()
	if err != nil {
		return err
	}
	e.Print("Disconnected contributions converged: counter=7, min=9, max=91; set removal and LWW agree")
	return demo.Require(n == 7 && len(values) == 0, "standalone causal helpers mismatch")
}
func main() { demo.Main("policies", run) }
