package main

import (
	"context"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
	"reflect"
)

func run(ctx context.Context, e *demo.Env) error {
	id, err := demo.ID()
	if err != nil {
		return err
	}
	opts := demo.Options()
	opts.DatabaseID = &id
	a, err := e.Open("a", opts)
	if err != nil {
		return err
	}
	b, err := e.Open("b", opts)
	if err != nil {
		return err
	}
	for _, db := range []*murmur.DB{a, b} {
		if err = db.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")); err != nil {
			return err
		}
	}
	if _, err = a.Exec("INSERT INTO docs VALUES(1,'original'),(2,'delete me')"); err != nil {
		return err
	}
	initial, err := a.ExportState()
	if err != nil {
		return err
	}
	if err = b.MergeState(initial); err != nil {
		return err
	}
	if _, err = a.Exec("UPDATE docs SET title='from A' WHERE id=1"); err != nil {
		return err
	}
	if _, err = b.Exec("UPDATE docs SET title='from B' WHERE id=1"); err != nil {
		return err
	}
	if _, err = b.Exec("DELETE FROM docs WHERE id=2"); err != nil {
		return err
	}
	sa, err := a.ExportState()
	if err != nil {
		return err
	}
	sb, err := b.ExportState()
	if err != nil {
		return err
	}
	if err = a.MergeState(sb); err != nil {
		return err
	}
	if err = b.MergeState(sa); err != nil {
		return err
	}
	if err = a.MergeState(sb); err != nil {
		return err
	} // duplicate delivery is safe
	ra, err := a.Query("SELECT * FROM docs ORDER BY id")
	if err != nil {
		return err
	}
	rb, err := b.Query("SELECT * FROM docs ORDER BY id")
	if err != nil {
		return err
	}
	state, err := a.ExportState()
	if err != nil {
		return err
	}
	if err = demo.Require(len(ra.Rows) == 1, "expected one resolved row"); err != nil {
		return err
	}
	e.Print("Offline peers agree on %q; %d tombstone(s) survive duplicate delivery", ra.Rows[0][1].Text, len(state.Tombstones))
	return demo.Require(reflect.DeepEqual(ra, rb) && len(ra.Rows) == 1 && len(state.Tombstones) > 0, "offline peers did not converge")
}
func main() { demo.Main("offline", run) }
