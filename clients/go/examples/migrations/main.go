package main

import (
	"context"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	db, err := e.Open("db", demo.Options())
	if err != nil {
		return err
	}
	if err = db.Migrate(murmur.NewMigration("CREATE TABLE items(id INTEGER PRIMARY KEY,title TEXT)")); err != nil {
		return err
	}
	if _, err = db.Exec("INSERT INTO items VALUES(1,'stable identity')"); err != nil {
		return err
	}
	if err = db.SetReplication("items", false); err != nil {
		return err
	}
	before, err := db.Schema()
	if err != nil {
		return err
	}
	migration := murmur.Migration{SQL: "ALTER TABLE items RENAME TO docs; ALTER TABLE docs RENAME COLUMN title TO body", Mappings: []murmur.TableMapping{{OldName: "items", NewName: "docs", Columns: [][2]string{{"id", "id"}, {"title", "body"}}}}}
	if err = db.Migrate(migration); err != nil {
		return err
	}
	after, err := db.Schema()
	if err != nil {
		return err
	}
	if err = demo.Require(before.Tables[0].ID == after.Tables[0].ID && before.Tables[0].Columns[1].ID == after.Tables[0].Columns[1].ID, "migration changed stable IDs"); err != nil {
		return err
	}
	enabled, err := db.ReplicationEnabled("docs")
	if err != nil {
		return err
	}
	if err = demo.Require(!enabled, "rename lost local setting"); err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	db, err = e.Open("db", murmur.DefaultOptions())
	if err != nil {
		return err
	}
	enabled, err = db.ReplicationEnabled("docs")
	if err != nil {
		return err
	}
	body, err := demo.Text(db, "SELECT body FROM docs WHERE id=1")
	if err != nil {
		return err
	}
	e.Print("Table/column IDs survived rename; local-only setting and %q survived recovery", body)
	return demo.Require(!enabled && body == "stable identity", "migration recovery mismatch")
}
func main() { demo.Main("migrations", run) }
