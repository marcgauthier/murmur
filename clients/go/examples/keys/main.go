package main

import (
	"context"
	"errors"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	db, err := e.Open("db", demo.Options())
	if err != nil {
		return err
	}
	if err = db.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY)")); err != nil {
		return err
	}
	if _, err = db.Exec("INSERT INTO docs VALUES(1)"); err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	before, err := db.KeyInventory()
	if err != nil {
		return err
	}
	if err = db.RotateDataKey(); err != nil {
		return err
	}
	after, err := db.KeyInventory()
	if err != nil {
		return err
	}
	if err = demo.Require(before.ActiveDataKeyID != after.ActiveDataKeyID, "data key did not rotate"); err != nil {
		return err
	}
	replacement, err := demo.Key()
	if err != nil {
		return err
	}
	if err = db.RotateMasterKey(replacement); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	old, err := murmur.Open(e.Path("db"), e.Key[:], murmur.DefaultOptions())
	if old != nil {
		old.Close()
	}
	if !errors.Is(err, murmur.ErrWrongKey) {
		return demo.Require(false, "old wrapping key accepted: %v", err)
	}
	reopened, err := murmur.Open(e.Path("db"), replacement[:], murmur.DefaultOptions())
	if err != nil {
		return err
	}
	e.Defer(reopened.Close)
	count, err := demo.Int(reopened, "SELECT count(*) FROM docs")
	if err != nil {
		return err
	}
	e.Print("Data key %d -> %d; new master key opens preserved rows, old key rejected", before.ActiveDataKeyID, after.ActiveDataKeyID)
	return demo.Require(count == 1, "rotation lost data")
}
func main() { demo.Main("keys", run) }
