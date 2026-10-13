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
	if err = db.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")); err != nil {
		return err
	}
	if _, err = db.Exec("INSERT INTO docs VALUES(1,'point in time')"); err != nil {
		return err
	}
	backup := e.Path("backup")
	if err = db.Backup(backup); err != nil {
		return err
	}
	if err = murmur.VerifyBackup(backup, e.Key[:]); err != nil {
		return err
	}
	if _, err = db.Exec("UPDATE docs SET title='newer write'"); err != nil {
		return err
	}
	freshDB, err := demo.ID()
	if err != nil {
		return err
	}
	freshNode, err := demo.ID()
	if err != nil {
		return err
	}
	for _, name := range []string{"restored", "fresh"} {
		restore := murmur.RestoreOptions{}
		if name == "fresh" {
			restore.NewDatabaseID = &freshDB
			restore.NewNodeID = &freshNode
		}
		if err = murmur.Restore(backup, e.Path(name), e.Key[:], restore); err != nil {
			return err
		}
		recovered, err := e.Open(name, murmur.DefaultOptions())
		if err != nil {
			return err
		}
		title, err := demo.Text(recovered, "SELECT title FROM docs")
		if err != nil {
			return err
		}
		if err = demo.Require(title == "point in time", "backup was not point-in-time"); err != nil {
			return err
		}
		if name == "fresh" {
			id, err := recovered.DatabaseID()
			if err != nil {
				return err
			}
			node, err := recovered.NodeID()
			if err != nil {
				return err
			}
			if err = demo.Require(id == freshDB && node == freshNode, "restore identity mismatch"); err != nil {
				return err
			}
		}
	}
	e.Print("Verified point-in-time backup; restored original and fresh identities")
	return nil
}
func main() { demo.Main("backup", run) }
