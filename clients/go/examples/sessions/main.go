package main

import (
	"context"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	raw, err := murmur.OpenInMemory()
	if err != nil {
		return err
	}
	e.Defer(raw.Close)
	if err = raw.Run(func(s *murmur.Sql) error {
		return s.ExecuteBatch("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")
	}); err != nil {
		return err
	}
	session, err := murmur.StartSession(raw)
	if err != nil {
		return err
	}
	e.Defer(session.Close)
	table := "docs"
	if err = session.Attach(&table); err != nil {
		return err
	}
	if err = session.SetEnabled(false); err != nil {
		return err
	}
	if err = raw.Run(func(s *murmur.Sql) error { _, err := s.Exec("INSERT INTO docs VALUES(1,'not captured')"); return err }); err != nil {
		return err
	}
	empty, err := session.IsEmpty()
	if err != nil {
		return err
	}
	if err = demo.Require(empty, "disabled session captured writes"); err != nil {
		return err
	}
	if err = session.SetEnabled(true); err != nil {
		return err
	}
	if err = raw.Run(func(s *murmur.Sql) error { _, err := s.Exec("INSERT INTO docs VALUES(2,'captured')"); return err }); err != nil {
		return err
	}
	changes, err := session.Changeset()
	if err != nil {
		return err
	}
	patch, err := session.Patchset()
	if err != nil {
		return err
	}
	e.Print("Unmanaged session: changeset %d bytes, patchset %d bytes; no persistence or replication", len(changes), len(patch))
	return demo.Require(len(changes) > 0 && len(patch) > 0, "session capture empty")
}
func main() { demo.Main("sessions", run) }
