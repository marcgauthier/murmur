package main

import (
	"context"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	c, err := demo.NewCluster(2)
	if err != nil {
		return err
	}
	a, err := demo.NewBackend(c.Database, c.Nodes[0], c.Configs[0])
	if err != nil {
		return err
	}
	e.Defer(a.Handle.Close)
	b, err := demo.NewBackend(c.Database, c.Nodes[1], c.Configs[1])
	if err != nil {
		return err
	}
	e.Defer(b.Handle.Close)
	if err = a.Commit([]murmur.Record{murmur.PutRecord([]byte("demo/empty"), []byte{})}); err != nil {
		return err
	}
	values, err := a.Handle.GetMany([][]byte{[]byte("demo/empty"), []byte("demo/missing")})
	if err != nil {
		return err
	}
	if err = demo.Require(len(values) == 2 && values[0] != nil && len(*values[0]) == 0 && values[1] == nil, "batch missing/empty distinction"); err != nil {
		return err
	}
	if err = murmur.RunBlocking(func() error { return nil }); err != nil {
		return err
	}
	ar, err := murmur.StartReplication(c.Configs[0], a.Handle)
	if err != nil {
		return err
	}
	e.Defer(ar.Close)
	br, err := murmur.StartReplication(c.Configs[1], b.Handle)
	if err != nil {
		return err
	}
	e.Defer(br.Close)
	status, err := br.Status()
	if err != nil {
		return err
	}
	if err = ar.AddPeer(*status.Address); err != nil {
		return err
	}
	if err = a.Publish([]murmur.Record{murmur.PutRecord([]byte("demo/message"), []byte("standalone replication"))}); err != nil {
		return err
	}
	if err = demo.Wait(ctx, "record-only backend replication", func() (bool, error) {
		value, ok, err := b.Get([]byte("demo/message"))
		return ok && string(value) == "standalone replication", err
	}); err != nil {
		return err
	}
	if err = a.Publish([]murmur.Record{murmur.DeleteRecord([]byte("demo/message"))}); err != nil {
		return err
	}
	if err = demo.Wait(ctx, "record deletion", func() (bool, error) { _, ok, err := b.Get([]byte("demo/message")); return !ok, err }); err != nil {
		return err
	}
	progress, err := a.Handle.Progress()
	if err != nil {
		return err
	}
	if err = demo.Require(len(progress) > 0, "backend progress missing"); err != nil {
		return err
	}
	e.Print("Custom callback backend replicated signed records and deletion; ephemeral demo has no durable checkpoints")
	return nil
}
func main() { demo.Main("backend", run) }
