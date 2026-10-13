package main

import (
	"context"
	"errors"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
	"sync/atomic"
	"time"
)

func run(ctx context.Context, e *demo.Env) error {
	var armed atomic.Bool
	opts := demo.Options()
	opts.CaptureInterval = time.Hour
	opts.FaultHooks = &murmur.FaultHooks{SegmentSync: func() error {
		if armed.Load() {
			return errors.New("demonstration sync failure")
		}
		return nil
	}}
	db, err := e.Open("db", opts)
	if err != nil {
		return err
	}
	e.Defer(func() error { armed.Store(false); return db.Close() })
	if err = db.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY)")); err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	if _, err = db.Exec("INSERT INTO docs VALUES(1)"); err != nil {
		return err
	}
	armed.Store(true)
	if err = db.Flush(); !errors.Is(err, murmur.ErrStorage) {
		return demo.Require(false, "expected storage error: %v", err)
	}
	if _, err = db.Exec("INSERT INTO docs VALUES(2)"); err == nil {
		return errors.New("write accepted after storage failure")
	}
	n, err := demo.Int(db, "SELECT count(*) FROM docs")
	if err != nil {
		return err
	}
	if err = demo.Require(n == 1, "failed storage made reads unavailable"); err != nil {
		return err
	}
	status, err := db.Status()
	if err != nil {
		return err
	}
	e.Print("Storage failure reported: %v; reads remain available", status.StorageError)
	armed.Store(false)
	if err = db.RetryStorage(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	opts = murmur.DefaultOptions()
	var ready atomic.Bool
	opts.OnOpenProgress = func(p murmur.OpenProgress) {
		if p.Phase == murmur.Ready {
			ready.Store(true)
		}
	}
	db, err = e.Open("db", opts)
	if err != nil {
		return err
	}
	n, err = demo.Int(db, "SELECT count(*) FROM docs")
	if err != nil {
		return err
	}
	progress, err := db.OpenProgress()
	if err != nil {
		return err
	}
	e.Print("Retry and reopen recovered %d row; progress=%v", n, progress)
	return demo.Require(n == 1 && ready.Load(), "recovery/progress mismatch")
}
func main() { demo.Main("recovery", run) }
