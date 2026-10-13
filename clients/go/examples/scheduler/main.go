package main

import (
	"context"
	"errors"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
	"time"
)

func run(ctx context.Context, e *demo.Env) error {
	config := murmur.DefaultOptions().Scheduler
	config.LocalShare = 90
	config.RemoteShare = 10
	config.MaxDebt = 100 * time.Millisecond
	config.MaintenanceReserveEvery = 10
	if err := config.Validate(); err != nil {
		return err
	}
	scheduler, err := murmur.NewWriterScheduler(config)
	if err != nil {
		return err
	}
	e.Defer(scheduler.Close)
	held, err := scheduler.Admit(murmur.WriterLocal)
	if err != nil {
		return err
	}
	e.Defer(held.Release)
	class, err := held.Class()
	if err != nil {
		return err
	}
	granted, err := held.GrantedAt()
	if err != nil {
		return err
	}
	dual, err := held.Dual()
	if err != nil {
		return err
	}
	if err = demo.Require(class == murmur.WriterLocal && !granted.After(time.Now()) && !dual, "ticket metadata mismatch"); err != nil {
		return err
	}
	if ticket, err := scheduler.AdmitTimeout(murmur.WriterRemote, 20*time.Millisecond); !errors.Is(err, murmur.ErrBusy) {
		if ticket != nil {
			ticket.Release()
		}
		return demo.Require(false, "admission timeout: %v", err)
	}
	if err = held.Release(); err != nil {
		return err
	}
	for _, class := range []murmur.WriterClass{murmur.WriterRemote, murmur.WriterMaintenance} {
		ticket, err := scheduler.Admit(class)
		if err != nil {
			return err
		}
		if err = ticket.Release(); err != nil {
			return err
		}
	}
	snapshot, err := scheduler.Snapshot()
	if err != nil {
		return err
	}
	e.Print("Standalone admissions local=%d remote=%d maintenance=%d; shares 90/10", snapshot.Local.Acquisitions, snapshot.Remote.Acquisitions, snapshot.Maintenance.Acquisitions)
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
	borrowed, err := db.Scheduler()
	if err != nil {
		return err
	}
	e.Defer(borrowed.Release) // Close would stop the database's scheduler.
	active, err := borrowed.Active()
	if err != nil {
		return err
	}
	metrics, err := db.SQLiteMetrics()
	if err != nil {
		return err
	}
	spool, err := db.SpoolMetrics()
	if err != nil {
		return err
	}
	status, err := db.Status()
	if err != nil {
		return err
	}
	e.Print("Database scheduler active=%v; SQLite rows=%d, Spool keys=%d; status=%+v", active, metrics.TotalRows, spool.Keys, status)
	return demo.Require(snapshot.Local.Acquisitions >= 1 && snapshot.Remote.Acquisitions >= 1 && snapshot.Maintenance.Acquisitions >= 1 && metrics.TotalRows == 1, "scheduler/metrics mismatch")
}
func main() { demo.Main("scheduler", run) }
