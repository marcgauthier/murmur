package rime_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
)

// Profiles are opt-in and never used as acceptance timings. Use one selected
// cell, -benchtime=1x -count=1, in a separate process for each capture kind.
func oltpProfile[T any](b *testing.B, cfg oltpCfg, round int) func() {
	b.Helper()
	dir := os.Getenv("RIME_OLTP_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	kind := os.Getenv("RIME_OLTP_PROFILE_KIND")
	if kind == "" {
		kind = "cpu"
	}
	if kind != "cpu" && kind != "contention" {
		b.Fatal("RIME_OLTP_PROFILE_KIND must be cpu or contention")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		b.Fatal(err)
	}
	prefix := filepath.Join(dir, fmt.Sprintf("%T-%s-%s-n%d-w%d-b%d-round%d", *new(T), cfg.op, cfg.mix, cfg.rows, cfg.workers, cfg.batch, round))
	write := func(name string) {
		f, err := os.Create(prefix + "-" + name + ".pprof")
		if err != nil {
			b.Fatal(err)
		}
		err = pprof.Lookup(name).WriteTo(f, 0)
		closeErr := f.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
	}
	if kind == "contention" {
		previous := runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)
		return func() {
			runtime.SetMutexProfileFraction(previous)
			runtime.SetBlockProfileRate(0)
			write("mutex")
			write("block")
		}
	}
	f, err := os.Create(prefix + "-alloc-base.pprof")
	if err != nil {
		b.Fatal(err)
	}
	err = pprof.Lookup("allocs").WriteTo(f, 0)
	closeErr := f.Close()
	if err != nil {
		b.Fatal(err)
	}
	if closeErr != nil {
		b.Fatal(closeErr)
	}
	cpu, err := os.Create(prefix + "-cpu.pprof")
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		cpu.Close()
		b.Fatal(err)
	}
	return func() {
		pprof.StopCPUProfile()
		if err := cpu.Close(); err != nil {
			b.Fatal(err)
		}
		runtime.GC() // flush allocation samples before final-state verification.
		write("allocs")
	}
}
