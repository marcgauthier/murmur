package state

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/spool"
)

// errTestCause stands in for a terminal storage failure cause.
var errTestCause = errors.New("state test: injected storage failure")

func TestFatalCaptureStickyFirstWins(t *testing.T) {
	var c fatalCapture
	if err := c.err(); err != nil {
		t.Fatalf("healthy err = %v, want nil", err)
	}
	c.noteFatal("first")
	c.noteFatal("second")
	err := c.err()
	if !IsStorageFailure(err) {
		t.Fatalf("err = %v, want storage failure", err)
	}
	if got, want := err.Error(), "first"; !contains(got, want) {
		t.Fatalf("err = %q, want it to mention %q", got, want)
	}
	if got := err.Error(); contains(got, "second") {
		t.Fatalf("err = %q, want first message to win", got)
	}
}

func TestFatalCaptureCauseWrapsENOSPC(t *testing.T) {
	var c fatalCapture
	c.noteTerminal("spool: fatal commit error", syscall.ENOSPC)
	err := c.err()
	if !IsStorageFailure(err) {
		t.Fatalf("err = %v, want storage failure", err)
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("err = %v, want ENOSPC cause", err)
	}
}

func TestFatalCaptureNilSafe(t *testing.T) {
	var c *fatalCapture
	c.noteFatal("x")
	c.noteTerminal("y", io.ErrUnexpectedEOF)
	if err := c.err(); err != nil {
		t.Fatalf("nil capture err = %v, want nil", err)
	}
}

// TestTerminalStorageFailureFailsClosed proves an injected Spool append
// failure trips the sticky fail-closed gate: the commit reports a
// storage failure with the underlying cause, and every later operation
// fails without touching storage.
func TestTerminalStorageFailureFailsClosed(t *testing.T) {
	var fail atomic.Bool
	opt := Options{
		Limits: codec.DefaultLimits(),
		Spool: spool.Options{
			Faults: &spool.FaultHooks{Append: func() error {
				if fail.Load() {
					return syscall.ENOSPC
				}
				return nil
			}},
		},
	}
	node := ids.NewNodeID()
	s, err := openSignedFixture(t.TempDir(), node, ids.DBID{}, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	row := ids.NewRowID()
	if _, err := s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: row, ColumnID: 2, Value: codec.Text("healthy")})); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	_, err = s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("doomed")}))
	if !IsStorageFailure(err) {
		t.Fatalf("commit err = %v, want storage failure", err)
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("commit err = %v, want ENOSPC cause", err)
	}
	if err := s.Failed(); !IsStorageFailure(err) {
		t.Fatalf("Failed = %v, want storage failure", err)
	}
	// Later reads and writes fail closed.
	if _, _, err := s.GetCell(7, row, 2); !IsStorageFailure(err) {
		t.Fatalf("GetCell err = %v, want storage failure", err)
	}
	if _, err := s.CommitLocal(ctx, localBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 7, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("late")})); !IsStorageFailure(err) {
		t.Fatalf("late commit err = %v, want storage failure", err)
	}
	if err := s.Sync(); !IsStorageFailure(err) {
		t.Fatalf("Sync err = %v, want storage failure", err)
	}
	// Restart recovery: the failed commit stayed absent while the
	// healthy write survived.
	if err := s.Close(); err != nil {
		t.Logf("close after failure: %v", err)
	}
	s2, err := openSignedFixture(s.openPath, node, s.DBID(), Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	st, ok, err := s2.GetCell(7, row, 2)
	if err != nil || !ok || st.Value.S != "healthy" {
		t.Fatalf("healthy readback after restart: %v %v %v", st, ok, err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
