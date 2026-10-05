package murmur

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/state"
)

func TestOpenProgressEmptyAndDisabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		cfg := testConfig(t.TempDir())
		var events []OpenProgress
		if enabled {
			cfg.OnOpenProgress = func(p OpenProgress) { events = append(events, p) }
		}
		db, err := openSignedFixture(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		p := db.Status().OpenProgress
		if !enabled {
			if p != nil || db.openProgress != nil {
				t.Fatal("disabled progress allocated a reporter")
			}
			continue
		}
		if p == nil || p.Phase != OpenReady || p.TotalItemsKnown || p.TotalItems != 0 || p.PercentComplete != 0 || p.EstimateKnown {
			t.Fatalf("empty terminal snapshot: %+v", p)
		}
		want := []OpenPhase{OpenOpening, OpenRebuilding, OpenIndexing, OpenFinalizing, OpenReady}
		var phases []OpenPhase
		for _, p := range events {
			if len(phases) == 0 || phases[len(phases)-1] != p.Phase {
				phases = append(phases, p.Phase)
			}
		}
		if len(phases) != len(want) {
			t.Fatalf("phases=%v", phases)
		}
		for i := range want {
			if phases[i] != want[i] {
				t.Fatalf("phases=%v", phases)
			}
		}
		elapsed := p.Elapsed
		p.TotalItems = 99
		if got := db.Status().OpenProgress; got.TotalItems != 0 || got.Elapsed != elapsed {
			t.Fatalf("snapshot aliased or clock not frozen: %+v", got)
		}
	}
}

func TestOpenProgressStorageFailure(t *testing.T) {
	cfg := testConfig(t.TempDir())
	if err := os.WriteFile(filepath.Join(cfg.Path, "data"), []byte("blocked store directory"), 0600); err != nil {
		t.Fatal(err)
	}
	var events []OpenProgress
	cfg.OnOpenProgress = func(p OpenProgress) { events = append(events, p) }
	live, err := openSignedFixture(context.Background(), cfg)
	if live != nil || err == nil {
		if live != nil {
			live.Close()
		}
		t.Fatal("expected storage failure")
	}
	if len(events) < 2 || events[len(events)-1].Phase != OpenFailed || events[len(events)-1].Error == nil {
		t.Fatalf("events=%v", events)
	}
	for _, p := range events {
		if p.Phase == OpenReady {
			t.Fatal("storage failure reported ready")
		}
	}
}

func TestOpenProgressDeletedAndIncompleteRows(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables[0].Columns[1].Nullable = false
	live, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		id := NewRowID()
		if _, err := live.ExecContext(context.Background(), "INSERT INTO contacts(id,name) VALUES(?,?)", id[:], "same"); err != nil {
			live.Close()
			t.Fatal(err)
		}
	}
	if _, err := live.ExecContext(context.Background(), "DELETE FROM contacts WHERE rowid=(SELECT max(rowid) FROM contacts)"); err != nil {
		live.Close()
		t.Fatal(err)
	}
	table := live.reg.Tables[0]
	id := NewRowID()
	_, err = live.store.CommitLocal(context.Background(), &codec.MutationBatch{
		ProtocolVersion: 1, TxID: NewTxID(), OriginNode: cfg.NodeID, HLC: live.store.ClockMax() + 1, SchemaEpoch: 1,
		Mutations: []codec.Mutation{{TableID: table.ID, RowID: id, ColumnID: table.Columns[0].ID, Value: codec.Blob(id[:])}},
	})
	if err != nil {
		live.Close()
		t.Fatal(err)
	}
	var expected uint64
	if err := live.store.IterateTable(table.ID, func(r *state.Row) error { expected += uint64(len(r.Cells)); return nil }); err != nil {
		live.Close()
		t.Fatal(err)
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.OnOpenProgress = func(OpenProgress) {}
	live, err = openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	p := live.Status().OpenProgress
	if p.TotalItemsKnown || p.TotalItems != 0 || p.ProcessedItems != expected || p.RowsInserted != 2 || p.RowsSkipped != 2 {
		t.Fatalf("progress=%+v expected cells=%d", p, expected)
	}
}

func TestOpenProgressTerminalFailures(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		cfg := testConfig(t.TempDir())
		ctx := context.Background()
		if cancelled {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		} else {
			cfg.Schema.Tables = nil
		}
		var events []OpenProgress
		cfg.OnOpenProgress = func(p OpenProgress) { events = append(events, p) }
		live, err := openSignedFixture(ctx, cfg)
		if err == nil || live != nil {
			t.Fatal("expected open error")
		}
		want := OpenFailed
		if cancelled {
			want = OpenCancelled
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		if len(events) < 2 || events[len(events)-1].Phase != want || events[len(events)-1].Error == nil {
			t.Fatalf("events=%+v", events)
		}
		terminal := 0
		for _, p := range events {
			if p.Phase == OpenReady {
				t.Fatal("failed open reported ready")
			}
			if p.Phase == want {
				terminal++
			}
		}
		if terminal != 1 {
			t.Fatalf("terminal events=%d", terminal)
		}
	}
}

func TestOpenProgressReporterHeartbeatAndSerialization(t *testing.T) {
	var active atomic.Int32
	var calls atomic.Int32
	var overlap atomic.Bool
	var events []OpenProgress
	r := newOpenProgressReporter(func(p OpenProgress) {
		if active.Add(1) != 1 {
			overlap.Store(true)
		}
		time.Sleep(10 * time.Millisecond)
		events = append(events, p)
		calls.Add(1)
		active.Add(-1)
	})
	r.phase(OpenRebuilding)
	r.processed.Store(25)
	r.mu.Lock()
	r.phaseStarted = time.Now().Add(-2 * time.Second)
	r.mu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	r.phase(OpenIndexing)
	r.phase(OpenFinalizing)
	r.finish(nil)
	if overlap.Load() {
		t.Fatal("callbacks overlap")
	}
	found := false
	for _, p := range events {
		if p.TotalItemsKnown || p.EstimateKnown || p.TotalItems != 0 || p.CountedItems != 0 || p.PercentComplete != 0 || p.EstimatedRemaining != 0 || p.Phase == OpenCounting {
			t.Fatalf("unexpected counting or estimate: %+v", p)
		}
		if p.Phase == OpenRebuilding && p.ProcessedItems == 25 && p.PhaseElapsed >= time.Second {
			found = true
		}
	}
	if !found {
		t.Fatal("missing work-done heartbeat")
	}
	if events[len(events)-1].Phase != OpenReady {
		t.Fatal("missing terminal callback")
	}
	select {
	case <-r.done:
	default:
		t.Fatal("reporter still running")
	}
}
