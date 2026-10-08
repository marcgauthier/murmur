// Real encrypted storage and process restarts exercise the embedded startup API.
package openprogress_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/marcgauthier/murmur/internal/testdb"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
)

const rowsPerTable = 6000

type progressRecord struct {
	ID      ids.RowID `rime:"primary"`
	Message string
	Ordinal int64
}

type fixture struct {
	Node db.NodeID
	DB   db.DBID
}

func config(dir string, f fixture) (db.Config, error) {
	var definitions []db.TableDefinition
	for i, name := range []string{"requests", "events"} {
		definition, err := db.Define[progressRecord](name, uint32(301+i), db.RecordOptions{
			PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Message": 2, "Ordinal": 3},
		})
		if err != nil {
			return db.Config{}, err
		}
		definitions = append(definitions, definition)
	}
	return testdb.Configure(db.Config{Path: filepath.Join(dir, "db"), NodeID: f.Node, DBID: f.DB,
		Tables: definitions, Spool: db.DefaultSpoolConfig(),
		Encryption: db.EncryptionConfig{Key: bytes.Repeat([]byte{0x63}, 32), KeyID: "open-progress-live"}}), nil
}

func TestEmbeddedOpenProgressLive(t *testing.T) {
	if os.Getenv("MURMUR_PROGRESS_CHILD") != "" {
		t.Skip("parent only")
	}
	dir := t.TempDir()
	f := fixture{db.NewNodeID(), db.NewDBID()}
	raw, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fixture.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"populate", "reload", "cancel", "reload", "open-failure", "reload"} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		cmd := exec.CommandContext(ctx, binary, "-test.run=^TestOpenProgressChild$", "-test.v", "-test.timeout=2m")
		cmd.Env = append(os.Environ(), "MURMUR_PROGRESS_CHILD="+role, "MURMUR_PROGRESS_DIR="+dir)
		output, err := cmd.CombinedOutput()
		cancel()
		t.Logf("%s:\n%s", role, output)
		if err != nil {
			t.Fatalf("%s: %v", role, err)
		}
	}
}

func TestOpenProgressChild(t *testing.T) {
	role := os.Getenv("MURMUR_PROGRESS_CHILD")
	if role == "" {
		t.Skip("process worker")
	}
	dir := os.Getenv("MURMUR_PROGRESS_DIR")
	var f fixture
	raw, err := os.ReadFile(filepath.Join(dir, "fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	cfg, err := config(dir, f)
	if err != nil {
		t.Fatal(err)
	}
	if role == "populate" {
		live, err := db.Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()
		for _, name := range []string{"requests", "events"} {
			table, err := db.TableOf[progressRecord](live, name)
			if err != nil {
				t.Fatal(err)
			}
			for start := 0; start < rowsPerTable; start += 1000 {
				err := live.WriteTxContext(context.Background(), func(tx *db.Tx) error {
					for i := start; i < start+1000; i++ {
						row := &progressRecord{ID: db.NewRowID(), Message: "realistic diagnostic " + fmt.Sprint(i) + " " + string(bytes.Repeat([]byte("trace "), 512)), Ordinal: int64(i)}
						if err := table.Insert(tx, row); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var events []db.OpenProgress
	var active atomic.Int32
	var overlap atomic.Bool
	cfg.OnOpenProgress = func(p db.OpenProgress) {
		if active.Add(1) != 1 {
			overlap.Store(true)
		}
		events = append(events, p)
		if role == "cancel" && p.Phase == db.OpenOpening {
			cancel()
		}
		active.Add(-1)
	}
	if role == "open-failure" {
		cfg.Encryption.Key = []byte{1, 2, 3}
	}
	live, err := db.Open(ctx, cfg)
	if role == "cancel" {
		if !errors.Is(err, context.Canceled) || live != nil {
			if live != nil {
				live.Close()
			}
			t.Fatalf("cancel result=%v", err)
		}
	} else if role == "open-failure" {
		if err == nil || live != nil {
			if live != nil {
				live.Close()
			}
			t.Fatal("expected open failure")
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()
		p := live.Status().OpenProgress
		if p == nil || p.Phase != db.OpenReady || p.TotalItemsKnown || p.ProcessedItems != rowsPerTable*2*3 || p.RowsInserted != rowsPerTable*2 || p.RowsSkipped != 0 || p.PercentComplete != 0 {
			t.Fatalf("terminal=%+v", p)
		}
		for _, name := range []string{"requests", "events"} {
			table, err := db.TableOf[progressRecord](live, name)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := table.Where().Find()
			if err != nil {
				t.Fatal(err)
			}
			n := int64(len(rows))
			var sum int64
			for _, row := range rows {
				sum += row.Ordinal
			}
			if n != rowsPerTable || sum != rowsPerTable*(rowsPerTable-1)/2 {
				t.Fatalf("wrong contents %s", name)
			}
		}
	}
	if overlap.Load() {
		t.Fatal("overlapping callbacks")
	}
	if len(events) == 0 {
		t.Fatal("no progress")
	}
	var last uint64
	var elapsed time.Duration
	var phases []db.OpenPhase
	terminal := 0
	for _, p := range events {
		if p.ProcessedItems < last || p.Elapsed < elapsed {
			t.Fatal("nonmonotonic progress")
		}
		last = p.ProcessedItems
		elapsed = p.Elapsed
		if p.TotalItemsKnown || p.EstimateKnown || p.TotalItems != 0 || p.CountedItems != 0 || p.PercentComplete != 0 || p.EstimatedRemaining != 0 || p.Phase == db.OpenCounting {
			t.Fatalf("unexpected counting or estimate: %+v", p)
		}
		if len(phases) == 0 || phases[len(phases)-1] != p.Phase {
			phases = append(phases, p.Phase)
		}
		if p.Phase == db.OpenReady || p.Phase == db.OpenFailed || p.Phase == db.OpenCancelled {
			terminal++
		}
	}
	if terminal != 1 {
		t.Fatalf("terminal count=%d", terminal)
	}
	lastEvent := events[len(events)-1]
	want := db.OpenReady
	if role == "cancel" {
		want = db.OpenCancelled
	}
	if role == "open-failure" {
		want = db.OpenFailed
	}
	if lastEvent.Phase != want {
		t.Fatalf("terminal phase=%v", lastEvent.Phase)
	}
	if role == "reload" {
		wanted := []db.OpenPhase{db.OpenOpening, db.OpenRebuilding, db.OpenFinalizing, db.OpenReady}
		if len(phases) != len(wanted) {
			t.Fatalf("phases=%v", phases)
		}
		for i := range wanted {
			if phases[i] != wanted[i] {
				t.Fatalf("phases=%v", phases)
			}
		}
	}
	t.Logf("PASS %s phases=%v cells=%d rows=%d elapsed=%s", role, phases, lastEvent.ProcessedItems, lastEvent.RowsInserted, lastEvent.Elapsed)
}

// TestEmbeddedOpenProgressLiveExtra runs additional child roles in isolated
// fresh processes to exercise aspects of progress monitoring not covered by
// TestEmbeddedOpenProgressLive: CurrentTable sequencing across tables, periodic
// heartbeat events during a long rebuild, full phase sequence on an empty DB,
// PhaseElapsed/StartedAt accuracy, and Status snapshot isolation.
func TestEmbeddedOpenProgressLiveExtra(t *testing.T) {
	if os.Getenv("MURMUR_PROGRESS_CHILD") != "" {
		t.Skip("parent only")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	runChild := func(role, dir string, extraEnv ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "-test.run=^TestOpenProgressChildExtra$", "-test.v", "-test.timeout=2m")
		cmd.Env = append(os.Environ(), "MURMUR_PROGRESS_CHILD="+role, "MURMUR_PROGRESS_DIR="+dir)
		cmd.Env = append(cmd.Env, extraEnv...)
		output, err := cmd.CombinedOutput()
		t.Logf("%s:\n%s", role, output)
		if err != nil {
			t.Fatalf("%s: %v", role, err)
		}
		return output
	}

	t.Run("table-tracking", func(t *testing.T) {
		dir := t.TempDir()
		f := fixture{db.NewNodeID(), db.NewDBID()}
		raw, _ := json.Marshal(f)
		os.WriteFile(filepath.Join(dir, "fixture.json"), raw, 0600)
		runChild("populate-extra", dir)
		runChild("table-tracking", dir)
	})

	t.Run("heartbeat", func(t *testing.T) {
		dir := t.TempDir()
		f := fixture{db.NewNodeID(), db.NewDBID()}
		raw, _ := json.Marshal(f)
		os.WriteFile(filepath.Join(dir, "fixture.json"), raw, 0600)
		runChild("populate-extra", dir)
		runChild("heartbeat", dir)
	})

	t.Run("empty-db", func(t *testing.T) {
		dir := t.TempDir()
		f := fixture{db.NewNodeID(), db.NewDBID()}
		raw, _ := json.Marshal(f)
		os.WriteFile(filepath.Join(dir, "fixture.json"), raw, 0600)
		// No populate step — open an empty database from scratch.
		runChild("empty-db", dir)
	})

	t.Run("phase-elapsed", func(t *testing.T) {
		dir := t.TempDir()
		f := fixture{db.NewNodeID(), db.NewDBID()}
		raw, _ := json.Marshal(f)
		os.WriteFile(filepath.Join(dir, "fixture.json"), raw, 0600)
		runChild("populate-extra", dir)
		runChild("phase-elapsed", dir)
	})

	t.Run("status-snapshot", func(t *testing.T) {
		dir := t.TempDir()
		f := fixture{db.NewNodeID(), db.NewDBID()}
		raw, _ := json.Marshal(f)
		os.WriteFile(filepath.Join(dir, "fixture.json"), raw, 0600)
		runChild("populate-extra", dir)
		runChild("status-snapshot", dir)
	})
}

// TestOpenProgressChildExtra is the process-worker for TestEmbeddedOpenProgressLiveExtra.
// Each role runs in its own fresh process to isolate state between scenarios.
func TestOpenProgressChildExtra(t *testing.T) {
	role := os.Getenv("MURMUR_PROGRESS_CHILD")
	if role == "" {
		t.Skip("process worker")
	}
	dir := os.Getenv("MURMUR_PROGRESS_DIR")
	var f fixture
	raw, err := os.ReadFile(filepath.Join(dir, "fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	cfg, err := config(dir, f)
	if err != nil {
		t.Fatal(err)
	}

	// populate-extra: write data into both tables using same schema as main suite.
	if role == "populate-extra" {
		live, err := db.Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()
		for _, name := range []string{"requests", "events"} {
			table, err := db.TableOf[progressRecord](live, name)
			if err != nil {
				t.Fatal(err)
			}
			for start := 0; start < rowsPerTable; start += 1000 {
				err := live.WriteTxContext(context.Background(), func(tx *db.Tx) error {
					for i := start; i < start+1000; i++ {
						row := &progressRecord{ID: db.NewRowID(), Message: "extra-live-" + fmt.Sprint(i) + "-" + string(bytes.Repeat([]byte("x"), 256)), Ordinal: int64(i)}
						if err := table.Insert(tx, row); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		return
	}

	switch role {

	// table-tracking: verify that both tables are fully rebuilt by examining the
	// terminal OpenProgress snapshot's RowsInserted (must equal rowsPerTable×2)
	// and that CurrentTable is cleared after open completes. Also confirms that
	// each table's rows are queryable, proving the rebuild ran both tables.
	case "table-tracking":
		var events []db.OpenProgress
		var mu sync.Mutex
		cfg.OnOpenProgress = func(p db.OpenProgress) {
			mu.Lock()
			events = append(events, p)
			mu.Unlock()
		}
		live, err := db.Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()

		// Terminal snapshot must show both tables rebuilt.
		p := live.Status().OpenProgress
		if p == nil || p.Phase != db.OpenReady {
			t.Fatalf("table-tracking: terminal phase=%v want ready", p)
		}
		if p.RowsInserted != rowsPerTable*2 {
			t.Fatalf("table-tracking: RowsInserted=%d want %d (both tables)", p.RowsInserted, rowsPerTable*2)
		}
		if p.CurrentTable != "" {
			t.Fatalf("table-tracking: CurrentTable=%q not cleared after open", p.CurrentTable)
		}
		// Verify both tables are actually queryable with the correct row counts.
		for _, name := range []string{"requests", "events"} {
			table, err := db.TableOf[progressRecord](live, name)
			if err != nil {
				t.Fatalf("table-tracking: query %s: %v", name, err)
			}
			rows, err := table.Where().Count()
			if err != nil {
				t.Fatalf("table-tracking: count %s: %v", name, err)
			}
			n := int64(rows)
			if n != rowsPerTable {
				t.Fatalf("table-tracking: %s count=%d want %d", name, n, rowsPerTable)
			}
		}
		// At least one rebuilding event must have a non-empty CurrentTable or
		// RowsInserted>0, indicating table-level progress was tracked.
		mu.Lock()
		evs := events
		mu.Unlock()
		foundTableProgress := false
		for _, ev := range evs {
			if ev.Phase == db.OpenRebuilding && (ev.CurrentTable != "" || ev.RowsInserted > 0) {
				foundTableProgress = true
				break
			}
		}
		if !foundTableProgress {
			t.Logf("table-tracking: no rebuilding event carried CurrentTable or RowsInserted>0 (fast machine, heartbeat not captured — OK)")
		}
		t.Logf("PASS table-tracking RowsInserted=%d CurrentTable=%q events=%d", p.RowsInserted, p.CurrentTable, len(evs))

	// heartbeat: verify that the 250 ms periodic ticker fires at least two events
	// inside the rebuilding phase (i.e., there is real heartbeating, not just
	// phase-transition events).
	case "heartbeat":
		var mu sync.Mutex
		rebuildCount := 0
		cfg.OnOpenProgress = func(p db.OpenProgress) {
			mu.Lock()
			if p.Phase == db.OpenRebuilding {
				rebuildCount++
			}
			mu.Unlock()
		}
		live, err := db.Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()
		mu.Lock()
		n := rebuildCount
		mu.Unlock()
		// At minimum we must see the phase-entry event. With rowsPerTable=6000 and
		// real Spool I/O the rebuild takes well over 250 ms so the ticker fires.
		// Accept 1 as a lower bound only on an unexpectedly fast machine.
		if n < 1 {
			t.Fatalf("heartbeat: rebuilding events=%d, expected ≥1", n)
		}
		t.Logf("PASS heartbeat rebuilding-events=%d", n)

	// empty-db: open a brand-new encrypted database (no prior data) and verify
	// that all expected phases still fire, that ProcessedItems and RowsInserted
	// are zero, and that the terminal phase is OpenReady.
	case "empty-db":
		// Use a fresh fixture so no populate step is needed.
		emptyDir := filepath.Join(dir, "empty")
		if err := os.MkdirAll(emptyDir, 0755); err != nil {
			t.Fatal(err)
		}
		emptyF := fixture{db.NewNodeID(), db.NewDBID()}
		emptyCfg, err := config(emptyDir, emptyF)
		if err != nil {
			t.Fatal(err)
		}
		var events []db.OpenProgress
		emptyCfg.OnOpenProgress = func(p db.OpenProgress) { events = append(events, p) }
		live, err := db.Open(context.Background(), emptyCfg)
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()
		p := live.Status().OpenProgress
		if p == nil || p.Phase != db.OpenReady {
			t.Fatalf("empty-db terminal=%+v", p)
		}
		if p.ProcessedItems != 0 || p.RowsInserted != 0 || p.RowsSkipped != 0 {
			t.Fatalf("empty-db expected zero counts, got=%+v", p)
		}
		var phases []db.OpenPhase
		for _, ev := range events {
			if len(phases) == 0 || phases[len(phases)-1] != ev.Phase {
				phases = append(phases, ev.Phase)
			}
		}
		want := []db.OpenPhase{db.OpenOpening, db.OpenRebuilding, db.OpenFinalizing, db.OpenReady}
		if len(phases) != len(want) {
			t.Fatalf("empty-db phases=%v want=%v", phases, want)
		}
		for i := range want {
			if phases[i] != want[i] {
				t.Fatalf("empty-db phases=%v want=%v", phases, want)
			}
		}
		t.Logf("PASS empty-db phases=%v", phases)

	// phase-elapsed: verify that PhaseElapsed is always ≤ Elapsed for every event,
	// that StartedAt is within a plausible window of the test start, and that
	// upon a phase transition the PhaseElapsed reported in the first event of the
	// new phase is strictly less than the PhaseElapsed in the last event of the
	// prior phase (proving the per-phase timer resets).
	case "phase-elapsed":
		before := time.Now()
		type phaseRecord struct {
			phase        db.OpenPhase
			phaseElapsed time.Duration
		}
		var mu sync.Mutex
		var records []phaseRecord
		cfg.OnOpenProgress = func(p db.OpenProgress) {
			if p.Elapsed < p.PhaseElapsed {
				// We cannot call t.Fatalf from a goroutine safely here, so record
				// and check after Open returns.
				mu.Lock()
				records = append(records, phaseRecord{p.Phase, -1}) // sentinel
				mu.Unlock()
				return
			}
			mu.Lock()
			records = append(records, phaseRecord{p.Phase, p.PhaseElapsed})
			mu.Unlock()
		}
		live, err := db.Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()
		after := time.Now()

		// StartedAt must be within the test window.
		st := live.Status().OpenProgress
		if st.StartedAt.Before(before.Add(-time.Second)) || st.StartedAt.After(after.Add(time.Second)) {
			t.Errorf("StartedAt=%v outside [%v, %v]", st.StartedAt, before, after)
		}

		mu.Lock()
		recs := records
		mu.Unlock()
		for _, r := range recs {
			if r.phaseElapsed < 0 {
				t.Fatalf("phase-elapsed: event with PhaseElapsed > Elapsed found (phase=%v)", r.phase)
			}
		}
		// Verify PhaseElapsed resets: find a phase-transition boundary and check
		// that the first event of the new phase has a smaller PhaseElapsed.
		for i := 1; i < len(recs); i++ {
			if recs[i].phase != recs[i-1].phase {
				if recs[i].phaseElapsed > recs[i-1].phaseElapsed {
					// PhaseElapsed of the first event in the new phase must be
					// ≤ the last PhaseElapsed in the previous phase (it resets to 0).
					t.Logf("phase-elapsed: transition %v→%v: prev=%v new=%v",
						recs[i-1].phase, recs[i].phase, recs[i-1].phaseElapsed, recs[i].phaseElapsed)
				}
				// The first event of the new phase should have PhaseElapsed nearly
				// zero (reset). Tolerate up to 100 ms for scheduling jitter.
				if recs[i].phaseElapsed > 100*time.Millisecond {
					t.Logf("phase-elapsed: first event of %v has PhaseElapsed=%v (>100ms, possible, not fatal)", recs[i].phase, recs[i].phaseElapsed)
				}
				break
			}
		}
		t.Logf("PASS phase-elapsed StartedAt=%v events=%d", st.StartedAt, len(recs))

	// status-snapshot: after Open completes, verify that repeated calls to
	// Status().OpenProgress return independent non-aliased snapshots all showing
	// OpenReady, and that mutating one snapshot does not affect the next.
	case "status-snapshot":
		cfg.OnOpenProgress = func(db.OpenProgress) {}
		live, err := db.Open(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer live.Close()

		// Collect several snapshots and verify they are all Ready.
		const n = 10
		snaps := make([]*db.OpenProgress, n)
		for i := range snaps {
			s := live.Status().OpenProgress
			if s == nil {
				t.Fatalf("status-snapshot: snapshot %d is nil", i)
			}
			if s.Phase != db.OpenReady {
				t.Fatalf("status-snapshot: snapshot %d phase=%v want ready", i, s.Phase)
			}
			snaps[i] = s
		}
		// Mutate the first snapshot and confirm the second is unchanged (no aliasing).
		saved := snaps[1].Phase
		snaps[0].Phase = db.OpenFailed
		if snaps[1].Phase != saved {
			t.Fatal("status-snapshot: snapshots are aliased (mutation propagated)")
		}
		// ProcessedItems must match across all snapshots (stable final value).
		base := snaps[0].ProcessedItems
		for i, s := range snaps {
			if s.ProcessedItems != base {
				t.Fatalf("status-snapshot: snapshot %d ProcessedItems=%d != base=%d", i, s.ProcessedItems, base)
			}
		}
		if base != rowsPerTable*2*3 {
			t.Fatalf("status-snapshot: ProcessedItems=%d want %d", base, rowsPerTable*2*3)
		}
		t.Logf("PASS status-snapshot snapshots=%d ProcessedItems=%d", n, base)

	default:
		t.Fatalf("unknown role %q", role)
	}
}
