package replicateddb

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur/sqlengine"
)

// OpenPhase identifies startup work. Only OpenReady means queries are available.
type OpenPhase string

const (
	OpenOpening OpenPhase = "opening"
	// OpenCounting is retained for compatibility; startup no longer emits it.
	OpenCounting   OpenPhase = "counting"
	OpenRebuilding OpenPhase = "rebuilding"
	OpenIndexing   OpenPhase = "indexing"
	OpenFinalizing OpenPhase = "finalizing"
	OpenReady      OpenPhase = "ready"
	OpenFailed     OpenPhase = "failed"
	OpenCancelled  OpenPhase = "cancelled"
)

// OpenProgress is an immutable startup snapshot. ProcessedItems counts decoded
// cells for registered tables, including cells in deleted or incomplete rows.
// Logs, metadata and separate tombstone keys are excluded. No preliminary
// counting scan is performed; total, percentage and estimate fields are retained
// for compatibility but remain zero with their known flags false.
type OpenProgress struct {
	Phase           OpenPhase
	StartedAt       time.Time
	Elapsed         time.Duration
	PhaseElapsed    time.Duration
	CountedItems    uint64
	TotalItems      uint64
	ProcessedItems  uint64
	TotalItemsKnown bool
	PercentComplete float64
	// EstimatedRemaining remains unknown. Only OpenReady signals completion.
	EstimatedRemaining time.Duration
	EstimateKnown      bool
	CurrentTable       string
	RowsInserted       uint64
	RowsSkipped        uint64
	Error              error
}

// openProgressReporter keeps user callbacks away from storage/SQL locks. Only
// phase transitions are queued; routine updates coalesce into the latest state.
type openProgressReporter struct {
	mu           sync.Mutex
	progress     OpenProgress
	phaseStarted time.Time
	finishedAt   time.Time
	pending      []OpenProgress
	processed    atomic.Uint64
	callback     func(OpenProgress)
	wake         chan struct{}
	stop         chan struct{}
	done         chan struct{}
}

func newOpenProgressReporter(callback func(OpenProgress)) *openProgressReporter {
	if callback == nil {
		return nil
	}
	now := time.Now()
	r := &openProgressReporter{
		progress: OpenProgress{Phase: OpenOpening, StartedAt: now}, phaseStarted: now,
		callback: callback, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
	r.pending = append(r.pending, r.progress)
	go r.run()
	r.notify()
	return r
}

func (r *openProgressReporter) notify() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *openProgressReporter) snapshotLocked(now time.Time) OpenProgress {
	p := r.progress
	if !r.finishedAt.IsZero() {
		now = r.finishedAt
	}
	p.Elapsed = now.Sub(p.StartedAt)
	p.PhaseElapsed = now.Sub(r.phaseStarted)
	p.ProcessedItems = r.processed.Load()
	return p
}

func (r *openProgressReporter) snapshot() *OpenProgress {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.snapshotLocked(time.Now())
	return &p
}

func (r *openProgressReporter) phase(phase OpenPhase) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.progress.Phase != phase {
		r.progress.Phase = phase
		r.progress.CurrentTable = ""
		r.phaseStarted = time.Now()
		r.pending = append(r.pending, r.snapshotLocked(r.phaseStarted))
	}
	r.mu.Unlock()
	r.notify()
}

func (r *openProgressReporter) engineProgress(p sqlengine.RebuildProgress) {
	r.mu.Lock()
	r.progress.CurrentTable = p.CurrentTable
	r.progress.RowsInserted = p.RowsInserted
	r.progress.RowsSkipped = p.RowsSkipped
	r.mu.Unlock()
	if p.Indexing {
		r.phase(OpenIndexing)
	}
}

func (r *openProgressReporter) deliver(latest bool) {
	r.mu.Lock()
	events := r.pending
	r.pending = nil
	if latest && r.finishedAt.IsZero() {
		events = append(events, r.snapshotLocked(time.Now()))
	}
	r.mu.Unlock()
	for _, p := range events {
		r.callback(p)
	}
}

func (r *openProgressReporter) run() {
	defer close(r.done)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.wake:
			r.deliver(false)
		case <-ticker.C:
			r.deliver(true)
		case <-r.stop:
			r.deliver(false)
			return
		}
	}
}

func (r *openProgressReporter) finish(err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.progress.Error = err
	r.progress.Phase = OpenReady
	if err != nil {
		r.progress.Phase = OpenFailed
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.progress.Phase = OpenCancelled
		}
	}
	r.progress.CurrentTable = ""
	r.finishedAt = time.Now()
	r.phaseStarted = r.finishedAt
	r.pending = append(r.pending, r.snapshotLocked(r.finishedAt))
	r.mu.Unlock()
	close(r.stop)
	<-r.done // No callbacks remain when Open returns.
}

func (db *DB) rebuildOnOpen(ctx context.Context) error {
	r := db.openProgress
	if r == nil {
		return db.engine.RebuildContext(ctx, db.store, nil)
	}
	r.phase(OpenRebuilding)
	view, err := db.store.NewRebuildSnapshot(ctx, func(delta uint64) { r.processed.Add(delta) })
	if err != nil {
		return err
	}
	defer view.Close()
	if err := db.engine.RebuildContext(ctx, view, r.engineProgress); err != nil {
		return err
	}
	return view.Close()
}
