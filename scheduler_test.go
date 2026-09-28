package replicateddb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/codec"
)

// holdWorker admits repeatedly, holding each ticket for service time, until
// stop closes. It reports completion on done.
func holdWorker(ctx context.Context, s *writerScheduler, class WriterClass, service time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()
	for {
		select {
		case <-stop:
			return
		default:
		}
		t, err := s.Admit(ctx, class)
		if err != nil {
			return
		}
		time.Sleep(service)
		t.Release()
	}
}

func runWorkers(ctx context.Context, t *testing.T, s *writerScheduler, class WriterClass, n int, service, window time.Duration) {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go holdWorker(ctx, s, class, service, stop, done)
	}
	time.Sleep(window)
	close(stop)
	for i := 0; i < n; i++ {
		<-done
	}
}

// TestWriterSharesConverge is the scheduling acceptance test: sustained
// local plus remote contention approaches the configured 90/10 service
// shares over the measurement window.
func TestWriterSharesConverge(t *testing.T) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()
	stop := make(chan struct{})
	done := make(chan struct{}, 8)
	for i := 0; i < 4; i++ {
		go holdWorker(ctx, s, WriterLocal, 2*time.Millisecond, stop, done)
		go holdWorker(ctx, s, WriterRemote, 2*time.Millisecond, stop, done)
	}
	time.Sleep(2 * time.Second)
	close(stop)
	for i := 0; i < 8; i++ {
		<-done
	}
	snap := s.Snapshot()
	local, remote := snap.Local.ServiceNanos, snap.Remote.ServiceNanos
	total := local + remote
	if total == 0 {
		t.Fatal("no service recorded")
	}
	share := float64(local) / float64(total)
	// 90/10 target with generous tolerance for scheduling jitter.
	if share < 0.80 || share > 0.97 {
		t.Fatalf("local share = %.3f (local=%d remote=%d)", share, local, remote)
	}
	if snap.Local.Acquisitions == 0 || snap.Remote.Acquisitions == 0 {
		t.Fatalf("acquisitions = %+v", snap)
	}
	if snap.DebtNanos > uint64(time.Second.Nanoseconds()) {
		t.Fatalf("debt = %d ns, exceeds bound", snap.DebtNanos)
	}
}

// TestSchedulerIdleBorrowing proves either class uses full capacity when
// the other is idle.
func TestSchedulerIdleBorrowing(t *testing.T) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()
	runWorkers(ctx, t, s, WriterRemote, 2, time.Millisecond, 500*time.Millisecond)
	snap := s.Snapshot()
	if snap.Remote.ServiceNanos < uint64((400 * time.Millisecond).Nanoseconds()) {
		t.Fatalf("remote-only service = %d ns, want ~500ms", snap.Remote.ServiceNanos)
	}
	if snap.Local.Acquisitions != 0 {
		t.Fatalf("local acquisitions = %d", snap.Local.Acquisitions)
	}
}

// TestSchedulerMaintenanceDefers proves maintenance waits while interactive
// classes are busy and runs once they drain.
func TestSchedulerMaintenanceDefers(t *testing.T) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()
	stop := make(chan struct{})
	done := make(chan struct{}, 2)
	// Two workers so interactive demand never drains between holds.
	for i := 0; i < 2; i++ {
		go holdWorker(ctx, s, WriterLocal, 5*time.Millisecond, stop, done)
	}
	// Wait until saturated: one holder plus one queued waiter.
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap := s.Snapshot()
		if s.Active() && snap.Local.Waiters >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("local workers never saturated")
		}
		time.Sleep(time.Millisecond)
	}

	// Maintenance admission must not jump the busy interactive queue: with
	// a continuous local holder it stays queued.
	granted := make(chan *Ticket, 1)
	go func() {
		tk, err := s.Admit(ctx, WriterMaintenance)
		if err != nil {
			return
		}
		granted <- tk
	}()
	select {
	case <-granted:
		t.Fatal("maintenance granted while local saturated")
	case <-time.After(100 * time.Millisecond):
	}
	if got := s.Snapshot().Maintenance.Waiters; got != 1 {
		t.Fatalf("maintenance waiters = %d, want 1", got)
	}
	// Draining local lets maintenance through.
	close(stop)
	<-done
	<-done
	select {
	case tk := <-granted:
		tk.Release()
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance never granted after drain")
	}
	if got := s.Snapshot().Maintenance.Acquisitions; got != 1 {
		t.Fatalf("maintenance acquisitions = %d, want 1", got)
	}
}

// TestSchedulerCancellation proves waiters honor context cancellation and
// the queue keeps serving the rest.
func TestSchedulerCancellation(t *testing.T) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()
	holder, err := s.Admit(ctx, WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()

	ctxCancel, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Admit(ctxCancel, WriterRemote)
		errCh <- err
	}()
	// Wait until queued, then cancel.
	deadline := time.Now().Add(5 * time.Second)
	for s.Snapshot().Remote.Waiters == 0 {
		if time.Now().After(deadline) {
			t.Fatal("waiter never queued")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled waiter never returned")
	}
	if got := s.Snapshot().Remote.Cancels; got != 1 {
		t.Fatalf("cancels = %d, want 1", got)
	}
	// The queue still serves a fresh waiter after the holder releases.
	holder.Release()
	tk, err := s.Admit(ctx, WriterRemote)
	if err != nil {
		t.Fatal(err)
	}
	tk.Release()
}

// TestSchedulerClose proves shutdown wakes waiters with ErrClosed, fails
// new admissions, and lets the active holder finish.
func TestSchedulerClose(t *testing.T) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()
	holder, err := s.Admit(ctx, WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Admit(ctx, WriterRemote)
		errCh <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for s.Snapshot().Remote.Waiters == 0 {
		if time.Now().After(deadline) {
			t.Fatal("waiter never queued")
		}
		time.Sleep(time.Millisecond)
	}
	s.Close()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("close err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never woken by close")
	}
	if _, err := s.Admit(ctx, WriterLocal); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close admit err = %v", err)
	}
	holder.Release() // active holder completes normally
	if s.Active() {
		t.Fatal("scheduler still active after release")
	}
}

// TestSchedulerConfigValidation proves share rules.
func TestSchedulerConfigValidation(t *testing.T) {
	cfg := DefaultWriterSchedulingConfig()
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	var zero WriterSchedulingConfig
	zero.withDefaults()
	if zero != cfg {
		t.Fatalf("defaults = %+v, want %+v", zero, cfg)
	}
	for _, bad := range []WriterSchedulingConfig{
		{LocalShare: 0, RemoteShare: 100, MaxDebt: time.Second},
		{LocalShare: 50, RemoteShare: 40, MaxDebt: time.Second},
		{LocalShare: 90, RemoteShare: 10, MaxDebt: 0},
	} {
		if err := bad.validate(); err == nil {
			t.Fatalf("%+v validated", bad)
		}
	}
	var empty Config
	if err := empty.Scheduling.validate(); err == nil {
		t.Fatal("zero config validated without defaults")
	}
}

// TestSchedulerExclusion proves tickets are exclusive under concurrency.
func TestSchedulerExclusion(t *testing.T) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()
	var mu sync.Mutex
	held := 0
	maxHeld := 0
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			class := WriterLocal
			if i%2 == 0 {
				class = WriterRemote
			}
			for j := 0; j < 25; j++ {
				tk, err := s.Admit(ctx, class)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				held++
				if held > maxHeld {
					maxHeld = held
				}
				mu.Unlock()
				time.Sleep(time.Millisecond)
				mu.Lock()
				held--
				mu.Unlock()
				tk.Release()
			}
		}(i)
	}
	wg.Wait()
	if maxHeld != 1 {
		t.Fatalf("max concurrent holders = %d, want 1", maxHeld)
	}
}

// TestCoordinatorCoversMutationPaths proves every state-mutation section
// admits through the coordinator under its class.
func TestCoordinatorCoversMutationPaths(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	acq := func() SchedulerSnapshot { return db.Metrics().Scheduler }

	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
		t.Fatal(err)
	}
	if got := acq().Local.Acquisitions; got != 1 {
		t.Fatalf("local acquisitions = %d, want 1", got)
	}

	// Remote apply and schema sync share the remote class (errors ignored:
	// admission precedes the work).
	_ = db.ApplyRemote(ctx, &codec.MutationBatch{OriginNode: NewNodeID(), Sequence: 1})
	_ = db.SyncSchemas(ctx, nil)
	if got := acq().Remote.Acquisitions; got != 2 {
		t.Fatalf("remote acquisitions = %d, want 2", got)
	}

	_ = db.Migrate(ctx, migrateTestTables())
	db.gcOnce(false)
	if got := acq().Maintenance.Acquisitions; got < 2 {
		t.Fatalf("maintenance acquisitions = %d, want >= 2 (migrate + GC)", got)
	}

	// The same snapshot flows through Status.
	if got := db.Status().Metrics.Scheduler.Local.Acquisitions; got != 1 {
		t.Fatalf("status local acquisitions = %d, want 1", got)
	}
}

// BenchmarkSchedulerAdmitRelease measures uncontended admission overhead
// (the per-transaction cost against the unscheduled mutex baseline).
func BenchmarkSchedulerAdmitRelease(b *testing.B) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk, err := s.Admit(ctx, WriterLocal)
		if err != nil {
			b.Fatal(err)
		}
		tk.Release()
	}
}
