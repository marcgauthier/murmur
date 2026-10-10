package murmur

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
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
// TestSchedulerDualServiceAttribution proves only service granted while
// the other interactive class waits counts as dual: a solo grant never
// does, a grant chosen between backlogged classes always does, and the
// grant after the other queue drains does not.
func TestSchedulerDualServiceAttribution(t *testing.T) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()

	l1, err := s.Admit(ctx, WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	admitAsync := func(class WriterClass) <-chan *Ticket {
		ch := make(chan *Ticket, 1)
		go func() {
			tk, err := s.Admit(ctx, class)
			if err != nil {
				return
			}
			ch <- tk
		}()
		return ch
	}
	r1ch := admitAsync(WriterRemote)
	waitWaiters := func(class WriterClass, want int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if got := s.Snapshot(); (class == WriterLocal && got.Local.Waiters >= want) ||
				(class == WriterRemote && got.Remote.Waiters >= want) {
				return
			}
			if time.Now().After(deadline) {
				l1.Release()
				s.Close()
				t.Fatalf("class %v waiters did not reach %d", class, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitWaiters(WriterRemote, 1)
	l2ch := admitAsync(WriterLocal)
	waitWaiters(WriterLocal, 1)

	// Both classes wait and local already accrued L1's service, so
	// releasing L1 grants R1 (least normalized service) with the dual
	// mark; L2 follows once the remote queue drains (not dual), and the
	// solo L1 grant was never dual either.
	l1.Release()
	select {
	case r1 := <-r1ch:
		time.Sleep(5 * time.Millisecond)
		r1.Release()
	case <-time.After(5 * time.Second):
		s.Close()
		t.Fatal("R1 never granted")
	}
	select {
	case l2 := <-l2ch:
		l2.Release()
	case <-time.After(5 * time.Second):
		s.Close()
		t.Fatal("L2 never granted")
	}
	st := s.Snapshot()
	if st.Remote.DualServiceNanos == 0 {
		t.Fatal("remote grant between backlogged classes recorded no dual service")
	}
	if st.Local.DualServiceNanos != 0 {
		t.Fatalf("local grants with empty remote queue recorded %d dual nanos", st.Local.DualServiceNanos)
	}
	if st.Local.ServiceNanos == 0 {
		t.Fatal("local service unrecorded")
	}
}

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

// TestSchedulerMaintenanceReserve proves the anti-starvation floor: with
// local saturated, a waiting maintenance ticket is granted after exactly
// maintenanceReserveEvery interactive grants, not deferred forever.
func TestSchedulerMaintenanceReserve(t *testing.T) {
	s := newWriterScheduler(DefaultWriterSchedulingConfig())
	ctx := context.Background()
	admitAsync := func(class WriterClass) <-chan *Ticket {
		ch := make(chan *Ticket, 1)
		go func() {
			tk, err := s.Admit(ctx, class)
			if err != nil {
				return
			}
			ch <- tk
		}()
		return ch
	}
	waitLocal := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if s.Snapshot().Local.Waiters >= 1 {
				return
			}
			if time.Now().After(deadline) {
				s.Close()
				t.Fatal("local waiter never queued")
			}
			time.Sleep(time.Millisecond)
		}
	}
	holder, err := s.Admit(ctx, WriterLocal)
	if err != nil {
		t.Fatal(err)
	}
	mch := admitAsync(WriterMaintenance)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if s.Snapshot().Maintenance.Waiters >= 1 {
			break
		}
		if time.Now().After(deadline) {
			holder.Release()
			s.Close()
			t.Fatal("maintenance waiter never queued")
		}
		time.Sleep(time.Millisecond)
	}
	// The initial grant counts, so 99 chained local grants bring the
	// reserve counter to exactly 100 with local still saturated.
	for i := 0; i < maintenanceReserveEvery-1; i++ {
		next := admitAsync(WriterLocal)
		waitLocal()
		holder.Release()
		select {
		case holder = <-next:
		case <-time.After(5 * time.Second):
			s.Close()
			t.Fatal("chained local grant lost")
		}
	}
	// Local still holds and maintenance still waits: the next decision
	// must divert to maintenance by the reserve.
	holder.Release()
	select {
	case tk := <-mch:
		tk.Release()
	case <-time.After(5 * time.Second):
		s.Close()
		t.Fatal("maintenance never granted after 100 saturated interactive grants")
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
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	acq := func() SchedulerSnapshot { return db.Metrics().Scheduler }

	if err := insertRecord(ctx, db, table, &facadeRecord{ID: NewRowID(), Name: "ann"}); err != nil {
		t.Fatal(err)
	}
	if got := acq().Local.Acquisitions; got != 1 {
		t.Fatalf("local acquisitions = %d, want 1", got)
	}

	// Remote apply and schema sync share the remote class (errors ignored:
	// admission precedes the work).
	_ = applyRemoteFixture(db, ctx, &codec.MutationBatch{OriginNode: NewNodeID(), Sequence: 1})
	_ = db.SyncSchemas(ctx, nil)
	if got := acq().Remote.Acquisitions; got != 2 {
		t.Fatalf("remote acquisitions = %d, want 2", got)
	}

	_ = db.MigrateRecords(ctx, []TableDefinition{recordDefinitionV2(t)})
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
