package replicateddb

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Writer scheduling: local and replication writer-time shares with idle
// borrowing, bounded service debt, and cancellation-aware admission
// (architecture/synchronization-and-overload.md, "Local and replication
// write scheduling").
//
// One coordinator guards admission to every state-mutation section, and
// admission always precedes the conflicting SQL/state locks:
//
//	Class            Sections
//	WriterLocal      BeginTx (whole transaction, SQL plus durable commit)
//	WriterRemote     ApplyRemote, ApplySnapshotChunk, SyncSchemas
//	WriterMaintenance Migrate, RewriteEncryptedFiles, log/receipt GC units
//
// Open-time rebuild and Close drains predate/follow service and use the
// locks directly. Nested admission never happens: sections admitted once
// run their whole lock nest under that ticket.
//
// The coordinator grants exclusive admission (active transactions are never
// preempted) ordered by virtual-time fair queueing over measured service
// time: the waiting class with the least serviceReceived/share is granted
// next, so sustained contention converges on the configured shares. Service
// time runs from grant to release (queue wait excluded). When only one
// class waits it is granted immediately (idle borrowing), and normalized
// service is clamped to a bounded debt window so idle time never accrues
// unlimited future credit. Maintenance is granted only when no interactive
// class waits. Waiters honor context cancellation and scheduler shutdown.

// WriterClass identifies a writer scheduling class.
type WriterClass uint8

const (
	// WriterLocal covers local user transactions.
	WriterLocal WriterClass = iota
	// WriterRemote covers mesh/High-Low remote apply and schema adoption.
	WriterRemote
	// WriterMaintenance covers migrations, rewrites, and GC units.
	WriterMaintenance
)

// String names the class for diagnostics.
func (c WriterClass) String() string {
	switch c {
	case WriterLocal:
		return "local"
	case WriterRemote:
		return "remote"
	case WriterMaintenance:
		return "maintenance"
	default:
		return "unknown"
	}
}

// WriterSchedulingConfig configures writer-time shares. All-zero resolves
// to the 90/10 defaults; otherwise both shares must be positive and total
// 100. MaxDebt bounds normalized service debt; zero resolves to 1s.
type WriterSchedulingConfig struct {
	LocalShare  int
	RemoteShare int
	MaxDebt     time.Duration
}

// DefaultWriterSchedulingConfig returns the 90/10 shares with a 1s debt bound.
func DefaultWriterSchedulingConfig() WriterSchedulingConfig {
	return WriterSchedulingConfig{LocalShare: 90, RemoteShare: 10, MaxDebt: time.Second}
}

func (c *WriterSchedulingConfig) withDefaults() {
	if c.LocalShare == 0 && c.RemoteShare == 0 {
		c.LocalShare, c.RemoteShare = 90, 10
	}
	if c.MaxDebt <= 0 {
		c.MaxDebt = time.Second
	}
}

func (c WriterSchedulingConfig) validate() error {
	if c.LocalShare <= 0 || c.RemoteShare <= 0 {
		return fmt.Errorf("replicateddb: writer shares must be positive (got %d/%d)", c.LocalShare, c.RemoteShare)
	}
	if c.LocalShare+c.RemoteShare != 100 {
		return fmt.Errorf("replicateddb: writer shares must total 100 (got %d/%d)", c.LocalShare, c.RemoteShare)
	}
	if c.MaxDebt <= 0 {
		return fmt.Errorf("replicateddb: writer max debt must be positive")
	}
	return nil
}

// Ticket is one granted writer admission. It must be released exactly once;
// service time runs from grant to Release.
type Ticket struct {
	class     WriterClass
	grantedAt time.Time
	sched     *writerScheduler
	released  bool
	// dual marks tickets granted while the other interactive class had
	// waiters: the scheduler chose between backlogged classes, so the
	// share policy binds and the service counts as dual contention.
	dual bool
}

// Class reports the ticket's scheduling class.
func (t *Ticket) Class() WriterClass { return t.class }

// Release returns the ticket, recording its service time.
func (t *Ticket) Release() {
	t.sched.release(t)
}

type schedWaiter struct {
	class   WriterClass
	grant   chan struct{}
	granted bool
	dual    bool
	result  error
	enqueue time.Time
}

// writerScheduler is the fair exclusive writer-admission coordinator. The
// zero value is unusable; construct with newWriterScheduler.
type writerScheduler struct {
	mu        sync.Mutex
	localW    uint64
	remoteW   uint64
	maxDebtNs uint64
	closed    bool

	active *schedWaiter
	queues map[WriterClass][]*schedWaiter

	// Diagnostics (all guarded by mu).
	acquisitions [3]uint64
	cancels      [3]uint64
	waitNanos    [3]uint64
	serviceNanos [3]uint64
	// dualServiceNanos is service granted while the other interactive
	// class had waiters (the share policy bound the decision).
	dualServiceNanos [3]uint64
	service          [2]uint64 // normalized-debt accounting: local, remote
}

func newWriterScheduler(cfg WriterSchedulingConfig) *writerScheduler {
	return &writerScheduler{
		localW:    uint64(cfg.LocalShare),
		remoteW:   uint64(cfg.RemoteShare),
		maxDebtNs: uint64(cfg.MaxDebt.Nanoseconds()),
		queues:    make(map[WriterClass][]*schedWaiter),
	}
}

// Admit queues for writer admission, granting in class-share order. It
// honors ctx cancellation while waiting and scheduler shutdown.
func (s *writerScheduler) Admit(ctx context.Context, class WriterClass) (*Ticket, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	w := &schedWaiter{class: class, grant: make(chan struct{}), enqueue: time.Now()}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	s.queues[class] = append(s.queues[class], w)
	s.grantNextLocked()
	for !w.granted && w.result == nil {
		s.mu.Unlock()
		select {
		case <-w.grant:
			s.mu.Lock()
		case <-ctx.Done():
			s.mu.Lock()
			if !w.granted && w.result == nil {
				s.dequeueLocked(w)
				s.cancels[class]++
				s.mu.Unlock()
				return nil, ctx.Err()
			}
			// Granted (or closed) concurrently: the grant wins and
			// the loop below serves it normally.
		}
	}
	if w.result != nil {
		s.mu.Unlock()
		return nil, w.result
	}
	// Granted (possibly racing a concurrent cancellation: the grant wins
	// and the ticket must be used and released normally).
	t := &Ticket{class: class, grantedAt: time.Now(), sched: s, dual: w.dual}
	s.mu.Unlock()
	return t, nil
}

// Active reports whether a ticket is currently held (for tests).
func (s *writerScheduler) Active() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active != nil
}

// Close wakes all waiters with ErrClosed. The active holder (if any) runs
// to completion; further admissions fail.
func (s *writerScheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for class, q := range s.queues {
		for _, w := range q {
			w.result = ErrClosed
			close(w.grant)
		}
		s.queues[class] = nil
	}
}

func (s *writerScheduler) release(t *Ticket) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.released {
		return
	}
	t.released = true
	service := uint64(now.Sub(t.grantedAt).Nanoseconds())
	s.serviceNanos[t.class] += service
	if t.dual {
		s.dualServiceNanos[t.class] += service
	}
	if t.class == WriterLocal {
		s.service[0] += service
	} else if t.class == WriterRemote {
		s.service[1] += service
	}
	s.clampDebtLocked()
	s.active = nil
	s.grantNextLocked()
}

// grantNextLocked grants the next waiter under the share policy. Maintenance
// runs only with no interactive waiters; among waiting interactive classes
// the least normalized service wins (ties favor local).
func (s *writerScheduler) grantNextLocked() {
	if s.closed || s.active != nil {
		return
	}
	localQ := len(s.queues[WriterLocal]) > 0
	remoteQ := len(s.queues[WriterRemote]) > 0
	var class WriterClass
	switch {
	case localQ && remoteQ:
		// Compare S_local/w_local <= S_remote/w_remote without floats.
		if s.service[0]*s.remoteW <= s.service[1]*s.localW {
			class = WriterLocal
		} else {
			class = WriterRemote
		}
	case localQ:
		class = WriterLocal
	case remoteQ:
		class = WriterRemote
	default:
		if len(s.queues[WriterMaintenance]) == 0 {
			return
		}
		class = WriterMaintenance
	}
	w := s.queues[class][0]
	s.queues[class] = s.queues[class][1:]
	w.granted = true
	// Dual contention: the grant chose between backlogged interactive
	// classes, so the share policy bound the decision. Maintenance is
	// granted only with no interactive waiters, hence never dual.
	w.dual = (class == WriterLocal && remoteQ) || (class == WriterRemote && localQ)
	s.active = w
	s.acquisitions[class]++
	s.waitNanos[class] += uint64(time.Since(w.enqueue).Nanoseconds())
	close(w.grant)
}

func (s *writerScheduler) dequeueLocked(target *schedWaiter) {
	q := s.queues[target.class]
	for i, w := range q {
		if w == target {
			s.queues[target.class] = append(q[:i], q[i+1:]...)
			return
		}
	}
}

// clampDebtLocked bounds normalized service debt so an idle class never
// accrues unlimited future credit.
func (s *writerScheduler) clampDebtLocked() {
	// Normalized service V_c = S_c / w_c; pull the ahead class back to
	// V_min + maxDebt.
	v0 := s.service[0] / s.localW
	v1 := s.service[1] / s.remoteW
	if v0 > v1+s.maxDebtNs {
		s.service[0] = (v1 + s.maxDebtNs) * s.localW
	} else if v1 > v0+s.maxDebtNs {
		s.service[1] = (v0 + s.maxDebtNs) * s.remoteW
	}
}

// SchedulerClassStats is one class's diagnostics.
type SchedulerClassStats struct {
	Acquisitions uint64
	Cancels      uint64
	WaitNanos    uint64
	ServiceNanos uint64
	// DualServiceNanos is service granted while the other interactive
	// class had waiters (the share policy bound the decision).
	DualServiceNanos uint64
	Waiters          int
	OldestWaitNanos  uint64
}

// SchedulerSnapshot is the coordinator diagnostics snapshot.
type SchedulerSnapshot struct {
	Local       SchedulerClassStats
	Remote      SchedulerClassStats
	Maintenance SchedulerClassStats
	// DebtNanos is the current normalized service spread between the
	// interactive classes (bounded by MaxDebt).
	DebtNanos uint64
	Shares    [2]int
}

// Snapshot copies the current diagnostics.
func (s *writerScheduler) Snapshot() SchedulerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	stat := func(class WriterClass) SchedulerClassStats {
		st := SchedulerClassStats{
			Acquisitions:     s.acquisitions[class],
			Cancels:          s.cancels[class],
			WaitNanos:        s.waitNanos[class],
			ServiceNanos:     s.serviceNanos[class],
			DualServiceNanos: s.dualServiceNanos[class],
			Waiters:          len(s.queues[class]),
		}
		for _, w := range s.queues[class] {
			if age := uint64(now.Sub(w.enqueue).Nanoseconds()); age > st.OldestWaitNanos {
				st.OldestWaitNanos = age
			}
		}
		return st
	}
	v0 := s.service[0] / s.localW
	v1 := s.service[1] / s.remoteW
	debt := v0 - v1
	if v1 > v0 {
		debt = v1 - v0
	}
	return SchedulerSnapshot{
		Local:       stat(WriterLocal),
		Remote:      stat(WriterRemote),
		Maintenance: stat(WriterMaintenance),
		DebtNanos:   debt,
		Shares:      [2]int{int(s.localW), int(s.remoteW)},
	}
}
