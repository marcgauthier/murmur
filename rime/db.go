package rime

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultShardCount is the default number of shards per table.
const DefaultShardCount = 64

// Option configures a DB.
type Option func(*dbConfig)

type dbConfig struct {
	shardCount   int
	maxResults   int
	maxScan      int
	maxMutations int
	maxTxAge     time.Duration
	gcInterval   time.Duration
	eventQueue   int
	eventDropOld bool
	enforceFK    bool
}

func defaultConfig() dbConfig {
	return dbConfig{
		shardCount: DefaultShardCount,
		eventQueue: 1024,
	}
}

// WithShardCount sets the default shard count for tables registered without
// an explicit WithTableShards option.
func WithShardCount(n int) Option {
	return func(c *dbConfig) {
		if n > 0 {
			c.shardCount = n
		}
	}
}

// WithMaxResults caps the number of records a single query may return.
// Zero means unlimited.
func WithMaxResults(n int) Option {
	return func(c *dbConfig) { c.maxResults = n }
}

// WithMaxScan caps the number of records a single query may scan.
// Zero means unlimited.
func WithMaxScan(n int) Option {
	return func(c *dbConfig) { c.maxScan = n }
}

// WithMaxMutations caps buffered writes per write transaction.
// Zero means unlimited.
func WithMaxMutations(n int) Option {
	return func(c *dbConfig) { c.maxMutations = n }
}

// WithMaxTxAge bounds how long a transaction may stay open; older snapshots
// pin MVCC GC. Zero means unlimited. Enforcement happens at commit and in GC.
func WithMaxTxAge(d time.Duration) Option {
	return func(c *dbConfig) { c.maxTxAge = d }
}

// WithGCInterval enables periodic background GC. Zero (default) disables it;
// call GC manually instead.
func WithGCInterval(d time.Duration) Option {
	return func(c *dbConfig) { c.gcInterval = d }
}

// WithEventQueueSize sets the async event queue depth (default 1024).
func WithEventQueueSize(n int) Option {
	return func(c *dbConfig) {
		if n > 0 {
			c.eventQueue = n
		}
	}
}

// WithEventDropOldest selects drop-oldest (true) vs block-producer (false)
// backpressure when the event queue is full. Default blocks the committer.
func WithEventDropOldest(drop bool) Option {
	return func(c *dbConfig) { c.eventDropOld = drop }
}

// WithForeignKeys enables foreign-key enforcement database-wide.
// Per-table options may override this.
func WithForeignKeys(enforce bool) Option {
	return func(c *dbConfig) { c.enforceFK = enforce }
}

// innerTable is the type-erased Table surface used by DB, Tx, and GC.
type innerTable interface {
	tableName() string
	schemaOf() *Schema
	preparedChange(key any, base TxID, old, next any, op Operation) PreparedChange
	gcOldest(ctx context.Context, oldest TxID) (reclaimed int)
	tableStats(snap TxID) TableStats
	fkTarget(field string, val any, snap TxID) bool
	runBeforeCommit(tx *Tx) error
	publishUniqueClaims(map[string]map[any]any)
	// Commit dispatch: nil-safe wrappers that honor patched per-row
	// callbacks when set and otherwise run allocation-free methods.
	checkBase(p *pendingWrite) (TxID, bool)
	releasePending(p *pendingWrite, st *commitState)
	checkAndRelease(p *pendingWrite, st *commitState) error
	validatePending(p *pendingWrite, st *commitState) error
	validateSingle(p *pendingWrite) error
	preparePendingChain(commit TxID, p *pendingWrite, previous any) any
	prepareIndexEffects(p *pendingWrite, reserve int) any
	finishPreparedIndexEffects([]*pendingWrite)
	abortPreparedIndexEffects([]*pendingWrite)
	prepareHashIndexes([]*pendingWrite) any
	installPreparedHashIndexes(any)
	prepareOrderedMaps([]*pendingWrite) any
	installPreparedOrderedMaps(any)
	prepareUniqueClaims(map[string]map[any]any) any
	installPreparedUniqueClaims(any)
	prepareShardRows(shard int, keys []any) any
	installPreparedShardRows(shard int, rows any)
	applyPending(commit TxID, p *pendingWrite) pendingEffect
	applyPendingSingle(commit TxID, p *pendingWrite) pendingEffect
	fireAfterEffect(e pendingEffect)
}

// DB is the database registry holding tables, the commit counter, and the
// active-transaction tracker that drives MVCC garbage collection.
type DB struct {
	cfg dbConfig

	mu     sync.Mutex
	tables map[string]innerTable

	commitMu      sync.Mutex                             // serializes write-transaction commits
	views         map[string]maintainedView              // guarded by commitMu
	viewsBySource map[innerTable]map[maintainedView]bool // guarded by commitMu
	viewMu        sync.RWMutex                           // coordinates publication, snapshot registration, indexes, and GC
	retainedFloor TxID                                   // guarded by viewMu; snapshots older than this may be reclaimed
	idSeq         atomic.Uint64                          // generated-record sequence, independent of commit IDs
	clock         atomic.Uint64

	activeMu sync.Mutex
	active   map[*Tx]txRecord
	txSeq    atomic.Uint64

	gcReclaimed atomic.Uint64
	gcRuns      atomic.Uint64
	poolGets    atomic.Uint64
	poolHits    atomic.Uint64

	closed atomic.Bool
	// faulted is set when publication panics after a managed durable commit.
	// The in-memory state may then be only partially installed, so this DB is
	// permanently read/write unavailable until reopened from its durable store.
	faulted  atomic.Bool
	bgStop   chan struct{}
	bgDone   chan struct{}
	lookupMu sync.RWMutex
	byType   map[reflect.Type]innerTable
}

// New creates an empty database.
func New(opts ...Option) *DB {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	db := &DB{
		cfg:    cfg,
		tables: map[string]innerTable{},
		byType: map[reflect.Type]innerTable{},
		active: map[*Tx]txRecord{},
		bgStop: make(chan struct{}),
		bgDone: make(chan struct{}),
	}
	if cfg.gcInterval > 0 {
		go db.bgGC()
	}
	return db
}

func (db *DB) bgGC() {
	defer close(db.bgDone)
	t := time.NewTicker(db.cfg.gcInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			db.GC()
		case <-db.bgStop:
			return
		}
	}
}

// Close stops background GC and event dispatchers. Tables remain readable
// through already-open transactions.
func (db *DB) Close() {
	if db.closed.Swap(true) {
		return
	}
	db.commitMu.Lock()
	db.viewMu.Lock()
	for _, v := range db.views {
		v.release()
	}
	db.views, db.viewsBySource = nil, nil
	db.viewMu.Unlock()
	db.commitMu.Unlock()
	if db.cfg.gcInterval > 0 {
		close(db.bgStop)
		<-db.bgDone
	}
	db.mu.Lock()
	tables := make([]innerTable, 0, len(db.tables))
	for _, t := range db.tables {
		tables = append(tables, t)
	}
	db.mu.Unlock()
	for _, t := range tables {
		if ec, ok := t.(interface{ closeEvents() }); ok {
			ec.closeEvents()
		}
	}
}

// latest returns the newest committed TxID.
func (db *DB) latest() TxID { return TxID(db.clock.Load()) }

// txRecord is the tracked state of one open transaction.
type txRecord struct {
	id    uint64
	snap  TxID
	start time.Time
	write bool
}

// TxInfo describes one open transaction for leak detection.
type TxInfo struct {
	ID       uint64
	Snapshot TxID
	Age      time.Duration
	Write    bool
}

func (db *DB) track(tx *Tx) {
	tx.id = db.txSeq.Add(1)
	db.activeMu.Lock()
	db.active[tx] = txRecord{id: tx.id, snap: tx.snap, start: tx.start, write: tx.write}
	db.activeMu.Unlock()
}

func (db *DB) untrack(tx *Tx) {
	db.activeMu.Lock()
	delete(db.active, tx)
	db.activeMu.Unlock()
}

// OpenTransactions lists every currently tracked open transaction, oldest
// first. It backs transaction-leak detection and tests.
func (db *DB) OpenTransactions() []TxInfo {
	now := time.Now()
	db.activeMu.Lock()
	defer db.activeMu.Unlock()
	out := make([]TxInfo, 0, len(db.active))
	for _, r := range db.active {
		out = append(out, TxInfo{ID: r.id, Snapshot: r.snap, Age: now.Sub(r.start), Write: r.write})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Age > out[j].Age })
	return out
}

// DetectLeaks returns open transactions older than maxAge, oldest first.
// A leaked (never closed) transaction pins MVCC GC via oldestActive unless
// WithMaxTxAge excludes it; callers use this to find the culprit.
func (db *DB) DetectLeaks(maxAge time.Duration) []TxInfo {
	var out []TxInfo
	for _, ti := range db.OpenTransactions() {
		if ti.Age > maxAge {
			out = append(out, ti)
		}
	}
	return out
}

// oldestActive returns the oldest snapshot held by an open transaction, or
// the latest committed ID when none are open. Transactions older than
// maxTxAge are ignored so a leaked transaction cannot pin GC forever.
func (db *DB) oldestActive() TxID {
	latest := db.latest()
	maxAge := db.cfg.maxTxAge
	now := time.Now()
	db.activeMu.Lock()
	defer db.activeMu.Unlock()
	oldest := latest
	found := false
	for _, r := range db.active {
		if maxAge > 0 && now.Sub(r.start) > maxAge {
			continue
		}
		if !found || r.snap < oldest {
			oldest = r.snap
			found = true
		}
	}
	return oldest
}

// activeCount returns the number of tracked open transactions.
func (db *DB) activeCount() int {
	db.activeMu.Lock()
	defer db.activeMu.Unlock()
	return len(db.active)
}

// tableByName resolves a registered table for FK checks.
func (db *DB) tableByName(name string) (innerTable, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	t, ok := db.tables[name]
	return t, ok
}

// tableNames returns sorted registered table names.
func (db *DB) tableNames() []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make([]string, 0, len(db.tables))
	for n := range db.tables {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
