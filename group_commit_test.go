package murmur

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
)

// groupTestConfig provisions testConfig plus origin-signing identity.
func groupTestConfig(t *testing.T) Config {
	t.Helper()
	cfg := testConfig(t.TempDir())
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := origin.NewKeyRegistry(map[ids.NodeID]ed25519.PublicKey{cfg.NodeID: pub})
	if err != nil {
		t.Fatal(err)
	}
	cfg.OriginSigning = OriginSigningConfig{PrivateKey: priv, TrustedKeys: reg}
	return cfg
}

func TestGroupCommitConfigValidation(t *testing.T) {
	cfg := groupTestConfig(t)
	cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		t.Fatalf("default group commit rejected: %v", err)
	}
	if cfg.Durability.GroupCommit.MaxDelay <= 0 {
		t.Fatal("default group commit MaxDelay is not positive")
	}

	cfg.Durability.GroupCommit.MaxDelay = 50 * time.Millisecond
	cfg.Durability.GroupCommit.MaxTransactions = 16
	cfg.Durability.GroupCommit.MaxBytes = 1 << 20
	if err := cfg.validate(); err != nil {
		t.Fatalf("explicit group commit rejected: %v", err)
	}

	cfg.Durability.GroupCommit.MaxDelay = -1
	if err := cfg.validate(); err != nil {
		t.Fatalf("disabled group commit rejected: %v", err)
	}

	cfg.Durability.GroupCommit.MaxDelay = 2 * time.Second
	if err := cfg.validate(); err == nil {
		t.Fatal("overlong MaxDelay accepted")
	}
	cfg.Durability.GroupCommit.MaxDelay = time.Millisecond

	cfg.Durability.GroupCommit.MaxTransactions = 513
	if err := cfg.validate(); err == nil {
		t.Fatal("oversize MaxTransactions accepted")
	}
	cfg.Durability.GroupCommit.MaxTransactions = 64

	cfg.Durability.GroupCommit.MaxBytes = 65 << 20
	if err := cfg.validate(); err == nil {
		t.Fatal("oversize MaxBytes accepted")
	}
	cfg.Durability.GroupCommit.MaxBytes = 4 << 20

	// Group settings are validated but ignored in asynchronous mode.
	cfg.Durability.Mode = DurabilityAsync
	cfg.Durability.GroupCommit.MaxDelay = time.Millisecond
	if err := cfg.validate(); err != nil {
		t.Fatalf("async mode with group settings rejected: %v", err)
	}
}

// TestGroupCommitConcurrentWriters proves concurrent synchronous writers
// share fsyncs: every insert is acknowledged only after durability, all
// rows survive a reopen, and fewer Pebble group commits ran than
// transactions committed.
func TestGroupCommitConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	cfg := groupTestConfig(t)
	cfg.Durability.GroupCommit.MaxDelay = 20 * time.Millisecond
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	const writers = 8
	const perWriter = 25
	total := writers * perWriter
	startGate := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(writers)
	errs := make(chan error, writers)
	for worker := 0; worker < writers; worker++ {
		go func(worker int) {
			var first error
			defer func() { errs <- first }()
			ready.Done()
			<-startGate
			for i := 0; i < perWriter; i++ {
				id := ids.NewRowID()
				if _, err := db.ExecContext(ctx,
					`INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], fmt.Sprintf("w%d-%d", worker, i)); err != nil {
					first = err
					return
				}
			}
		}(worker)
	}
	ready.Wait()
	close(startGate)
	for range writers {
		if err := <-errs; err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	m := db.Metrics()
	if m.GroupCommitMembers != uint64(total) {
		_ = db.Close()
		t.Fatalf("group members = %d, want %d", m.GroupCommitMembers, total)
	}
	if m.GroupCommits == 0 || m.GroupCommits >= uint64(total) {
		_ = db.Close()
		t.Fatalf("group commits = %d for %d transactions, want shared fsyncs", m.GroupCommits, total)
	}
	if m.LocalCommits != uint64(total) {
		_ = db.Close()
		t.Fatalf("local commits = %d, want %d", m.LocalCommits, total)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM contacts`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != total {
		t.Fatalf("durable rows = %d; acknowledged inserts = %d", count, total)
	}
}

// TestGroupCommitDisabledCommitsAlone proves the escape hatch: with a
// negative MaxDelay every transaction takes the direct path and no group
// commit is recorded.
func TestGroupCommitDisabledCommitsAlone(t *testing.T) {
	ctx := context.Background()
	cfg := groupTestConfig(t)
	cfg.Durability.GroupCommit.MaxDelay = -1
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.groupCommitEnabled() {
		t.Fatal("group commit enabled despite negative MaxDelay")
	}
	id := ids.NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "solo"); err != nil {
		t.Fatal(err)
	}
	m := db.Metrics()
	if m.GroupCommits != 0 || m.GroupCommitMembers != 0 {
		t.Fatalf("group metrics = %+v, want zeros", m)
	}
	if m.LocalCommits != 1 {
		t.Fatalf("local commits = %d, want 1", m.LocalCommits)
	}
}

// TestGroupCommitContendedRowConverges proves intra-group conflicts resolve
// to the last SQL commit both durably and in the materializer.
func TestGroupCommitContendedRowConverges(t *testing.T) {
	ctx := context.Background()
	cfg := groupTestConfig(t)
	cfg.Durability.GroupCommit.MaxDelay = 20 * time.Millisecond
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := ids.NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "seed"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	const writers = 4
	const perWriter = 10
	startGate := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(writers)
	errs := make(chan error, writers)
	for worker := 0; worker < writers; worker++ {
		go func(worker int) {
			var first error
			defer func() { errs <- first }()
			ready.Done()
			<-startGate
			for i := 0; i < perWriter; i++ {
				if _, err := db.ExecContext(ctx,
					`UPDATE contacts SET name = ? WHERE id = ?`, fmt.Sprintf("w%d-%d", worker, i), id[:]); err != nil {
					first = err
					return
				}
			}
		}(worker)
	}
	ready.Wait()
	close(startGate)
	for range writers {
		if err := <-errs; err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	var before string
	if err := db.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id = ?`, id[:]).Scan(&before); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var after string
	if err := db.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id = ?`, id[:]).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("materialized %q != durable %q after contended group commits", before, after)
	}
}

func TestGroupMemberNeedsRepair(t *testing.T) {
	if groupMemberNeedsRepair(10, 12, 5, 7) {
		t.Fatal("equal deltas (local-only interleave) require no repair")
	}
	if !groupMemberNeedsRepair(10, 13, 5, 7) {
		t.Fatal("generation ahead of local sequence must repair")
	}
	if !groupMemberNeedsRepair(12, 10, 7, 7) {
		t.Fatal("non-monotonic generation probe must repair")
	}
	if !groupMemberNeedsRepair(10, 10, 7, 5) {
		t.Fatal("non-monotonic sequence probe must repair")
	}
}
