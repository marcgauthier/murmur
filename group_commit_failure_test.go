package murmur

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
)

func startTypedGroupWrites(db *DB, table *RecordTable[testContactRecord], count int) <-chan error {
	results := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			id := ids.NewRowID()
			results <- db.WriteTxContext(context.Background(), func(tx *Tx) error {
				return table.Insert(tx, &testContactRecord{ID: id, Name: fmt.Sprintf("group-%d", i)})
			})
		}(i)
	}
	return results
}

func typedContactCount(t *testing.T, db *DB) int {
	t.Helper()
	table, err := TableOf[testContactRecord](db, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	n, err := table.Where().Count()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestGroupCommitSharedFsyncFailureFailsEveryMember(t *testing.T) {
	const members = 12
	var armed atomic.Bool
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	cfg := groupTestConfig(t)
	cfg.Durability.GroupCommit.MaxDelay = time.Second
	cfg.Durability.GroupCommit.MaxTransactions = members
	cfg.Spool.Faults = &spool.FaultHooks{SegmentSync: func() error {
		if !armed.Load() {
			return nil
		}
		calls.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		return syscall.ENOSPC
	}}
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	db.grouper.maxDelay = time.Hour
	table, err := TableOf[testContactRecord](db, "contacts")
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	var beforeDurable atomic.Int64
	db.crash = &crashHooks{beforeDurable: func() error {
		beforeDurable.Add(1)
		armed.Store(true)
		return nil
	}}
	groupSize := make(chan int, 1)
	commit := db.grouper.commit
	db.grouper.commit = func(group []*groupMember) []groupMemberResult {
		groupSize <- len(group)
		return commit(group)
	}
	results := startTypedGroupWrites(db, table, members)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		close(release)
		_ = db.Close()
		t.Fatal("group did not reach Spool sync")
	}
	select {
	case got := <-groupSize:
		if got != members || beforeDurable.Load() != members {
			t.Fatalf("group=%d prepared members=%d, want %d", got, beforeDurable.Load(), members)
		}
	case <-time.After(10 * time.Second):
		close(release)
		_ = db.Close()
		t.Fatal("group did not reach commit callback")
	}
	select {
	case err := <-results:
		t.Fatalf("member returned before shared sync resolved: %v", err)
	default:
	}
	close(release)
	for i := 0; i < members; i++ {
		err := <-results
		if !state.IsStorageFailure(err) || !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("member %d acknowledged incorrectly: %v", i, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("failed group syncs=%d, want one", calls.Load())
	}
	if db.Status().State != StateFailed {
		t.Fatalf("failed group did not fail closed: %+v", db.Status())
	}
	_ = db.Close()
}

func TestGroupCommitCrashBeforeSpoolCommit(t *testing.T) {
	const members = 8
	if path := os.Getenv("MURMUR_GROUP_CRASH_PATH"); path != "" {
		cfg := testConfig(path)
		node, err := ids.ParseNodeID(os.Getenv("MURMUR_GROUP_CRASH_NODE"))
		if err != nil {
			t.Fatal(err)
		}
		cfg.NodeID = node
		cfg.Durability.GroupCommit.MaxDelay = time.Second
		cfg.Durability.GroupCommit.MaxTransactions = members
		db, err := openSignedFixture(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		db.grouper.maxDelay = time.Hour
		var prepared atomic.Int64
		db.crash = &crashHooks{beforeDurable: func() error {
			if prepared.Add(1) == members {
				os.Exit(86)
			}
			return nil
		}}
		table, err := TableOf[testContactRecord](db, "contacts")
		if err != nil {
			t.Fatal(err)
		}
		for err := range startTypedGroupWrites(db, table, members) {
			if err != nil {
				t.Fatalf("child write failed before crash boundary: %v", err)
			}
		}
		t.Fatal("group returned without hitting pre-Spool crash boundary")
	}
	cfg := groupTestConfig(t)
	cfg.Durability.GroupCommit.MaxDelay = time.Second
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	db.grouper.maxDelay = time.Hour
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGroupCommitCrashBeforeSpoolCommit$", "-test.count=1")
	cmd.Env = append(os.Environ(), "MURMUR_GROUP_CRASH_PATH="+cfg.Path, "MURMUR_GROUP_CRASH_NODE="+cfg.NodeID.String())
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 86 {
		t.Fatalf("child missed verified pre-Spool boundary: %v\n%s", err, output)
	}
	reopened, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := typedContactCount(t, reopened); got != 0 {
		t.Fatalf("uncommitted typed group survived crash: %d rows", got)
	}
}

func TestGroupCommitCloseWhileGroupPending(t *testing.T) {
	const members = 8
	cfg := groupTestConfig(t)
	cfg.Durability.GroupCommit.MaxDelay = time.Second
	cfg.Durability.GroupCommit.MaxTransactions = members + 1
	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	db.grouper.maxDelay = time.Hour
	table, err := TableOf[testContactRecord](db, "contacts")
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	results := startTypedGroupWrites(db, table, members)
	deadline := time.Now().Add(10 * time.Second)
	for {
		db.writeMu.Lock()
		db.grouper.mu.Lock()
		queued := 0
		if db.grouper.current != nil {
			queued = len(db.grouper.current.members)
		}
		db.grouper.mu.Unlock()
		db.writeMu.Unlock()
		if queued == members {
			break
		}
		if time.Now().After(deadline) {
			_ = db.Close()
			t.Fatalf("pending group members=%d, want %d", queued, members)
		}
		time.Sleep(time.Millisecond)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	for i := 0; i < members; i++ {
		if err := <-results; err != nil {
			t.Fatalf("admitted pending member lost during Close: %v", err)
		}
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	reopened, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := typedContactCount(t, reopened); got != members {
		t.Fatalf("Close lost acknowledged members: %d/%d", got, members)
	}
}
