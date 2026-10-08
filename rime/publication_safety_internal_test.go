package rime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type pubSafetyRow struct {
	ID    string `rime:"primary"`
	Value int
}

type pubSafetyUniqueRow struct {
	ID    string `rime:"primary"`
	Email string `rime:"unique"`
}

type pubSafetyOrderedRow struct {
	ID    string `rime:"primary"`
	Value int    `rime:"ordered"`
}

func TestManagedPrepareBuildsUniqueMapsBeforePublish(t *testing.T) {
	db := New()
	defer db.Close()
	row, err := Register[pubSafetyUniqueRow](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := row.Upsert(&pubSafetyUniqueRow{ID: "a", Email: "old@example.test"}); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Update("a", func(rec *pubSafetyUniqueRow) error {
		rec.Email = "new@example.test"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	token, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	defer token.Abort()
	if len(token.uniqueMaps) != 1 {
		t.Fatalf("prepared unique map sets = %d, want 1", len(token.uniqueMaps))
	}
	row.idx.mu.RLock()
	live := row.idx.uniqueStr["Email"]
	_, oldLive := live["old@example.test"]
	_, newLive := live["new@example.test"]
	row.idx.mu.RUnlock()
	if !oldLive || newLive {
		t.Fatalf("live unique map changed before publish: old=%v new=%v", oldLive, newLive)
	}

	prepared := token.uniqueMaps[0].maps.([]preparedUniqueField)
	var preparedEmail *preparedUniqueField
	for _, field := range prepared {
		if field.field == "Email" {
			preparedEmail = &field
		}
	}
	if preparedEmail == nil {
		t.Fatalf("prepared unique fields = %+v", prepared)
	}
	if preparedEmail.replace {
		t.Fatal("same-size unique update unnecessarily replaced the whole map")
	}
	if len(preparedEmail.updates) != 2 {
		t.Fatalf("prepared unique claims = %+v, want release plus claim", preparedEmail.updates)
	}
	if err := token.Publish(); err != nil {
		t.Fatal(err)
	}
	row.idx.mu.RLock()
	_, oldLive = row.idx.uniqueStr["Email"]["old@example.test"]
	newOwner := row.idx.uniqueStr["Email"]["new@example.test"]
	row.idx.mu.RUnlock()
	if oldLive || newOwner != "a" {
		t.Fatalf("published unique map old=%v new owner=%v", oldLive, newOwner)
	}
}

func TestManagedPrepareReplacesUniqueMapOnlyAtGrowthThreshold(t *testing.T) {
	db := New()
	defer db.Close()
	row, err := Register[pubSafetyUniqueRow](db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < uniqueMapInitialHint; i++ {
		rec := &pubSafetyUniqueRow{ID: fmt.Sprintf("id-%d", i), Email: fmt.Sprintf("%d@example.test", i)}
		if err := row.Upsert(rec); err != nil {
			t.Fatal(err)
		}
	}

	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Insert(&pubSafetyUniqueRow{ID: "growth", Email: "growth@example.test"}); err != nil {
		t.Fatal(err)
	}
	token, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	defer token.Abort()
	if len(token.uniqueMaps) != 1 {
		t.Fatalf("prepared unique map sets = %d, want 1", len(token.uniqueMaps))
	}
	prepared := token.uniqueMaps[0].maps.([]preparedUniqueField)
	var email *preparedUniqueField
	for _, field := range prepared {
		if field.field == "Email" {
			email = &field
		}
	}
	if email == nil || !email.replace {
		t.Fatalf("unique map did not replace at tracked growth threshold: %+v", prepared)
	}
	values := email.values.(map[string]any)
	if len(values) != uniqueMapInitialHint+1 || values["growth@example.test"] != "growth" {
		t.Fatalf("prepared growth map len=%d new owner=%v", len(values), values["growth@example.test"])
	}
	row.idx.mu.RLock()
	liveLen := len(row.idx.uniqueStr["Email"])
	_, liveGrowth := row.idx.uniqueStr["Email"]["growth@example.test"]
	row.idx.mu.RUnlock()
	if liveLen != uniqueMapInitialHint || liveGrowth {
		t.Fatalf("live unique map changed before publish: len=%d growth=%v", liveLen, liveGrowth)
	}
	if err := token.Publish(); err != nil {
		t.Fatal(err)
	}
	row.idx.mu.RLock()
	liveLen = len(row.idx.uniqueStr["Email"])
	liveGrowth = row.idx.uniqueStr["Email"]["growth@example.test"] == "growth"
	row.idx.mu.RUnlock()
	if liveLen != uniqueMapInitialHint+1 || !liveGrowth {
		t.Fatalf("published unique map len=%d growth=%v", liveLen, liveGrowth)
	}
}

// faultDatabase drives one managed commit whose installation panics, like a
// post-durability publication failure. It returns the faulted database, the
// table, the token's commit generation, and the Publish error.
func faultDatabase(t *testing.T) (*DB, *Table[pubSafetyRow], TxID, error) {
	t.Helper()
	db := New()
	row, err := Register[pubSafetyRow](db)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Insert(&pubSafetyRow{ID: "faulted"}); err != nil {
		t.Fatal(err)
	}
	token, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	commit := token.CommitID()
	tx.pending[0].apply = func(TxID) func() { panic("injected publication failure") }
	return db, row, commit, token.Publish()
}

func TestFaultedDatabaseRejectsNilTxReads(t *testing.T) {
	db, row, _, err := faultDatabase(t)
	if !errors.Is(err, ErrDBFaulted) {
		t.Fatalf("Publish error = %v, want ErrDBFaulted", err)
	}
	defer db.Close()
	// Get(nil) reads through the direct-snapshot path, not a read
	// transaction: it must still refuse a faulted database.
	if _, err := row.Get("faulted"); !errors.Is(err, ErrDBFaulted) {
		t.Fatalf("Get(nil) after fault = %v, want ErrDBFaulted", err)
	}
}

func TestPublishPanicReportsCommitID(t *testing.T) {
	db, _, commit, err := faultDatabase(t)
	defer db.Close()
	var uncertain *UncertainCommitError
	if !errors.As(err, &uncertain) {
		t.Fatalf("Publish error type = %T, want *UncertainCommitError", err)
	}
	if uncertain.Commit != commit {
		t.Fatalf("uncertain commit = %d, want token generation %d", uncertain.Commit, commit)
	}
	if !errors.Is(err, ErrDBFaulted) {
		t.Fatalf("Publish error = %v, want errors.Is ErrDBFaulted", err)
	}
}

func TestManagedPrepareBuildsMVCCChainsBeforePublish(t *testing.T) {
	db := New()
	defer db.Close()
	row, err := Register[pubSafetyRow](db, WithTableShards[pubSafetyRow](1), WithCompound[pubSafetyRow]("value", "Value"))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Insert(&pubSafetyRow{ID: "prepared", Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Update("prepared", func(rec *pubSafetyRow) error {
		rec.Value = 2
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := row.In(tx).Insert(&pubSafetyRow{ID: fmt.Sprintf("other-%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	token, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	if len(tx.pending) != 8 {
		t.Fatalf("staged writes = %d, want 8", len(tx.pending))
	}
	if len(token.rowMaps) != 1 {
		t.Fatalf("prepared shard maps = %d, want 1 for the new key", len(token.rowMaps))
	}
	preparedRows := token.rowMaps[0].rows.(preparedShardRows[pubSafetyRow]).rows
	if _, ok := preparedRows["prepared"]; ok {
		t.Fatal("prepared shard map contains an unpublished key")
	}
	for i, pending := range tx.pending {
		if pending.preparedChain == nil {
			t.Fatalf("pending write %d has no prebuilt MVCC chain", i)
		}
		if pending.preparedIndexEffects == nil {
			t.Fatalf("pending write %d has no precomputed compound index keys", i)
		}
	}
	firstEffect := tx.pending[0].preparedIndexEffects.(managedIndexEffects).compound[0]
	if firstEffect.oldValues != nil || len(firstEffect.newValues) != 1 || firstEffect.newValues[0] != 1 {
		t.Fatalf("prepared insert index effect = %+v", firstEffect)
	}
	updateEffect := tx.pending[1].preparedIndexEffects.(managedIndexEffects).compound[0]
	if len(updateEffect.oldValues) != 1 || updateEffect.oldValues[0] != 1 || len(updateEffect.newValues) != 1 || updateEffect.newValues[0] != 2 {
		t.Fatalf("prepared update index effect = %+v", updateEffect)
	}
	prepared := tx.pending[1].preparedChain.(*chain[pubSafetyRow])
	latest, ok := prepared.latest()
	if !ok || latest.commit != token.CommitID() || latest.val.Value != 2 {
		t.Fatalf("prepared head = %+v, want final value 2 at commit %d", latest, token.CommitID())
	}
	if _, err := row.Get("prepared"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prepared row visible before publish: %v", err)
	}
	if err := token.Publish(); err != nil {
		t.Fatal(err)
	}
	row.idx.mu.RLock()
	indexed := row.idx.comp["value"].lookup([]any{2})
	row.idx.mu.RUnlock()
	if _, ok := indexed["prepared"]; !ok {
		t.Fatal("published compound index is missing the prepared row")
	}
	row.idx.mu.RLock()
	stale := row.idx.comp["value"].lookup([]any{1})
	row.idx.mu.RUnlock()
	if _, ok := stale["prepared"]; ok {
		t.Fatal("published compound index retained the superseded key")
	}
	got, err := row.Get("prepared")
	if err != nil || got.Value != 2 {
		t.Fatalf("published row = %+v, %v; want value 2", got, err)
	}
	for i := 0; i < 6; i++ {
		if _, err := row.Get(fmt.Sprintf("other-%d", i)); err != nil {
			t.Fatalf("published shard row %d missing: %v", i, err)
		}
	}
}

func TestManagedPrepareGrowsOrderedDirectoryAtCapacityThreshold(t *testing.T) {
	db := New()
	defer db.Close()
	row, err := Register[pubSafetyOrderedRow](db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := row.Insert(&pubSafetyOrderedRow{ID: fmt.Sprintf("%d", i), Value: i}); err != nil {
			t.Fatal(err)
		}
	}
	oldMap := row.idx.ordered["Value"].byKey
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Insert(&pubSafetyOrderedRow{ID: "growth", Value: 8}); err != nil {
		t.Fatal(err)
	}
	token, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	defer token.Abort()
	if len(token.orderedMaps) != 1 {
		t.Fatalf("prepared ordered directory maps = %d, want one threshold replacement", len(token.orderedMaps))
	}
	prepared := token.orderedMaps[0].rows.([]preparedOrderedMap)
	if len(prepared) != 1 || prepared[0].index != row.idx.ordered["Value"] || prepared[0].hint < 9 {
		t.Fatalf("prepared ordered directory = %+v, want capacity for 9", prepared)
	}
	if len(prepared[0].values) != 8 || len(oldMap) != 8 {
		t.Fatal("replacement ordered directory was not privately prebuilt")
	}
	if err := token.Publish(); err != nil {
		t.Fatal(err)
	}
	index := row.idx.ordered["Value"]
	if index.len() != 9 || index.byKeyHint < 9 {
		t.Fatalf("published ordered directory len=%d hint=%d", index.len(), index.byKeyHint)
	}
}

func TestManagedPrepareBuildsOrderedIndexObjectsBeforePublish(t *testing.T) {
	db := New()
	defer db.Close()
	row, err := Register[pubSafetyOrderedRow](db)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Insert(&pubSafetyOrderedRow{ID: "a", Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Update("a", func(rec *pubSafetyOrderedRow) error {
		rec.Value = 2
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	token, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	defer token.Abort()
	if len(token.orderedMaps) != 0 {
		t.Fatalf("ordinary ordered write prepared directory replacement: %d", len(token.orderedMaps))
	}
	for i, pending := range tx.pending {
		effects := pending.preparedIndexEffects.(managedIndexEffects).ordered
		if len(effects) != 1 || effects[0].node == nil || effects[0].item == nil {
			t.Fatalf("pending %d ordered effects = %+v, want prebuilt node and item", i, effects)
		}
	}
	row.idx.mu.RLock()
	before := row.idx.ordered["Value"].len()
	row.idx.mu.RUnlock()
	if before != 0 {
		t.Fatalf("ordered index visible before publish has %d keys", before)
	}
	if err := token.Publish(); err != nil {
		t.Fatal(err)
	}
	row.idx.mu.RLock()
	index := row.idx.ordered["Value"]
	indexed := index.byKey["a"]
	row.idx.mu.RUnlock()
	if indexed == nil || indexed.owner.val != int64(2) {
		t.Fatalf("published ordered entry = %+v, want value 2", indexed)
	}
	if err := row.Insert(&pubSafetyOrderedRow{ID: "b", Value: 3}); err != nil {
		t.Fatalf("standalone insert after managed publication: %v", err)
	}
}

func TestPublishAfterHookPanicDoesNotFault(t *testing.T) {
	db := New()
	defer db.Close()
	row, err := Register[pubSafetyRow](db)
	if err != nil {
		t.Fatal(err)
	}
	row.AfterSave(func(Change[pubSafetyRow]) { panic("hook boom") })
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := row.In(tx).Insert(&pubSafetyRow{ID: "hooked", Value: 7}); err != nil {
		t.Fatal(err)
	}
	token, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	commit := token.CommitID()
	func() {
		defer func() {
			if recovered := recover(); recovered != "hook boom" {
				t.Fatalf("recovered = %v, want hook panic to propagate", recovered)
			}
		}()
		_ = token.Publish()
		t.Fatal("Publish with panicking hook did not panic")
	}()
	// The commit was already visible: no terminal fault, and the row reads.
	if db.faulted.Load() {
		t.Fatal("after-hook panic faulted a fully published database")
	}
	if got := db.latest(); got != commit {
		t.Fatalf("latest = %d, want published generation %d", got, commit)
	}
	rec, err := row.Get("hooked")
	if err != nil {
		t.Fatalf("Get after hook panic = %v", err)
	}
	if rec.Value != 7 {
		t.Fatalf("row = %+v, want Value 7", rec)
	}
	next, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx after hook panic = %v", err)
	}
	if err := next.Rollback(); err != nil {
		t.Fatalf("Rollback after hook panic = %v", err)
	}
}

// TestManagedTokenHoldsCommitCoordinator proves the behavior the durable
// adapter depends on: while a prepared token is outstanding, other commits
// wait (they neither fail nor deadlock), reads proceed, and both Publish and
// Abort release the waiter exactly once.
func TestManagedTokenHoldsCommitCoordinator(t *testing.T) {
	for _, release := range []string{"publish", "abort"} {
		t.Run(release, func(t *testing.T) {
			db := New()
			defer db.Close()
			row, err := Register[pubSafetyRow](db)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := row.In(tx).Insert(&pubSafetyRow{ID: "managed"}); err != nil {
				t.Fatal(err)
			}
			token, err := tx.PrepareCommit()
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				done <- row.Upsert(&pubSafetyRow{ID: "standalone", Value: 1})
			}()
			// The writer must still be waiting on the coordinator.
			select {
			case err := <-done:
				t.Fatalf("standalone commit slipped past held token: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			// Reads never take the coordinator.
			if _, err := row.Get("managed"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("prepared row visible early: %v", err)
			}
			rtx := db.ReadTx()
			if _, err := row.In(rtx).Get("managed"); !errors.Is(err, ErrNotFound) {
				rtx.Close()
				t.Fatalf("read-tx saw prepared row: %v", err)
			}
			rtx.Close()
			if release == "publish" {
				if err := token.Publish(); err != nil {
					t.Fatal(err)
				}
			} else if err := token.Abort(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("standalone commit after %s: %v", release, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("standalone commit stuck after %s", release)
			}
			if _, err := row.Get("standalone"); err != nil {
				t.Fatalf("standalone row missing: %v", err)
			}
			_, err = row.Get("managed")
			if release == "publish" && err != nil {
				t.Fatalf("published row missing: %v", err)
			}
			if release == "abort" && !errors.Is(err, ErrNotFound) {
				t.Fatalf("aborted row visible: %v", err)
			}
		})
	}
}
