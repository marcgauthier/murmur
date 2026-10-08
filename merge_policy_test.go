package murmur

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/rime"
)

type mergeTransactionRecord struct {
	ID    ids.RowID `rime:"primary"`
	Count int64
	Tags  []string
	Max   int64
	Min   float64
}

// Typed CRDT transaction, replication, snapshot, and concurrent-writer
// behavior is exercised through RIME APIs in this file and records_crdt_test.go.
func TestTypedMergePolicyTransaction(t *testing.T) {
	ctx := context.Background()
	definition, err := Define[mergeTransactionRecord]("merge_items", 102, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Count": 2, "Tags": 3, "Max": 4, "Min": 5},
		MergePolicies: map[string]RecordMergePolicy{
			"Count": RecordMergeCounter,
			"Tags":  RecordMergeORSet,
			"Max":   RecordMergeMax,
			"Min":   RecordMergeMin,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{definition}
	d, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[mergeTransactionRecord](d, "merge_items")
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	if err := d.WriteTxContext(ctx, func(tx *Tx) error {
		if err := table.Insert(tx, &mergeTransactionRecord{ID: row, Max: 4, Min: 4}); err != nil {
			return err
		}
		for _, delta := range []int64{5, 7, -3} {
			if err := RecordCounterAdd(tx, table, row, "Count", delta); err != nil {
				return err
			}
		}
		if err := RecordSetAdd(tx, table, row, "Tags", "red"); err != nil {
			return err
		}
		if err := RecordSetAdd(tx, table, row, "Tags", "one"); err != nil {
			return err
		}
		if err := RecordSetRemove(tx, table, row, "Tags", "red"); err != nil {
			return err
		}
		if err := RecordMax(tx, table, row, "Max", int64(2)); err != nil {
			return err
		}
		return RecordMin(tx, table, row, "Min", float64(8))
	}); err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(row)
	if err != nil || got.Count != 9 || !equalStringSet(got.Tags, []string{"one"}) || got.Max != 4 || got.Min != 4 {
		t.Fatalf("typed merge projection=%+v err=%v", got, err)
	}
	if err := d.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Update(tx, row, func(value *mergeTransactionRecord) error { value.Count = 200; return nil })
	}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("direct typed counter assignment error=%v", err)
	}
	stats, err := d.MergePolicyStats(ctx)
	if err != nil || stats.ActorComponents != 2 || stats.SetAdditions != 2 || stats.SetRemovals != 1 || stats.MetadataBytes == 0 || stats.MergeAttempts == 0 {
		t.Fatalf("metadata stats: %+v %v", stats, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	table, err = TableOf[mergeTransactionRecord](d, "merge_items")
	if err != nil {
		t.Fatal(err)
	}
	got, err = table.Get(row)
	if err != nil || got.Count != 9 || !equalStringSet(got.Tags, []string{"one"}) {
		t.Fatalf("reopened typed merge row=%+v err=%v", got, err)
	}
}
func TestMergePolicyRemoteConvergence(t *testing.T) {
	ctx := context.Background()
	definition, err := Define[mergeConvergenceRecord]("merge_rows", 101, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Count": 2, "Tags": 3},
		MergePolicies: map[string]RecordMergePolicy{
			"Count": RecordMergeCounter,
			"Tags":  RecordMergeORSet,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.DBID = ids.NewDBID()
	cfg.Tables = []TableDefinition{definition}
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	other := cfg
	other.Path = t.TempDir()
	other.NodeID = ids.NewNodeID()
	other.OriginSigning = testidentity.Config(other.NodeID)
	b, err := Open(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	aTable, err := TableOf[mergeConvergenceRecord](a, "merge_rows")
	if err != nil {
		t.Fatal(err)
	}
	bTable, err := TableOf[mergeConvergenceRecord](b, "merge_rows")
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	if err = a.WriteTxContext(ctx, func(tx *Tx) error {
		return aTable.Insert(tx, &mergeConvergenceRecord{ID: row})
	}); err != nil {
		t.Fatal(err)
	}
	forward := func(src, dst *DB, origin ids.NodeID, seq uint64) uint64 {
		t.Helper()
		last, err := src.ScanReplicationLog(ctx, origin, seq, 100, 1<<20, func(batch *codec.MutationBatch) error { return dst.ApplyRemote(ctx, batch) })
		if err != nil {
			t.Fatal(err)
		}
		return last
	}
	forward(a, b, cfg.NodeID, 1)
	for _, write := range []struct {
		db    *DB
		table *RecordTable[mergeConvergenceRecord]
		inc   int64
		tag   string
	}{{a, aTable, 1, "red"}, {b, bTable, 2, "blue"}} {
		if err := write.db.WriteTxContext(ctx, func(tx *Tx) error {
			if err := RecordCounterAdd(tx, write.table, row, "Count", write.inc); err != nil {
				return err
			}
			return RecordSetAdd(tx, write.table, row, "Tags", write.tag)
		}); err != nil {
			t.Fatal(err)
		}
	}
	forward(a, b, cfg.NodeID, 2)
	forward(b, a, other.NodeID, 1)
	for _, table := range []*RecordTable[mergeConvergenceRecord]{aTable, bTable} {
		got, err := table.Get(row)
		if err != nil || got.Count != 3 || !equalStringSet(got.Tags, []string{"blue", "red"}) {
			t.Fatalf("converged typed merge record=%+v err=%v", got, err)
		}
	}
}

func TestTypedMergePolicyConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	definition, err := Define[mergeTransactionRecord]("merge_items", 102, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Count": 2, "Tags": 3, "Max": 4, "Min": 5},
		MergePolicies: map[string]RecordMergePolicy{
			"Count": RecordMergeCounter,
			"Tags":  RecordMergeORSet,
			"Max":   RecordMergeMax,
			"Min":   RecordMergeMin,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{definition}
	d, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	table, err := TableOf[mergeTransactionRecord](d, "merge_items")
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	if err := d.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Insert(tx, &mergeTransactionRecord{ID: row})
	}); err != nil {
		t.Fatal(err)
	}
	const writers = 8
	const iterations = 12
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				for attempt := 0; attempt < 100; attempt++ {
					err := d.WriteTxContext(ctx, func(tx *Tx) error {
						if err := RecordCounterAdd(tx, table, row, "Count", 1); err != nil {
							return err
						}
						return RecordSetAdd(tx, table, row, "Tags", fmt.Sprintf("%d/%d", w, i))
					})
					if err == nil {
						break
					}
					if errors.Is(err, rime.ErrConflict) && attempt < 99 {
						continue
					}
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("%v (metrics=%+v)", err, d.Metrics())
	}
	got, err := table.Get(row)
	if err != nil || got.Count != writers*iterations || len(got.Tags) != writers*iterations {
		t.Fatalf("concurrent typed merge result=%+v err=%v", got, err)
	}
}
