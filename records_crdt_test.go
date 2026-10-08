package murmur

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/spool"
)

type mergeConvergenceRecord struct {
	ID    ids.RowID `rime:"primary"`
	Count int64
	Tags  []string
}

type counterRecord struct {
	ID    ids.RowID `rime:"primary"`
	Count int64
}

type setRecord struct {
	ID   ids.RowID `rime:"primary"`
	Tags []string
}

type extremaRecord struct {
	ID   ids.RowID `rime:"primary"`
	High int64
	Low  float64
}

func extremaDefinition(t *testing.T) TableDefinition {
	t.Helper()
	d, err := Define[extremaRecord]("extrema_records", 93, RecordOptions{
		PrimaryField:  "ID",
		FieldIDs:      map[string]uint32{"ID": 1, "High": 2, "Low": 3},
		MergePolicies: map[string]RecordMergePolicy{"High": RecordMergeMax, "Low": RecordMergeMin},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func setDefinition(t *testing.T) TableDefinition {
	t.Helper()
	d, err := Define[setRecord]("set_records", 92, RecordOptions{
		PrimaryField:  "ID",
		FieldIDs:      map[string]uint32{"ID": 1, "Tags": 2},
		MergePolicies: map[string]RecordMergePolicy{"Tags": RecordMergeORSet},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func counterDefinition(t *testing.T) TableDefinition {
	t.Helper()
	d, err := Define[counterRecord]("counter_records", 91, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Count": 2},
		MergePolicies: map[string]RecordMergePolicy{
			"Count": RecordMergeCounter,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestTypedCounterUsesDurablePNComponents(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{counterDefinition(t)}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[counterRecord](db, "counter_records")
	if err != nil {
		t.Fatal(err)
	}
	want := &counterRecord{ID: ids.NewRowID()}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		if err := table.Insert(tx, want); err != nil {
			return err
		}
		if err := RecordCounterAdd(tx, table, want.ID, "Count", 5); err != nil {
			return err
		}
		return RecordCounterAdd(tx, table, want.ID, "Count", -2)
	}); err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(want.ID)
	if err != nil || got.Count != 3 {
		t.Fatalf("typed counter after +5,-2 = %+v, %v; want 3", got, err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Update(tx, want.ID, func(row *counterRecord) error {
			row.Count++
			return nil
		})
	}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("direct counter assignment error = %v, want ErrUnsupportedSchema", err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return RecordCounterAdd(tx, table, want.ID, "Count", math.MaxInt64)
	}); err == nil {
		t.Fatal("counter accepted an int64 overflow")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err = TableOf[counterRecord](db, "counter_records")
	if err != nil {
		t.Fatal(err)
	}
	got, err = table.Get(want.ID)
	if err != nil || got.Count != 3 {
		t.Fatalf("reopened typed counter = %+v, %v; want 3", got, err)
	}
}

func TestTypedORSetUsesDurableCausalRecords(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{setDefinition(t)}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[setRecord](db, "set_records")
	if err != nil {
		t.Fatal(err)
	}
	want := &setRecord{ID: ids.NewRowID()}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		if err := table.Insert(tx, want); err != nil {
			return err
		}
		if err := RecordSetAdd(tx, table, want.ID, "Tags", "z"); err != nil {
			return err
		}
		if err := RecordSetAdd(tx, table, want.ID, "Tags", "aa"); err != nil {
			return err
		}
		if err := RecordSetAdd(tx, table, want.ID, "Tags", "red"); err != nil {
			return err
		}
		return RecordSetRemove(tx, table, want.ID, "Tags", "red")
	}); err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(want.ID)
	if err != nil || !equalStringSet(got.Tags, []string{"aa", "z"}) {
		t.Fatalf("typed set projection after updates = %+v, %v", got, err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Update(tx, want.ID, func(row *setRecord) error { row.Tags = []string{"direct"}; return nil })
	}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("direct OR_SET assignment error = %v, want ErrUnsupportedSchema", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err = TableOf[setRecord](db, "set_records")
	if err != nil {
		t.Fatal(err)
	}
	got, err = table.Get(want.ID)
	if err != nil || !equalStringSet(got.Tags, []string{"aa", "z"}) {
		t.Fatalf("reopened typed set projection = %+v, %v", got, err)
	}
	state, err := db.store.CRDTRecords(92, want.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != 4 {
		t.Fatalf("causal OR_SET records=%d, want 4 (three adds, one remove)", len(state))
	}
}

func TestTypedUncertainCommitReceiptResolvesAfterReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var armed atomic.Bool
	faults := &spool.FaultHooks{SegmentSync: func() error {
		if armed.Load() {
			return errors.New("injected typed commit sync failure")
		}
		return nil
	}}
	cfg := testConfig(dir)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{setDefinition(t)}
	cfg.Spool.Faults = faults
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[setRecord](db, "set_records")
	if err != nil {
		t.Fatal(err)
	}
	want := &setRecord{ID: ids.NewRowID()}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error { return table.Insert(tx, want) }); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	err = db.WriteTxContext(ctx, func(tx *Tx) error { return RecordSetAdd(tx, table, want.ID, "Tags", "possibly-durable") })
	armed.Store(false)
	if !errors.Is(err, ErrCommitOutcomeUncertain) {
		t.Fatalf("typed write error=%v, want ErrCommitOutcomeUncertain", err)
	}
	var uncertain *CommitOutcomeUncertainError
	if !errors.As(err, &uncertain) || uncertain.TxID.IsZero() {
		t.Fatalf("typed error has no transaction ID: %T %v", err, err)
	}
	_ = db.Close()
	cfg.Spool.Faults = nil
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen after uncertain typed write: %v", err)
	}
	defer db.Close()
	has, err := db.HasTransactionReceipt(uncertain.TxID)
	if err != nil {
		t.Fatal(err)
	}
	table, err = TableOf[setRecord](db, "set_records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if has != equalStringSet(got.Tags, []string{"possibly-durable"}) {
		t.Fatalf("receipt=%v disagrees with recovered projection=%v", has, got.Tags)
	}
}

func TestTypedNumericExtremaUseDurablePolicies(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{extremaDefinition(t)}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[extremaRecord](db, "extrema_records")
	if err != nil {
		t.Fatal(err)
	}
	want := &extremaRecord{ID: ids.NewRowID(), High: -10, Low: 10}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		if err := table.Insert(tx, want); err != nil {
			return err
		}
		if err := RecordMax(tx, table, want.ID, "High", int64(-20)); err != nil {
			return err
		}
		if err := RecordMax(tx, table, want.ID, "High", int64(15)); err != nil {
			return err
		}
		if err := RecordMin(tx, table, want.ID, "Low", float64(20)); err != nil {
			return err
		}
		return RecordMin(tx, table, want.ID, "Low", float64(-4))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return RecordMin(tx, table, want.ID, "Low", math.NaN())
	}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("non-finite extrema error=%v, want ErrUnsupportedSchema", err)
	}
	got, err := table.Get(want.ID)
	if err != nil || got.High != 15 || got.Low != -4 {
		t.Fatalf("typed extrema = %+v, %v; want High=15 Low=-4", got, err)
	}
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.Update(tx, want.ID, func(row *extremaRecord) error { row.High = 100; return nil })
	}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("direct extrema assignment error=%v, want ErrUnsupportedSchema", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err = TableOf[extremaRecord](db, "extrema_records")
	if err != nil {
		t.Fatal(err)
	}
	got, err = table.Get(want.ID)
	if err != nil || got.High != 15 || got.Low != -4 {
		t.Fatalf("reopened typed extrema = %+v, %v; want High=15 Low=-4", got, err)
	}
}

func TestTypedMergeSnapshotPreservesRemovedSetHistory(t *testing.T) {
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
	source, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	sourceTable, err := TableOf[mergeConvergenceRecord](source, "merge_rows")
	if err != nil {
		t.Fatal(err)
	}
	row, empty := ids.NewRowID(), ids.NewRowID()
	if err := source.WriteTxContext(ctx, func(tx *Tx) error {
		if err := sourceTable.Insert(tx, &mergeConvergenceRecord{ID: row}); err != nil {
			return err
		}
		if err := RecordCounterAdd(tx, sourceTable, row, "Count", 20); err != nil {
			return err
		}
		if err := RecordSetAdd(tx, sourceTable, row, "Tags", "red"); err != nil {
			return err
		}
		return sourceTable.Insert(tx, &mergeConvergenceRecord{ID: empty})
	}); err != nil {
		t.Fatal(err)
	}
	if err := source.WriteTxContext(ctx, func(tx *Tx) error {
		return RecordSetRemove(tx, sourceTable, row, "Tags", "red")
	}); err != nil {
		t.Fatal(err)
	}

	destinationConfig := cfg
	destinationConfig.Path = t.TempDir()
	destinationConfig.NodeID = ids.NewNodeID()
	destinationConfig.OriginSigning = testidentity.Config(destinationConfig.NodeID)
	destination, err := Open(ctx, destinationConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	destinationTable, err := TableOf[mergeConvergenceRecord](destination, "merge_rows")
	if err != nil {
		t.Fatal(err)
	}
	var index uint64
	if err := source.store.ExportSnapshot(2, func(manifest *codec.SnapshotManifest, cells []codec.SnapshotCell, last bool) error {
		_, applyErr := destination.ApplySnapshotChunk(ctx, manifest, index, cells, last)
		index++
		return applyErr
	}); err != nil {
		t.Fatal(err)
	}
	got, err := destinationTable.Get(row)
	if err != nil || got.Count != 20 || len(got.Tags) != 0 {
		t.Fatalf("snapshot typed merge row=%+v err=%v", got, err)
	}
	got, err = destinationTable.Get(empty)
	if err != nil || got.Count != 0 || len(got.Tags) != 0 {
		t.Fatalf("snapshot neutral row=%+v err=%v", got, err)
	}
	records, err := destination.store.CRDTRecords(101, row, 3)
	if err != nil || len(records) != 2 {
		t.Fatalf("snapshot lost removed OR_SET history: records=%d err=%v", len(records), err)
	}
}

func equalStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
