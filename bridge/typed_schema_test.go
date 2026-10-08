package bridge

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/state"
)

type typedBridgeSchemaRecord struct {
	ID   ids.RowID `rime:"primary"`
	Name string
}

type typedBridgeContactRecord struct {
	ID    ids.RowID `rime:"primary"`
	Name  *string
	Score *int64
}

type typedBridgeContactExpanded struct {
	ID    ids.RowID `rime:"primary"`
	Name  *string
	Score *int64
	Email *string
}

type typedBridgeContactRatioRecord struct {
	ID    ids.RowID `rime:"primary"`
	Name  *string
	Score *int64
	Ratio float64
}

type typedBridgeNopeRecord struct {
	ID ids.RowID `rime:"primary"`
	C  *string
}

type typedBridgeCRDTRecord struct {
	ID    ids.RowID `rime:"primary"`
	Count int64
	Tags  []string
	Peak  int64
}

func TestImporterSchemaGateUsesTypedManifest(t *testing.T) {
	ctx := context.Background()
	node := ids.NewNodeID()
	definition, err := db.Define[typedBridgeSchemaRecord]("typed_rows", 91, db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := db.Config{
		Path:          t.TempDir(),
		NodeID:        node,
		OriginSigning: testidentity.Config(node),
		Spool:         db.DefaultSpoolConfig(),
		Encryption: db.EncryptionConfig{
			Key:   make([]byte, 32),
			KeyID: "typed-bridge-test",
		},
		Tables: []db.TableDefinition{definition},
	}
	database, err := db.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	importer, err := NewImporter(database)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := importer.describeTable(ctx, "typed_rows")
	if err != nil {
		t.Fatal(err)
	}
	if desc == nil || desc.pk != "ID" || desc.cols["Name"].typ != "BLOB" || desc.cols["Name"].notNull {
		t.Fatalf("typed schema descriptor = %+v", desc)
	}
	if got, err := importer.pkColumn(ctx, "typed_rows"); err != nil || got != "ID" {
		t.Fatalf("typed primary key = %q, %v; want ID", got, err)
	}
	typedTable, err := db.TableOf[typedBridgeSchemaRecord](database, "typed_rows")
	if err != nil {
		t.Fatal(err)
	}
	want := &typedBridgeSchemaRecord{ID: ids.NewRowID(), Name: "existing"}
	if err := database.WriteTxContext(ctx, func(tx *db.Tx) error {
		return typedTable.Insert(tx, want)
	}); err != nil {
		t.Fatal(err)
	}
	if exists, err := importer.rowExists(ctx, "typed_rows", want.ID); err != nil || !exists {
		t.Fatalf("typed row lookup = %v, %v; want present", exists, err)
	}
	if exists, err := importer.rowExists(ctx, "typed_rows", ids.NewRowID()); err != nil || exists {
		t.Fatalf("typed missing-row lookup = %v, %v; want absent", exists, err)
	}
	bundle := &Bundle{Batches: []Batch{{Records: []Record{{
		Table: "typed_rows", Row: ids.NewRowID(), Op: RecordPut,
		Columns: []ColumnValue{{Column: "Name", Policy: schema.LWW, Value: codec.Blob([]byte("field"))}},
	}}}}}
	if err := importer.checkSchema(ctx, bundle); err != nil {
		t.Fatalf("typed manifest rejected a compatible field: %v", err)
	}
}

func TestImporterAppliesTypedRowsWithAtomicProvenance(t *testing.T) {
	ctx := context.Background()
	node := ids.NewNodeID()
	options := db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
	}
	definition, err := db.Define[typedBridgeSchemaRecord]("typed_rows", 92, options)
	if err != nil {
		t.Fatal(err)
	}
	cfg := db.Config{
		Path:          t.TempDir(),
		NodeID:        node,
		OriginSigning: testidentity.Config(node),
		Spool:         db.DefaultSpoolConfig(),
		Encryption:    db.EncryptionConfig{Key: make([]byte, 32), KeyID: "typed-bridge-import-test"},
		Tables:        []db.TableDefinition{definition},
	}
	database, err := db.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	importer, err := NewImporter(database)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := recordcodec.Compile(reflect.TypeFor[typedBridgeSchemaRecord](), recordcodec.CompileOptions{
		TableID: 92, PrimaryField: "ID", FieldIDs: options.FieldIDs,
	})
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	value := &typedBridgeSchemaRecord{ID: row, Name: "low-value"}
	payloads := make([]ColumnValue, 0, 2)
	for _, field := range []struct {
		id uint32
	}{{1}, {2}} {
		payload, err := recordcodec.EncodeField(compiled, field.id, value, nil, recordcodec.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		fieldName := "ID"
		if field.id == 2 {
			fieldName = "Name"
		}
		payloads = append(payloads, ColumnValue{Column: fieldName, Policy: schema.LWW, Value: codec.Blob(payload)})
	}
	batch := Batch{TxID: ids.NewTxID(), Origin: ids.NewNodeID(), Sequence: 1, HLC: 1, Records: []Record{{
		Table: "typed_rows", Row: row, Op: RecordPut, Columns: payloads,
	}}}
	bundle := &Bundle{Manifest: Manifest{
		BundleID: ids.NewTxID(), SourceDomain: ids.NewDBID(), Stream: "typed-import", SeqFirst: 1, SeqLast: 1,
	}, Batches: []Batch{batch}}
	if err := importer.ApplyBundle(ctx, bundle); err != nil {
		t.Fatalf("apply typed bridge bundle: %v", err)
	}
	table, err := db.TableOf[typedBridgeSchemaRecord](database, "typed_rows")
	if err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(row)
	if err != nil || got.Name != "low-value" {
		t.Fatalf("typed imported row = %+v, %v", got, err)
	}
	policy, ok, err := database.BridgeRowProvenance("typed_rows", row)
	if err != nil || !ok || policy.Owner != db.BridgeOwnerLow || policy.SourceDomain != bundle.Manifest.SourceDomain {
		t.Fatalf("typed imported provenance = %+v, present=%v, err=%v", policy, ok, err)
	}
	if has, err := database.HasTransactionReceipt(sourceReceiptID(bundle, batch)); err != nil || !has {
		t.Fatalf("typed source receipt present=%v err=%v", has, err)
	}
	if err := database.WriteTxContext(ctx, func(tx *db.Tx) error {
		return table.Update(tx, row, func(record *typedBridgeSchemaRecord) error {
			record.Name = "high-value"
			return nil
		})
	}); err != nil {
		t.Fatalf("High typed override: %v", err)
	}
	fieldPolicy, ok, err := database.BridgeFieldProvenance("typed_rows", row, "Name")
	if err != nil || !ok || fieldPolicy.Owner != db.BridgeOwnerHigh {
		t.Fatalf("typed High field ownership = %+v, present=%v, err=%v", fieldPolicy, ok, err)
	}
	nextValue := &typedBridgeSchemaRecord{ID: row, Name: "low-update"}
	nextColumns := make([]ColumnValue, 0, 2)
	for _, fieldID := range []uint32{1, 2} {
		payload, err := recordcodec.EncodeField(compiled, fieldID, nextValue, nil, recordcodec.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		fieldName := "ID"
		if fieldID == 2 {
			fieldName = "Name"
		}
		nextColumns = append(nextColumns, ColumnValue{Column: fieldName, Policy: schema.LWW, Value: codec.Blob(payload)})
	}
	nextBatch := Batch{TxID: ids.NewTxID(), Origin: batch.Origin, Sequence: 2, HLC: 2, Records: []Record{{
		Table: "typed_rows", Row: row, Op: RecordPut, Columns: nextColumns,
	}}}
	nextBundle := &Bundle{Manifest: Manifest{
		BundleID: ids.NewTxID(), SourceDomain: bundle.Manifest.SourceDomain, Stream: bundle.Manifest.Stream, SeqFirst: 2, SeqLast: 2,
	}, Batches: []Batch{nextBatch}}
	if err := importer.ApplyBundle(ctx, nextBundle); err != nil {
		t.Fatalf("apply typed bundle after High override: %v", err)
	}
	got, err = table.Get(row)
	if err != nil || got.Name != "high-value" {
		t.Fatalf("Low import replaced a High-owned typed field: %+v, %v", got, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = db.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen typed High database: %v", err)
	}
	defer database.Close()
	table, err = db.TableOf[typedBridgeSchemaRecord](database, "typed_rows")
	if err != nil {
		t.Fatal(err)
	}
	importer, err = NewImporter(database)
	if err != nil {
		t.Fatal(err)
	}
	got, err = table.Get(row)
	if err != nil || got.Name != "high-value" {
		t.Fatalf("typed restart did not resolve High-owned shadow: %+v, %v", got, err)
	}
	if applied, ok, err := database.BridgeStreamProgress(bundle.Manifest.Stream); err != nil || !ok || applied != 2 {
		t.Fatalf("typed stream progress=%d present=%v err=%v", applied, ok, err)
	}
	if err := database.ReleaseBridgeOwnership(ctx, "typed_rows", row, "Name"); err != nil {
		t.Fatalf("release typed field ownership: %v", err)
	}
	fieldPolicy, ok, err = database.BridgeFieldProvenance("typed_rows", row, "Name")
	if err != nil || !ok || fieldPolicy.Owner != db.BridgeOwnerLow {
		t.Fatalf("released typed field provenance = %+v, present=%v, err=%v", fieldPolicy, ok, err)
	}
	thirdValue := &typedBridgeSchemaRecord{ID: row, Name: "low-after-release"}
	thirdColumns := make([]ColumnValue, 0, 2)
	for _, fieldID := range []uint32{1, 2} {
		payload, err := recordcodec.EncodeField(compiled, fieldID, thirdValue, nil, recordcodec.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		fieldName := "ID"
		if fieldID == 2 {
			fieldName = "Name"
		}
		thirdColumns = append(thirdColumns, ColumnValue{Column: fieldName, Policy: schema.LWW, Value: codec.Blob(payload)})
	}
	thirdBatch := Batch{TxID: ids.NewTxID(), Origin: batch.Origin, Sequence: 3, HLC: 3, Records: []Record{{
		Table: "typed_rows", Row: row, Op: RecordPut, Columns: thirdColumns,
	}}}
	thirdBundle := &Bundle{Manifest: Manifest{
		BundleID: ids.NewTxID(), SourceDomain: bundle.Manifest.SourceDomain, Stream: bundle.Manifest.Stream, SeqFirst: 3, SeqLast: 3,
	}, Batches: []Batch{thirdBatch}}
	if err := importer.ApplyBundle(ctx, thirdBundle); err != nil {
		t.Fatalf("apply typed bundle after ownership release: %v", err)
	}
	got, err = table.Get(row)
	if err != nil || got.Name != "low-after-release" {
		t.Fatalf("released typed field rejected Low update: %+v, %v", got, err)
	}
}

func TestImporterPreservesTypedCRDTCausality(t *testing.T) {
	ctx := context.Background()
	node := ids.NewNodeID()
	options := db.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Count": 2, "Tags": 3, "Peak": 4},
		MergePolicies: map[string]db.RecordMergePolicy{
			"Count": db.RecordMergeCounter,
			"Tags":  db.RecordMergeORSet,
			"Peak":  db.RecordMergeMax,
		},
	}
	definition, err := db.Define[typedBridgeCRDTRecord]("typed_crdt_rows", 93, options)
	if err != nil {
		t.Fatal(err)
	}
	cfg := db.Config{
		Path:          t.TempDir(),
		NodeID:        node,
		OriginSigning: testidentity.Config(node),
		Spool:         db.DefaultSpoolConfig(),
		Encryption:    db.EncryptionConfig{Key: make([]byte, 32), KeyID: "typed-bridge-crdt-test"},
		Tables:        []db.TableDefinition{definition},
	}
	database, err := db.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	importer, err := NewImporter(database)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := recordcodec.Compile(reflect.TypeFor[typedBridgeCRDTRecord](), recordcodec.CompileOptions{
		TableID: 93, PrimaryField: "ID", FieldIDs: options.FieldIDs,
		MergePolicies: map[string]recordcodec.MergePolicy{
			"Count": recordcodec.MergeCounter,
			"Tags":  recordcodec.MergeORSet,
			"Peak":  recordcodec.MergeMax,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	source, origin, sourceTx, row := ids.NewDBID(), ids.NewNodeID(), ids.NewTxID(), ids.NewRowID()
	value := &typedBridgeCRDTRecord{ID: row}
	primary, err := recordcodec.EncodeField(compiled, 1, value, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	countKey := append([]byte{'p'}, source[:]...)
	countKey = append(countKey, origin[:]...)
	countRecord := codec.CRDTRecord{Key: countKey, Data: []byte{9}}
	tag := state.SetTag(source, origin, sourceTx, 0)
	setValue, err := codec.SetString("blue").Encode()
	if err != nil {
		t.Fatal(err)
	}
	batch := Batch{
		TxID: sourceTx, Origin: origin, Sequence: 1, HLC: 1,
		Records: []Record{{Table: "typed_crdt_rows", Row: row, Op: RecordPut, Columns: []ColumnValue{
			{Column: "ID", Policy: schema.LWW, Value: codec.Blob(primary)},
			{Column: "Count", Policy: schema.PN_COUNTER, Value: codec.Null(), Records: []codec.CRDTRecord{countRecord}},
			{Column: "Tags", Policy: schema.OR_SET, Value: codec.Null(), Records: []codec.CRDTRecord{{Key: tag, Data: setValue}}},
			{Column: "Peak", Policy: schema.MAX, Value: codec.Int(17)},
		}}},
	}
	bundle := &Bundle{Manifest: Manifest{
		BundleID: ids.NewTxID(), SourceDomain: source, Stream: "typed-crdt", SeqFirst: 1, SeqLast: 1,
	}, Batches: []Batch{batch}}
	if err := importer.ApplyBundle(ctx, bundle); err != nil {
		t.Fatalf("apply typed CRDT bridge bundle: %v", err)
	}
	table, err := db.TableOf[typedBridgeCRDTRecord](database, "typed_crdt_rows")
	if err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(row)
	if err != nil || got.Count != 9 || len(got.Tags) != 1 || got.Tags[0] != "blue" || got.Peak != 17 {
		t.Fatalf("typed CRDT import = %+v, %v", got, err)
	}
	var foundCounter bool
	_, err = database.BridgeLogSource().ScanLog(ctx, database.NodeID(), 1, 10, 1<<20, func(mb *codec.MutationBatch) error {
		for _, mutation := range mb.Mutations {
			if mutation.TableID != 93 || mutation.RowID != row || mutation.ColumnID != 2 || mutation.Policy != schema.PN_COUNTER {
				continue
			}
			for _, causal := range mutation.Records {
				if bytes.Equal(causal.Key, countKey) && mutation.Flags&codec.FlagCRDTImport != 0 {
					foundCounter = true
				}
			}
		}
		return nil
	})
	if err != nil || !foundCounter {
		t.Fatalf("typed bridge log lost source counter causality: found=%v err=%v", foundCounter, err)
	}
}
