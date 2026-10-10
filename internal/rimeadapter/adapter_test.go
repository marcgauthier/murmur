package rimeadapter

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
)

type adapterRecord struct {
	ID   ids.RowID `rime:"primary"`
	Name string
}

func TestFailedSpoolAppendReturnsUncertainOutcome(t *testing.T) {
	node := ids.NewNodeID()
	private := testidentity.Key(node)
	opts := recordcodec.CompileOptions{TableID: 78, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}}
	record, err := recordcodec.Compile(reflectType[adapterRecord](), opts)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := recordcodec.MarshalDescriptor(record)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := schema.NewGenesis([]schema.TableSchema{{
		ID: 78, Name: "records", PK: 1, RecordDescriptor: desc,
		Columns: []schema.ColumnSchema{
			{ID: 1, Name: "id", Type: schema.ColBlob, MergePolicy: schema.LWW},
			{ID: 2, Name: "name", Type: schema.ColBlob, MergePolicy: schema.LWW},
		},
	}}, 1, node, 1)
	if err != nil {
		t.Fatal(err)
	}
	var failAppend atomic.Bool
	faults := &spool.FaultHooks{Append: func() error {
		if failAppend.Load() {
			return errors.New("injected append failure")
		}
		return nil
	}}
	store, err := state.Open(t.TempDir(), node, testidentity.DBID, state.Options{
		OriginSigning: testidentity.Config(node), Limits: codec.DefaultLimits(),
		Spool: spool.Options{Encryption: spool.EncryptionAES256GCM, MasterKey: private[:32], Faults: faults},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.StoreSchemaRevision(manifest); err != nil {
		t.Fatal(err)
	}
	rdb := rime.New()
	defer rdb.Close()
	a, err := New(store, rdb, manifest, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	table, err := Register[adapterRecord](a, "records", opts)
	if err != nil {
		t.Fatal(err)
	}
	failAppend.Store(true)
	value := &adapterRecord{ID: ids.NewRowID(), Name: "uncertain"}
	err = a.Write(context.Background(), func(tx *Tx) error { return table.Insert(tx, value) })
	if !errors.Is(err, ErrUncertain) {
		t.Fatalf("Write error = %v, want uncertain outcome", err)
	}
	var uncertain *UncertainCommitError
	if !errors.As(err, &uncertain) || uncertain.TxID.IsZero() {
		t.Fatalf("Write error lacks transaction identity: %T %v", err, err)
	}
	if _, err := table.inner.Get(value.ID); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("failed durable write was published into RIME: %v", err)
	}
	if err := a.Write(context.Background(), func(*Tx) error { return nil }); !errors.Is(err, ErrMaterializer) {
		t.Fatalf("adapter after uncertain storage error = %v, want failed closed", err)
	}
}

func TestDurableWritePublishesAndRollbackHasNoEffect(t *testing.T) {
	ctx := context.Background()
	node := ids.NewNodeID()
	storePath := t.TempDir()
	opts := recordcodec.CompileOptions{TableID: 77, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}}
	record, err := recordcodec.Compile(reflectType[adapterRecord](), opts)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := recordcodec.MarshalDescriptor(record)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := schema.NewGenesis([]schema.TableSchema{{
		ID: 77, Name: "records", PK: 1, RecordDescriptor: desc,
		Columns: []schema.ColumnSchema{
			{ID: 1, Name: "id", Type: schema.ColBlob, MergePolicy: schema.LWW},
			{ID: 2, Name: "name", Type: schema.ColBlob, MergePolicy: schema.LWW},
		},
	}}, 1, node, 1)
	if err != nil {
		t.Fatal(err)
	}
	private := testidentity.Key(node)
	store, err := state.Open(storePath, node, testidentity.DBID, state.Options{
		OriginSigning: testidentity.Config(node), Limits: codec.DefaultLimits(),
		Spool: spool.Options{Encryption: spool.EncryptionAES256GCM, MasterKey: private[:32]},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := store.StoreSchemaRevision(manifest); err != nil {
		t.Fatal(err)
	}
	rdb := rime.New()
	defer func() { rdb.Close() }()
	a, err := New(store, rdb, manifest, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	table, err := Register[adapterRecord](a, "records", opts)
	if err != nil {
		t.Fatal(err)
	}
	row := &adapterRecord{ID: ids.NewRowID(), Name: "persisted"}
	if err := a.Write(ctx, func(tx *Tx) error { return table.Insert(tx, row) }); err != nil {
		t.Fatal(err)
	}
	got, err := table.inner.Get(row.ID)
	if err != nil || got.Name != row.Name {
		t.Fatalf("RIME read = %#v, %v", got, err)
	}
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	concurrent := []*adapterRecord{{ID: ids.NewRowID(), Name: "parallel-a"}, {ID: ids.NewRowID(), Name: "parallel-b"}}
	var writers sync.WaitGroup
	for _, value := range concurrent {
		value := value
		writers.Add(1)
		go func() {
			defer writers.Done()
			results <- a.Write(ctx, func(tx *Tx) error {
				ready <- struct{}{}
				<-release
				return table.Insert(tx, value)
			})
		}()
	}
	for range concurrent {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			close(release)
			writers.Wait()
			t.Fatal("adapter serialized transaction callbacks")
		}
	}
	close(release)
	writers.Wait()
	for range concurrent {
		if err := <-results; err != nil {
			t.Fatalf("concurrent adapter write: %v", err)
		}
	}
	name, ok, err := store.GetCell(77, row.ID, 2)
	if err != nil || !ok || name.Value.Type != codec.TypeBlob {
		t.Fatalf("durable field = %#v, %v, %v", name, ok, err)
	}
	before, ok, err := store.GetCell(77, row.ID, 2)
	if err != nil || !ok {
		t.Fatalf("GetCell before rollback: %v %v", ok, err)
	}
	stop := errors.New("abort callback")
	err = a.Write(ctx, func(tx *Tx) error {
		if err := table.Upsert(tx, &adapterRecord{ID: row.ID, Name: "discarded"}); err != nil {
			return err
		}
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Write error = %v, want callback error", err)
	}
	got, err = table.inner.Get(row.ID)
	if err != nil || got.Name != row.Name {
		t.Fatalf("callback rollback changed RIME record: %#v, %v", got, err)
	}
	after, ok, err := store.GetCell(77, row.ID, 2)
	if err != nil || !ok || !before.Value.Equal(after.Value) {
		t.Fatalf("rollback changed durable cell: before=%#v after=%#v err=%v", before, after, err)
	}
	primaryBefore, primaryOK, err := store.GetCell(77, row.ID, 1)
	if err != nil || !primaryOK {
		t.Fatalf("GetCell primary before update: %v %v", primaryOK, err)
	}
	updated := &adapterRecord{ID: row.ID, Name: "updated"}
	if err := a.Write(ctx, func(tx *Tx) error { return table.Upsert(tx, updated) }); err != nil {
		t.Fatal(err)
	}
	row = updated
	primaryAfter, primaryOK, err := store.GetCell(77, row.ID, 1)
	if err != nil || !primaryOK || !primaryBefore.Value.Equal(primaryAfter.Value) || primaryBefore.Version == primaryAfter.Version {
		t.Fatalf("immutable primary field was not retained and re-emitted: before=%#v after=%#v err=%v", primaryBefore, primaryAfter, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = state.Open(storePath, node, testidentity.DBID, state.Options{
		OriginSigning: testidentity.Config(node), Limits: codec.DefaultLimits(),
		Spool: spool.Options{Encryption: spool.EncryptionAES256GCM, MasterKey: private[:32]},
	})
	if err != nil {
		t.Fatalf("reopen encrypted Spool: %v", err)
	}
	rdb.Close()
	rdb = rime.New()
	a, err = New(store, rdb, manifest, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	table, err = Register[adapterRecord](a, "records", opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Rebuild(ctx, 1); err != nil {
		t.Fatalf("rebuild from reopened Spool: %v", err)
	}
	got, err = table.inner.Get(row.ID)
	if err != nil || got.Name != row.Name {
		t.Fatalf("rebuilt RIME read = %#v, %v", got, err)
	}
	for _, expected := range concurrent {
		got, err = table.inner.Get(expected.ID)
		if err != nil || got.Name != expected.Name {
			t.Fatalf("rebuilt concurrent record = %#v, %v", got, err)
		}
	}
}

func TestApplyRemoteTombstoneRemovesTypedRow(t *testing.T) {
	ctx := context.Background()
	node := ids.NewNodeID()
	private := testidentity.Key(node)
	opts := recordcodec.CompileOptions{TableID: 79, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}}
	record, err := recordcodec.Compile(reflectType[adapterRecord](), opts)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := recordcodec.MarshalDescriptor(record)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := schema.NewGenesis([]schema.TableSchema{{
		ID: 79, Name: "records", PK: 1, RecordDescriptor: desc,
		Columns: []schema.ColumnSchema{
			{ID: 1, Name: "id", Type: schema.ColBlob, MergePolicy: schema.LWW},
			{ID: 2, Name: "name", Type: schema.ColBlob, MergePolicy: schema.LWW},
		},
	}}, 1, node, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(t.TempDir(), node, testidentity.DBID, state.Options{
		OriginSigning: testidentity.Config(node), Limits: codec.DefaultLimits(),
		Spool: spool.Options{Encryption: spool.EncryptionAES256GCM, MasterKey: private[:32]},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.StoreSchemaRevision(manifest); err != nil {
		t.Fatal(err)
	}
	rdb := rime.New()
	defer rdb.Close()
	a, err := New(store, rdb, manifest, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	table, err := Register[adapterRecord](a, "records", opts)
	if err != nil {
		t.Fatal(err)
	}
	row := &adapterRecord{ID: ids.NewRowID(), Name: "will be deleted"}
	if err := a.Write(ctx, func(tx *Tx) error { return table.Insert(tx, row) }); err != nil {
		t.Fatal(err)
	}
	if _, err := table.inner.Get(row.ID); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	remote := ids.NewNodeID()
	epoch, hash, err := store.SchemaEpoch()
	if err != nil {
		t.Fatal(err)
	}
	batch := &codec.MutationBatch{
		DBID: store.DBID(), OriginNode: remote, Sequence: 1, TxID: ids.NewTxID(),
		HLC: store.ClockNow() + 10, SchemaEpoch: epoch, SchemaHash: hash,
		Mutations: []codec.Mutation{{Policy: schema.LWW, TableID: 79, RowID: row.ID, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone}},
	}
	result, err := store.CommitRemote(ctx, testidentity.Sign(batch, store.DBID()))
	if err != nil {
		t.Fatalf("accept remote tombstone: %v", err)
	}
	if !result.Applied || len(result.Winners) != 1 {
		t.Fatalf("remote merge result = %#v", result)
	}
	if err := a.ApplyRemote(ctx, result); err != nil {
		t.Fatalf("materialize remote tombstone: %v", err)
	}
	if _, err := table.inner.Get(row.ID); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("row after remote tombstone = %v, want not found", err)
	}
	rows, err := store.GetRows([]state.RowRef{{Table: 79, ID: row.ID}})
	if err != nil || len(rows) != 1 || rows[0].Visible() {
		t.Fatalf("authoritative tombstone row = %#v, %v", rows, err)
	}

	// A tombstone may be the first mutation this node receives for a row.
	// Applying the authoritative snapshot must treat its absent RIME row as
	// already reconciled rather than failing the materializer.
	missingRow := ids.NewRowID()
	missingBatch := &codec.MutationBatch{
		DBID: store.DBID(), OriginNode: remote, Sequence: 2, TxID: ids.NewTxID(),
		HLC: batch.HLC + 10, SchemaEpoch: epoch, SchemaHash: hash,
		Mutations: []codec.Mutation{{Policy: schema.LWW, TableID: 79, RowID: missingRow, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone}},
	}
	missingResult, err := store.CommitRemote(ctx, testidentity.Sign(missingBatch, store.DBID()))
	if err != nil {
		t.Fatalf("accept tombstone for unmaterialized row: %v", err)
	}
	if err := a.ApplyRemote(ctx, missingResult); err != nil {
		t.Fatalf("materialize tombstone for unmaterialized row: %v", err)
	}
	if _, err := table.inner.Get(missingRow); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("unmaterialized row after tombstone = %v, want not found", err)
	}

	// A winning payload cannot move one durable row into a different RIME key.
	otherRow := ids.NewRowID()
	badRecord := &adapterRecord{ID: ids.NewRowID(), Name: "mis-keyed"}
	idPayload, err := recordcodec.EncodeField(record, 1, badRecord, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	namePayload, err := recordcodec.EncodeField(record, 2, badRecord, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	badBatch := &codec.MutationBatch{
		DBID: store.DBID(), OriginNode: remote, Sequence: 3, TxID: ids.NewTxID(),
		HLC: batch.HLC + 20, SchemaEpoch: epoch, SchemaHash: hash,
		Mutations: []codec.Mutation{
			{Policy: schema.LWW, TableID: 79, RowID: otherRow, ColumnID: 1, Value: codec.Blob(idPayload)},
			{Policy: schema.LWW, TableID: 79, RowID: otherRow, ColumnID: 2, Value: codec.Blob(namePayload)},
		},
	}
	badResult, err := store.CommitRemote(ctx, testidentity.Sign(badBatch, store.DBID()))
	if err != nil {
		t.Fatalf("accept malformed row identity fixture: %v", err)
	}
	if err := a.ApplyRemote(ctx, badResult); !errors.Is(err, ErrMaterializer) {
		t.Fatalf("mis-keyed remote record apply = %v, want failed-closed materialization error", err)
	}
}

func TestLocalUpdateAfterConcurrentTombstoneKeepsPrimaryFieldForRebuild(t *testing.T) {
	ctx := context.Background()
	node := ids.NewNodeID()
	private := testidentity.Key(node)
	opts := recordcodec.CompileOptions{TableID: 81, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}}
	record, err := recordcodec.Compile(reflectType[adapterRecord](), opts)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := recordcodec.MarshalDescriptor(record)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := schema.NewGenesis([]schema.TableSchema{{
		ID: 81, Name: "records", PK: 1, RecordDescriptor: desc,
		Columns: []schema.ColumnSchema{
			{ID: 1, Name: "id", Type: schema.ColBlob, MergePolicy: schema.LWW},
			{ID: 2, Name: "name", Type: schema.ColBlob, MergePolicy: schema.LWW},
		},
	}}, 1, node, 1)
	if err != nil {
		t.Fatal(err)
	}
	storePath := t.TempDir()
	storeOptions := state.Options{
		OriginSigning: testidentity.Config(node), Limits: codec.DefaultLimits(),
		Spool: spool.Options{Encryption: spool.EncryptionAES256GCM, MasterKey: private[:32]},
	}
	store, err := state.Open(storePath, node, testidentity.DBID, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			_ = store.Close()
		}
	})
	if err := store.StoreSchemaRevision(manifest); err != nil {
		t.Fatal(err)
	}
	rdb := rime.New()
	defer rdb.Close()
	a, err := New(store, rdb, manifest, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	table, err := Register[adapterRecord](a, "records", opts)
	if err != nil {
		t.Fatal(err)
	}
	row := &adapterRecord{ID: ids.NewRowID(), Name: "before tombstone"}
	if err := a.Write(ctx, func(tx *Tx) error { return table.Insert(tx, row) }); err != nil {
		t.Fatal(err)
	}

	// Simulate the race where durable state receives a remote tombstone while
	// the local RIME materialization still contains the row being updated.
	remote := ids.NewNodeID()
	epoch, hash, err := store.SchemaEpoch()
	if err != nil {
		t.Fatal(err)
	}
	tombstone := &codec.MutationBatch{
		DBID: store.DBID(), OriginNode: remote, Sequence: 1, TxID: ids.NewTxID(),
		HLC: store.ClockNow() + 10, SchemaEpoch: epoch, SchemaHash: hash,
		Mutations: []codec.Mutation{{Policy: schema.LWW, TableID: 81, RowID: row.ID, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone}},
	}
	if _, err := store.CommitRemote(ctx, testidentity.Sign(tombstone, store.DBID())); err != nil {
		t.Fatalf("commit concurrent remote tombstone: %v", err)
	}
	updated := &adapterRecord{ID: row.ID, Name: "resurrected after tombstone"}
	if err := a.Write(ctx, func(tx *Tx) error { return table.Upsert(tx, updated) }); err != nil {
		t.Fatalf("local resurrection update: %v", err)
	}

	rows, err := store.GetRows([]state.RowRef{{Table: 81, ID: row.ID}})
	if err != nil || len(rows) != 1 || !rows[0].Visible() {
		t.Fatalf("resurrected authoritative row = %#v, %v", rows, err)
	}
	if _, ok := rows[0].Cells[1]; !ok {
		t.Fatal("resurrected durable row lost its primary-key field")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close durable store before reopen: %v", err)
	}
	store = nil
	store, err = state.Open(storePath, node, testidentity.DBID, storeOptions)
	if err != nil {
		t.Fatalf("reopen durable store after tombstone resurrection: %v", err)
	}

	// Startup rebuild must be able to recover the resurrected row from only
	// the re-opened encrypted store, including its immutable identity.
	rebuiltDB := rime.New()
	defer rebuiltDB.Close()
	rebuiltAdapter, err := New(store, rebuiltDB, manifest, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	rebuiltTable, err := Register[adapterRecord](rebuiltAdapter, "records", opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := rebuiltAdapter.Rebuild(ctx, 16); err != nil {
		t.Fatalf("rebuild after tombstone resurrection: %v", err)
	}
	got, err := rebuiltTable.inner.Get(row.ID)
	if err != nil || got.Name != updated.Name {
		t.Fatalf("rebuilt resurrected row = %#v, %v", got, err)
	}
}

func TestApplyRemoteStreamsRowsBeyondBulkReadLimit(t *testing.T) {
	ctx := context.Background()
	node := ids.NewNodeID()
	private := testidentity.Key(node)
	opts := recordcodec.CompileOptions{TableID: 80, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}}
	record, err := recordcodec.Compile(reflectType[adapterRecord](), opts)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := recordcodec.MarshalDescriptor(record)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := schema.NewGenesis([]schema.TableSchema{{
		ID: 80, Name: "records", PK: 1, RecordDescriptor: desc,
		Columns: []schema.ColumnSchema{
			{ID: 1, Name: "id", Type: schema.ColBlob, MergePolicy: schema.LWW},
			{ID: 2, Name: "name", Type: schema.ColBlob, MergePolicy: schema.LWW},
		},
	}}, 1, node, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(t.TempDir(), node, testidentity.DBID, state.Options{
		OriginSigning: testidentity.Config(node), Limits: codec.DefaultLimits(),
		Spool: spool.Options{Encryption: spool.EncryptionAES256GCM, MasterKey: private[:32]},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.StoreSchemaRevision(manifest); err != nil {
		t.Fatal(err)
	}
	rdb := rime.New()
	defer rdb.Close()
	a, err := New(store, rdb, manifest, nil, recordcodec.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	table, err := Register[adapterRecord](a, "records", opts)
	if err != nil {
		t.Fatal(err)
	}

	const rowCount = state.MaxRowsPerRead + 1
	rows := make([]*adapterRecord, rowCount)
	epoch, hash, err := store.SchemaEpoch()
	if err != nil {
		t.Fatal(err)
	}
	batch := &codec.MutationBatch{
		DBID: store.DBID(), OriginNode: node, TxID: ids.NewTxID(), HLC: store.ClockNow(),
		SchemaEpoch: epoch, SchemaHash: hash,
		Mutations: make([]codec.Mutation, 0, rowCount*2),
	}
	for i := range rows {
		rows[i] = &adapterRecord{ID: ids.NewRowID(), Name: fmt.Sprintf("remote-%d", i)}
		for _, fieldID := range []uint32{1, 2} {
			payload, err := recordcodec.EncodeField(record, fieldID, rows[i], nil, recordcodec.Limits{})
			if err != nil {
				t.Fatalf("encode row %d field %d: %v", i, fieldID, err)
			}
			batch.Mutations = append(batch.Mutations, codec.Mutation{
				TableID: 80, RowID: rows[i].ID, ColumnID: fieldID,
				Policy: schema.LWW, Value: codec.Blob(payload),
			})
		}
	}
	result, err := store.CommitLocal(ctx, batch)
	if err != nil {
		t.Fatalf("accept %d-row durable batch: %v", rowCount, err)
	}
	if len(result.Winners) < rowCount {
		t.Fatalf("accepted batch produced %d winners, want at least %d", len(result.Winners), rowCount)
	}
	if err := a.ApplyRemote(ctx, result); err != nil {
		t.Fatalf("materialize %d rows in one RIME transaction: %v", rowCount, err)
	}
	for _, index := range []int{0, state.MaxRowsPerRead, rowCount - 1} {
		got, err := table.inner.Get(rows[index].ID)
		if err != nil || got.Name != rows[index].Name {
			t.Fatalf("materialized row %d = %#v, %v; want %q", index, got, err, rows[index].Name)
		}
	}
}

func reflectType[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }
