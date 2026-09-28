package replicateddb

import (
	"context"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/replication"
	"github.com/nomadsql/replicateddb/schema"
)

// shadowTestContacts resolves the contacts table IDs of a test DB.
func shadowTestContacts(t *testing.T, db *DB) (table, idCol, nameCol, scoreCol uint32) {
	t.Helper()
	reg := db.schemaRegistry()
	if reg == nil {
		t.Fatal("schema not ready")
	}
	ct := reg.Table("contacts")
	if ct == nil {
		t.Fatal("contacts table missing")
	}
	col := func(name string) uint32 {
		for _, c := range ct.Columns {
			if c.Name == name {
				return c.ID
			}
		}
		t.Fatalf("column %q missing", name)
		return 0
	}
	return ct.ID, col("id"), col("name"), col("score")
}

// shadowImportBatch crafts a Low import batch (values + Low provenance) as
// the bridge importer would commit it, at an explicit HLC.
func shadowImportBatch(db *DB, row ids.RowID, hlc, seq uint64, origin ids.NodeID, source ids.DBID, table, idCol, nameCol, scoreCol uint32, name string, score int64, lastSeq uint64) *codec.MutationBatch {
	prov := func(col uint32) codec.Mutation {
		return bridgePolicyMutation(table, row, col, BridgeProvenance{
			Owner: BridgeOwnerLow, SourceDomain: source, Stream: "low",
			FirstSeq: 1, LastSeq: lastSeq,
		})
	}
	mutations := []codec.Mutation{
		{TableID: table, RowID: row, ColumnID: idCol, Value: codec.Blob(row[:])},
		{TableID: table, RowID: row, ColumnID: nameCol, Value: codec.Text(name)},
		{TableID: table, RowID: row, ColumnID: scoreCol, Value: codec.Int(score)},
		prov(0), prov(nameCol), prov(scoreCol),
	}
	ident := db.schemaIdentity()
	return &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            ids.NewTxID(),
		OriginNode:      origin,
		Sequence:        seq,
		HLC:             hlc,
		SchemaEpoch:     ident.Epoch,
		SchemaHash:      ident.Hash,
		Mutations:       mutations,
	}
}

// shadowLocalBatch returns one locally committed batch from the log.
func shadowLocalBatch(t *testing.T, db *DB, seq uint64) *codec.MutationBatch {
	t.Helper()
	var out *codec.MutationBatch
	_, err := db.store.LogScan(db.cfg.NodeID, seq, 1, 1<<20, func(mb *codec.MutationBatch) error {
		out = mb
		return nil
	})
	if err != nil || out == nil {
		t.Fatalf("log scan seq %d: batch=%v err=%v", seq, out != nil, err)
	}
	return out
}

func shadowQueryName(t *testing.T, db *DB, row ids.RowID) (string, bool) {
	t.Helper()
	var name string
	err := db.QueryRowContext(context.Background(), `SELECT name FROM contacts WHERE id=?`, row[:]).Scan(&name)
	if err != nil {
		return "", false
	}
	return name, true
}

// TestBridgeShadowReorderConvergence is the acceptance test for reordered
// policy/value delivery: a High override must survive a newer-HLC Low
// import on every peer regardless of arrival order, and an explicit
// release must converge back to the Low value.
func TestBridgeShadowReorderConvergence(t *testing.T) {
	ctx := context.Background()
	newDB := func() *DB {
		cfg := testConfig(t.TempDir())
		cfg.QueryStore.RemoteApplyMaxTransactions = 1
		db, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	dbA, dbB := newDB(), newDB()
	table, idCol, nameCol, scoreCol := shadowTestContacts(t, dbA)
	shadowTestContacts(t, dbB)
	row := ids.NewRowID()
	source := ids.NewDBID()
	lowNode := ids.NewNodeID()

	// Identical seed on both peers.
	seedA := shadowImportBatch(dbA, row, 100, 1, lowNode, source, table, idCol, nameCol, scoreCol, "low-1", 1, 1)
	seedB := shadowImportBatch(dbB, row, 100, 1, lowNode, source, table, idCol, nameCol, scoreCol, "low-1", 1, 1)
	if err := dbA.ApplyRemote(ctx, seedA); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, seedB); err != nil {
		t.Fatal(err)
	}

	// High override on A (local SQL write).
	if _, err := dbA.ExecContext(ctx, `UPDATE contacts SET name=? WHERE id=?`, "high", row[:]); err != nil {
		t.Fatal(err)
	}
	overCell, ok, err := dbA.store.GetCell(table, row, nameCol)
	if err != nil || !ok {
		t.Fatalf("override cell present=%v err=%v", ok, err)
	}

	// A newer-HLC Low update, crafted once and applied in opposite orders:
	// A sees override-then-import, B sees import-then-override.
	lowHLC := overCell.Version.HLC + 1000
	upA := shadowImportBatch(dbA, row, lowHLC, 2, lowNode, source, table, idCol, nameCol, scoreCol, "low-2", 2, 2)
	upB := shadowImportBatch(dbB, row, lowHLC, 2, lowNode, source, table, idCol, nameCol, scoreCol, "low-2", 2, 2)
	if err := dbA.ApplyRemote(ctx, upA); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, upB); err != nil {
		t.Fatal(err)
	}
	// The override batch replicates to B after its import.
	if err := dbB.ApplyRemote(ctx, shadowLocalBatch(t, dbA, 1)); err != nil {
		t.Fatal(err)
	}
	waitForRemoteMaterialization(t, dbA)
	waitForRemoteMaterialization(t, dbB)

	for name, db := range map[string]*DB{"A": dbA, "B": dbB} {
		got, present := shadowQueryName(t, db, row)
		if !present || got != "high" {
			t.Fatalf("peer %s: name=%q present=%v, want High-owned %q", name, got, present, "high")
		}
		pol, ok, err := db.BridgeFieldProvenance("contacts", row, "name")
		if err != nil || !ok || pol.Owner != BridgeOwnerHigh {
			t.Fatalf("peer %s: provenance=%+v present=%v err=%v, want High", name, pol, ok, err)
		}
	}

	// Effective state survives a materializer rebuild on both peers.
	for _, db := range []*DB{dbA, dbB} {
		db.applyMu.Lock()
		rerr := db.rebuildLocked()
		db.applyMu.Unlock()
		if rerr != nil {
			t.Fatal(rerr)
		}
		got, present := shadowQueryName(t, db, row)
		if !present || got != "high" {
			t.Fatalf("after rebuild: name=%q present=%v, want %q", got, present, "high")
		}
	}

	// Explicit release converges both peers back to the Low value.
	if err := dbA.ReleaseBridgeOwnership(ctx, "contacts", row, "name"); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, shadowLocalBatch(t, dbA, 2)); err != nil {
		t.Fatal(err)
	}
	waitForRemoteMaterialization(t, dbB)
	for name, db := range map[string]*DB{"A": dbA, "B": dbB} {
		got, present := shadowQueryName(t, db, row)
		if !present || got != "low-2" {
			t.Fatalf("peer %s after release: name=%q present=%v, want %q", name, got, present, "low-2")
		}
	}
}

// shadowDeleteBatch crafts a Low delete batch (tombstone + Low row policy).
func shadowDeleteBatch(db *DB, row ids.RowID, hlc, seq uint64, origin ids.NodeID, source ids.DBID, table uint32, lastSeq uint64) *codec.MutationBatch {
	ident := db.schemaIdentity()
	return &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            ids.NewTxID(),
		OriginNode:      origin,
		Sequence:        seq,
		HLC:             hlc,
		SchemaEpoch:     ident.Epoch,
		SchemaHash:      ident.Hash,
		Mutations: []codec.Mutation{
			{TableID: table, RowID: row, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone},
			bridgePolicyMutation(table, row, 0, BridgeProvenance{
				Owner: BridgeOwnerLow, SourceDomain: source, Stream: "low",
				FirstSeq: 1, LastSeq: lastSeq, Deleted: true,
			}),
		},
	}
}

// TestBridgeShadowDeleteReorderConvergence: a Low delete cannot hide a
// High-owned row on any peer regardless of arrival order; releasing the
// field lets the delete take effect everywhere.
func TestBridgeShadowDeleteReorderConvergence(t *testing.T) {
	ctx := context.Background()
	newDB := func() *DB {
		cfg := testConfig(t.TempDir())
		cfg.QueryStore.RemoteApplyMaxTransactions = 1
		db, err := Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	dbA, dbB := newDB(), newDB()
	table, idCol, nameCol, scoreCol := shadowTestContacts(t, dbA)
	row := ids.NewRowID()
	source := ids.NewDBID()
	lowNode := ids.NewNodeID()

	if err := dbA.ApplyRemote(ctx, shadowImportBatch(dbA, row, 100, 1, lowNode, source, table, idCol, nameCol, scoreCol, "low-1", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, shadowImportBatch(dbB, row, 100, 1, lowNode, source, table, idCol, nameCol, scoreCol, "low-1", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := dbA.ExecContext(ctx, `UPDATE contacts SET name=? WHERE id=?`, "high", row[:]); err != nil {
		t.Fatal(err)
	}
	overCell, ok, err := dbA.store.GetCell(table, row, nameCol)
	if err != nil || !ok {
		t.Fatal("override cell missing")
	}
	delHLC := overCell.Version.HLC + 1000
	if err := dbA.ApplyRemote(ctx, shadowDeleteBatch(dbA, row, delHLC, 2, lowNode, source, table, 2)); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, shadowDeleteBatch(dbB, row, delHLC, 2, lowNode, source, table, 2)); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, shadowLocalBatch(t, dbA, 1)); err != nil {
		t.Fatal(err)
	}
	waitForRemoteMaterialization(t, dbA)
	waitForRemoteMaterialization(t, dbB)
	for name, db := range map[string]*DB{"A": dbA, "B": dbB} {
		got, present := shadowQueryName(t, db, row)
		if !present || got != "high" {
			t.Fatalf("peer %s: name=%q present=%v, want protected %q", name, got, present, "high")
		}
	}
	if err := dbA.ReleaseBridgeOwnership(ctx, "contacts", row, "name"); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, shadowLocalBatch(t, dbA, 2)); err != nil {
		t.Fatal(err)
	}
	waitForRemoteMaterialization(t, dbB)
	for name, db := range map[string]*DB{"A": dbA, "B": dbB} {
		if got, present := shadowQueryName(t, db, row); present {
			t.Fatalf("peer %s after release: row visible with %q, want deleted", name, got)
		}
	}
}

// TestBridgeShadowFileReorderConvergence: file metadata converges like SQL
// fields under reordered delivery, and file release restores Low metadata.
func TestBridgeShadowFileReorderConvergence(t *testing.T) {
	ctx := context.Background()
	newDB := func() *DB {
		db, err := Open(ctx, fileTestConfig(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	dbA, dbB := newDB(), newDB()
	fids, err := resolveFileIDs()
	if err != nil {
		t.Fatal(err)
	}
	row := fileRowID("doc")
	source := ids.NewDBID()
	lowNode := ids.NewNodeID()
	var lowD1, lowD2 [32]byte
	for i := range lowD1 {
		lowD1[i], lowD2[i] = 0x11, 0x22
	}
	// One shared seed: A imports locally, B takes A's batch (same TxID).
	if err := dbA.BridgeApplyFileMetadata(ctx, source, "low", ids.NewTxID(), 1, 1, false,
		[]BridgeFilePut{{Row: row, Name: "doc", Digest: lowD1, Size: 10}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, shadowLocalBatch(t, dbA, 1)); err != nil {
		t.Fatal(err)
	}

	high := uploadBytes(t, dbA, "doc", []byte("high-bytes"))
	upCell, ok, err := dbA.store.GetCell(fids.table, row, fids.digest)
	if err != nil || !ok {
		t.Fatal("upload cell missing")
	}
	lowHLC := upCell.Version.HLC + 1000
	mkUpdate := func(db *DB) *codec.MutationBatch {
		prov := func(col uint32) codec.Mutation {
			return bridgePolicyMutation(fids.table, row, col, BridgeProvenance{
				Owner: BridgeOwnerLow, SourceDomain: source, Stream: "low",
				FirstSeq: 1, LastSeq: 2,
			})
		}
		ident := db.schemaIdentity()
		return &codec.MutationBatch{
			ProtocolVersion: replication.ProtocolVersion,
			TxID:            ids.NewTxID(),
			OriginNode:      lowNode,
			Sequence:        1, // first lowNode batch on both DBs (seed came from A)
			HLC:             lowHLC,
			SchemaEpoch:     ident.Epoch,
			SchemaHash:      ident.Hash,
			Mutations: []codec.Mutation{
				{TableID: fids.table, RowID: row, ColumnID: fids.id, Value: codec.Blob(row[:])},
				{TableID: fids.table, RowID: row, ColumnID: fids.name, Value: codec.Text("doc")},
				{TableID: fids.table, RowID: row, ColumnID: fids.digest, Value: codec.Blob(lowD2[:])},
				{TableID: fids.table, RowID: row, ColumnID: fids.size, Value: codec.Int(20)},
				prov(0), prov(fids.name), prov(fids.digest), prov(fids.size),
			},
		}
	}
	if err := dbA.ApplyRemote(ctx, mkUpdate(dbA)); err != nil {
		t.Fatal(err)
	}
	if err := dbB.ApplyRemote(ctx, mkUpdate(dbB)); err != nil {
		t.Fatal(err)
	}
	// dbA local seq 1 is the file seed import; seq 2 is the upload.
	if err := dbB.ApplyRemote(ctx, shadowLocalBatch(t, dbA, 2)); err != nil {
		t.Fatal(err)
	}
	for name, db := range map[string]*DB{"A": dbA, "B": dbB} {
		st, err := db.FileStatus(ctx, "doc")
		if err != nil || !st.Exists || st.Deleted || st.Digest != high.Digest {
			t.Fatalf("peer %s: status=%+v err=%v, want High digest", name, st, err)
		}
		pol, ok, err := db.BridgeFileFieldProvenance(row, "digest")
		if err != nil || !ok || pol.Owner != BridgeOwnerHigh {
			t.Fatalf("peer %s: provenance=%+v present=%v err=%v, want High", name, pol, ok, err)
		}
	}
	listed, err := dbB.ListFiles(ctx, "", 0)
	if err != nil || len(listed) != 1 || listed[0].Digest != high.Digest {
		t.Fatalf("list=%+v err=%v, want one High file", listed, err)
	}

	// Releasing every field converges both peers back to the Low metadata.
	fields := []string{"name", "digest", "size"}
	for i, f := range fields {
		if err := dbA.ReleaseBridgeFileOwnership(ctx, row, f); err != nil {
			t.Fatal(err)
		}
		if err := dbB.ApplyRemote(ctx, shadowLocalBatch(t, dbA, uint64(3+i))); err != nil {
			t.Fatal(err)
		}
	}
	for name, db := range map[string]*DB{"A": dbA, "B": dbB} {
		st, err := db.FileStatus(ctx, "doc")
		if err != nil || !st.Exists || st.Deleted || st.Digest != lowD2 || st.Size != 20 {
			t.Fatalf("peer %s after release: status=%+v err=%v, want Low metadata", name, st, err)
		}
	}
}

func TestBridgeShadowCodec(t *testing.T) {
	lim := codec.DefaultLimits()
	for _, v := range []codec.Value{
		codec.Text("hi"), codec.Int(-7), codec.Blob([]byte{1, 2, 3}),
		codec.Blob(nil), {Type: codec.TypeNull},
	} {
		set, got, err := decodeBridgeShadow(codec.CellState{Value: encodeBridgeShadowSet(v)}, lim)
		if err != nil || !set || got.Type != v.Type || got.S != v.S || got.I != v.I || string(got.B) != string(v.B) {
			t.Fatalf("set round trip of %+v: set=%v got=%+v err=%v", v, set, got, err)
		}
	}
	set, _, err := decodeBridgeShadow(codec.CellState{Value: encodeBridgeShadowClear()}, lim)
	if err != nil || set {
		t.Fatalf("clear decode: set=%v err=%v", set, err)
	}
	bad := []codec.Value{
		{Type: codec.TypeNull},
		codec.Text("x"),
		codec.Blob(nil),
		codec.Blob([]byte{0x7f}),
		codec.Blob([]byte{bridgeShadowClear, 0x00}),
		codec.Blob([]byte{bridgeShadowSet}),
		codec.Blob([]byte{bridgeShadowSet, 0xde, 0xad}),
	}
	for i, v := range bad {
		if _, _, err := decodeBridgeShadow(codec.CellState{Value: v}, lim); err == nil {
			t.Fatalf("malformed %d decoded without error", i)
		}
	}
}

func TestBridgeShadowMapping(t *testing.T) {
	if s, ok := bridgeShadowColumn(5); !ok || s != 5^0x80000000 {
		t.Fatalf("mapping 5 -> %d,%v", s, ok)
	}
	if _, ok := bridgeShadowColumn(0x80000000); ok {
		t.Fatal("0x80000000 must not map (lands on 0)")
	}
	if _, ok := bridgeShadowColumn(0x7FFFFFFF); ok {
		t.Fatal("0x7FFFFFFF must not map (lands on ColumnTombstone)")
	}
	cols := map[uint32]bool{3: true, 9: true}
	if c, ok := bridgeShadowOf(3^0x80000000, cols); !ok || c != 3 {
		t.Fatalf("inverse: %d,%v", c, ok)
	}
	if _, ok := bridgeShadowOf(3, cols); ok {
		t.Fatal("app column misread as shadow")
	}
	if _, ok := bridgeShadowOf(77, cols); ok {
		t.Fatal("unknown column misread as shadow")
	}
	// Legacy ambiguous pair: the app column wins.
	amb := map[uint32]bool{3: true, 3 ^ 0x80000000: true}
	if _, ok := bridgeShadowOf(3^0x80000000, amb); ok {
		t.Fatal("ambiguous column must read as app column")
	}
}

func TestBridgeShadowResolveRow(t *testing.T) {
	lim := codec.DefaultLimits()
	cols := map[uint32]bool{1: true, 2: true}
	raw := map[uint32]codec.CellState{
		1:              {Value: codec.Text("low")},
		2:              {Value: codec.Text("low2")},
		1 ^ 0x80000000: {Value: encodeBridgeShadowSet(codec.Text("high"))},
		2 ^ 0x80000000: {Value: encodeBridgeShadowClear()},
	}
	eff, err := resolveBridgeRow(raw, cols, lim)
	if err != nil {
		t.Fatal(err)
	}
	if len(eff) != 2 || eff[1].Value.S != "high" || eff[2].Value.S != "low2" {
		t.Fatalf("effective=%+v", eff)
	}
	shadowed, err := bridgeRowShadowed(raw, cols, lim)
	if err != nil || !shadowed {
		t.Fatalf("shadowed=%v err=%v", shadowed, err)
	}
	delete(raw, 1^0x80000000)
	shadowed, err = bridgeRowShadowed(raw, cols, lim)
	if err != nil || shadowed {
		t.Fatalf("cleared row shadowed=%v err=%v", shadowed, err)
	}
	raw[1^0x80000000] = codec.CellState{Value: codec.Blob([]byte{0x7f})}
	if _, err := resolveBridgeRow(raw, cols, lim); err == nil {
		t.Fatal("malformed shadow resolved without error")
	}
}

func TestBridgeShadowSchemaGuards(t *testing.T) {
	// Ambiguous high-bit pairs and reserved mappings are rejected.
	mkcol := func(name string, id uint32) schema.ColumnSchema {
		return schema.ColumnSchema{Name: name, ID: id, Type: schema.ColText, Nullable: true}
	}
	bad := [][]schema.TableSchema{
		{{Name: "t", Columns: []schema.ColumnSchema{mkcol("a", 7), mkcol("b", 7^0x80000000)}}},
		{{Name: "t", Columns: []schema.ColumnSchema{mkcol("a", 0x80000000)}}},
		{{Name: "t", Columns: []schema.ColumnSchema{mkcol("a", 0x7FFFFFFF)}}},
	}
	for i, tables := range bad {
		if _, err := schema.BuildRegistry(1, tables); err == nil {
			t.Fatalf("schema %d accepted despite shadow ambiguity", i)
		}
	}
	// The reserved file IDs are shadow-safe: no reserved mappings, no
	// pairs, and no shadow lands on a file column.
	fids, err := resolveFileIDs()
	if err != nil {
		t.Fatal(err)
	}
	fcols := []uint32{fids.id, fids.name, fids.digest, fids.size}
	seen := map[uint32]bool{}
	for _, c := range fcols {
		s, ok := bridgeShadowColumn(c)
		if !ok {
			t.Fatalf("file column %d has no shadow mapping", c)
		}
		seen[c] = true
		if seen[s] {
			t.Fatalf("file shadow %d collides", s)
		}
	}
	for _, c := range fcols {
		s, _ := bridgeShadowColumn(c)
		for _, d := range fcols {
			if s == d {
				t.Fatalf("file shadow %d lands on file column %d", s, d)
			}
		}
	}
}

func TestBatchTouchesBridgePolicy(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, _, nameCol, _ := shadowTestContacts(t, db)
	row := ids.NewRowID()
	plain := []codec.Mutation{{TableID: table, RowID: row, ColumnID: nameCol, Value: codec.Text("x")}}
	if batchTouchesBridgePolicy(db, plain) {
		t.Fatal("plain batch flagged")
	}
	pol := []codec.Mutation{bridgePolicyMutation(table, row, nameCol, BridgeProvenance{Owner: BridgeOwnerLow})}
	if !batchTouchesBridgePolicy(db, pol) {
		t.Fatal("policy batch missed")
	}
	s, _ := bridgeShadowColumn(nameCol)
	sh := []codec.Mutation{{TableID: table, RowID: row, ColumnID: s, Value: encodeBridgeShadowClear()}}
	if !batchTouchesBridgePolicy(db, sh) {
		t.Fatal("shadow batch missed")
	}
}
