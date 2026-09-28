package schema

import (
	"errors"
	"strings"
	"testing"

	"github.com/nomadsql/replicateddb/ids"
)

func manifestTestTables() []TableSchema {
	return []TableSchema{{
		Name: "contacts",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColBlob},
			{Name: "name", Type: ColText, Nullable: true},
		},
	}}
}

func mustRegistry(t *testing.T, epoch uint64, tables []TableSchema) *Registry {
	t.Helper()
	reg, err := BuildRegistry(epoch, tables)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func registryTables(reg *Registry) []TableSchema {
	out := make([]TableSchema, len(reg.Tables))
	for i, p := range reg.Tables {
		out[i] = *p
		out[i].Columns = append([]ColumnSchema(nil), p.Columns...)
	}
	return out
}

func TestManifestRoundTrip(t *testing.T) {
	reg := mustRegistry(t, 3, manifestTestTables())
	m, err := NewGenesis(registryTables(reg), 3, ids.NewNodeID(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if m.Hash != reg.Hash {
		t.Fatal("manifest hash must equal registry hash for identical declarations")
	}
	enc := EncodeManifest(m)
	back, err := DecodeManifest(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !EqualRevision(m, back) {
		t.Fatal("round trip changed the revision")
	}
	if _, err := back.Registry(); err != nil {
		t.Fatal(err)
	}
	// Tampering with content-covered bytes (version, hash, tables) fails
	// closed. Authorship/time/parents bind through the revision identity
	// instead: flipping them yields a different revision ID, so existing
	// references no longer resolve to the altered bytes.
	hashByte := 4 + 8 + 16 + 8 + 7
	for _, flip := range []int{4, hashByte, len(enc) - 1} {
		bad := append([]byte(nil), enc...)
		bad[flip] ^= 0xff
		if _, err := DecodeManifest(bad); err == nil {
			t.Fatalf("tampered byte %d accepted", flip)
		}
	}
	authorFlip := append([]byte(nil), enc...)
	authorFlip[20] ^= 0xff
	alt, err := DecodeManifest(authorFlip)
	if err != nil {
		t.Fatal(err)
	}
	if EqualRevision(m, alt) {
		t.Fatal("altered authorship kept the same revision identity")
	}
	if _, err := DecodeManifest(enc[:len(enc)/2]); err == nil {
		t.Fatal("truncated manifest accepted")
	}
	if _, err := DecodeManifest(append(enc, 0)); err == nil {
		t.Fatal("manifest with trailing bytes accepted")
	}
}

func TestManifestHashStableAcrossOrder(t *testing.T) {
	a := []TableSchema{
		{Name: "b", Columns: []ColumnSchema{{Name: "id", Type: ColBlob}, {Name: "x", Type: ColText, Nullable: true}}},
		{Name: "a", Columns: []ColumnSchema{{Name: "id", Type: ColBlob}}},
	}
	b := []TableSchema{
		{Name: "a", Columns: []ColumnSchema{{Name: "id", Type: ColBlob}}},
		{Name: "b", Columns: []ColumnSchema{{Name: "x", Type: ColText, Nullable: true}, {Name: "id", Type: ColBlob}}},
	}
	ra := mustRegistry(t, 1, a)
	rb := mustRegistry(t, 1, b)
	ma, err := NewGenesis(registryTables(ra), 1, ids.NodeID{1}, 10)
	if err != nil {
		t.Fatal(err)
	}
	mb, err := NewGenesis(registryTables(rb), 1, ids.NodeID{2}, 99)
	if err != nil {
		t.Fatal(err)
	}
	// Same content hashes identically despite order/authorship/time.
	if ma.Hash != mb.Hash {
		t.Fatal("content hash depends on declaration order or authorship")
	}
	// Revision identity binds author and time.
	if EqualRevision(ma, mb) {
		t.Fatal("distinct authorships share a revision identity")
	}
}

func TestAuthoredRevisionAdditive(t *testing.T) {
	node := ids.NewNodeID()
	reg := mustRegistry(t, 1, manifestTestTables())
	gen, err := NewGenesis(registryTables(reg), 1, node, 10)
	if err != nil {
		t.Fatal(err)
	}
	next := []TableSchema{{
		Name: "contacts",
		Columns: []ColumnSchema{
			{Name: "id", Type: ColBlob},
			{Name: "name", Type: ColText, Nullable: true},
			{Name: "phone", Type: ColText, Nullable: true},
		},
	}}
	assigned, err := AssignIDs(gen.Tables, next)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := NewAuthoredRevision(gen, registryTables(mustRegistry(t, 2, assigned)), node, 20)
	if err != nil {
		t.Fatal(err)
	}
	if rev.Version != 2 || len(rev.Parents) != 1 || rev.Parents[0] != RevisionID(gen) {
		t.Fatal("authored revision must chain the parent at version+1")
	}
	if !IsSuperset(gen.Tables, rev.Tables) {
		t.Fatal("authored revision must superset its parent")
	}
}

func TestAuthoredRevisionRejectsDestructive(t *testing.T) {
	node := ids.NewNodeID()
	reg := mustRegistry(t, 1, manifestTestTables())
	gen, err := NewGenesis(registryTables(reg), 1, node, 10)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]TableSchema{
		"drop column": {{
			Name:    "contacts",
			Columns: []ColumnSchema{{Name: "id", Type: ColBlob}},
		}},
		"change type": {{
			Name: "contacts",
			Columns: []ColumnSchema{
				{Name: "id", Type: ColBlob},
				{Name: "name", Type: ColInteger, Nullable: true},
			},
		}},
		"change nullability": {{
			Name: "contacts",
			Columns: []ColumnSchema{
				{Name: "id", Type: ColBlob},
				{Name: "name", Type: ColText},
			},
		}},
		"drop table": {},
	}
	for name, tables := range cases {
		assigned, err := AssignIDs(gen.Tables, tables)
		if err != nil {
			continue // ID-level rejection is also fail-closed
		}
		if _, err := NewAuthoredRevision(gen, assigned, node, 20); err == nil {
			t.Fatalf("%s accepted", name)
		} else if !errors.Is(err, ErrUnsupportedSchema) {
			t.Fatalf("%s: %v is not ErrUnsupportedSchema", name, err)
		}
	}
}

func TestAssignIDsPreservesStableIDs(t *testing.T) {
	reg := mustRegistry(t, 1, manifestTestTables())
	gen, err := NewGenesis(registryTables(reg), 1, ids.NewNodeID(), 10)
	if err != nil {
		t.Fatal(err)
	}
	next := []TableSchema{{
		Name: "CONTACTS", // case-insensitive match preserves IDs
		Columns: []ColumnSchema{
			{Name: "ID", Type: ColBlob},
			{Name: "Name", Type: ColText, Nullable: true},
			{Name: "phone", Type: ColText, Nullable: true},
		},
	}}
	assigned, err := AssignIDs(gen.Tables, next)
	if err != nil {
		t.Fatal(err)
	}
	if assigned[0].ID != gen.Tables[0].ID {
		t.Fatal("table id not preserved")
	}
	if assigned[0].Columns[0].ID != gen.Tables[0].Columns[0].ID {
		t.Fatal("column id not preserved")
	}
	if assigned[0].Columns[2].ID != 0 {
		t.Fatal("new column should keep zero id for derivation")
	}
	// Explicit ID change fails.
	bad := cloneTables(next)
	bad[0].Columns[0].ID = 4242
	if _, err := AssignIDs(gen.Tables, bad); err == nil {
		t.Fatal("explicit column id change accepted")
	}
}

func TestUnionConflictsIdentifyObject(t *testing.T) {
	base := registryTables(mustRegistry(t, 1, manifestTestTables()))
	renameID := cloneTables(base)
	renameID[0].ID++
	typeChange := cloneTables(base)
	typeChange[0].Columns[1].Type = ColInteger
	nullChange := cloneTables(base)
	nullChange[0].Columns[1].Nullable = false
	pkChange := cloneTables(base)
	pkChange[0].PK = pkChange[0].Columns[1].ID
	idCollision := cloneTables(base)
	idCollision[0].Columns = append(idCollision[0].Columns, ColumnSchema{
		Name: "other", Type: ColText, ID: base[0].Columns[0].ID, Nullable: true,
	})
	for name, other := range map[string][]TableSchema{
		"table id":    renameID,
		"type":        typeChange,
		"nullability": nullChange,
		"pk":          pkChange,
		"id reuse":    idCollision,
	} {
		if _, err := UnionTables(base, other); err == nil {
			t.Fatalf("%s conflict accepted", name)
		} else if !errors.Is(err, ErrUnsupportedSchema) || !strings.Contains(err.Error(), "contacts") {
			t.Fatalf("%s: error %v does not identify the object", name, err)
		}
	}
}

func TestDeriveMergeDeterministic(t *testing.T) {
	nodeA := ids.NodeID{1}
	nodeB := ids.NodeID{2}
	reg := mustRegistry(t, 1, manifestTestTables())
	gen, err := NewGenesis(registryTables(reg), 1, nodeA, 10)
	if err != nil {
		t.Fatal(err)
	}
	branch := func(node ids.NodeID, tm uint64, col string) *Manifest {
		next := cloneTables(gen.Tables)
		next[0].Columns = append(next[0].Columns, ColumnSchema{Name: col, Type: ColText, Nullable: true})
		assigned, err := AssignIDs(gen.Tables, next)
		if err != nil {
			t.Fatal(err)
		}
		r := mustRegistry(t, 2, assigned)
		rev, err := NewAuthoredRevision(gen, registryTables(r), node, tm)
		if err != nil {
			t.Fatal(err)
		}
		return rev
	}
	revA := branch(nodeA, 20, "phone")
	revB := branch(nodeB, 30, "email")
	// Same epoch, different content: the ambiguous case ancestry resolves.
	if revA.Version != revB.Version || revA.Hash == revB.Hash {
		t.Fatal("test setup must share an epoch with different content")
	}
	revs := map[[32]byte]*Manifest{
		RevisionID(gen):  gen,
		RevisionID(revA): revA,
		RevisionID(revB): revB,
	}
	frontier, err := Frontier(revs, RevisionID(revA), RevisionID(revB))
	if err != nil {
		t.Fatal(err)
	}
	if len(frontier) != 2 {
		t.Fatalf("frontier has %d members", len(frontier))
	}
	mergeAB, err := DeriveMerge(revs, frontier)
	if err != nil {
		t.Fatal(err)
	}
	// Order independence: reversed frontier derives the identical merge.
	mergeBA, err := DeriveMerge(revs, [][32]byte{frontier[1], frontier[0]})
	if err != nil {
		t.Fatal(err)
	}
	if !EqualRevision(mergeAB, mergeBA) {
		t.Fatal("merge depends on frontier order")
	}
	if mergeAB.Version != 3 {
		t.Fatalf("merge version = %d, want 3", mergeAB.Version)
	}
	if mergeAB.CreatedOnNode != nodeB || mergeAB.TimeCreated != 30 {
		t.Fatal("merge authorship must come from the greatest frontier tuple")
	}
	if len(mergeAB.Tables[0].Columns) != 4 {
		t.Fatalf("merge has %d columns, want 4", len(mergeAB.Tables[0].Columns))
	}
	// Coverage: the published merge has both branches as ancestors, so
	// re-receiving either branch is a no-op for a node at the merge tip.
	// Frontiers flatten synthetic merges back to authored revisions.
	revs[RevisionID(mergeAB)] = mergeAB
	for _, br := range [][32]byte{RevisionID(revA), RevisionID(revB)} {
		if ok, err := AncestorOf(revs, br, RevisionID(mergeAB)); err != nil || !ok {
			t.Fatal("merge must cover both branches")
		}
	}
	f2, err := Frontier(revs, RevisionID(mergeAB))
	if err != nil {
		t.Fatal(err)
	}
	if len(f2) != 2 {
		t.Fatalf("merge tip must flatten to 2 authored revisions, got %d", len(f2))
	}
	if ok, err := AncestorOf(revs, RevisionID(gen), RevisionID(mergeAB)); err != nil || !ok {
		t.Fatal("genesis must be a merge ancestor")
	}
	if ok, err := AncestorOf(revs, RevisionID(revA), RevisionID(revB)); err != nil || ok {
		t.Fatal("concurrent branches must not be ancestors")
	}
}

func TestMigrationDDL(t *testing.T) {
	old := registryTables(mustRegistry(t, 1, manifestTestTables()))
	next := cloneTables(old)
	next[0].Columns = append(next[0].Columns, ColumnSchema{Name: "phone", Type: ColText, Nullable: true, ID: 777})
	next = append(next, TableSchema{
		Name: "orders",
		ID:   888,
		PK:   889,
		Columns: []ColumnSchema{
			{Name: "id", Type: ColBlob, ID: 889},
			{Name: "total", Type: ColInteger, ID: 890, Nullable: true},
		},
	})
	ddl, err := MigrationDDL(old, next)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(ddl, "\n")
	if !strings.Contains(joined, "ALTER TABLE \"contacts\" ADD COLUMN \"phone\" TEXT") {
		t.Fatalf("missing add-column DDL: %v", ddl)
	}
	if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS \"orders\"") {
		t.Fatalf("missing create-table DDL: %v", ddl)
	}
	if _, err := MigrationDDL(next, old); err == nil {
		t.Fatal("destructive DDL accepted")
	}
}

func TestFrontierUnknownRevisionFails(t *testing.T) {
	if _, err := Frontier(nil, [32]byte{1}); err == nil {
		t.Fatal("unknown revision accepted")
	}
}
