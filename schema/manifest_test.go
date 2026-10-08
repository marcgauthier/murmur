package schema

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
	"github.com/marcgauthier/murmur/rime"
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
		out[i].RecordDescriptor = append([]byte(nil), p.RecordDescriptor...)
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

func TestRichRecordDescriptorManifestRoundTrip(t *testing.T) {
	type row struct {
		ID   [16]byte
		Name string
	}
	type changedRow struct {
		ID   [16]byte
		Name int
	}
	makeDescriptor := func(rt reflect.Type) []byte {
		t.Helper()
		compiled, e := recordcodec.Compile(rt, recordcodec.CompileOptions{TableID: 4, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2}})
		if e != nil {
			t.Fatal(e)
		}
		raw, e := recordcodec.MarshalDescriptor(compiled)
		if e != nil {
			t.Fatal(e)
		}
		return raw
	}
	tables := manifestTestTables()
	tables[0].ID, tables[0].PK = 4, 1
	tables[0].Columns[0].ID, tables[0].Columns[1].ID = 1, 2
	tables[0].RecordDescriptor = makeDescriptor(reflect.TypeFor[row]())
	reg, err := BuildRegistry(1, tables)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reg.Table("contacts").RecordDescriptor, tables[0].RecordDescriptor) {
		t.Fatal("registry dropped the rich record descriptor")
	}
	m, err := NewGenesis(tables, 1, ids.NodeID{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw := EncodeManifest(m)
	if string(raw[:4]) != "SMF3" {
		t.Fatalf("rich schema manifest marker %q", raw[:4])
	}
	back, err := DecodeManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back.Tables[0].RecordDescriptor, tables[0].RecordDescriptor) || back.Hash != m.Hash {
		t.Fatal("rich descriptor/hash did not round trip")
	}
	changed := cloneTables(tables)
	changed[0].RecordDescriptor = makeDescriptor(reflect.TypeFor[changedRow]())
	if _, err = NewAuthoredRevision(m, changed, ids.NodeID{}, 2); err == nil {
		t.Fatal("incompatible descriptor change accepted")
	}
}

func TestRichManifestAdditiveBranchesMerge(t *testing.T) {
	type base struct {
		ID   [16]byte
		Name string
	}
	type ageRow struct {
		ID   [16]byte
		Name string
		Age  rime.Optional[int]
	}
	type cityRow struct {
		ID   [16]byte
		Name string
		City rime.Optional[string]
	}
	compile := func(rt reflect.Type, fields map[string]uint32) []byte {
		t.Helper()
		s, e := recordcodec.Compile(rt, recordcodec.CompileOptions{TableID: 44, PrimaryField: "ID", FieldIDs: fields})
		if e != nil {
			t.Fatal(e)
		}
		b, e := recordcodec.MarshalDescriptor(s)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	baseDesc := compile(reflect.TypeFor[base](), map[string]uint32{"ID": 1, "Name": 2})
	ageDesc := compile(reflect.TypeFor[ageRow](), map[string]uint32{"ID": 1, "Name": 2, "Age": 10})
	cityDesc := compile(reflect.TypeFor[cityRow](), map[string]uint32{"ID": 1, "Name": 2, "City": 11})
	if !recordcodec.DescriptorSuperset(baseDesc, ageDesc) {
		t.Fatal("base is not a descriptor superset of age extension")
	}
	if !recordcodec.DescriptorSuperset(baseDesc, cityDesc) {
		t.Fatal("base is not a descriptor superset of city extension")
	}
	tables := manifestTestTables()
	tables[0].ID, tables[0].PK = 8, 1
	tables[0].Columns[0].ID, tables[0].Columns[1].ID = 1, 2
	tables[0].RecordDescriptor = baseDesc
	gen, err := NewGenesis(tables, 1, ids.NodeID{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	ageTables := cloneTables(gen.Tables)
	ageTables[0].RecordDescriptor = ageDesc
	cityTables := cloneTables(gen.Tables)
	cityTables[0].RecordDescriptor = cityDesc
	a, err := NewAuthoredRevision(gen, ageTables, ids.NodeID{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewAuthoredRevision(gen, cityTables, ids.NodeID{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	revs := map[[32]byte]*Manifest{RevisionID(gen): gen, RevisionID(a): a, RevisionID(b): b}
	merged, err := DeriveMerge(revs, [][32]byte{RevisionID(a), RevisionID(b)})
	if err != nil {
		t.Fatal(err)
	}
	if !recordcodec.DescriptorSuperset(ageDesc, merged.Tables[0].RecordDescriptor) || !recordcodec.DescriptorSuperset(cityDesc, merged.Tables[0].RecordDescriptor) {
		t.Fatal("manifest merge dropped concurrent rich fields")
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

func TestFrontierUnknownRevisionFails(t *testing.T) {
	if _, err := Frontier(nil, [32]byte{1}); err == nil {
		t.Fatal("unknown revision accepted")
	}
}
