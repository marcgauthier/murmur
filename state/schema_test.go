package state

import (
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

func schemaTestTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "contacts",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}
}

func mustSchemaReg(t *testing.T, epoch uint64, tables []schema.TableSchema) *schema.Registry {
	t.Helper()
	reg, err := schema.BuildRegistry(epoch, tables)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func schemaRegTables(reg *schema.Registry) []schema.TableSchema {
	out := make([]schema.TableSchema, len(reg.Tables))
	for i, p := range reg.Tables {
		out[i] = *p
		out[i].Columns = append([]schema.ColumnSchema(nil), p.Columns...)
	}
	return out
}

func TestSchemaRevisionRoundTrip(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	if m, err := s.LoadSchemaManifest(); err != nil || m != nil {
		t.Fatalf("fresh store manifest = %v, %v", m, err)
	}
	reg := mustSchemaReg(t, 1, schemaTestTables())
	gen, err := schema.NewGenesis(schemaRegTables(reg), 1, ids.NewNodeID(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreSchemaRevision(gen); err != nil {
		t.Fatal(err)
	}
	cur, err := s.LoadSchemaManifest()
	if err != nil {
		t.Fatal(err)
	}
	if !schema.EqualRevision(gen, cur) {
		t.Fatal("current manifest changed")
	}
	// Sys epoch/hash stay in sync for cheap checks.
	epoch, hash, err := s.SchemaEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 1 || hash != gen.Hash {
		t.Fatal("sys schema epoch/hash out of sync with manifest")
	}
	// Revision directly addressable.
	byID, err := s.LoadSchemaRevision(schema.RevisionID(gen))
	if err != nil || !schema.EqualRevision(gen, byID) {
		t.Fatal("revision not retrievable by id")
	}
	if m, err := s.LoadSchemaRevision([32]byte{9}); err != nil || m != nil {
		t.Fatalf("missing revision = %v, %v", m, err)
	}
	// Invalid manifests are refused, current untouched.
	bad := *gen
	bad.Tables = append([]schema.TableSchema(nil), gen.Tables...)
	bad.Tables[0].Name = "renamed"
	if err := s.StoreSchemaRevision(&bad); err == nil {
		t.Fatal("hash-mismatched revision stored")
	}
	cur2, err := s.LoadSchemaManifest()
	if err != nil || !schema.EqualRevision(gen, cur2) {
		t.Fatal("current manifest changed by refused write")
	}
}

func TestSchemaAncestryAndProvenance(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	node := ids.NewNodeID()
	reg := mustSchemaReg(t, 1, schemaTestTables())
	gen, err := schema.NewGenesis(schemaRegTables(reg), 1, node, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreSchemaRevision(gen); err != nil {
		t.Fatal(err)
	}
	next := append([]schema.TableSchema(nil), gen.Tables...)
	next[0].Columns = append(next[0].Columns, schema.ColumnSchema{Name: "phone", Type: schema.ColText, Nullable: true})
	assigned, err := schema.AssignIDs(gen.Tables, next)
	if err != nil {
		t.Fatal(err)
	}
	child, err := schema.NewAuthoredRevision(gen,
		schemaRegTables(mustSchemaReg(t, 2, assigned)), node, 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreSchemaRevision(child); err != nil {
		t.Fatal(err)
	}
	// Ancestry walk from the tip finds both revisions, nothing missing.
	found, missing, err := s.CollectSchemaAncestry([][32]byte{schema.RevisionID(child)}, MaxSchemaAncestryWalk)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 || len(found) != 2 {
		t.Fatalf("found=%d missing=%d", len(found), len(missing))
	}
	// Unknown tips report missing instead of guessing.
	found, missing, err = s.CollectSchemaAncestry([][32]byte{{7}}, MaxSchemaAncestryWalk)
	if err != nil || len(found) != 0 || len(missing) != 1 {
		t.Fatalf("found=%d missing=%d err=%v", len(found), len(missing), err)
	}
	// Provenance: current and ancestor known, strangers unknown.
	ok, err := s.SchemaProvenanceKnown(child.Version, child.Hash, MaxSchemaAncestryWalk)
	if err != nil || !ok {
		t.Fatal("current provenance unknown")
	}
	ok, err = s.SchemaProvenanceKnown(gen.Version, gen.Hash, MaxSchemaAncestryWalk)
	if err != nil || !ok {
		t.Fatal("ancestor provenance unknown")
	}
	ok, err = s.SchemaProvenanceKnown(1, [32]byte{8}, MaxSchemaAncestryWalk)
	if err != nil || ok {
		t.Fatal("stranger provenance claimed known")
	}
}

func TestSchemaMergeResultReuse(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	frontier := [][32]byte{{1}, {2}}
	if _, ok, err := s.LoadMergeResult(frontier); err != nil || ok {
		t.Fatalf("fresh merge lookup = %v, %v", ok, err)
	}
	want := [32]byte{3}
	if err := s.StoreMergeResult(frontier, want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LoadMergeResult(frontier)
	if err != nil || !ok || got != want {
		t.Fatalf("merge lookup = %x, %v, %v", got, ok, err)
	}
	// Frontier order does not matter.
	got, ok, err = s.LoadMergeResult([][32]byte{{2}, {1}})
	if err != nil || !ok || got != want {
		t.Fatal("merge lookup depends on frontier order")
	}
}

func TestStoreSchemaRevisionsKeepsCurrent(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	reg := mustSchemaReg(t, 1, schemaTestTables())
	gen, err := schema.NewGenesis(schemaRegTables(reg), 1, ids.NewNodeID(), 10)
	if err != nil {
		t.Fatal(err)
	}
	other, err := schema.NewGenesis(schemaRegTables(reg), 1, ids.NodeID{9}, 11)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreSchemaRevision(gen); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreSchemaRevisions([]*schema.Manifest{other}); err != nil {
		t.Fatal(err)
	}
	cur, err := s.LoadSchemaManifest()
	if err != nil || !schema.EqualRevision(gen, cur) {
		t.Fatal("ancestry persist moved current")
	}
	// Refused ancestry stores nothing.
	bad := *other
	bad.Tables = nil
	if err := s.StoreSchemaRevisions([]*schema.Manifest{&bad}); err == nil {
		t.Fatal("invalid ancestry stored")
	}
}

// TestSchemaRevisionSurvivesRestart proves a merged revision, its current
// pointer, and the sys epoch/hash survive close/reopen, and that
// re-storing the identical revision after restart is a no-op.
func TestSchemaRevisionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	node := ids.NewNodeID()
	s, err := openSignedFixture(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	reg := mustSchemaReg(t, 1, schemaTestTables())
	gen, err := schema.NewGenesis(schemaRegTables(reg), 1, ids.NewNodeID(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreSchemaRevision(gen); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openSignedFixture(dir, node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cur, err := s.LoadSchemaManifest()
	if err != nil || !schema.EqualRevision(gen, cur) {
		t.Fatalf("reopened manifest differs: %v", err)
	}
	epoch, hash, err := s.SchemaEpoch()
	if err != nil || epoch != 1 || hash != gen.Hash {
		t.Fatalf("reopened epoch/hash = %d/%x, %v", epoch, hash, err)
	}
	byID, err := s.LoadSchemaRevision(schema.RevisionID(gen))
	if err != nil || !schema.EqualRevision(gen, byID) {
		t.Fatal("revision not retrievable after reopen")
	}
	if err := s.StoreSchemaRevision(gen); err != nil {
		t.Fatal(err)
	}
	if epoch2, _, err := s.SchemaEpoch(); err != nil || epoch2 != epoch {
		t.Fatalf("re-store moved epoch %d -> %d, %v", epoch, epoch2, err)
	}
}
