package state

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"math/big"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/schema"
)

func policyState(t *testing.T) *Store {
	t.Helper()
	s := openTestStore(t, ids.NewNodeID())
	tables := []schema.TableSchema{{ID: 1, Name: "items", PK: 1, Columns: []schema.ColumnSchema{{ID: 1, Name: "id", Type: schema.ColBlob}, {ID: 2, Name: "count", Type: schema.ColText, MergePolicy: schema.PN_COUNTER}, {ID: 3, Name: "tags", Type: schema.ColText, MergePolicy: schema.OR_SET}, {ID: 4, Name: "maxv", Type: schema.ColReal, MergePolicy: schema.MAX}, {ID: 5, Name: "minv", Type: schema.ColReal, MergePolicy: schema.MIN}}}}
	m, err := schema.NewGenesis(tables, 1, s.NodeID(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.StoreSchemaRevision(m); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPolicyJoinPermutationAndDuplicateLaws(t *testing.T) {
	s := policyState(t)
	row, node := ids.NewRowID(), ids.NewNodeID()
	p := policyCounterKey(s.DBID(), node, false)
	n := policyCounterKey(s.DBID(), node, true)
	tag := SetTag(s.DBID(), node, ids.NewTxID(), 0)
	red, _ := codec.SetString("red").Encode()
	tag2 := SetTag(s.DBID(), ids.NewNodeID(), ids.NewTxID(), 0)
	type operation struct {
		m codec.Mutation
		v crdt.Version
	}
	var ops []operation
	add := func(col uint32, policy schema.MergePolicy, value codec.Value, records ...codec.CRDTRecord) {
		ops = append(ops, operation{codec.Mutation{TableID: 1, RowID: row, ColumnID: col, Policy: policy, Value: value, Records: records}, crdt.Version{HLC: uint64(len(ops) + 1), NodeID: node}})
	}
	add(2, schema.PN_COUNTER, codec.Null(), codec.CRDTRecord{Key: p, Data: big.NewInt(5).Bytes()})
	add(2, schema.PN_COUNTER, codec.Null(), codec.CRDTRecord{Key: p, Data: big.NewInt(20).Bytes()})
	add(2, schema.PN_COUNTER, codec.Null(), codec.CRDTRecord{Key: n, Data: big.NewInt(3).Bytes()})
	add(3, schema.OR_SET, codec.Null(), codec.CRDTRecord{Key: tag, Data: red})
	removed := append([]byte(nil), tag...)
	removed[0] = 'r'
	add(3, schema.OR_SET, codec.Null(), codec.CRDTRecord{Key: removed, Data: red})
	add(3, schema.OR_SET, codec.Null(), codec.CRDTRecord{Key: tag2, Data: red})
	for _, col := range []uint32{4, 5} {
		policy := schema.MAX
		if col == 5 {
			policy = schema.MIN
		}
		for _, v := range []codec.Value{codec.Int(1), codec.Real(1), codec.Int(10), codec.Real(-2)} {
			add(col, policy, v)
		}
	}
	var want map[string]codec.CellState
	rng := rand.New(rand.NewSource(7))
	for iteration := 0; iteration < 100; iteration++ {
		stage := make(map[string]*remoteGroupCell)
		var order []string
		sequence := rng.Perm(len(ops))
		sequence = append(sequence, rng.Perm(len(ops))...)
		for _, i := range sequence {
			op := ops[i]
			if err := s.mergePolicyMutation(&op.m, op.v, stage, &order); err != nil {
				t.Fatal(err)
			}
		}
		got := make(map[string]codec.CellState)
		for k, c := range stage {
			got[k] = codec.CellState{Version: c.version, Value: c.value}
		}
		if iteration == 0 {
			want = got
		} else if !reflect.DeepEqual(want, got) {
			t.Fatalf("noncommutative join on permutation %d", iteration)
		}
		if got[string(CellKey(1, row, 2))].Value.S != "17" {
			t.Fatal("counter projection")
		}
		if got[string(CellKey(1, row, 3))].Value.S != `[{"type":"string","value":"red"}]` {
			t.Fatal("add-wins projection")
		}
	}
}

func TestPolicyRejectsSignedActorForgeryAndTampering(t *testing.T) {
	s := policyState(t)
	node := ids.NewNodeID()
	victim := ids.NewNodeID()
	batch := &codec.MutationBatch{ProtocolVersion: 5, TxID: ids.NewTxID(), OriginNode: node, Sequence: 1, HLC: 2, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Policy: schema.PN_COUNTER, Value: codec.Null(), Records: []codec.CRDTRecord{{Key: policyCounterKey(s.DBID(), victim, false), Data: []byte{1}}}}}}
	testidentity.Sign(batch, s.DBID())
	if _, err := s.CommitRemote(context.Background(), batch); err == nil {
		t.Fatal("accepted foreign actor component")
	}
	if wm, _ := s.ReceiveWatermark(node); wm != 0 {
		t.Fatal("advanced invalid watermark")
	}
	batch.Mutations[0].Records[0].Key = policyCounterKey(s.DBID(), node, false)
	testidentity.Sign(batch, s.DBID())
	batch.Mutations[0].Records[0].Data = []byte{9}
	if _, err := s.CommitRemote(context.Background(), batch); err == nil {
		t.Fatal("accepted modified signed component")
	}
}

func TestSignedFormatFourMigrationPreservesHistory(t *testing.T) {
	path := t.TempDir()
	node := ids.NewNodeID()
	domain := ids.NewDBID()
	s, err := openSignedFixture(path, node, domain, Options{})
	if err != nil {
		t.Fatal(err)
	}
	old := testidentity.Sign(securityBatch(node, 1), domain)
	if _, err = s.CommitRemote(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	before, err := s.getDirect(LogKey(node, 1))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{sysFormat, sysMinReader, sysMinWriter} {
		if err = s.dbSet(SysKey(name), encodeU64(4), true); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if bad, err := openSignedFixture(path, node, domain, Options{}); err == nil {
		bad.Close()
		t.Fatal("implicit upgrade accepted")
	} else if !strings.Contains(err.Error(), "MigrateMergePolicies") {
		t.Fatal(err)
	}
	s, err = openSignedFixture(path, node, domain, Options{MigrateMergePolicies: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after, err := s.getDirect(LogKey(node, 1))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("historical signed bytes changed", err)
	}
	decoded, _, err := codec.DecodeBatch(after, codec.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err = codec.VerifyOrigin(decoded, domain, testidentity.Key(node).Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
	format, reader, writer, err := s.FormatInfo()
	if err != nil || format != 6 || reader != 6 || writer != 6 {
		t.Fatalf("markers %d/%d/%d: %v", format, reader, writer, err)
	}
}

func policyCounterKey(db ids.DBID, node ids.NodeID, negative bool) []byte {
	kind := byte('p')
	if negative {
		kind = 'n'
	}
	key := append([]byte{kind}, db[:]...)
	return append(key, node[:]...)
}

func TestPolicyOwnershipEpochReorderingRetainsHistory(t *testing.T) {
	s := policyState(t)
	row, node := ids.NewRowID(), ids.NewNodeID()
	release := crdt.Version{HLC: 3, NodeID: node}
	shadow := uint32(2) ^ 0x80000000
	component := policyCounterKey(s.DBID(), node, false)
	old := codec.Mutation{TableID: 1, RowID: row, ColumnID: shadow, Policy: schema.PN_COUNTER, Value: codec.Null(), Records: []codec.CRDTRecord{{Key: codec.EpochRecord(crdt.Version{}, component), Data: []byte{20}}}}
	clear := codec.Mutation{TableID: 1, RowID: row, ColumnID: shadow, Value: codec.Blob([]byte{0})}
	next := codec.Mutation{TableID: 1, RowID: row, ColumnID: shadow, Policy: schema.PN_COUNTER, Value: codec.Null(), Records: []codec.CRDTRecord{{Key: codec.EpochRecord(release, component), Data: []byte{2}}}}
	mutations := []codec.Mutation{old, clear, next}
	times := []uint64{4, 3, 5}
	var want map[string]codec.CellState
	for _, sequence := range [][]int{{0, 1, 2}, {1, 0, 2}, {2, 1, 0}, {2, 0, 1}, {1, 2, 0}, {0, 2, 1}} {
		staged := make(map[string]*remoteGroupCell)
		var order []string
		b := s.mem.newBatch()
		for _, i := range sequence {
			if err := s.mergeRemoteGroupBatch(b, &codec.MutationBatch{HLC: times[i], OriginNode: node, Mutations: []codec.Mutation{mutations[i]}}, staged, &order); err != nil {
				t.Fatal(err)
			}
		}
		b.Close()
		got := make(map[string]codec.CellState)
		for k, c := range staged {
			got[k] = codec.CellState{Version: c.version, Value: c.value}
		}
		if want == nil {
			want = got
		} else if !reflect.DeepEqual(want, got) {
			t.Fatalf("ownership depends on order %v", sequence)
		}
		epoch, value, active, err := codec.ShadowValue(got[string(CellKey(1, row, shadow))].Value, codec.DefaultLimits())
		if err != nil || !active || epoch != release || value.S != "2" {
			t.Fatalf("branch: %v %v %v %v", epoch, value, active, err)
		}
		if len(got) != 3 {
			t.Fatal("retired causal history discarded")
		}
	}
}
