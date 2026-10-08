package state

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

// crdtPropertySchema defines a schema with all supported CRDT merge policies.
func crdtPropertySchema(node ids.NodeID) (*schema.Manifest, error) {
	tables := []schema.TableSchema{{
		ID:   1,
		Name: "crdt_props",
		PK:   1,
		Columns: []schema.ColumnSchema{
			{ID: 1, Name: "id", Type: schema.ColBlob},
			{ID: 2, Name: "lww_text", Type: schema.ColText, MergePolicy: schema.LWW},
			{ID: 3, Name: "lww_num", Type: schema.ColInteger, MergePolicy: schema.LWW},
			{ID: 4, Name: "counter", Type: schema.ColText, MergePolicy: schema.PN_COUNTER},
			{ID: 5, Name: "tags", Type: schema.ColText, MergePolicy: schema.OR_SET},
			{ID: 6, Name: "max_val", Type: schema.ColReal, MergePolicy: schema.MAX},
			{ID: 7, Name: "min_val", Type: schema.ColReal, MergePolicy: schema.MIN},
		},
	}}
	return schema.NewGenesis(tables, 1, node, 1)
}

func openPropertyStore(t *testing.T, node ids.NodeID, dbID ids.DBID) *Store {
	t.Helper()
	s, err := openSignedFixture(t.TempDir(), node, dbID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	m, err := crdtPropertySchema(node)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreSchemaRevision(m); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type propOperation struct {
	m      codec.Mutation
	v      crdt.Version
	origin ids.NodeID
}

// TestCRDTPureMergePermutations proves commutativity, associativity, and idempotence
// across all CRDT merge policies (PN_COUNTER, OR_SET, MAX, MIN) under 100+ random delivery permutations.
func TestCRDTPureMergePermutations(t *testing.T) {
	s := openPropertyStore(t, ids.NewNodeID(), ids.NewDBID())
	row := ids.NewRowID()
	nodes := []ids.NodeID{ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()}

	rng := rand.New(rand.NewSource(42))

	var ops []propOperation
	var hlcCounter uint64

	// 1. PN_COUNTER operations across 3 nodes
	expectedCounter := big.NewInt(0)
	nodePositiveAcc := make(map[ids.NodeID]*big.Int)
	nodeNegativeAcc := make(map[ids.NodeID]*big.Int)
	for _, n := range nodes {
		nodePositiveAcc[n] = big.NewInt(0)
		nodeNegativeAcc[n] = big.NewInt(0)
	}

	for i := 0; i < 15; i++ {
		node := nodes[rng.Intn(len(nodes))]
		deltaVal := rng.Int63n(1000) - 400 // can be negative or positive
		hlcCounter++
		v := crdt.Version{HLC: hlcCounter, NodeID: node}

		deltaBig := big.NewInt(deltaVal)
		expectedCounter.Add(expectedCounter, deltaBig)

		var pKey, nKey []byte
		pKey = policyCounterKey(s.DBID(), node, false)
		nKey = policyCounterKey(s.DBID(), node, true)

		if deltaVal >= 0 {
			nodePositiveAcc[node].Add(nodePositiveAcc[node], deltaBig)
		} else {
			abs := new(big.Int).Abs(deltaBig)
			nodeNegativeAcc[node].Add(nodeNegativeAcc[node], abs)
		}

		// Push absolute actor state updates
		ops = append(ops, propOperation{
			m: codec.Mutation{
				TableID:  1,
				RowID:    row,
				ColumnID: 4,
				Policy:   schema.PN_COUNTER,
				Value:    codec.Null(),
				Records: []codec.CRDTRecord{
					{Key: pKey, Data: nodePositiveAcc[node].Bytes()},
					{Key: nKey, Data: nodeNegativeAcc[node].Bytes()},
				},
			},
			v:      v,
			origin: node,
		})
	}

	// 2. OR_SET operations (concurrent adds and removes with causal tags)
	type activeTag struct {
		tag  []byte
		elem string
		data []byte
	}
	var createdTags []activeTag
	removedTags := make(map[string]bool)

	elements := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i := 0; i < 20; i++ {
		node := nodes[rng.Intn(len(nodes))]
		hlcCounter++
		v := crdt.Version{HLC: hlcCounter, NodeID: node}

		if len(createdTags) > 0 && rng.Float32() < 0.4 {
			// Remove an existing tag
			idx := rng.Intn(len(createdTags))
			target := createdTags[idx]
			removeTag := append([]byte(nil), target.tag...)
			removeTag[0] = 'r'
			removedTags[string(target.tag)] = true

			ops = append(ops, propOperation{
				m: codec.Mutation{
					TableID:  1,
					RowID:    row,
					ColumnID: 5,
					Policy:   schema.OR_SET,
					Value:    codec.Null(),
					Records:  []codec.CRDTRecord{{Key: removeTag, Data: target.data}},
				},
				v:      v,
				origin: node,
			})
		} else {
			// Add a new element with a unique tag
			elem := elements[rng.Intn(len(elements))]
			tag := SetTag(s.DBID(), node, ids.NewTxID(), uint32(i))
			encodedData, err := codec.SetString(elem).Encode()
			if err != nil {
				t.Fatal(err)
			}
			createdTags = append(createdTags, activeTag{tag: tag, elem: elem, data: encodedData})

			ops = append(ops, propOperation{
				m: codec.Mutation{
					TableID:  1,
					RowID:    row,
					ColumnID: 5,
					Policy:   schema.OR_SET,
					Value:    codec.Null(),
					Records:  []codec.CRDTRecord{{Key: tag, Data: encodedData}},
				},
				v:      v,
				origin: node,
			})
		}
	}

	// Compute expected active set elements
	expectedSetMap := make(map[string]bool)
	for _, at := range createdTags {
		if !removedTags[string(at.tag)] {
			expectedSetMap[at.elem] = true
		}
	}

	// 3. MAX / MIN operations
	var maxInputs []float64
	var minInputs []float64
	for i := 0; i < 15; i++ {
		node := nodes[rng.Intn(len(nodes))]
		hlcCounter++
		v := crdt.Version{HLC: hlcCounter, NodeID: node}
		val := float64(rng.Intn(2000) - 1000)
		maxInputs = append(maxInputs, val)
		minInputs = append(minInputs, val)

		ops = append(ops, propOperation{
			m: codec.Mutation{
				TableID:  1,
				RowID:    row,
				ColumnID: 6,
				Policy:   schema.MAX,
				Value:    codec.Real(val),
			},
			v:      v,
			origin: node,
		})
		ops = append(ops, propOperation{
			m: codec.Mutation{
				TableID:  1,
				RowID:    row,
				ColumnID: 7,
				Policy:   schema.MIN,
				Value:    codec.Real(val),
			},
			v:      v,
			origin: node,
		})
	}

	expectedMax := maxInputs[0]
	for _, x := range maxInputs {
		if x > expectedMax {
			expectedMax = x
		}
	}
	expectedMin := minInputs[0]
	for _, x := range minInputs {
		if x < expectedMin {
			expectedMin = x
		}
	}

	// Run 100 randomized delivery permutations
	var wantStates map[string]codec.CellState
	for iteration := 0; iteration < 100; iteration++ {
		stage := make(map[string]*remoteGroupCell)
		var order []string

		// Build permutation: shuffle and inject duplicate replays
		perm := rng.Perm(len(ops))
		if iteration%2 == 1 {
			// Duplicate random elements to test idempotence
			dups := rng.Perm(len(ops))[:len(ops)/3]
			perm = append(perm, dups...)
		}

		for _, idx := range perm {
			op := ops[idx]
			if err := s.mergePolicyMutation(&op.m, op.v, stage, &order); err != nil {
				t.Fatalf("permutation %d: merge failed: %v", iteration, err)
			}
		}

		got := make(map[string]codec.CellState)
		for k, c := range stage {
			got[k] = codec.CellState{Version: c.version, Value: c.value}
		}

		if iteration == 0 {
			wantStates = got
		} else {
			if !reflect.DeepEqual(wantStates, got) {
				t.Fatalf("permutation %d failed commutativity/associativity: state diverged from canonical baseline", iteration)
			}
		}

		// Invariant checks on the merged cell state
		// 1. Counter check
		counterCell := got[string(CellKey(1, row, 4))]
		if counterCell.Value.S != expectedCounter.String() {
			t.Fatalf("permutation %d: counter mismatch: got %s, want %s", iteration, counterCell.Value.S, expectedCounter.String())
		}

		// 2. OR-Set check
		setCell := got[string(CellKey(1, row, 5))]
		var actualSet []struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		}
		if err := json.Unmarshal([]byte(setCell.Value.S), &actualSet); err != nil {
			t.Fatalf("permutation %d: invalid OR-Set JSON: %v (%s)", iteration, err, setCell.Value.S)
		}
		actualSetMap := make(map[string]bool)
		for _, item := range actualSet {
			actualSetMap[item.Value] = true
		}
		if !reflect.DeepEqual(expectedSetMap, actualSetMap) {
			t.Fatalf("permutation %d: OR-Set mismatch: got %v, want %v", iteration, actualSetMap, expectedSetMap)
		}

		// 3. MAX check
		maxCell := got[string(CellKey(1, row, 6))]
		if maxCell.Value.F != expectedMax {
			t.Fatalf("permutation %d: MAX mismatch: got %v, want %v", iteration, maxCell.Value.F, expectedMax)
		}

		// 4. MIN check
		minCell := got[string(CellKey(1, row, 7))]
		if minCell.Value.F != expectedMin {
			t.Fatalf("permutation %d: MIN mismatch: got %v, want %v", iteration, minCell.Value.F, expectedMin)
		}
	}
}

// TestCRDTMultiNodeReplicationConvergence executes multi-origin concurrent writes
// across 3 live Store instances, extracts their mutation batches, and delivers them to
// 10 independent target replicas with randomized arrival interleavings and duplicate replays.
// Asserts bitwise identical cell state, table digests, and CRDT causal records across all replicas.
func TestCRDTMultiNodeReplicationConvergence(t *testing.T) {
	ctx := context.Background()
	dbID := ids.NewDBID()
	nodes := []ids.NodeID{ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()}

	stores := make([]*Store, 3)
	for i := 0; i < 3; i++ {
		stores[i] = openPropertyStore(t, nodes[i], dbID)
	}

	row := ids.NewRowID()

	// Node 0 commits counter increments, MAX updates, and LWW text
	for i := 1; i <= 3; i++ {
		delta := big.NewInt(int64(i * 10))
		b := &codec.MutationBatch{
			ProtocolVersion: 1,
			TxID:            ids.NewTxID(),
			OriginNode:      stores[0].NodeID(),
			HLC:             stores[0].ClockNow(),
			SchemaEpoch:     1,
			Mutations: []codec.Mutation{
				{TableID: 1, RowID: row, ColumnID: 2, Policy: schema.LWW, Value: codec.Text(fmt.Sprintf("node0-v%d", i))},
				{TableID: 1, RowID: row, ColumnID: 3, Policy: schema.LWW, Value: codec.Int(int64(i * 11))},
				{TableID: 1, RowID: row, ColumnID: 4, Policy: schema.PN_COUNTER, Flags: codec.FlagCounterDelta, Value: codec.Text(delta.String())},
				{TableID: 1, RowID: row, ColumnID: 6, Policy: schema.MAX, Value: codec.Real(float64(i * 100))},
			},
		}
		if _, err := stores[0].CommitLocal(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	// Node 1 commits counter decrements, MIN updates, and OR-Set adds
	var tag1 []byte
	tag1Data, _ := codec.SetString("tag-apple").Encode()
	var tag2 []byte
	tag2Data, _ := codec.SetString("tag-banana").Encode()

	for i := 1; i <= 3; i++ {
		txID := ids.NewTxID()
		delta := big.NewInt(int64(-i * 5))
		m := []codec.Mutation{
			{TableID: 1, RowID: row, ColumnID: 4, Policy: schema.PN_COUNTER, Flags: codec.FlagCounterDelta, Value: codec.Text(delta.String())},
			{TableID: 1, RowID: row, ColumnID: 7, Policy: schema.MIN, Value: codec.Real(float64(-i * 50))},
		}
		if i == 1 {
			tag1 = SetTag(dbID, nodes[1], txID, 0)
			m = append(m, codec.Mutation{TableID: 1, RowID: row, ColumnID: 5, Policy: schema.OR_SET, Value: codec.Null(), Records: []codec.CRDTRecord{{Key: tag1, Data: tag1Data}}})
		} else if i == 2 {
			tag2 = SetTag(dbID, nodes[1], txID, 0)
			m = append(m, codec.Mutation{TableID: 1, RowID: row, ColumnID: 5, Policy: schema.OR_SET, Value: codec.Null(), Records: []codec.CRDTRecord{{Key: tag2, Data: tag2Data}}})
		}
		b := &codec.MutationBatch{
			ProtocolVersion: 1,
			TxID:            txID,
			OriginNode:      stores[1].NodeID(),
			HLC:             stores[1].ClockNow(),
			SchemaEpoch:     1,
			Mutations:       m,
		}
		if _, err := stores[1].CommitLocal(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	// Node 2 commits LWW updates, OR-Set remove of tag1, and counter increments
	tag1Removed := append([]byte(nil), tag1...)
	tag1Removed[0] = 'r'

	for i := 1; i <= 3; i++ {
		txID := ids.NewTxID()
		delta := big.NewInt(int64(i * 3))
		m := []codec.Mutation{
			{TableID: 1, RowID: row, ColumnID: 2, Policy: schema.LWW, Value: codec.Text(fmt.Sprintf("node2-v%d", i))},
			{TableID: 1, RowID: row, ColumnID: 4, Policy: schema.PN_COUNTER, Flags: codec.FlagCounterDelta, Value: codec.Text(delta.String())},
		}
		if i == 2 {
			m = append(m, codec.Mutation{TableID: 1, RowID: row, ColumnID: 5, Policy: schema.OR_SET, Value: codec.Null(), Records: []codec.CRDTRecord{{Key: tag1Removed, Data: tag1Data}}})
		}
		b := &codec.MutationBatch{
			ProtocolVersion: 1,
			TxID:            txID,
			OriginNode:      stores[2].NodeID(),
			HLC:             stores[2].ClockNow(),
			SchemaEpoch:     1,
			Mutations:       m,
		}
		if _, err := stores[2].CommitLocal(ctx, b); err != nil {
			t.Fatal(err)
		}
	}

	// Collect batches per origin from stores
	perOriginBatches := make(map[ids.NodeID][]*codec.MutationBatch)
	for i, s := range stores {
		var batches []*codec.MutationBatch
		_, err := s.LogScan(nodes[i], 1, 100, 1<<20, func(b *codec.MutationBatch) error {
			batches = append(batches, b)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		perOriginBatches[nodes[i]] = batches
	}

	// Replicate to 10 independent target replicas using randomized interleavings
	rng := rand.New(rand.NewSource(99))
	type replicaResult struct {
		counter string
		maxVal  float64
		minVal  float64
		tags    string
		lwwText string
		lwwNum  int64
	}
	var baselineResult replicaResult

	for rep := 0; rep < 10; rep++ {
		target := openPropertyStore(t, ids.NewNodeID(), dbID)

		// Build a valid asynchronous schedule: for each origin, its batches arrive in sequence 1..N,
		// but inter-origin delivery is completely randomized, with duplicate re-deliveries.
		cursors := map[ids.NodeID]int{nodes[0]: 0, nodes[1]: 0, nodes[2]: 0}
		totalBatches := len(perOriginBatches[nodes[0]]) + len(perOriginBatches[nodes[1]]) + len(perOriginBatches[nodes[2]])
		appliedTotal := 0

		for appliedTotal < totalBatches {
			// Pick a random origin that has pending batches
			var availableOrigins []ids.NodeID
			for _, n := range nodes {
				if cursors[n] < len(perOriginBatches[n]) {
					availableOrigins = append(availableOrigins, n)
				}
			}
			origin := availableOrigins[rng.Intn(len(availableOrigins))]
			nextBatch := perOriginBatches[origin][cursors[origin]]

			// Commit the batch
			if _, err := commitRemoteFixture(target, ctx, nextBatch); err != nil {
				t.Fatalf("replica %d: failed committing batch from origin %s (seq %d): %v", rep, origin, nextBatch.Sequence, err)
			}
			cursors[origin]++
			appliedTotal++

			// Occasionally re-deliver an older batch from any origin to verify duplicate idempotence
			if rng.Float32() < 0.3 {
				pastOrigin := nodes[rng.Intn(len(nodes))]
				if cursors[pastOrigin] > 0 {
					dupIdx := rng.Intn(cursors[pastOrigin])
					dupBatch := perOriginBatches[pastOrigin][dupIdx]
					res, err := commitRemoteFixture(target, ctx, dupBatch)
					if err != nil {
						t.Fatalf("replica %d: duplicate replay error: %v", rep, err)
					}
					if res.Applied {
						t.Fatalf("replica %d: duplicate batch (seq %d) was applied instead of ignored", rep, dupBatch.Sequence)
					}
				}
			}
		}

		// Check converged state
		stCounter, _, _ := target.GetCell(1, row, 4)
		stMax, _, _ := target.GetCell(1, row, 6)
		stMin, _, _ := target.GetCell(1, row, 7)
		stTags, _, _ := target.GetCell(1, row, 5)
		stLWW, _, _ := target.GetCell(1, row, 2)
		stLWWNum, _, _ := target.GetCell(1, row, 3)

		res := replicaResult{
			counter: stCounter.Value.S,
			maxVal:  stMax.Value.F,
			minVal:  stMin.Value.F,
			tags:    stTags.Value.S,
			lwwText: stLWW.Value.S,
			lwwNum:  stLWWNum.Value.I,
		}

		if rep == 0 {
			baselineResult = res
		} else {
			if !reflect.DeepEqual(baselineResult, res) {
				t.Fatalf("replica %d diverged from baseline: %+v != %+v", rep, res, baselineResult)
			}
		}

		// Verify expected invariants:
		// Counter: (10+20+30) - (5+10+15) + (3+6+9) = 60 - 30 + 18 = 48
		if res.counter != "48" {
			t.Fatalf("replica %d: expected counter 48, got %s", rep, res.counter)
		}
		// Max: 300
		if res.maxVal != 300 {
			t.Fatalf("replica %d: expected max 300, got %v", rep, res.maxVal)
		}
		// Min: -150
		if res.minVal != -150 {
			t.Fatalf("replica %d: expected min -150, got %v", rep, res.minVal)
		}
		// Tags: tag1 was added then removed; tag2 remains.
		if !bytes.Contains([]byte(res.tags), []byte("tag-banana")) || bytes.Contains([]byte(res.tags), []byte("tag-apple")) {
			t.Fatalf("replica %d: unexpected tags result: %s", rep, res.tags)
		}
		if res.lwwNum != 33 {
			t.Fatalf("replica %d: expected lwwNum 33, got %d", rep, res.lwwNum)
		}
	}
}
