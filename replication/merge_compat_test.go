package replication

import (
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/state"
	"testing"
)

func TestHistoricalSignedBatchSurvivesProtocolFiveCutover(t *testing.T) {
	domain, node := ids.NewDBID(), ids.NewNodeID()
	store, err := openSignedFixture(t.TempDir(), ids.NewNodeID(), domain, state.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch := testidentity.Sign(&codec.MutationBatch{ProtocolVersion: 4, OriginNode: node, TxID: ids.NewTxID(), Sequence: 1, HLC: 1, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("historical")}}}, domain)
	manager := &Manager{cfg: ManagerConfig{Store: store, DBID: domain}}
	if err = manager.validateBatch(batch); err != nil {
		t.Fatal(err)
	}
	raw := codec.EncodeBatch(nil, batch)
	decoded, rest, err := codec.DecodeBatch(raw, codec.DefaultLimits())
	if err != nil || len(rest) != 0 {
		t.Fatal(err)
	}
	if err = manager.validateBatch(decoded); err != nil {
		t.Fatal(err)
	}
	// Peer sessions still require the new capability and minimum protocol.
	if err = manager.validateIdentity(&Hello{NodeID: node, DBID: domain, ProtocolVersion: 4, MinProtocolVersion: 4, Capabilities: CapOriginSignatures}, node); err == nil {
		t.Fatal("old peer admitted")
	}
}
