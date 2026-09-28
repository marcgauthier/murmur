package replication

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/state"
)

type committingChunkApplier struct{ store *state.Store }

func (a committingChunkApplier) ApplyRemote(ctx context.Context, b *codec.MutationBatch) error {
	_, err := a.store.CommitRemote(ctx, b)
	return err
}
func (committingChunkApplier) ApplySnapshotChunk(context.Context, *codec.SnapshotManifest, uint64, []codec.SnapshotCell, bool) (bool, error) {
	return false, nil
}

func TestReceiveTransactionChunksAppliesOnlyAfterComplete(t *testing.T) {
	store, err := state.Open(t.TempDir(), ids.NewNodeID(), ids.NewDBID(), state.Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	origin := ids.NewNodeID()
	batch := &codec.MutationBatch{ProtocolVersion: ProtocolVersion, TxID: ids.NewTxID(), OriginNode: origin, Sequence: 1, HLC: 100, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text(strings.Repeat("z", 140<<10))}}}
	chunks, err := codec.EncodeTransactionChunks(batch, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatalf("wanted several chunks, got %d", len(chunks))
	}
	m := &Manager{cfg: ManagerConfig{Store: store, Applier: committingChunkApplier{store}, Limits: codec.DefaultLimits(), MaxTransactionBytes: 1 << 20}, ctx: context.Background(), notifyCh: make(chan struct{}, 1), peers: make(map[ids.NodeID]*peerState), chunkRepairAt: make(map[ids.TxID]time.Time)}
	p := newPeerState(ids.NewNodeID(), nil, true)
	p.caps = CapTransactionChunks
	if err := m.onTransactionChunk(p, chunks[0]); err != nil {
		t.Fatal(err)
	}
	if wm, _ := store.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("partial chunks advanced watermark to %d", wm)
	}
	for _, chunk := range chunks[1:] {
		if err := m.onTransactionChunk(p, chunk); err != nil {
			t.Fatal(err)
		}
	}
	if wm, _ := store.ReceiveWatermark(origin); wm != 1 {
		t.Fatalf("completed transaction watermark = %d", wm)
	}
	if staged, err := store.StagedTransactionChunks(batch.TxID); err != nil || len(staged) != 0 {
		t.Fatalf("staged fragments remain: %v %v", staged, err)
	}
}
