package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

// TestUnknownAdvertisedNeedsPullsMissingOrigins pins the pull decision:
// origins the peer advertises that are absent from our durable
// watermarks are requested from sequence 1; known origins (even
// behind) are left to the watermark pull.
func TestUnknownAdvertisedNeedsPullsMissingOrigins(t *testing.T) {
	known, unknown, silent := ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()
	wms := []codec.OriginWatermark{{Origin: known, Sequence: 4}}
	advertised := map[ids.NodeID]uint64{known: 9, unknown: 10, silent: 0}

	needs := unknownAdvertisedNeeds(advertised, wms)
	if len(needs) != 1 {
		t.Fatalf("needs = %+v, want exactly one", needs)
	}
	if needs[0].Origin != unknown || needs[0].FromSeq != 1 {
		t.Fatalf("needs[0] = %+v, want {origin %s from 1}", needs[0], unknown)
	}

	if got := unknownAdvertisedNeeds(nil, wms); len(got) != 0 {
		t.Fatalf("empty advertisements produced %+v", got)
	}
	if got := unknownAdvertisedNeeds(advertised, append(wms,
		codec.OriginWatermark{Origin: unknown, Sequence: 10})); len(got) != 0 {
		t.Fatalf("fully known origins produced %+v", got)
	}
}

// failFirstApplier drops the first failCalls deliveries with a generic
// (non-gap, non-conflict) error, then applies durably through the store
// so redelivery converges the receive watermark.
type failFirstApplier struct {
	mu        sync.Mutex
	store     *state.Store
	failCalls int
	calls     int
	succeeded int
}

func (f *failFirstApplier) ApplyRemote(ctx context.Context, b *codec.MutationBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failCalls {
		return errors.New("injected transient apply failure")
	}
	if _, err := f.store.CommitRemote(ctx, b); err != nil {
		return err
	}
	f.succeeded++
	return nil
}

func (f *failFirstApplier) ApplySnapshotChunk(_ context.Context, _ *codec.SnapshotManifest, _ uint64, _ []codec.SnapshotCell, _ bool) (bool, error) {
	return false, nil
}

func (f *failFirstApplier) snapshot() (calls, succeeded int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.succeeded
}

// TestFirstDeliveryLossHealsViaUnknownOriginPull reproduces the live
// unknown-origin stall end to end: the receiver drops the entire first
// delivery of a never-before-seen origin after the sender's sent-cursor
// advanced. The receiver must pull the advertised-but-unknown origin so
// the sender resends; without the pull the origin stalls forever.
func TestFirstDeliveryLossHealsViaUnknownOriginPull(t *testing.T) {
	cluster := newTestCluster(t)
	nodeA, nodeB := ids.NewNodeID(), ids.NewNodeID()

	start := func(t *testing.T, node ids.NodeID, applier Applier) (*Manager, *state.Store, string) {
		t.Helper()
		st, err := state.Open(t.TempDir(), node, cluster.dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		var hash [32]byte
		hash[0] = 1
		mgr, err := NewManager(ManagerConfig{
			Store: st, Applier: applier, Creds: cluster.creds(t, node),
			Local: node, DBID: cluster.dbid, SchemaEpoch: 1, SchemaHash: hash,
			ListenAddr:   "127.0.0.1:0",
			Limits:       codec.Limits{MaxValueBytes: 64, MaxMutations: 100},
			SendInterval: 20 * time.Millisecond,
			DialInterval: 50 * time.Millisecond,
			AckInterval:  50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = mgr.Run(ctx); close(done) }()
		t.Cleanup(func() { cancel(); <-done })
		deadline := time.Now().Add(5 * time.Second)
		for {
			if addr := mgr.Addr(); addr != "" {
				return mgr, st, addr
			}
			if time.Now().After(deadline) {
				t.Fatal("listener never came up")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	recv := &failFirstApplier{failCalls: 10}
	mgrA, storeA, addrA := start(t, nodeA, &fakeApplier{})
	mgrB, storeB, addrB := start(t, nodeB, recv)
	recv.store = storeB

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if _, err := storeA.CommitLocal(ctx, validTestBatch(nodeA, uint64(i+1))); err != nil {
			t.Fatalf("seed origin log: %v", err)
		}
	}

	mgrA.AddPeer(nodeB, []string{addrB})
	mgrB.AddPeer(nodeA, []string{addrA})

	deadline := time.Now().Add(15 * time.Second)
	for {
		calls, succeeded := recv.snapshot()
		// Threshold, not exact equality: pull-triggered redelivery can
		// overlap the periodic resend, so duplicate batches may land
		// between polls and skip straight past succeeded == 10.
		// Idempotent apply makes duplicates harmless; the watermark
		// is the real convergence property.
		if succeeded >= 10 {
			if calls < 20 {
				t.Fatalf("applier calls = %d, want at least 20 (10 dropped + 10 redelivered)", calls)
			}
			if wm, err := storeB.ReceiveWatermark(nodeA); err != nil || wm != 10 {
				t.Fatalf("receiver watermark = %d, err = %v, want 10", wm, err)
			}
			// Delivery must quiesce: stale queued Needs can still
			// drain one bounded duplicate tail after convergence
			// (idempotent apply absorbs it), but a true resend
			// loop would grow calls forever. Require a full
			// quiet second within budget.
			quietNeed := time.Second
			quietStart := time.Now()
			lastCalls := calls
			stableDeadline := time.Now().Add(10 * time.Second)
			for {
				time.Sleep(100 * time.Millisecond)
				cur, _ := recv.snapshot()
				if cur != lastCalls {
					lastCalls = cur
					quietStart = time.Now()
				}
				if time.Since(quietStart) >= quietNeed {
					return
				}
				if time.Now().After(stableDeadline) {
					t.Fatalf("applier calls kept growing after convergence (%d -> %d): resend loop", calls, cur)
				}
			}
		}
		if time.Now().After(deadline) {
			wm, werr := storeB.ReceiveWatermark(nodeA)
			t.Fatalf("stall unrepaired: calls = %d, succeeded = %d, watermark = %d (err = %v), want watermark 10",
				calls, succeeded, wm, werr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
