package replication

import (
	"context"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

// TestRestartChurnHealsInProcess restarts one manager of a live 3-mesh
// back-to-back while records flow, proving every restart heals. It runs
// in-process (no daemons) so a wedge leaves full goroutine stacks in the
// test log. Regression test for the peerSession close/send ABBA deadlock
// (close took dataWriteMu under peer.mu while senders took peer.mu under
// dataWriteMu), which wedged one mesh link permanently after a restart.
func TestRestartChurnHealsInProcess(t *testing.T) {
	cluster := newTestCluster(t)
	nodes := make([]*churnNode, 3)
	for i := range nodes {
		nodes[i] = startChurnNode(t, cluster, i)
	}
	meshChurn(nodes)

	const rounds = 25
	const writesPerRound = 20
	for r := 0; r < rounds; r++ {
		// Writes flow on every node, including across the restart.
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for _, n := range nodes {
			wg.Add(1)
			go func(n *churnNode) {
				defer wg.Done()
				for i := 0; i < writesPerRound; i++ {
					select {
					case <-stop:
						return
					default:
					}
					if !n.paused.Load() {
						n.commit(t)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}(n)
		}
		// Restart one node mid-write (victim rotates over all three
		// for variety). Its writer pauses while the store is closed,
		// like the live test.
		time.Sleep(30 * time.Millisecond)
		victim := nodes[r%3]
		victim.paused.Store(true)
		time.Sleep(50 * time.Millisecond)
		victim.restart(t, cluster)
		meshChurn(nodes)
		victim.paused.Store(false)
		close(stop)
		wg.Wait()
		waitChurnConverged(t, nodes, 30*time.Second)
		t.Logf("churn round %d/%d healed", r+1, rounds)
	}
	for _, n := range nodes {
		n.close()
	}
}

type churnNode struct {
	idx    int
	id     ids.NodeID
	dir    string
	store  *state.Store
	mgr    *Manager
	cancel context.CancelFunc
	done   chan struct{}
	appl   *groupedFakeApplier
	mu     sync.Mutex
	own    uint64 // locally committed batches
	paused atomic.Bool
	// life serializes live handle use (commit) against restart swaps.
	life sync.RWMutex
}

// totalApplied counts batches applied via either applier entrypoint.
func (f *groupedFakeApplier) totalApplied() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(f.batches)
	for _, g := range f.groups {
		n += len(g)
	}
	return n
}

func startChurnNode(t *testing.T, cluster *testCluster, idx int) *churnNode {
	t.Helper()
	n := &churnNode{idx: idx, id: ids.NewNodeID(), dir: t.TempDir(), appl: &groupedFakeApplier{}}
	n.start(t, cluster)
	return n
}

func (n *churnNode) start(t *testing.T, cluster *testCluster) {
	t.Helper()
	st, err := state.Open(n.dir, n.id, cluster.dbid, state.Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	n.store = st
	var hash [32]byte
	hash[0] = 1
	mgr, err := NewManager(ManagerConfig{
		Store: st, Applier: n.appl, Creds: cluster.creds(t, n.id),
		Local: n.id, DBID: cluster.dbid, SchemaEpoch: 1, SchemaHash: hash,
		ListenAddr: "127.0.0.1:0", Fanout: 4,
		DialInterval: 100 * time.Millisecond, AckInterval: 100 * time.Millisecond,
		SendInterval: 20 * time.Millisecond, AntiEntropyInterval: time.Second,
		PeerRotationInterval: time.Minute,
		Limits:               codec.DefaultLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	n.mgr = mgr
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.done = make(chan struct{})
	go func() { _ = mgr.Run(ctx); close(n.done) }()
	deadline := time.Now().Add(10 * time.Second)
	for n.mgr.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n.mgr.Addr() == "" {
		t.Fatal("churn node listener never came up")
	}
}

func (n *churnNode) restart(t *testing.T, cluster *testCluster) {
	t.Helper()
	n.life.Lock()
	defer n.life.Unlock()
	n.cancel()
	<-n.done
	_ = n.mgr.Close()
	_ = n.store.Close()
	n.start(t, cluster)
}

func (n *churnNode) close() {
	n.cancel()
	<-n.done
	_ = n.mgr.Close()
	_ = n.store.Close()
}

func (n *churnNode) commit(t *testing.T) {
	t.Helper()
	n.life.RLock()
	defer n.life.RUnlock()
	var hash [32]byte
	hash[0] = 1
	b := &codec.MutationBatch{
		ProtocolVersion: ProtocolVersion, TxID: ids.NewTxID(),
		OriginNode: n.id, HLC: uint64(time.Now().UnixNano()), SchemaEpoch: 1, SchemaHash: hash,
		Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1)}},
	}
	if _, err := n.store.CommitLocal(context.Background(), b); err != nil {
		t.Errorf("commit: %v", err)
		return
	}
	n.mu.Lock()
	n.own++
	n.mu.Unlock()
	n.mgr.NotifyLocal()
}

func meshChurn(nodes []*churnNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a == b {
				continue
			}
			a.mgr.AddPeer(b.id, []string{b.mgr.Addr()})
		}
	}
}

// waitChurnConverged waits until every node applied every remote batch,
// dumping goroutine stacks and peer states on stall.
func waitChurnConverged(t *testing.T, nodes []*churnNode, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		total := uint64(0)
		for _, n := range nodes {
			n.mu.Lock()
			total += n.own
			n.mu.Unlock()
		}
		ok := true
		for _, n := range nodes {
			n.mu.Lock()
			want := total - n.own
			n.mu.Unlock()
			if uint64(n.appl.totalApplied()) < want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for i, n := range nodes {
				n.mu.Lock()
				own := n.own
				n.mu.Unlock()
				st := n.mgr.Stats()
				t.Logf("node%d own=%d applied=%d batchesReceived=%d invalid=%d deferred=%d gaps=%d applyFailures=%d",
					i, own, n.appl.totalApplied(), st.BatchesReceived, st.BatchesInvalid,
					st.BatchesDeferred, st.GapsDetected, st.ApplyFailures)
				t.Logf("node%d peers: %+v", i, n.mgr.PeerStatus())
			}
			var buf []byte
			w := &sliceWriter{buf: &buf}
			_ = pprof.Lookup("goroutine").WriteTo(w, 2)
			t.Logf("goroutine dump on churn stall:\n%s", buf)
			t.Fatalf("churn mesh did not converge within %v", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

type sliceWriter struct {
	buf *[]byte
}

func (w *sliceWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}
