package replication

import (
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

func TestPeerScalingCapsAcrossChurn(t *testing.T) {
	cluster := newTestCluster(t)
	local := ids.NewNodeID()
	st, err := state.Open(t.TempDir(), local, cluster.dbid, state.Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := NewManager(ManagerConfig{
		Store: st, Applier: &fakeApplier{}, Creds: cluster.creds(t, local), Local: local, DBID: cluster.dbid,
		Fanout: 3, MaxReplicationSessions: 8, MaxQUICConnections: 32, MaxConcurrentRepairs: 1,
	})
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close(); _ = st.Close() })

	var members []ids.NodeID
	for _, target := range []int{10, 100, 1000} {
		for len(members) < target {
			id := ids.NewNodeID()
			members = append(members, id)
			mgr.OnPeerDiscovered(id, fmt.Sprintf("127.0.0.1:%d", 10000+len(members)), NodeMetadata{DBID: cluster.dbid})
		}
		selected := 0
		for _, peer := range mgr.PeerStatus() {
			if peer.Selected {
				selected++
			}
		}
		if selected != 3 {
			t.Fatalf("members=%d selected targets=%d, want fixed fanout 3", target, selected)
		}
		pool := mgr.pool.Stats()
		if pool.MaxConnections != 32 || pool.MaxSessions != 8 || pool.SelectedTargets > 3 || pool.TotalSessions > 8 {
			t.Fatalf("members=%d exceeded configured connection/session bounds: %+v", target, pool)
		}
		// Simulate a partition/churn wave by removing a quarter of the known
		// members. Selection must refill from the surviving membership view.
		for i := 0; i < target; i += 4 {
			mgr.OnPeerLeft(members[i])
		}
		status := mgr.PeerStatus()
		selected = 0
		for _, peer := range status {
			if peer.Selected {
				selected++
			}
		}
		if selected != 3 {
			t.Fatalf("members=%d after churn selected=%d, want 3", target, selected)
		}
		pool = mgr.pool.Stats()
		if pool.MaxConnections != 32 || pool.MaxSessions != 8 || pool.ActiveConnections > 32 || pool.TotalSessions > 8 {
			t.Fatalf("members=%d after churn exceeded caps: %+v", target, pool)
		}
	}
}
