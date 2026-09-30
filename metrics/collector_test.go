package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	replicateddb "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/replication"
)

// TestCollectorSeries proves node, counter, and per-peer series expose the
// snapshot values with no global registration.
func TestCollectorSeries(t *testing.T) {
	node := replicateddb.NewNodeID()
	dbid := replicateddb.NewDBID()
	peer := replicateddb.NewNodeID()
	retired := replicateddb.NewNodeID()
	st := replicateddb.Status{
		State:           replicateddb.StateReady,
		NodeID:          node,
		DBID:            dbid,
		StateGeneration: 7,
		PeerCount:       1,
		ConnectedPeers:  1,
		MembershipCount: 2,
		SelectedPeers:   1,
		QUICConnections: 1,
		Metrics: replicateddb.MetricsSnapshot{
			LocalCommits:            3,
			LocalCommitMutations:    12,
			LocalCommitLatencyNanos: uint64(time.Second),
			StmtCacheHits:           9,
			StmtCacheMisses:         2,
			Scheduler: replicateddb.SchedulerSnapshot{
				Local:     replicateddb.SchedulerClassStats{Acquisitions: 5, ServiceNanos: uint64(2 * time.Second), DualServiceNanos: uint64(1500 * time.Millisecond)},
				Remote:    replicateddb.SchedulerClassStats{Acquisitions: 2, Waiters: 1, DualServiceNanos: uint64(200 * time.Millisecond)},
				DebtNanos: uint64(100 * time.Millisecond),
			},
		},
		Pool: replicateddb.PoolStats{
			ActiveConnections: 1,
			SelectedTargets:   1,
			TotalSessions:     2,
			ConnDeferrals:     1,
			SessionDeferrals:  2,
			Evictions:         3,
			Dials:             10,
			DialsCoalesced:    4,
			DialsReused:       5,
		},
		Membership: replicateddb.MembershipStats{
			NumMembers:             2,
			NumAlive:               2,
			NumSuspect:             0,
			NumDead:                0,
			ProbesCompleted:        15,
			ProbeFailures:          1,
			Refutations:            2,
			Suspicions:             3,
			BootstrapAttempts:      4,
			BootstrapSuccesses:     4,
			EventDrops:             0,
			ReconciledJoins:        1,
			ReconciledLeaves:       0,
			ReconciledUpdates:      1,
			PacketsSent:            50,
			PacketsReceived:        45,
			PacketBytesSent:        5000,
			PacketBytesReceived:    4500,
			PacketDrops:            0,
			StreamDrops:            0,
			DatagramOversizeErrors: 0,
			DatagramEnvelopeErrors: 0,
			DatagramDBIDMismatches: 0,
			StreamsDialed:          5,
			StreamsAccepted:        5,
			StreamDialFailures:     0,
		},
		Replication: replication.StatsSnapshot{
			SessionsOpened:        2,
			BatchesSent:           5,
			MemberAdmissions:      1,
			GatingMembers:         1,
			SnapshotsBusyDeferred: 3,
			SnapshotBusyReceived:  1,
		},
		Peers: []replicateddb.PeerDiagnostics{{
			NodeID:                 peer,
			Connected:              true,
			Selected:               true,
			SchemaAgreed:           true,
			AwaitingSnapshot:       true,
			SnapshotChunksReceived: 3,
			SnapshotChunksTotal:    7,
			MembershipState:        "alive",
			RetirementDeadline:     time.Unix(1800000000, 0),
			BytesSent:              100,
			LagByOrigin:            map[replicateddb.NodeID]uint64{node: 4},
		}, {
			NodeID:          retired,
			Retired:         true,
			Excluded:        true,
			MembershipState: "unknown",
		}},
	}
	c := NewCollector(func() replicateddb.Status { return st })

	want := fmt.Sprintf(`
# HELP spedsql_info Node identity and lifecycle state.
# TYPE spedsql_info gauge
spedsql_info{db_id=%q,node_id=%q,state="ready"} 1
# HELP spedsql_state_generation Durable state generation.
# TYPE spedsql_state_generation gauge
spedsql_state_generation 7
# HELP spedsql_membership_alive_count SWIM alive members in cluster view.
# TYPE spedsql_membership_alive_count gauge
spedsql_membership_alive_count 2
# HELP spedsql_quic_sessions_active Total active QUIC replication sessions in pool.
# TYPE spedsql_quic_sessions_active gauge
spedsql_quic_sessions_active 2
# HELP spedsql_local_commits_total Durable local commits.
# TYPE spedsql_local_commits_total counter
spedsql_local_commits_total 3
# HELP spedsql_local_commit_latency_seconds_total Total local commit latency.
# TYPE spedsql_local_commit_latency_seconds_total counter
spedsql_local_commit_latency_seconds_total 1
# HELP spedsql_repl_sessions_opened_total Attached sessions.
# TYPE spedsql_repl_sessions_opened_total counter
spedsql_repl_sessions_opened_total 2
# HELP spedsql_swim_probes_completed_total Completed direct/indirect SWIM probes.
# TYPE spedsql_swim_probes_completed_total counter
spedsql_swim_probes_completed_total 15
# HELP spedsql_swim_refutations_total SWIM suspicion refutations sent.
# TYPE spedsql_swim_refutations_total counter
spedsql_swim_refutations_total 2
# HELP spedsql_swim_packets_sent_total Sent SWIM packet datagrams.
# TYPE spedsql_swim_packets_sent_total counter
spedsql_swim_packets_sent_total 50
# HELP spedsql_pool_conn_deferrals_total Connection dials deferred by pool quota limit.
# TYPE spedsql_pool_conn_deferrals_total counter
spedsql_pool_conn_deferrals_total 1
# HELP spedsql_pool_evictions_total Idle connections or sessions evicted by pool.
# TYPE spedsql_pool_evictions_total counter
spedsql_pool_evictions_total 3
# HELP spedsql_pool_dials_coalesced_total Outbound connection dials coalesced via singleflight.
# TYPE spedsql_pool_dials_coalesced_total counter
spedsql_pool_dials_coalesced_total 4
# HELP spedsql_peer_connected Peer has a live session.
# TYPE spedsql_peer_connected gauge
spedsql_peer_connected{peer=%q} 1
spedsql_peer_connected{peer=%q} 0
# HELP spedsql_peer_selected Peer is selected as a replication target.
# TYPE spedsql_peer_selected gauge
spedsql_peer_selected{peer=%q} 1
spedsql_peer_selected{peer=%q} 0
# HELP spedsql_peer_retirement_deadline_seconds Unix timestamp when peer retention obligation expires.
# TYPE spedsql_peer_retirement_deadline_seconds gauge
spedsql_peer_retirement_deadline_seconds{peer=%q} 1.8e+09
spedsql_peer_retirement_deadline_seconds{peer=%q} 0
# HELP spedsql_peer_bytes_sent_total Session frame bytes sent to the peer.
# TYPE spedsql_peer_bytes_sent_total counter
spedsql_peer_bytes_sent_total{peer=%q} 100
spedsql_peer_bytes_sent_total{peer=%q} 0
# HELP spedsql_peer_lag_sequences Applied watermark minus peer observed watermark per origin.
# TYPE spedsql_peer_lag_sequences gauge
spedsql_peer_lag_sequences{origin=%q,peer=%q} 4
# HELP spedsql_gating_members Persisted members with a live retention obligation (the GC gating set).
# TYPE spedsql_gating_members gauge
spedsql_gating_members 1
# HELP spedsql_repl_member_admissions_total Persisted member admissions on first authenticated handshake.
# TYPE spedsql_repl_member_admissions_total counter
spedsql_repl_member_admissions_total 1
# HELP spedsql_peer_retired Peer is explicitly retired (holds no retention obligation).
# TYPE spedsql_peer_retired gauge
spedsql_peer_retired{peer=%q} 0
spedsql_peer_retired{peer=%q} 1
# HELP spedsql_peer_excluded Peer is locally excluded (retired and refusing sessions).
# TYPE spedsql_peer_excluded gauge
spedsql_peer_excluded{peer=%q} 0
spedsql_peer_excluded{peer=%q} 1
# HELP spedsql_sched_acquisitions_total Writer admission grants by class.
# TYPE spedsql_sched_acquisitions_total counter
spedsql_sched_acquisitions_total{class="local"} 5
spedsql_sched_acquisitions_total{class="maintenance"} 0
spedsql_sched_acquisitions_total{class="remote"} 2
# HELP spedsql_sched_service_seconds_total Total admitted writer service time by class.
# TYPE spedsql_sched_service_seconds_total counter
spedsql_sched_service_seconds_total{class="local"} 2
spedsql_sched_service_seconds_total{class="maintenance"} 0
spedsql_sched_service_seconds_total{class="remote"} 0
# HELP spedsql_sched_dual_service_seconds_total Admitted writer service time granted while the other interactive class had waiters.
# TYPE spedsql_sched_dual_service_seconds_total counter
spedsql_sched_dual_service_seconds_total{class="local"} 1.5
spedsql_sched_dual_service_seconds_total{class="maintenance"} 0
spedsql_sched_dual_service_seconds_total{class="remote"} 0.2
# HELP spedsql_sched_debt_seconds Normalized service-time debt spread between local and remote classes.
# TYPE spedsql_sched_debt_seconds gauge
spedsql_sched_debt_seconds 0.1
# HELP spedsql_repl_snapshots_busy_deferred_total Snapshot requests deferred while a transfer was in flight.
# TYPE spedsql_repl_snapshots_busy_deferred_total counter
spedsql_repl_snapshots_busy_deferred_total 3
# HELP spedsql_repl_snapshot_busy_received_total Snapshot busy deferrals received from sources.
# TYPE spedsql_repl_snapshot_busy_received_total counter
spedsql_repl_snapshot_busy_received_total 1
# HELP spedsql_peer_awaiting_snapshot Outbound snapshot request awaiting completion.
# TYPE spedsql_peer_awaiting_snapshot gauge
spedsql_peer_awaiting_snapshot{peer=%q} 1
spedsql_peer_awaiting_snapshot{peer=%q} 0
# HELP spedsql_peer_snapshot_chunks_received Inbound snapshot chunks received from the peer.
# TYPE spedsql_peer_snapshot_chunks_received gauge
spedsql_peer_snapshot_chunks_received{peer=%q} 3
spedsql_peer_snapshot_chunks_received{peer=%q} 0
# HELP spedsql_peer_snapshot_chunks_total Inbound snapshot chunk count advertised by the peer manifest.
# TYPE spedsql_peer_snapshot_chunks_total gauge
spedsql_peer_snapshot_chunks_total{peer=%q} 7
spedsql_peer_snapshot_chunks_total{peer=%q} 0
# HELP spedsql_stmt_cache_hits_total Prepared-statement cache hits.
# TYPE spedsql_stmt_cache_hits_total counter
spedsql_stmt_cache_hits_total 9
# HELP spedsql_stmt_cache_misses_total Prepared-statement cache misses.
# TYPE spedsql_stmt_cache_misses_total counter
spedsql_stmt_cache_misses_total 2
`, dbid.String(), node.String(), peer.String(), retired.String(), peer.String(), retired.String(), peer.String(), retired.String(), peer.String(), retired.String(),
		node.String(), peer.String(), peer.String(), retired.String(), peer.String(), retired.String(),
		peer.String(), retired.String(), peer.String(), retired.String(), peer.String(), retired.String())
	if err := testutil.CollectAndCompare(c, strings.NewReader(want),
		"spedsql_info", "spedsql_state_generation", "spedsql_membership_alive_count",
		"spedsql_quic_sessions_active", "spedsql_local_commits_total",
		"spedsql_local_commit_latency_seconds_total", "spedsql_repl_sessions_opened_total",
		"spedsql_swim_probes_completed_total", "spedsql_swim_refutations_total",
		"spedsql_swim_packets_sent_total", "spedsql_pool_conn_deferrals_total",
		"spedsql_pool_evictions_total", "spedsql_pool_dials_coalesced_total",
		"spedsql_peer_connected", "spedsql_peer_selected", "spedsql_peer_retirement_deadline_seconds",
		"spedsql_peer_bytes_sent_total",
		"spedsql_peer_lag_sequences", "spedsql_gating_members",
		"spedsql_repl_member_admissions_total", "spedsql_peer_retired",
		"spedsql_peer_excluded", "spedsql_sched_acquisitions_total",
		"spedsql_sched_service_seconds_total", "spedsql_sched_dual_service_seconds_total", "spedsql_sched_debt_seconds",
		"spedsql_repl_snapshots_busy_deferred_total", "spedsql_repl_snapshot_busy_received_total",
		"spedsql_peer_awaiting_snapshot", "spedsql_peer_snapshot_chunks_received",
		"spedsql_peer_snapshot_chunks_total",
		"spedsql_stmt_cache_hits_total", "spedsql_stmt_cache_misses_total"); err != nil {
		t.Fatal(err)
	}
}
