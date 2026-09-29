// Package metrics exposes Murmur-SQL diagnostics to Prometheus.
//
// The Collector scrapes a replicateddb.Status snapshot on each collection;
// it registers nothing globally and starts no HTTP listener. Applications
// wire it into their own registry:
//
//	reg := prometheus.NewRegistry()
//	reg.MustRegister(metrics.NewCollector(db.Status))
//
// Unimplemented subsystems (SWIM membership, bounded peer selection,
// Plumtree, overload budgets, High/Low bridge, file objects, subscriptions)
// have no series here; their Status fields stay zero and must not be
// mistaken for measured zeros.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	replicateddb "github.com/marcgauthier/spedsql"
)

// SnapshotFunc returns the current diagnostic snapshot. DB.Status satisfies it.
type SnapshotFunc func() replicateddb.Status

type seriesDef struct {
	name string
	help string
	get  func(*replicateddb.Status) float64
}

func nanosSeconds(n uint64) float64 { return float64(n) / float64(time.Second) }

func gauges() []seriesDef {
	return []seriesDef{
		{"state_generation", "Durable state generation.", func(s *replicateddb.Status) float64 { return float64(s.StateGeneration) }},
		{"materialized_generation", "Generation materialized into the query engine.", func(s *replicateddb.Status) float64 { return float64(s.MaterializedGeneration) }},
		{"hlc", "Maximum observed hybrid logical clock.", func(s *replicateddb.Status) float64 { return float64(s.HLC) }},
		{"local_seq", "Local origin sequence.", func(s *replicateddb.Status) float64 { return float64(s.LocalSeq) }},
		{"schema_epoch", "Published schema epoch.", func(s *replicateddb.Status) float64 { return float64(s.SchemaEpoch) }},
		{"peer_count", "Known peers (configured plus inbound-discovered).", func(s *replicateddb.Status) float64 { return float64(s.PeerCount) }},
		{"connected_peers", "Peers with a live session.", func(s *replicateddb.Status) float64 { return float64(s.ConnectedPeers) }},
		{"membership_count", "Membership view size.", func(s *replicateddb.Status) float64 { return float64(s.MembershipCount) }},
		{"membership_alive_count", "SWIM alive members in cluster view.", func(s *replicateddb.Status) float64 { return float64(s.Membership.NumAlive) }},
		{"membership_suspect_count", "SWIM suspect members in cluster view.", func(s *replicateddb.Status) float64 { return float64(s.Membership.NumSuspect) }},
		{"membership_dead_count", "SWIM dead members in cluster view.", func(s *replicateddb.Status) float64 { return float64(s.Membership.NumDead) }},
		{"selected_peers", "Selected replication targets.", func(s *replicateddb.Status) float64 { return float64(s.SelectedPeers) }},
		{"quic_connections", "Live QUIC replication sessions.", func(s *replicateddb.Status) float64 { return float64(s.QUICConnections) }},
		{"quic_connections_active", "Active underlying QUIC peer connections in pool.", func(s *replicateddb.Status) float64 { return float64(s.Pool.ActiveConnections) }},
		{"quic_sessions_active", "Total active QUIC replication sessions in pool.", func(s *replicateddb.Status) float64 { return float64(s.Pool.TotalSessions) }},
		{"quic_sessions_target", "Active replication target sessions in pool.", func(s *replicateddb.Status) float64 { return float64(s.Pool.SelectedTargets) }},
		{"quic_sessions_repair", "Active anti-entropy repair sessions in pool.", func(s *replicateddb.Status) float64 { return float64(s.Pool.ActiveRepairs) }},
		{"quic_sessions_inbound", "Active inbound replication sessions in pool.", func(s *replicateddb.Status) float64 { return float64(s.Pool.InboundSessions) }},
		{"quic_sessions_generic", "Active generic replication sessions in pool.", func(s *replicateddb.Status) float64 { return float64(s.Pool.GenericSessions) }},
		{"pending_dials", "Configured peers without a session.", func(s *replicateddb.Status) float64 { return float64(s.PendingDials) }},
		{"pending_apply", "In-flight remote applies (synchronous path; no apply queue yet).", func(s *replicateddb.Status) float64 { return float64(s.PendingApply) }},
		{"pending_send", "Queued outbound control/need/schema frames.", func(s *replicateddb.Status) float64 { return float64(s.PendingSend) }},
		{"gating_members", "Persisted members with a live retention obligation (the GC gating set).", func(s *replicateddb.Status) float64 { return float64(s.Replication.GatingMembers) }},
		{"queued_need", "Queued gap-pull requests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.QueuedNeed) }},
		{"queued_ctrl", "Queued control frames.", func(s *replicateddb.Status) float64 { return float64(s.Replication.QueuedCtrl) }},
		{"queued_schema_req", "Queued outbound schema requests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.QueuedSchemaReq) }},
		{"queued_schema_resp", "Queued inbound schema requests awaiting service.", func(s *replicateddb.Status) float64 { return float64(s.Replication.QueuedSchemaResp) }},
		{"apply_inflight", "Remote applies currently executing.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.ApplyInflight) }},
		{"pebble_size_bytes", "Pebble disk usage in bytes.", func(s *replicateddb.Status) float64 { return float64(s.PebbleSizeBytes) }},
		{"pebble_memtable_bytes", "Pebble memtable memory in bytes.", func(s *replicateddb.Status) float64 { return float64(s.PebbleMemTableBytes) }},
		{"uptime_seconds", "Seconds since Open.", func(s *replicateddb.Status) float64 { return s.Uptime.Seconds() }},
	}
}

func counters() []seriesDef {
	return []seriesDef{
		{"pebble_cache_hits_total", "Pebble block cache hits.", func(s *replicateddb.Status) float64 { return float64(s.PebbleCacheHits) }},
		{"pebble_cache_misses_total", "Pebble block cache misses.", func(s *replicateddb.Status) float64 { return float64(s.PebbleCacheMisses) }},
		{"stmt_cache_hits_total", "Prepared-statement cache hits.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.StmtCacheHits) }},
		{"stmt_cache_misses_total", "Prepared-statement cache misses.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.StmtCacheMisses) }},
		{"local_commits_total", "Durable local commits.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.LocalCommits) }},
		{"local_commit_mutations_total", "Mutations in durable local commits.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.LocalCommitMutations) }},
		{"local_commit_latency_seconds_total", "Total local commit latency.", func(s *replicateddb.Status) float64 { return nanosSeconds(s.Metrics.LocalCommitLatencyNanos) }},
		{"write_acquisitions_total", "Write coordinator acquisitions.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.WriteAcquisitions) }},
		{"write_queue_wait_seconds_total", "Total write coordinator queue wait.", func(s *replicateddb.Status) float64 { return nanosSeconds(s.Metrics.WriteQueueWaitNanos) }},
		{"remote_applies_total", "Applied remote batches.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.RemoteApplies) }},
		{"remote_apply_mutations_total", "Mutations in applied remote batches.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.RemoteApplyMutations) }},
		{"remote_apply_winners_total", "Winning cells materialized from remote batches.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.RemoteApplyWinners) }},
		{"remote_apply_latency_seconds_total", "Total remote apply latency.", func(s *replicateddb.Status) float64 { return nanosSeconds(s.Metrics.RemoteApplyLatencyNanos) }},
		{"remote_apply_failures_total", "Failed remote applies.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.RemoteApplyFailures) }},
		{"snapshot_chunks_applied_total", "Applied snapshot chunks.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SnapshotChunksApplied) }},
		{"snapshot_applies_completed_total", "Completed snapshot publications.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SnapshotAppliesCompleted) }},
		{"snapshot_apply_failures_total", "Failed snapshot chunk applies.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SnapshotApplyFailures) }},
		{"rebuilds_total", "Query-engine rebuilds.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.Rebuilds) }},
		{"rebuild_seconds_total", "Total rebuild time.", func(s *replicateddb.Status) float64 { return nanosSeconds(s.Metrics.RebuildNanos) }},
		{"repairs_total", "Local commit row repairs after interleaved durable commits.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.Repairs) }},
		{"gc_runs_total", "Log GC passes.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.GCRuns) }},
		{"gc_seconds_total", "Total log GC time.", func(s *replicateddb.Status) float64 { return nanosSeconds(s.Metrics.GCNanos) }},
		{"gc_log_collected_total", "Origin-log batches collected.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.GCLogCollected) }},
		{"gc_receipts_collected_total", "Receipts collected.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.GCReceiptsCollected) }},
		{"gc_failures_total", "Failed GC passes.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.GCFailures) }},
		{"schema_conflicts_total", "Incompatible schema decisions.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SchemaConflicts) }},
		{"schema_agreements_total", "Schema agreements reached.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SchemaAgreements) }},
		{"schema_sync_needs_total", "Schema rounds needing more ancestry.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SchemaSyncNeeds) }},
		{"schema_adoptions_total", "Adopted remote schema revisions.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SchemaAdoptions) }},
		{"schema_merges_total", "Published deterministic schema merges.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SchemaMerges) }},
		{"schema_merge_reuse_total", "Reused persisted merge results.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SchemaMergeReuse) }},
		{"schema_migrations_total", "Successful local migrations.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.SchemaMigrations) }},
		{"peers_added_total", "AddPeer calls.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.PeersAdded) }},
		{"peers_removed_total", "RemovePeer calls.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.PeersRemoved) }},
		{"force_syncs_total", "ForceSync calls.", func(s *replicateddb.Status) float64 { return float64(s.Metrics.ForceSyncs) }},
		{"repl_dials_total", "Outbound dial attempts.", func(s *replicateddb.Status) float64 { return float64(s.Replication.Dials) }},
		{"repl_dial_failures_total", "Failed dials.", func(s *replicateddb.Status) float64 { return float64(s.Replication.DialFailures) }},
		{"repl_dial_policy_denials_total", "Dials refused by address policy.", func(s *replicateddb.Status) float64 { return float64(s.Replication.DialPolicyDenials) }},
		{"repl_accepts_total", "Accepted inbound connections.", func(s *replicateddb.Status) float64 { return float64(s.Replication.Accepts) }},
		{"repl_accept_failures_total", "Failed accepts.", func(s *replicateddb.Status) float64 { return float64(s.Replication.AcceptFailures) }},
		{"repl_accept_policy_denials_total", "Accepts refused by address policy.", func(s *replicateddb.Status) float64 { return float64(s.Replication.AcceptPolicyDenials) }},
		{"repl_handshake_failures_total", "Failed handshakes.", func(s *replicateddb.Status) float64 { return float64(s.Replication.HandshakeFailures) }},
		{"repl_handshake_identity_refusals_total", "Handshakes refused on identity/protocol.", func(s *replicateddb.Status) float64 { return float64(s.Replication.HandshakeIdentityRefusals) }},
		{"repl_handshake_schema_refusals_total", "Handshakes refused on schema (strict mode).", func(s *replicateddb.Status) float64 { return float64(s.Replication.HandshakeSchemaRefusals) }},
		{"repl_handshake_capability_refusals_total", "Handshakes refused on unknown required capabilities.", func(s *replicateddb.Status) float64 { return float64(s.Replication.HandshakeCapabilityRefl) }},
		{"repl_handshake_retired_refusals_total", "Handshakes refused for retired members.", func(s *replicateddb.Status) float64 { return float64(s.Replication.HandshakeRetiredRefusals) }},
		{"repl_member_admissions_total", "Persisted member admissions on first authenticated handshake.", func(s *replicateddb.Status) float64 { return float64(s.Replication.MemberAdmissions) }},
		{"repl_sessions_opened_total", "Attached sessions.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SessionsOpened) }},
		{"repl_sessions_recycled_total", "Recycled suspect sessions.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SessionsRecycled) }},
		{"repl_sessions_dedupe_closed_total", "Sessions closed by dedupe tie-break.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SessionsDedupeClosed) }},
		{"repl_frames_received_total", "Received session frames.", func(s *replicateddb.Status) float64 { return float64(s.Replication.FramesReceived) }},
		{"repl_frame_bytes_sent_total", "Sent session frame bytes.", func(s *replicateddb.Status) float64 { return float64(s.Replication.FrameBytesSent) }},
		{"repl_frame_bytes_received_total", "Received session frame bytes.", func(s *replicateddb.Status) float64 { return float64(s.Replication.FrameBytesReceived) }},
		{"repl_batches_sent_total", "Sent mutation batches.", func(s *replicateddb.Status) float64 { return float64(s.Replication.BatchesSent) }},
		{"repl_batches_received_total", "Applied received batches.", func(s *replicateddb.Status) float64 { return float64(s.Replication.BatchesReceived) }},
		{"repl_mutations_sent_total", "Sent mutations.", func(s *replicateddb.Status) float64 { return float64(s.Replication.MutationsSent) }},
		{"repl_mutations_received_total", "Mutations in applied received batches.", func(s *replicateddb.Status) float64 { return float64(s.Replication.MutationsReceived) }},
		{"repl_batch_bytes_sent_total", "Sent batch payload bytes.", func(s *replicateddb.Status) float64 { return float64(s.Replication.BatchBytesSent) }},
		{"repl_batch_bytes_received_total", "Received batch payload bytes.", func(s *replicateddb.Status) float64 { return float64(s.Replication.BatchBytesReceived) }},
		{"repl_batches_invalid_total", "Dropped invalid batches.", func(s *replicateddb.Status) float64 { return float64(s.Replication.BatchesInvalid) }},
		{"repl_batches_deferred_total", "Batches deferred for unknown schema.", func(s *replicateddb.Status) float64 { return float64(s.Replication.BatchesDeferred) }},
		{"repl_gaps_detected_total", "Detected origin-log gaps.", func(s *replicateddb.Status) float64 { return float64(s.Replication.GapsDetected) }},
		{"repl_apply_retries_total", "Remote apply conflict retries.", func(s *replicateddb.Status) float64 { return float64(s.Replication.ApplyRetries) }},
		{"repl_apply_failures_total", "Remote apply failures after retries.", func(s *replicateddb.Status) float64 { return float64(s.Replication.ApplyFailures) }},
		{"repl_acks_sent_total", "Sent watermark acknowledgements.", func(s *replicateddb.Status) float64 { return float64(s.Replication.AcksSent) }},
		{"repl_acks_received_total", "Received watermark acknowledgements.", func(s *replicateddb.Status) float64 { return float64(s.Replication.AcksReceived) }},
		{"repl_needs_sent_total", "Sent gap-pull requests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.NeedsSent) }},
		{"repl_needs_received_total", "Received gap-pull requests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.NeedsReceived) }},
		{"repl_pings_sent_total", "Sent liveness pings.", func(s *replicateddb.Status) float64 { return float64(s.Replication.PingsSent) }},
		{"repl_pings_received_total", "Received liveness pings.", func(s *replicateddb.Status) float64 { return float64(s.Replication.PingsReceived) }},
		{"repl_pongs_received_total", "Received pong replies.", func(s *replicateddb.Status) float64 { return float64(s.Replication.PongsReceived) }},
		{"repl_errors_received_total", "Received error frames.", func(s *replicateddb.Status) float64 { return float64(s.Replication.ErrorsReceived) }},
		{"repl_snapshot_required_sent_total", "Sent snapshot-required errors.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotRequiredSent) }},
		{"repl_snapshot_required_received_total", "Received snapshot-required errors.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotRequiredReceived) }},
		{"repl_snapshot_requests_sent_total", "Sent snapshot requests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotRequestsSent) }},
		{"repl_snapshots_sent_total", "Completed outbound snapshots.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotsSent) }},
		{"repl_snapshots_send_failed_total", "Failed outbound snapshots.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotsSendFailed) }},
		{"repl_snapshots_received_total", "Completed inbound snapshots.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotsReceived) }},
		{"repl_snapshot_manifests_received_total", "Received snapshot manifests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotManifestsReceived) }},
		{"repl_snapshot_manifests_rejected_total", "Rejected snapshot manifests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotManifestsRejected) }},
		{"repl_snapshot_chunks_sent_total", "Sent snapshot chunks.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotChunksSent) }},
		{"repl_snapshot_chunks_received_total", "Received snapshot chunks.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotChunksReceived) }},
		{"repl_snapshot_bytes_sent_total", "Sent snapshot chunk bytes.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotBytesSent) }},
		{"repl_snapshot_bytes_received_total", "Received snapshot chunk bytes.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotBytesReceived) }},
		{"repl_snapshot_rerequested_total", "Re-requested incomplete snapshots.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotReRequested) }},
		{"repl_snapshots_busy_deferred_total", "Snapshot requests deferred while a transfer was in flight.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotsBusyDeferred) }},
		{"repl_snapshot_busy_received_total", "Snapshot busy deferrals received from sources.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SnapshotBusyReceived) }},
		{"repl_schema_requests_sent_total", "Sent schema requests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaRequestsSent) }},
		{"repl_schema_requests_received_total", "Received schema requests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaRequestsReceived) }},
		{"repl_schema_requests_served_total", "Served schema requests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaRequestsServed) }},
		{"repl_schema_manifests_sent_total", "Sent schema manifests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaManifestsSent) }},
		{"repl_schema_manifests_received_total", "Received schema manifests.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaManifestsReceived) }},
		{"repl_schema_acks_sent_total", "Sent schema acknowledgements.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaAcksSent) }},
		{"repl_schema_acks_received_total", "Received schema acknowledgements.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaAcksReceived) }},
		{"repl_schema_conflicts_total", "Session-level schema conflicts.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaConflicts) }},
		{"repl_schema_agreements_total", "Session-level schema agreements.", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaAgreements) }},
		{"repl_ctrl_drops_total", "Dropped control frames (peer re-requests).", func(s *replicateddb.Status) float64 { return float64(s.Replication.CtrlDrops) }},
		{"repl_need_drops_total", "Dropped gap-pull requests (peer re-requests).", func(s *replicateddb.Status) float64 { return float64(s.Replication.NeedDrops) }},
		{"repl_schema_req_drops_total", "Dropped outbound schema requests (re-requested).", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaReqDrops) }},
		{"repl_schema_resp_drops_total", "Dropped inbound schema requests (peer re-requests).", func(s *replicateddb.Status) float64 { return float64(s.Replication.SchemaRespDrops) }},
		{"repl_peer_rotations_total", "Selected target peer rotations.", func(s *replicateddb.Status) float64 { return float64(s.Replication.PeerRotations) }},
		{"repl_anti_entropy_runs_total", "Initiated anti-entropy synchronization rounds.", func(s *replicateddb.Status) float64 { return float64(s.Replication.AntiEntropyRuns) }},
		{"repl_anti_entropy_failures_total", "Failed anti-entropy synchronization attempts.", func(s *replicateddb.Status) float64 { return float64(s.Replication.AntiEntropyFailures) }},
		{"swim_probes_completed_total", "Completed direct/indirect SWIM probes.", func(s *replicateddb.Status) float64 { return float64(s.Membership.ProbesCompleted) }},
		{"swim_probe_failures_total", "Failed direct/indirect SWIM probes.", func(s *replicateddb.Status) float64 { return float64(s.Membership.ProbeFailures) }},
		{"swim_refutations_total", "SWIM suspicion refutations sent.", func(s *replicateddb.Status) float64 { return float64(s.Membership.Refutations) }},
		{"swim_suspicions_total", "SWIM suspicion transitions observed.", func(s *replicateddb.Status) float64 { return float64(s.Membership.Suspicions) }},
		{"swim_bootstrap_attempts_total", "SWIM partial-seed bootstrap attempts.", func(s *replicateddb.Status) float64 { return float64(s.Membership.BootstrapAttempts) }},
		{"swim_bootstrap_successes_total", "Successful SWIM bootstrap joins.", func(s *replicateddb.Status) float64 { return float64(s.Membership.BootstrapSuccesses) }},
		{"swim_bootstrap_failures_total", "Failed SWIM bootstrap join attempts.", func(s *replicateddb.Status) float64 { return float64(s.Membership.BootstrapFailures) }},
		{"swim_event_drops_total", "Dropped membership events due to full queues.", func(s *replicateddb.Status) float64 { return float64(s.Membership.EventDrops) }},
		{"swim_reconciled_joins_total", "SWIM periodic reconciliation join corrections.", func(s *replicateddb.Status) float64 { return float64(s.Membership.ReconciledJoins) }},
		{"swim_reconciled_leaves_total", "SWIM periodic reconciliation leave corrections.", func(s *replicateddb.Status) float64 { return float64(s.Membership.ReconciledLeaves) }},
		{"swim_reconciled_updates_total", "SWIM periodic reconciliation update corrections.", func(s *replicateddb.Status) float64 { return float64(s.Membership.ReconciledUpdates) }},
		{"swim_packets_sent_total", "Sent SWIM packet datagrams.", func(s *replicateddb.Status) float64 { return float64(s.Membership.PacketsSent) }},
		{"swim_packets_received_total", "Received SWIM packet datagrams.", func(s *replicateddb.Status) float64 { return float64(s.Membership.PacketsReceived) }},
		{"swim_packet_bytes_sent_total", "Sent SWIM packet bytes.", func(s *replicateddb.Status) float64 { return float64(s.Membership.PacketBytesSent) }},
		{"swim_packet_bytes_received_total", "Received SWIM packet bytes.", func(s *replicateddb.Status) float64 { return float64(s.Membership.PacketBytesReceived) }},
		{"swim_packet_drops_total", "Dropped incoming SWIM packet datagrams due to full buffer.", func(s *replicateddb.Status) float64 { return float64(s.Membership.PacketDrops) }},
		{"swim_stream_drops_total", "Dropped incoming SWIM stream connections due to full buffer.", func(s *replicateddb.Status) float64 { return float64(s.Membership.StreamDrops) }},
		{"swim_datagram_oversize_errors_total", "SWIM datagram packets rejected due to oversize payload.", func(s *replicateddb.Status) float64 { return float64(s.Membership.DatagramOversizeErrors) }},
		{"swim_datagram_envelope_errors_total", "SWIM datagram packets rejected due to envelope or magic mismatch.", func(s *replicateddb.Status) float64 { return float64(s.Membership.DatagramEnvelopeErrors) }},
		{"swim_datagram_dbid_mismatches_total", "SWIM datagram packets rejected due to cluster DBID mismatch.", func(s *replicateddb.Status) float64 { return float64(s.Membership.DatagramDBIDMismatches) }},
		{"swim_streams_dialed_total", "Stream connections dialed by memberlist transport.", func(s *replicateddb.Status) float64 { return float64(s.Membership.StreamsDialed) }},
		{"swim_streams_accepted_total", "Stream connections accepted by memberlist transport.", func(s *replicateddb.Status) float64 { return float64(s.Membership.StreamsAccepted) }},
		{"swim_stream_dial_failures_total", "Failed stream dials in memberlist transport.", func(s *replicateddb.Status) float64 { return float64(s.Membership.StreamDialFailures) }},
		{"pool_conn_deferrals_total", "Connection dials deferred by pool quota limit.", func(s *replicateddb.Status) float64 { return float64(s.Pool.ConnDeferrals) }},
		{"pool_session_deferrals_total", "Session admissions deferred by pool quota limit.", func(s *replicateddb.Status) float64 { return float64(s.Pool.SessionDeferrals) }},
		{"pool_evictions_total", "Idle connections or sessions evicted by pool.", func(s *replicateddb.Status) float64 { return float64(s.Pool.Evictions) }},
		{"pool_dials_total", "Outbound connection dials via shared pool.", func(s *replicateddb.Status) float64 { return float64(s.Pool.Dials) }},
		{"pool_dials_coalesced_total", "Outbound connection dials coalesced via singleflight.", func(s *replicateddb.Status) float64 { return float64(s.Pool.DialsCoalesced) }},
		{"pool_dials_reused_total", "Connection dials satisfied by reusing active connection.", func(s *replicateddb.Status) float64 { return float64(s.Pool.DialsReused) }},
	}
}

// Collector scrapes Status snapshots into Prometheus series. It holds no
// export state, so collection is safe for concurrent use. Each collection
// takes exactly one snapshot, so all series describe the same instant.
type Collector struct {
	src        SnapshotFunc
	info       *prometheus.Desc
	gaugeDescs []*prometheus.Desc
	gaugeGets  []func(*replicateddb.Status) float64
	cntDescs   []*prometheus.Desc
	cntGets    []func(*replicateddb.Status) float64

	peerConnected          *prometheus.Desc
	peerSelected           *prometheus.Desc
	peerRTT                *prometheus.Desc
	peerSchemaAgreed       *prometheus.Desc
	peerSnapshotRequired   *prometheus.Desc
	peerAwaitingSnapshot   *prometheus.Desc
	peerSnapChunksReceived *prometheus.Desc
	peerSnapChunksTotal    *prometheus.Desc
	peerRetired            *prometheus.Desc
	peerExcluded           *prometheus.Desc
	peerRetirementDeadline *prometheus.Desc

	schedAcquisitions *prometheus.Desc
	schedCancels      *prometheus.Desc
	schedWait         *prometheus.Desc
	schedService      *prometheus.Desc
	schedDualService  *prometheus.Desc
	schedWaiters      *prometheus.Desc
	schedOldestWait   *prometheus.Desc
	schedDebt         *prometheus.Desc
	peerBytesSent     *prometheus.Desc
	peerBytesReceived *prometheus.Desc
	peerQueuedNeed    *prometheus.Desc
	peerQueuedCtrl    *prometheus.Desc
	peerQueuedSchema  *prometheus.Desc
	peerLag           *prometheus.Desc
}

// NewCollector returns a Collector scraping src on each collection.
func NewCollector(src SnapshotFunc) *Collector {
	c := &Collector{src: src}
	c.info = prometheus.NewDesc("spedsql_info", "Node identity and lifecycle state.", []string{"state", "node_id", "db_id"}, nil)
	for _, g := range gauges() {
		c.gaugeDescs = append(c.gaugeDescs, prometheus.NewDesc("spedsql_"+g.name, g.help, nil, nil))
		c.gaugeGets = append(c.gaugeGets, g.get)
	}
	for _, ct := range counters() {
		c.cntDescs = append(c.cntDescs, prometheus.NewDesc("spedsql_"+ct.name, ct.help, nil, nil))
		c.cntGets = append(c.cntGets, ct.get)
	}
	c.peerConnected = prometheus.NewDesc("spedsql_peer_connected", "Peer has a live session.", []string{"peer"}, nil)
	c.peerSelected = prometheus.NewDesc("spedsql_peer_selected", "Peer is selected as a replication target.", []string{"peer"}, nil)
	c.peerRTT = prometheus.NewDesc("spedsql_peer_rtt_seconds", "Last measured peer round-trip time.", []string{"peer"}, nil)
	c.peerSchemaAgreed = prometheus.NewDesc("spedsql_peer_schema_agreed", "Peer session has agreed schemas.", []string{"peer"}, nil)
	c.peerSnapshotRequired = prometheus.NewDesc("spedsql_peer_snapshot_required", "Peer must snapshot-resync for a collected log range.", []string{"peer"}, nil)
	c.peerAwaitingSnapshot = prometheus.NewDesc("spedsql_peer_awaiting_snapshot", "Outbound snapshot request awaiting completion.", []string{"peer"}, nil)
	c.peerSnapChunksReceived = prometheus.NewDesc("spedsql_peer_snapshot_chunks_received", "Inbound snapshot chunks received from the peer.", []string{"peer"}, nil)
	c.peerSnapChunksTotal = prometheus.NewDesc("spedsql_peer_snapshot_chunks_total", "Inbound snapshot chunk count advertised by the peer manifest.", []string{"peer"}, nil)
	c.peerRetired = prometheus.NewDesc("spedsql_peer_retired", "Peer is explicitly retired (holds no retention obligation).", []string{"peer"}, nil)
	c.peerExcluded = prometheus.NewDesc("spedsql_peer_excluded", "Peer is locally excluded (retired and refusing sessions).", []string{"peer"}, nil)
	c.peerRetirementDeadline = prometheus.NewDesc("spedsql_peer_retirement_deadline_seconds", "Unix timestamp when peer retention obligation expires.", []string{"peer"}, nil)
	c.schedAcquisitions = prometheus.NewDesc("spedsql_sched_acquisitions_total", "Writer admission grants by class.", []string{"class"}, nil)
	c.schedCancels = prometheus.NewDesc("spedsql_sched_cancels_total", "Canceled writer admissions by class.", []string{"class"}, nil)
	c.schedWait = prometheus.NewDesc("spedsql_sched_wait_seconds_total", "Total writer admission queue wait by class.", []string{"class"}, nil)
	c.schedService = prometheus.NewDesc("spedsql_sched_service_seconds_total", "Total admitted writer service time by class.", []string{"class"}, nil)
	c.schedDualService = prometheus.NewDesc("spedsql_sched_dual_service_seconds_total", "Admitted writer service time granted while the other interactive class had waiters.", []string{"class"}, nil)
	c.schedWaiters = prometheus.NewDesc("spedsql_sched_waiters", "Current writer admission waiters by class.", []string{"class"}, nil)
	c.schedOldestWait = prometheus.NewDesc("spedsql_sched_oldest_wait_seconds", "Oldest queued writer admission by class.", []string{"class"}, nil)
	c.schedDebt = prometheus.NewDesc("spedsql_sched_debt_seconds", "Normalized service-time debt spread between local and remote classes.", nil, nil)
	c.peerBytesSent = prometheus.NewDesc("spedsql_peer_bytes_sent_total", "Session frame bytes sent to the peer.", []string{"peer"}, nil)
	c.peerBytesReceived = prometheus.NewDesc("spedsql_peer_bytes_received_total", "Session frame bytes received from the peer.", []string{"peer"}, nil)
	c.peerQueuedNeed = prometheus.NewDesc("spedsql_peer_queued_need", "Queued gap-pull requests for the peer.", []string{"peer"}, nil)
	c.peerQueuedCtrl = prometheus.NewDesc("spedsql_peer_queued_ctrl", "Queued control frames for the peer.", []string{"peer"}, nil)
	c.peerQueuedSchema = prometheus.NewDesc("spedsql_peer_queued_schema", "Queued schema frames for the peer.", []string{"peer"}, nil)
	c.peerLag = prometheus.NewDesc("spedsql_peer_lag_sequences", "Applied watermark minus peer observed watermark per origin.", []string{"peer", "origin"}, nil)
	return c
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.info
	for _, d := range c.gaugeDescs {
		ch <- d
	}
	for _, d := range c.cntDescs {
		ch <- d
	}
	ch <- c.peerConnected
	ch <- c.peerSelected
	ch <- c.peerRTT
	ch <- c.peerSchemaAgreed
	ch <- c.peerSnapshotRequired
	ch <- c.peerAwaitingSnapshot
	ch <- c.peerSnapChunksReceived
	ch <- c.peerSnapChunksTotal
	ch <- c.peerRetired
	ch <- c.peerExcluded
	ch <- c.peerRetirementDeadline
	ch <- c.schedAcquisitions
	ch <- c.schedCancels
	ch <- c.schedWait
	ch <- c.schedService
	ch <- c.schedDualService
	ch <- c.schedWaiters
	ch <- c.schedOldestWait
	ch <- c.schedDebt
	ch <- c.peerBytesSent
	ch <- c.peerBytesReceived
	ch <- c.peerQueuedNeed
	ch <- c.peerQueuedCtrl
	ch <- c.peerQueuedSchema
	ch <- c.peerLag
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	st := c.src()
	ch <- prometheus.MustNewConstMetric(c.info, prometheus.GaugeValue, 1,
		st.State.String(), st.NodeID.String(), st.DBID.String())
	for i, d := range c.gaugeDescs {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, c.gaugeGets[i](&st))
	}
	for i, d := range c.cntDescs {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, c.cntGets[i](&st))
	}
	classes := []struct {
		name string
		st   replicateddb.SchedulerClassStats
	}{
		{"local", st.Metrics.Scheduler.Local},
		{"remote", st.Metrics.Scheduler.Remote},
		{"maintenance", st.Metrics.Scheduler.Maintenance},
	}
	for _, cl := range classes {
		ch <- prometheus.MustNewConstMetric(c.schedAcquisitions, prometheus.CounterValue, float64(cl.st.Acquisitions), cl.name)
		ch <- prometheus.MustNewConstMetric(c.schedCancels, prometheus.CounterValue, float64(cl.st.Cancels), cl.name)
		ch <- prometheus.MustNewConstMetric(c.schedWait, prometheus.CounterValue, nanosSeconds(cl.st.WaitNanos), cl.name)
		ch <- prometheus.MustNewConstMetric(c.schedService, prometheus.CounterValue, nanosSeconds(cl.st.ServiceNanos), cl.name)
		ch <- prometheus.MustNewConstMetric(c.schedDualService, prometheus.CounterValue, nanosSeconds(cl.st.DualServiceNanos), cl.name)
		ch <- prometheus.MustNewConstMetric(c.schedWaiters, prometheus.GaugeValue, float64(cl.st.Waiters), cl.name)
		ch <- prometheus.MustNewConstMetric(c.schedOldestWait, prometheus.GaugeValue, nanosSeconds(cl.st.OldestWaitNanos), cl.name)
	}
	ch <- prometheus.MustNewConstMetric(c.schedDebt, prometheus.GaugeValue, nanosSeconds(st.Metrics.Scheduler.DebtNanos))
	for i := range st.Peers {
		p := &st.Peers[i]
		peer := p.NodeID.String()
		boolVal := func(b bool) float64 {
			if b {
				return 1
			}
			return 0
		}
		ch <- prometheus.MustNewConstMetric(c.peerConnected, prometheus.GaugeValue, boolVal(p.Connected), peer)
		ch <- prometheus.MustNewConstMetric(c.peerSelected, prometheus.GaugeValue, boolVal(p.Selected), peer)
		ch <- prometheus.MustNewConstMetric(c.peerRTT, prometheus.GaugeValue, p.RTT.Seconds(), peer)
		ch <- prometheus.MustNewConstMetric(c.peerSchemaAgreed, prometheus.GaugeValue, boolVal(p.SchemaAgreed), peer)
		ch <- prometheus.MustNewConstMetric(c.peerSnapshotRequired, prometheus.GaugeValue, boolVal(p.SnapshotRequired), peer)
		ch <- prometheus.MustNewConstMetric(c.peerAwaitingSnapshot, prometheus.GaugeValue, boolVal(p.AwaitingSnapshot), peer)
		ch <- prometheus.MustNewConstMetric(c.peerSnapChunksReceived, prometheus.GaugeValue, float64(p.SnapshotChunksReceived), peer)
		ch <- prometheus.MustNewConstMetric(c.peerSnapChunksTotal, prometheus.GaugeValue, float64(p.SnapshotChunksTotal), peer)
		ch <- prometheus.MustNewConstMetric(c.peerRetired, prometheus.GaugeValue, boolVal(p.Retired), peer)
		ch <- prometheus.MustNewConstMetric(c.peerExcluded, prometheus.GaugeValue, boolVal(p.Excluded), peer)
		var deadlineSec float64
		if !p.RetirementDeadline.IsZero() {
			deadlineSec = float64(p.RetirementDeadline.Unix())
		}
		ch <- prometheus.MustNewConstMetric(c.peerRetirementDeadline, prometheus.GaugeValue, deadlineSec, peer)
		ch <- prometheus.MustNewConstMetric(c.peerBytesSent, prometheus.CounterValue, float64(p.BytesSent), peer)
		ch <- prometheus.MustNewConstMetric(c.peerBytesReceived, prometheus.CounterValue, float64(p.BytesReceived), peer)
		ch <- prometheus.MustNewConstMetric(c.peerQueuedNeed, prometheus.GaugeValue, float64(p.QueuedNeed), peer)
		ch <- prometheus.MustNewConstMetric(c.peerQueuedCtrl, prometheus.GaugeValue, float64(p.QueuedCtrl), peer)
		ch <- prometheus.MustNewConstMetric(c.peerQueuedSchema, prometheus.GaugeValue, float64(p.QueuedSchema), peer)
		for origin, lag := range p.LagByOrigin {
			ch <- prometheus.MustNewConstMetric(c.peerLag, prometheus.GaugeValue, float64(lag), peer, origin.String())
		}
	}
}
