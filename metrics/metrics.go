// Package metrics renders Murmur diagnostic snapshots as JSON over HTTP.
//
// Unimplemented subsystems (SWIM membership, bounded peer selection,
// Plumtree, overload budgets, High/Low bridge, file objects, subscriptions)
// have no series here; their Status fields stay zero and must not be
// mistaken for measured zeros.
package metrics

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/marcgauthier/murmur"
)

// SnapshotFunc returns the current diagnostic snapshot. DB.Status satisfies it.
type SnapshotFunc func() murmur.Status

type seriesDef struct {
	name string
	help string
	get  func(*murmur.Status) float64
}

func nanosSeconds(n uint64) float64 { return float64(n) / float64(time.Second) }

func gauges() []seriesDef {
	return []seriesDef{
		{"state_generation", "Durable state generation.", func(s *murmur.Status) float64 { return float64(s.StateGeneration) }},
		{"materialized_generation", "Generation materialized into the query engine.", func(s *murmur.Status) float64 { return float64(s.MaterializedGeneration) }},
		{"hlc", "Maximum observed hybrid logical clock.", func(s *murmur.Status) float64 { return float64(s.HLC) }},
		{"local_seq", "Local origin sequence.", func(s *murmur.Status) float64 { return float64(s.LocalSeq) }},
		{"schema_epoch", "Published schema epoch.", func(s *murmur.Status) float64 { return float64(s.SchemaEpoch) }},
		{"peer_count", "Known peers (configured plus inbound-discovered).", func(s *murmur.Status) float64 { return float64(s.PeerCount) }},
		{"connected_peers", "Peers with a live session.", func(s *murmur.Status) float64 { return float64(s.ConnectedPeers) }},
		{"membership_count", "Membership view size.", func(s *murmur.Status) float64 { return float64(s.MembershipCount) }},
		{"membership_alive_count", "SWIM alive members in cluster view.", func(s *murmur.Status) float64 { return float64(s.Membership.NumAlive) }},
		{"membership_suspect_count", "SWIM suspect members in cluster view.", func(s *murmur.Status) float64 { return float64(s.Membership.NumSuspect) }},
		{"membership_dead_count", "SWIM dead members in cluster view.", func(s *murmur.Status) float64 { return float64(s.Membership.NumDead) }},
		{"selected_peers", "Selected replication targets.", func(s *murmur.Status) float64 { return float64(s.SelectedPeers) }},
		{"quic_connections", "Live QUIC replication sessions.", func(s *murmur.Status) float64 { return float64(s.QUICConnections) }},
		{"quic_connections_active", "Active underlying QUIC peer connections in pool.", func(s *murmur.Status) float64 { return float64(s.Pool.ActiveConnections) }},
		{"quic_sessions_active", "Total active QUIC replication sessions in pool.", func(s *murmur.Status) float64 { return float64(s.Pool.TotalSessions) }},
		{"quic_sessions_target", "Active replication target sessions in pool.", func(s *murmur.Status) float64 { return float64(s.Pool.SelectedTargets) }},
		{"quic_sessions_repair", "Active anti-entropy repair sessions in pool.", func(s *murmur.Status) float64 { return float64(s.Pool.ActiveRepairs) }},
		{"quic_sessions_inbound", "Active inbound replication sessions in pool.", func(s *murmur.Status) float64 { return float64(s.Pool.InboundSessions) }},
		{"quic_sessions_generic", "Active generic replication sessions in pool.", func(s *murmur.Status) float64 { return float64(s.Pool.GenericSessions) }},
		{"pending_dials", "Configured peers without a session.", func(s *murmur.Status) float64 { return float64(s.PendingDials) }},
		{"pending_apply", "In-flight remote applies (synchronous path; no apply queue yet).", func(s *murmur.Status) float64 { return float64(s.PendingApply) }},
		{"pending_send", "Queued outbound control/need/schema frames.", func(s *murmur.Status) float64 { return float64(s.PendingSend) }},
		{"gating_members", "Persisted members with a live retention obligation (the GC gating set).", func(s *murmur.Status) float64 { return float64(s.Replication.GatingMembers) }},
		{"queued_need", "Queued gap-pull requests.", func(s *murmur.Status) float64 { return float64(s.Replication.QueuedNeed) }},
		{"queued_ctrl", "Queued control frames.", func(s *murmur.Status) float64 { return float64(s.Replication.QueuedCtrl) }},
		{"queued_schema_req", "Queued outbound schema requests.", func(s *murmur.Status) float64 { return float64(s.Replication.QueuedSchemaReq) }},
		{"queued_schema_resp", "Queued inbound schema requests awaiting service.", func(s *murmur.Status) float64 { return float64(s.Replication.QueuedSchemaResp) }},
		{"apply_inflight", "Remote applies currently executing.", func(s *murmur.Status) float64 { return float64(s.Metrics.ApplyInflight) }},
		{"spool_disk_bytes", "Spool disk usage in bytes.", func(s *murmur.Status) float64 { return float64(s.SpoolDiskBytes) }},
		{"spool_pending_bytes", "Spool pending write buffer memory in bytes.", func(s *murmur.Status) float64 { return float64(s.SpoolPendingBytes) }},
		{"spool_keys", "Spool indexed keys count.", func(s *murmur.Status) float64 { return float64(s.SpoolKeys) }},
		{"uptime_seconds", "Seconds since Open.", func(s *murmur.Status) float64 { return s.Uptime.Seconds() }},
	}
}

func counters() []seriesDef {
	return []seriesDef{
		{"spool_blocks_written_total", "Spool blocks written.", func(s *murmur.Status) float64 { return float64(s.SpoolBlocksWritten) }},
		{"spool_bytes_written_total", "Spool bytes written.", func(s *murmur.Status) float64 { return float64(s.SpoolBytesWritten) }},
		{"spool_compactions_total", "Spool compaction passes completed.", func(s *murmur.Status) float64 { return float64(s.SpoolCompactions) }},
		{"local_commits_total", "Durable local commits.", func(s *murmur.Status) float64 { return float64(s.Metrics.LocalCommits) }},
		{"local_commit_mutations_total", "Mutations in durable local commits.", func(s *murmur.Status) float64 { return float64(s.Metrics.LocalCommitMutations) }},
		{"local_commit_latency_seconds_total", "Total local commit latency.", func(s *murmur.Status) float64 { return nanosSeconds(s.Metrics.LocalCommitLatencyNanos) }},
		{"write_acquisitions_total", "Write coordinator acquisitions.", func(s *murmur.Status) float64 { return float64(s.Metrics.WriteAcquisitions) }},
		{"write_queue_wait_seconds_total", "Total write coordinator queue wait.", func(s *murmur.Status) float64 { return nanosSeconds(s.Metrics.WriteQueueWaitNanos) }},
		{"remote_applies_total", "Applied remote batches.", func(s *murmur.Status) float64 { return float64(s.Metrics.RemoteApplies) }},
		{"remote_apply_mutations_total", "Mutations in applied remote batches.", func(s *murmur.Status) float64 { return float64(s.Metrics.RemoteApplyMutations) }},
		{"remote_apply_winners_total", "Winning cells materialized from remote batches.", func(s *murmur.Status) float64 { return float64(s.Metrics.RemoteApplyWinners) }},
		{"remote_apply_latency_seconds_total", "Total remote apply latency.", func(s *murmur.Status) float64 { return nanosSeconds(s.Metrics.RemoteApplyLatencyNanos) }},
		{"remote_apply_failures_total", "Failed remote applies.", func(s *murmur.Status) float64 { return float64(s.Metrics.RemoteApplyFailures) }},
		{"snapshot_chunks_applied_total", "Applied snapshot chunks.", func(s *murmur.Status) float64 { return float64(s.Metrics.SnapshotChunksApplied) }},
		{"snapshot_applies_completed_total", "Completed snapshot publications.", func(s *murmur.Status) float64 { return float64(s.Metrics.SnapshotAppliesCompleted) }},
		{"snapshot_apply_failures_total", "Failed snapshot chunk applies.", func(s *murmur.Status) float64 { return float64(s.Metrics.SnapshotApplyFailures) }},
		{"rebuilds_total", "Query-engine rebuilds.", func(s *murmur.Status) float64 { return float64(s.Metrics.Rebuilds) }},
		{"rebuild_seconds_total", "Total rebuild time.", func(s *murmur.Status) float64 { return nanosSeconds(s.Metrics.RebuildNanos) }},
		{"repairs_total", "Local commit row repairs after interleaved durable commits.", func(s *murmur.Status) float64 { return float64(s.Metrics.Repairs) }},
		{"gc_runs_total", "Log GC passes.", func(s *murmur.Status) float64 { return float64(s.Metrics.GCRuns) }},
		{"gc_seconds_total", "Total log GC time.", func(s *murmur.Status) float64 { return nanosSeconds(s.Metrics.GCNanos) }},
		{"gc_log_collected_total", "Origin-log batches collected.", func(s *murmur.Status) float64 { return float64(s.Metrics.GCLogCollected) }},
		{"gc_receipts_collected_total", "Receipts collected.", func(s *murmur.Status) float64 { return float64(s.Metrics.GCReceiptsCollected) }},
		{"gc_failures_total", "Failed GC passes.", func(s *murmur.Status) float64 { return float64(s.Metrics.GCFailures) }},
		{"schema_conflicts_total", "Incompatible schema decisions.", func(s *murmur.Status) float64 { return float64(s.Metrics.SchemaConflicts) }},
		{"schema_agreements_total", "Schema agreements reached.", func(s *murmur.Status) float64 { return float64(s.Metrics.SchemaAgreements) }},
		{"schema_sync_needs_total", "Schema rounds needing more ancestry.", func(s *murmur.Status) float64 { return float64(s.Metrics.SchemaSyncNeeds) }},
		{"schema_adoptions_total", "Adopted remote schema revisions.", func(s *murmur.Status) float64 { return float64(s.Metrics.SchemaAdoptions) }},
		{"schema_merges_total", "Published deterministic schema merges.", func(s *murmur.Status) float64 { return float64(s.Metrics.SchemaMerges) }},
		{"schema_merge_reuse_total", "Reused persisted merge results.", func(s *murmur.Status) float64 { return float64(s.Metrics.SchemaMergeReuse) }},
		{"schema_migrations_total", "Successful local migrations.", func(s *murmur.Status) float64 { return float64(s.Metrics.SchemaMigrations) }},
		{"peers_added_total", "AddPeer calls.", func(s *murmur.Status) float64 { return float64(s.Metrics.PeersAdded) }},
		{"peers_removed_total", "RemovePeer calls.", func(s *murmur.Status) float64 { return float64(s.Metrics.PeersRemoved) }},
		{"force_syncs_total", "ForceSync calls.", func(s *murmur.Status) float64 { return float64(s.Metrics.ForceSyncs) }},
		{"repl_dials_total", "Outbound dial attempts.", func(s *murmur.Status) float64 { return float64(s.Replication.Dials) }},
		{"repl_dial_failures_total", "Failed dials.", func(s *murmur.Status) float64 { return float64(s.Replication.DialFailures) }},
		{"repl_dial_policy_denials_total", "Dials refused by address policy.", func(s *murmur.Status) float64 { return float64(s.Replication.DialPolicyDenials) }},
		{"repl_accepts_total", "Accepted inbound connections.", func(s *murmur.Status) float64 { return float64(s.Replication.Accepts) }},
		{"repl_accept_failures_total", "Failed accepts.", func(s *murmur.Status) float64 { return float64(s.Replication.AcceptFailures) }},
		{"repl_accept_policy_denials_total", "Accepts refused by address policy.", func(s *murmur.Status) float64 { return float64(s.Replication.AcceptPolicyDenials) }},
		{"repl_handshake_failures_total", "Failed handshakes.", func(s *murmur.Status) float64 { return float64(s.Replication.HandshakeFailures) }},
		{"repl_handshake_identity_refusals_total", "Handshakes refused on identity/protocol.", func(s *murmur.Status) float64 { return float64(s.Replication.HandshakeIdentityRefusals) }},
		{"repl_handshake_schema_refusals_total", "Handshakes refused on schema (strict mode).", func(s *murmur.Status) float64 { return float64(s.Replication.HandshakeSchemaRefusals) }},
		{"repl_handshake_capability_refusals_total", "Handshakes refused on unknown required capabilities.", func(s *murmur.Status) float64 { return float64(s.Replication.HandshakeCapabilityRefl) }},
		{"repl_handshake_retired_refusals_total", "Handshakes refused for retired members.", func(s *murmur.Status) float64 { return float64(s.Replication.HandshakeRetiredRefusals) }},
		{"repl_member_admissions_total", "Persisted member admissions on first authenticated handshake.", func(s *murmur.Status) float64 { return float64(s.Replication.MemberAdmissions) }},
		{"repl_sessions_opened_total", "Attached sessions.", func(s *murmur.Status) float64 { return float64(s.Replication.SessionsOpened) }},
		{"repl_sessions_recycled_total", "Recycled suspect sessions.", func(s *murmur.Status) float64 { return float64(s.Replication.SessionsRecycled) }},
		{"repl_sessions_dedupe_closed_total", "Sessions closed by dedupe tie-break.", func(s *murmur.Status) float64 { return float64(s.Replication.SessionsDedupeClosed) }},
		{"repl_frames_received_total", "Received session frames.", func(s *murmur.Status) float64 { return float64(s.Replication.FramesReceived) }},
		{"repl_frame_bytes_sent_total", "Sent session frame bytes.", func(s *murmur.Status) float64 { return float64(s.Replication.FrameBytesSent) }},
		{"repl_frame_bytes_received_total", "Received session frame bytes.", func(s *murmur.Status) float64 { return float64(s.Replication.FrameBytesReceived) }},
		{"repl_batches_sent_total", "Sent mutation batches.", func(s *murmur.Status) float64 { return float64(s.Replication.BatchesSent) }},
		{"repl_batches_received_total", "Applied received batches.", func(s *murmur.Status) float64 { return float64(s.Replication.BatchesReceived) }},
		{"repl_mutations_sent_total", "Sent mutations.", func(s *murmur.Status) float64 { return float64(s.Replication.MutationsSent) }},
		{"repl_mutations_received_total", "Mutations in applied received batches.", func(s *murmur.Status) float64 { return float64(s.Replication.MutationsReceived) }},
		{"repl_batch_bytes_sent_total", "Sent batch payload bytes.", func(s *murmur.Status) float64 { return float64(s.Replication.BatchBytesSent) }},
		{"repl_batch_bytes_received_total", "Received batch payload bytes.", func(s *murmur.Status) float64 { return float64(s.Replication.BatchBytesReceived) }},
		{"repl_origin_unsigned_total", "Unsigned transactions rejected.", func(s *murmur.Status) float64 { return float64(s.Replication.OriginUnsigned) }},
		{"repl_origin_unknown_total", "Untrusted transaction origins rejected.", func(s *murmur.Status) float64 { return float64(s.Replication.OriginUnknown) }},
		{"repl_origin_signature_invalid_total", "Invalid origin signatures rejected.", func(s *murmur.Status) float64 { return float64(s.Replication.OriginSignatureInvalid) }},
		{"repl_origin_digest_mismatch_total", "Signed mutation digest mismatches rejected.", func(s *murmur.Status) float64 { return float64(s.Replication.OriginDigestMismatch) }},
		{"repl_snapshot_sources_denied_total", "Snapshot recovery attempts denied by source trust policy.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotSourcesDenied) }},
		{"repl_batches_invalid_total", "Dropped invalid batches.", func(s *murmur.Status) float64 { return float64(s.Replication.BatchesInvalid) }},
		{"repl_batches_deferred_total", "Batches deferred for unknown schema.", func(s *murmur.Status) float64 { return float64(s.Replication.BatchesDeferred) }},
		{"repl_gaps_detected_total", "Detected origin-log gaps.", func(s *murmur.Status) float64 { return float64(s.Replication.GapsDetected) }},
		{"repl_apply_retries_total", "Remote apply conflict retries.", func(s *murmur.Status) float64 { return float64(s.Replication.ApplyRetries) }},
		{"repl_apply_failures_total", "Remote apply failures after retries.", func(s *murmur.Status) float64 { return float64(s.Replication.ApplyFailures) }},
		{"repl_acks_sent_total", "Sent watermark acknowledgements.", func(s *murmur.Status) float64 { return float64(s.Replication.AcksSent) }},
		{"repl_acks_received_total", "Received watermark acknowledgements.", func(s *murmur.Status) float64 { return float64(s.Replication.AcksReceived) }},
		{"repl_needs_sent_total", "Sent gap-pull requests.", func(s *murmur.Status) float64 { return float64(s.Replication.NeedsSent) }},
		{"repl_needs_received_total", "Received gap-pull requests.", func(s *murmur.Status) float64 { return float64(s.Replication.NeedsReceived) }},
		{"repl_pings_sent_total", "Sent liveness pings.", func(s *murmur.Status) float64 { return float64(s.Replication.PingsSent) }},
		{"repl_pings_received_total", "Received liveness pings.", func(s *murmur.Status) float64 { return float64(s.Replication.PingsReceived) }},
		{"repl_pongs_received_total", "Received pong replies.", func(s *murmur.Status) float64 { return float64(s.Replication.PongsReceived) }},
		{"repl_errors_received_total", "Received error frames.", func(s *murmur.Status) float64 { return float64(s.Replication.ErrorsReceived) }},
		{"repl_snapshot_required_sent_total", "Sent snapshot-required errors.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotRequiredSent) }},
		{"repl_snapshot_required_received_total", "Received snapshot-required errors.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotRequiredReceived) }},
		{"repl_snapshot_requests_sent_total", "Sent snapshot requests.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotRequestsSent) }},
		{"repl_snapshots_sent_total", "Completed outbound snapshots.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotsSent) }},
		{"repl_snapshots_send_failed_total", "Failed outbound snapshots.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotsSendFailed) }},
		{"repl_snapshots_received_total", "Completed inbound snapshots.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotsReceived) }},
		{"repl_snapshot_manifests_received_total", "Received snapshot manifests.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotManifestsReceived) }},
		{"repl_snapshot_manifests_rejected_total", "Rejected snapshot manifests.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotManifestsRejected) }},
		{"repl_snapshot_chunks_sent_total", "Sent snapshot chunks.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotChunksSent) }},
		{"repl_snapshot_chunks_received_total", "Received snapshot chunks.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotChunksReceived) }},
		{"repl_snapshot_bytes_sent_total", "Sent snapshot chunk bytes.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotBytesSent) }},
		{"repl_snapshot_bytes_received_total", "Received snapshot chunk bytes.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotBytesReceived) }},
		{"repl_snapshot_rerequested_total", "Re-requested incomplete snapshots.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotReRequested) }},
		{"repl_snapshots_busy_deferred_total", "Snapshot requests deferred while a transfer was in flight.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotsBusyDeferred) }},
		{"repl_snapshot_busy_received_total", "Snapshot busy deferrals received from sources.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotBusyReceived) }},
		{"repl_snapshot_frames_suppressed_total", "Snapshot frames ignored from non-active sources.", func(s *murmur.Status) float64 { return float64(s.Replication.SnapshotFramesSuppressed) }},
		{"repl_schema_requests_sent_total", "Sent schema requests.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaRequestsSent) }},
		{"repl_schema_requests_received_total", "Received schema requests.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaRequestsReceived) }},
		{"repl_schema_requests_served_total", "Served schema requests.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaRequestsServed) }},
		{"repl_schema_manifests_sent_total", "Sent schema manifests.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaManifestsSent) }},
		{"repl_schema_manifests_received_total", "Received schema manifests.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaManifestsReceived) }},
		{"repl_schema_acks_sent_total", "Sent schema acknowledgements.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaAcksSent) }},
		{"repl_schema_acks_received_total", "Received schema acknowledgements.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaAcksReceived) }},
		{"repl_schema_conflicts_total", "Session-level schema conflicts.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaConflicts) }},
		{"repl_schema_agreements_total", "Session-level schema agreements.", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaAgreements) }},
		{"repl_ctrl_drops_total", "Dropped control frames (peer re-requests).", func(s *murmur.Status) float64 { return float64(s.Replication.CtrlDrops) }},
		{"repl_need_drops_total", "Dropped gap-pull requests (peer re-requests).", func(s *murmur.Status) float64 { return float64(s.Replication.NeedDrops) }},
		{"repl_schema_req_drops_total", "Dropped outbound schema requests (re-requested).", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaReqDrops) }},
		{"repl_schema_resp_drops_total", "Dropped inbound schema requests (peer re-requests).", func(s *murmur.Status) float64 { return float64(s.Replication.SchemaRespDrops) }},
		{"repl_peer_rotations_total", "Selected target peer rotations.", func(s *murmur.Status) float64 { return float64(s.Replication.PeerRotations) }},
		{"repl_anti_entropy_runs_total", "Initiated anti-entropy synchronization rounds.", func(s *murmur.Status) float64 { return float64(s.Replication.AntiEntropyRuns) }},
		{"repl_anti_entropy_failures_total", "Failed anti-entropy synchronization attempts.", func(s *murmur.Status) float64 { return float64(s.Replication.AntiEntropyFailures) }},
		{"swim_probes_completed_total", "Completed direct/indirect SWIM probes.", func(s *murmur.Status) float64 { return float64(s.Membership.ProbesCompleted) }},
		{"swim_probe_failures_total", "Failed direct/indirect SWIM probes.", func(s *murmur.Status) float64 { return float64(s.Membership.ProbeFailures) }},
		{"swim_refutations_total", "SWIM suspicion refutations sent.", func(s *murmur.Status) float64 { return float64(s.Membership.Refutations) }},
		{"swim_suspicions_total", "SWIM suspicion transitions observed.", func(s *murmur.Status) float64 { return float64(s.Membership.Suspicions) }},
		{"swim_bootstrap_attempts_total", "SWIM partial-seed bootstrap attempts.", func(s *murmur.Status) float64 { return float64(s.Membership.BootstrapAttempts) }},
		{"swim_bootstrap_successes_total", "Successful SWIM bootstrap joins.", func(s *murmur.Status) float64 { return float64(s.Membership.BootstrapSuccesses) }},
		{"swim_bootstrap_failures_total", "Failed SWIM bootstrap join attempts.", func(s *murmur.Status) float64 { return float64(s.Membership.BootstrapFailures) }},
		{"swim_event_drops_total", "Dropped membership events due to full queues.", func(s *murmur.Status) float64 { return float64(s.Membership.EventDrops) }},
		{"swim_reconciled_joins_total", "SWIM periodic reconciliation join corrections.", func(s *murmur.Status) float64 { return float64(s.Membership.ReconciledJoins) }},
		{"swim_reconciled_leaves_total", "SWIM periodic reconciliation leave corrections.", func(s *murmur.Status) float64 { return float64(s.Membership.ReconciledLeaves) }},
		{"swim_reconciled_updates_total", "SWIM periodic reconciliation update corrections.", func(s *murmur.Status) float64 { return float64(s.Membership.ReconciledUpdates) }},
		{"swim_packets_sent_total", "Sent SWIM packet datagrams.", func(s *murmur.Status) float64 { return float64(s.Membership.PacketsSent) }},
		{"swim_packets_received_total", "Received SWIM packet datagrams.", func(s *murmur.Status) float64 { return float64(s.Membership.PacketsReceived) }},
		{"swim_packet_bytes_sent_total", "Sent SWIM packet bytes.", func(s *murmur.Status) float64 { return float64(s.Membership.PacketBytesSent) }},
		{"swim_packet_bytes_received_total", "Received SWIM packet bytes.", func(s *murmur.Status) float64 { return float64(s.Membership.PacketBytesReceived) }},
		{"swim_packet_drops_total", "Dropped incoming SWIM packet datagrams due to full buffer.", func(s *murmur.Status) float64 { return float64(s.Membership.PacketDrops) }},
		{"swim_stream_drops_total", "Dropped incoming SWIM stream connections due to full buffer.", func(s *murmur.Status) float64 { return float64(s.Membership.StreamDrops) }},
		{"swim_datagram_oversize_errors_total", "SWIM datagram packets rejected due to oversize payload.", func(s *murmur.Status) float64 { return float64(s.Membership.DatagramOversizeErrors) }},
		{"swim_datagram_envelope_errors_total", "SWIM datagram packets rejected due to envelope or magic mismatch.", func(s *murmur.Status) float64 { return float64(s.Membership.DatagramEnvelopeErrors) }},
		{"swim_datagram_dbid_mismatches_total", "SWIM datagram packets rejected due to cluster DBID mismatch.", func(s *murmur.Status) float64 { return float64(s.Membership.DatagramDBIDMismatches) }},
		{"swim_streams_dialed_total", "Stream connections dialed by memberlist transport.", func(s *murmur.Status) float64 { return float64(s.Membership.StreamsDialed) }},
		{"swim_streams_accepted_total", "Stream connections accepted by memberlist transport.", func(s *murmur.Status) float64 { return float64(s.Membership.StreamsAccepted) }},
		{"swim_stream_dial_failures_total", "Failed stream dials in memberlist transport.", func(s *murmur.Status) float64 { return float64(s.Membership.StreamDialFailures) }},
		{"pool_conn_deferrals_total", "Connection dials deferred by pool quota limit.", func(s *murmur.Status) float64 { return float64(s.Pool.ConnDeferrals) }},
		{"pool_session_deferrals_total", "Session admissions deferred by pool quota limit.", func(s *murmur.Status) float64 { return float64(s.Pool.SessionDeferrals) }},
		{"pool_evictions_total", "Idle connections or sessions evicted by pool.", func(s *murmur.Status) float64 { return float64(s.Pool.Evictions) }},
		{"pool_dials_total", "Outbound connection dials via shared pool.", func(s *murmur.Status) float64 { return float64(s.Pool.Dials) }},
		{"pool_dials_coalesced_total", "Outbound connection dials coalesced via singleflight.", func(s *murmur.Status) float64 { return float64(s.Pool.DialsCoalesced) }},
		{"pool_dials_reused_total", "Connection dials satisfied by reusing active connection.", func(s *murmur.Status) float64 { return float64(s.Pool.DialsReused) }},
	}
}

// Series is one scalar metric sample. Labels distinguish peer and scheduler
// samples. Values use JSON numbers; consumers should decode them as float64,
// matching the precision and units of the prior exposition.
type Series struct {
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

// Response is the stable shape returned by the metrics HTTP handler.
type Response struct {
	Metrics []Series `json:"metrics"`
}

// Handler returns a standard-library HTTP handler that serializes one
// diagnostic snapshot per request. The endpoint is JSON, not Prometheus text
// exposition. Metric names and label keys are retained for straightforward
// migration of existing consumers.
func Handler(src SnapshotFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if src == nil {
			http.Error(w, "metrics snapshot source is unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(Export(src()))
	})
}

// Export converts a point-in-time status snapshot into named scalar samples.
func Export(st murmur.Status) Response {
	out := Response{Metrics: make([]Series, 0, len(gauges())+len(counters())+80)}
	add := func(name string, value float64, labels ...string) {
		var labelMap map[string]string
		if len(labels) > 0 {
			labelMap = make(map[string]string, len(labels)/2)
			for i := 0; i+1 < len(labels); i += 2 {
				labelMap[labels[i]] = labels[i+1]
			}
		}
		out.Metrics = append(out.Metrics, Series{Name: "spedsql_" + name, Value: value, Labels: labelMap})
	}
	boolValue := func(v bool) float64 {
		if v {
			return 1
		}
		return 0
	}
	add("info", 1, "state", st.State.String(), "node_id", st.NodeID.String(), "db_id", st.DBID.String())
	for _, g := range gauges() {
		add(g.name, g.get(&st))
	}
	for _, c := range counters() {
		add(c.name, c.get(&st))
	}
	classes := []struct {
		name  string
		stats murmur.SchedulerClassStats
	}{
		{"local", st.Metrics.Scheduler.Local}, {"remote", st.Metrics.Scheduler.Remote}, {"maintenance", st.Metrics.Scheduler.Maintenance},
	}
	for _, cl := range classes {
		label := []string{"class", cl.name}
		add("sched_acquisitions_total", float64(cl.stats.Acquisitions), label...)
		add("sched_cancels_total", float64(cl.stats.Cancels), label...)
		add("sched_wait_seconds_total", nanosSeconds(cl.stats.WaitNanos), label...)
		add("sched_service_seconds_total", nanosSeconds(cl.stats.ServiceNanos), label...)
		add("sched_dual_service_seconds_total", nanosSeconds(cl.stats.DualServiceNanos), label...)
		add("sched_waiters", float64(cl.stats.Waiters), label...)
		add("sched_oldest_wait_seconds", nanosSeconds(cl.stats.OldestWaitNanos), label...)
	}
	add("sched_debt_seconds", nanosSeconds(st.Metrics.Scheduler.DebtNanos))
	peers := append([]murmur.PeerDiagnostics(nil), st.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i].NodeID.String() < peers[j].NodeID.String() })
	for _, p := range peers {
		peer := p.NodeID.String()
		label := []string{"peer", peer}
		add("peer_connected", boolValue(p.Connected), label...)
		add("peer_selected", boolValue(p.Selected), label...)
		add("peer_rtt_seconds", p.RTT.Seconds(), label...)
		add("peer_schema_agreed", boolValue(p.SchemaAgreed), label...)
		add("peer_snapshot_required", boolValue(p.SnapshotRequired), label...)
		add("peer_awaiting_snapshot", boolValue(p.AwaitingSnapshot), label...)
		add("peer_snapshot_chunks_received", float64(p.SnapshotChunksReceived), label...)
		add("peer_snapshot_chunks_total", float64(p.SnapshotChunksTotal), label...)
		add("peer_retired", boolValue(p.Retired), label...)
		add("peer_excluded", boolValue(p.Excluded), label...)
		var deadline float64
		if !p.RetirementDeadline.IsZero() {
			deadline = float64(p.RetirementDeadline.Unix())
		}
		add("peer_retirement_deadline_seconds", deadline, label...)
		add("peer_bytes_sent_total", float64(p.BytesSent), label...)
		add("peer_bytes_received_total", float64(p.BytesReceived), label...)
		add("peer_queued_need", float64(p.QueuedNeed), label...)
		add("peer_queued_ctrl", float64(p.QueuedCtrl), label...)
		add("peer_queued_schema", float64(p.QueuedSchema), label...)
		origins := make([]murmur.NodeID, 0, len(p.LagByOrigin))
		for origin := range p.LagByOrigin {
			origins = append(origins, origin)
		}
		sort.Slice(origins, func(i, j int) bool { return origins[i].String() < origins[j].String() })
		for _, origin := range origins {
			add("peer_lag_sequences", float64(p.LagByOrigin[origin]), "peer", peer, "origin", origin.String())
		}
	}
	return out
}
