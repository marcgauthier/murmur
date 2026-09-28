package replication

import (
	"sync/atomic"
	"time"
)

// stats holds monotonically increasing replication counters. All fields are
// updated with atomic operations; Manager is always used by pointer so the
// struct is never copied after first use.
//
// The counters cover the diagnostics surface in
// architecture/runtime-and-diagnostics.md section 52 for the subsystems that
// exist today: static-peer QUIC sessions, origin-log repair, snapshots, and
// schema synchronization. Membership (SWIM), bounded peer selection,
// Plumtree, and overload budgets have no counters yet because those
// subsystems are not implemented; their Status fields stay zero and must not
// be mistaken for measured zeros.
type stats struct {
	// Connection and handshake.
	dials                    atomic.Uint64
	dialFailures             atomic.Uint64
	dialPolicyDenials        atomic.Uint64
	accepts                  atomic.Uint64
	acceptFailures           atomic.Uint64
	acceptPolicyDenials      atomic.Uint64
	handshakeFailures        atomic.Uint64
	handshakeIdentityRefl    atomic.Uint64
	handshakeSchemaRefusals  atomic.Uint64
	handshakeCapabilityRefl  atomic.Uint64
	handshakeRetiredRefusals atomic.Uint64
	sessionsOpened           atomic.Uint64
	sessionsRecycled         atomic.Uint64
	sessionsDedupeClosed     atomic.Uint64
	memberAdmissions         atomic.Uint64
	dataStreamsOpened        atomic.Uint64
	dataStreamsAccepted      atomic.Uint64
	snapStreamsOpened        atomic.Uint64
	snapStreamsAccepted      atomic.Uint64

	// Session traffic.
	framesReceived     atomic.Uint64
	frameBytesSent     atomic.Uint64
	frameBytesReceived atomic.Uint64

	// Origin-log repair.
	batchesSent        atomic.Uint64
	batchesReceived    atomic.Uint64
	mutationsSent      atomic.Uint64
	mutationsReceived  atomic.Uint64
	batchBytesSent     atomic.Uint64
	batchBytesReceived atomic.Uint64
	batchesInvalid     atomic.Uint64
	batchesDeferred    atomic.Uint64
	gapsDetected       atomic.Uint64
	applyRetries       atomic.Uint64
	applyFailures      atomic.Uint64

	// Acknowledgements, pulls, and liveness.
	acksSent       atomic.Uint64
	acksReceived   atomic.Uint64
	needsSent      atomic.Uint64
	needsReceived  atomic.Uint64
	pingsSent      atomic.Uint64
	pingsReceived  atomic.Uint64
	pongsReceived  atomic.Uint64
	errorsReceived atomic.Uint64

	// Snapshots.
	snapshotRequiredSent      atomic.Uint64
	snapshotRequiredReceived  atomic.Uint64
	snapshotRequestsSent      atomic.Uint64
	snapshotsSent             atomic.Uint64
	snapshotsSendFailed       atomic.Uint64
	snapshotsReceived         atomic.Uint64
	snapshotManifestsReceived atomic.Uint64
	snapshotManifestsRejected atomic.Uint64
	snapshotChunksSent        atomic.Uint64
	snapshotChunksReceived    atomic.Uint64
	snapshotBytesSent         atomic.Uint64
	snapshotBytesReceived     atomic.Uint64
	snapshotReRequested       atomic.Uint64

	// Schema synchronization.
	schemaRequestsSent      atomic.Uint64
	schemaRequestsReceived  atomic.Uint64
	schemaRequestsServed    atomic.Uint64
	schemaManifestsSent     atomic.Uint64
	schemaManifestsReceived atomic.Uint64
	schemaAcksSent          atomic.Uint64
	schemaAcksReceived      atomic.Uint64
	schemaConflicts         atomic.Uint64
	schemaAgreements        atomic.Uint64

	// Overload: drops are safe (peers re-request) but observable.
	ctrlDrops       atomic.Uint64
	retryDrops      atomic.Uint64
	needDrops       atomic.Uint64
	schemaReqDrops  atomic.Uint64
	schemaRespDrops atomic.Uint64

	// Peer selection, rotation, and anti-entropy.
	peerRotations       atomic.Uint64
	antiEntropyRuns     atomic.Uint64
	antiEntropyFailures atomic.Uint64
}

// StatsSnapshot is a point-in-time copy of Manager counters plus queue-depth
// gauges. It never contains secrets: only counts, depths, and sizes.
type StatsSnapshot struct {
	PeerCount      int
	ConnectedPeers int
	// GatingMembers counts persisted members whose retention obligation
	// is live (active, non-excluded, deadline unexpired). It is the GC
	// gating set, independent of sessions and selection.
	GatingMembers int

	QueuedNeed         int
	QueuedCtrl         int
	QueuedSchemaReq    int
	QueuedSchemaResp   int
	QueueBudgetBytes   int64
	QueueBudgetEntries int64

	Dials                     uint64
	DialFailures              uint64
	DialPolicyDenials         uint64
	Accepts                   uint64
	AcceptFailures            uint64
	AcceptPolicyDenials       uint64
	HandshakeFailures         uint64
	HandshakeIdentityRefusals uint64
	HandshakeSchemaRefusals   uint64
	HandshakeCapabilityRefl   uint64
	HandshakeRetiredRefusals  uint64
	SessionsOpened            uint64
	SessionsRecycled          uint64
	SessionsDedupeClosed      uint64
	MemberAdmissions          uint64
	DataStreamsOpened         uint64
	DataStreamsAccepted       uint64
	SnapStreamsOpened         uint64
	SnapStreamsAccepted       uint64

	FramesReceived     uint64
	FrameBytesSent     uint64
	FrameBytesReceived uint64

	BatchesSent        uint64
	BatchesReceived    uint64
	MutationsSent      uint64
	MutationsReceived  uint64
	BatchBytesSent     uint64
	BatchBytesReceived uint64
	BatchesInvalid     uint64
	BatchesDeferred    uint64
	GapsDetected       uint64
	ApplyRetries       uint64
	ApplyFailures      uint64

	AcksSent       uint64
	AcksReceived   uint64
	NeedsSent      uint64
	NeedsReceived  uint64
	PingsSent      uint64
	PingsReceived  uint64
	PongsReceived  uint64
	ErrorsReceived uint64

	SnapshotRequiredSent      uint64
	SnapshotRequiredReceived  uint64
	SnapshotRequestsSent      uint64
	SnapshotsSent             uint64
	SnapshotsSendFailed       uint64
	SnapshotsReceived         uint64
	SnapshotManifestsReceived uint64
	SnapshotManifestsRejected uint64
	SnapshotChunksSent        uint64
	SnapshotChunksReceived    uint64
	SnapshotBytesSent         uint64
	SnapshotBytesReceived     uint64
	SnapshotReRequested       uint64

	SchemaRequestsSent      uint64
	SchemaRequestsReceived  uint64
	SchemaRequestsServed    uint64
	SchemaManifestsSent     uint64
	SchemaManifestsReceived uint64
	SchemaAcksSent          uint64
	SchemaAcksReceived      uint64
	SchemaConflicts         uint64
	SchemaAgreements        uint64

	SelectedPeers int

	CtrlDrops       uint64
	RetryDrops      uint64
	NeedDrops       uint64
	SchemaReqDrops  uint64
	SchemaRespDrops uint64

	PeerRotations       uint64
	AntiEntropyRuns     uint64
	AntiEntropyFailures uint64
}

// Stats copies the current counters and queue depths.
func (m *Manager) Stats() StatsSnapshot {
	s := &m.st
	out := StatsSnapshot{
		Dials:                     s.dials.Load(),
		DialFailures:              s.dialFailures.Load(),
		DialPolicyDenials:         s.dialPolicyDenials.Load(),
		Accepts:                   s.accepts.Load(),
		AcceptFailures:            s.acceptFailures.Load(),
		AcceptPolicyDenials:       s.acceptPolicyDenials.Load(),
		HandshakeFailures:         s.handshakeFailures.Load(),
		HandshakeIdentityRefusals: s.handshakeIdentityRefl.Load(),
		HandshakeSchemaRefusals:   s.handshakeSchemaRefusals.Load(),
		HandshakeCapabilityRefl:   s.handshakeCapabilityRefl.Load(),
		HandshakeRetiredRefusals:  s.handshakeRetiredRefusals.Load(),
		SessionsOpened:            s.sessionsOpened.Load(),
		SessionsRecycled:          s.sessionsRecycled.Load(),
		SessionsDedupeClosed:      s.sessionsDedupeClosed.Load(),
		MemberAdmissions:          s.memberAdmissions.Load(),
		DataStreamsOpened:         s.dataStreamsOpened.Load(),
		DataStreamsAccepted:       s.dataStreamsAccepted.Load(),
		SnapStreamsOpened:         s.snapStreamsOpened.Load(),
		SnapStreamsAccepted:       s.snapStreamsAccepted.Load(),

		FramesReceived:     s.framesReceived.Load(),
		FrameBytesSent:     s.frameBytesSent.Load(),
		FrameBytesReceived: s.frameBytesReceived.Load(),

		BatchesSent:        s.batchesSent.Load(),
		BatchesReceived:    s.batchesReceived.Load(),
		MutationsSent:      s.mutationsSent.Load(),
		MutationsReceived:  s.mutationsReceived.Load(),
		BatchBytesSent:     s.batchBytesSent.Load(),
		BatchBytesReceived: s.batchBytesReceived.Load(),
		BatchesInvalid:     s.batchesInvalid.Load(),
		BatchesDeferred:    s.batchesDeferred.Load(),
		GapsDetected:       s.gapsDetected.Load(),
		ApplyRetries:       s.applyRetries.Load(),
		ApplyFailures:      s.applyFailures.Load(),

		AcksSent:       s.acksSent.Load(),
		AcksReceived:   s.acksReceived.Load(),
		NeedsSent:      s.needsSent.Load(),
		NeedsReceived:  s.needsReceived.Load(),
		PingsSent:      s.pingsSent.Load(),
		PingsReceived:  s.pingsReceived.Load(),
		PongsReceived:  s.pongsReceived.Load(),
		ErrorsReceived: s.errorsReceived.Load(),

		SnapshotRequiredSent:      s.snapshotRequiredSent.Load(),
		SnapshotRequiredReceived:  s.snapshotRequiredReceived.Load(),
		SnapshotRequestsSent:      s.snapshotRequestsSent.Load(),
		SnapshotsSent:             s.snapshotsSent.Load(),
		SnapshotsSendFailed:       s.snapshotsSendFailed.Load(),
		SnapshotsReceived:         s.snapshotsReceived.Load(),
		SnapshotManifestsReceived: s.snapshotManifestsReceived.Load(),
		SnapshotManifestsRejected: s.snapshotManifestsRejected.Load(),
		SnapshotChunksSent:        s.snapshotChunksSent.Load(),
		SnapshotChunksReceived:    s.snapshotChunksReceived.Load(),
		SnapshotBytesSent:         s.snapshotBytesSent.Load(),
		SnapshotBytesReceived:     s.snapshotBytesReceived.Load(),
		SnapshotReRequested:       s.snapshotReRequested.Load(),

		SchemaRequestsSent:      s.schemaRequestsSent.Load(),
		SchemaRequestsReceived:  s.schemaRequestsReceived.Load(),
		SchemaRequestsServed:    s.schemaRequestsServed.Load(),
		SchemaManifestsSent:     s.schemaManifestsSent.Load(),
		SchemaManifestsReceived: s.schemaManifestsReceived.Load(),
		SchemaAcksSent:          s.schemaAcksSent.Load(),
		SchemaAcksReceived:      s.schemaAcksReceived.Load(),
		SchemaConflicts:         s.schemaConflicts.Load(),
		SchemaAgreements:        s.schemaAgreements.Load(),

		CtrlDrops:       s.ctrlDrops.Load(),
		RetryDrops:      s.retryDrops.Load(),
		NeedDrops:       s.needDrops.Load(),
		SchemaReqDrops:  s.schemaReqDrops.Load(),
		SchemaRespDrops: s.schemaRespDrops.Load(),

		PeerRotations:       s.peerRotations.Load(),
		AntiEntropyRuns:     s.antiEntropyRuns.Load(),
		AntiEntropyFailures: s.antiEntropyFailures.Load(),
	}
	if m.queueBudget != nil {
		budget := m.queueBudget.Snapshot()
		out.QueueBudgetBytes = budget.Bytes
		out.QueueBudgetEntries = budget.Entries
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out.SelectedPeers = len(m.selected)
	for _, p := range m.peers {
		out.PeerCount++
		p.mu.Lock()
		if p.session != nil {
			out.ConnectedPeers++
		}
		out.QueuedNeed += len(p.needCh)
		out.QueuedCtrl += len(p.ctrlCh) + len(p.retryCh)
		out.QueuedSchemaReq += len(p.schemaReqCh)
		out.QueuedSchemaResp += len(p.schemaRespCh)
		p.mu.Unlock()
	}
	// GC gating set from persisted membership (independent of sessions).
	// Unit-constructed managers may lack a store; leave the gauge zero.
	if m.cfg.Store != nil {
		if members, err := m.cfg.Store.ListMembers(); err == nil {
			now := time.Now().UnixMilli()
			for _, mb := range members {
				if mb.Gating(now) {
					out.GatingMembers++
				}
			}
		}
	}
	return out
}
