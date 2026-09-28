package bridge

import (
	"time"
)

// StatusOptions bounds diagnostics collection. Zero values select
// defaults.
type StatusOptions struct {
	// MaxEntries caps listed items per section and stream.
	MaxEntries int
}

func (o StatusOptions) withDefaults() StatusOptions {
	if o.MaxEntries <= 0 {
		o.MaxEntries = 64
	}
	return o
}

// ExportEvent summarizes one unpublished export event. It carries routing
// identities and failure state only, never record payloads.
type ExportEvent struct {
	Seq         uint64    `json:"seq"`
	Origin      string    `json:"origin"`
	OriginSeq   uint64    `json:"origin_seq"`
	SchemaEpoch uint64    `json:"schema_epoch"`
	Attempts    int       `json:"attempts"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
	LastError   string    `json:"last_error,omitempty"`
}

// ExportStatus reports exporter backlog and publication failures with
// exact totals. Lists are capped; totals are not.
type ExportStatus struct {
	Pending         []ExportEvent `json:"pending"`
	PendingTotal    int           `json:"pending_total"`
	Failed          []ExportEvent `json:"failed"`
	FailedTotal     int           `json:"failed_total"`
	PublishedSeq    uint64        `json:"published_seq"`
	HasBacklog      bool          `json:"has_backlog"`
	OldestBacklogAt time.Time     `json:"oldest_backlog_at,omitempty"`
	Capacity        CapacityUsage `json:"capacity"`
}

// ExportStatus snapshots the outbox backlog and publication failures.
func (o *Outbox) ExportStatus(opts StatusOptions) ExportStatus {
	max := opts.withDefaults().MaxEntries
	o.mu.Lock()
	defer o.mu.Unlock()
	var out ExportStatus
	var oldest time.Time
	haveOldest := false
	for _, seq := range o.sortedSeqsLocked() {
		ev := o.bySeq[seq]
		switch ev.State {
		case EventPublished:
			if seq > out.PublishedSeq {
				out.PublishedSeq = seq
			}
		case EventFailed:
			out.FailedTotal++
			if len(out.Failed) < max {
				out.Failed = append(out.Failed, summarizeEvent(ev))
			}
			if !haveOldest || ev.EnqueuedAt.Before(oldest) {
				oldest, haveOldest = ev.EnqueuedAt, true
			}
		default: // EventPending
			out.PendingTotal++
			if len(out.Pending) < max {
				out.Pending = append(out.Pending, summarizeEvent(ev))
			}
			if !haveOldest || ev.EnqueuedAt.Before(oldest) {
				oldest, haveOldest = ev.EnqueuedAt, true
			}
		}
	}
	if haveOldest {
		out.HasBacklog = true
		out.OldestBacklogAt = oldest
	}
	out.Capacity = CapacityUsage{BytesUsed: o.used, BytesMax: o.limits.MaxStagingBytes,
		EntriesUsed: len(o.bySeq), EntriesMax: o.limits.MaxStagingEntries}
	return out
}

func summarizeEvent(ev *Event) ExportEvent {
	return ExportEvent{Seq: ev.Seq, Origin: ev.Origin.String(), OriginSeq: ev.OriginSeq,
		SchemaEpoch: ev.SchemaEpoch, Attempts: ev.Attempts, EnqueuedAt: ev.EnqueuedAt,
		LastError: ev.LastError}
}

// ImportStatus reports receiver stream progress, backlog age, and import
// holds. Quarantine records (with failure reasons) ride in Streams.
type ImportStatus struct {
	Streams         []StreamProgress `json:"streams"`
	HasBacklog      bool             `json:"has_backlog"`
	OldestBacklogAt time.Time        `json:"oldest_backlog_at,omitempty"`
	Capacity        CapacityUsage    `json:"capacity"`
}

// ImportStatus snapshots per-stream progress with bounded lists.
func (in *Inbox) ImportStatus(opts StatusOptions) ImportStatus {
	now := time.Now()
	out := ImportStatus{Streams: in.ProgressBounded(opts.withDefaults().MaxEntries)}
	if age, ok := in.BacklogAge(now); ok {
		out.HasBacklog = true
		out.OldestBacklogAt = now.Add(-age).UTC()
	}
	out.Capacity = in.Usage()
	return out
}

// KeyStatus reports trusted bridge key identities: public identifiers
// only, never private key material.
type KeyStatus struct {
	Signers    []SignerIdentity `json:"signers"`
	Recipients []string         `json:"recipients"`
}

// KeyStatus snapshots the trust store's public identities.
func (t *TrustStore) KeyStatus() KeyStatus {
	signers, recipients := t.Identities()
	return KeyStatus{Signers: signers, Recipients: recipients}
}

// Describe composes a full bridge diagnostics snapshot. Components are
// optional (nil sections are omitted); it never includes key material or
// plaintext payloads. Applied/Observed report the maximum across streams.
func (b *Bridge) Describe(box *Outbox, in *Inbox, trust *TrustStore, opts StatusOptions) Status {
	st := b.Status()
	st.GeneratedAt = time.Now().UTC()
	if box != nil {
		ex := box.ExportStatus(opts)
		st.Export = &ex
		st.Exported = ex.PublishedSeq
	}
	if in != nil {
		im := in.ImportStatus(opts)
		st.Import = &im
		for _, sp := range im.Streams {
			if sp.Applied > st.Applied {
				st.Applied = sp.Applied
			}
			if sp.Observed > st.Observed {
				st.Observed = sp.Observed
			}
		}
		if rs, err := in.ReplayStatus(opts.withDefaults().MaxEntries); err == nil {
			st.Replay = &rs
		}
	}
	if trust != nil {
		ks := trust.KeyStatus()
		st.Keys = &ks
	}
	return st
}
