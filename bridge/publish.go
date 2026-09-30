package bridge

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/ids"
)

// DestinationPublisher adapts a backup.Destination blob store (local
// directory/removable media, HTTPS, FTP) to the bridge Publisher interface.
// Artifact names already satisfy the shared filename policy.
type DestinationPublisher struct {
	dest   backup.Destination
	limits Limits
}

func wrapDestination(dest backup.Destination, limits Limits) *DestinationPublisher {
	return &DestinationPublisher{dest: dest, limits: limits.withDefaults()}
}

// NewDirPublisher publishes artifacts as files under dir (usable with
// removable media by mounting it there).
func NewDirPublisher(dir string, limits Limits) (*DestinationPublisher, error) {
	dest, err := backup.NewLocalDestination(dir)
	if err != nil {
		return nil, err
	}
	return wrapDestination(dest, limits), nil
}

// NewHTTPPublisher publishes artifacts via HTTP PUT to opt.BaseURL.
func NewHTTPPublisher(opt backup.HTTPSOptions, limits Limits) (*DestinationPublisher, error) {
	dest, err := backup.NewHTTPSDestination(opt)
	if err != nil {
		return nil, err
	}
	return wrapDestination(dest, limits), nil
}

// NewFTPPublisher publishes artifacts via FTP/FTPS upload.
func NewFTPPublisher(opt backup.FTPOptions, limits Limits) (*DestinationPublisher, error) {
	dest, err := backup.NewFTPDestination(opt)
	if err != nil {
		return nil, err
	}
	return wrapDestination(dest, limits), nil
}

// Publish validates and uploads one artifact.
func (p *DestinationPublisher) Publish(ctx context.Context, a Artifact) error {
	if err := ValidateArtifact(a, p.limits); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.dest.WriteBackup(ctx, a.Name, bytes.NewReader(a.Data), int64(len(a.Data)))
}

// RetryPolicy bounds redelivery of failed publications. Only transport
// failures retry; validation failures are permanent.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 5
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 200 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 30 * time.Second
	}
	return p
}

// PublishWithRetry publishes until success, context end, or attempt
// exhaustion (with exponential backoff capped at MaxDelay).
func PublishWithRetry(ctx context.Context, pub Publisher, a Artifact, policy RetryPolicy) error {
	policy = policy.withDefaults()
	delay := policy.BaseDelay
	var last error
	for attempt := 1; ; attempt++ {
		if err := pub.Publish(ctx, a); err == nil {
			return nil
		} else {
			last = err
		}
		if attempt >= policy.MaxAttempts {
			return fmt.Errorf("bridge: publish %q failed after %d attempts: %w", a.Name, attempt, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > policy.MaxDelay {
			delay = policy.MaxDelay
		}
	}
}

// Sealer encodes pending batches into one publishable artifact. first/last
// are the bundle's contiguous export sequence range; schema carries the
// batches' shared writer schema identity.
type Sealer func(batches []Batch, first, last uint64, schemaEpoch uint64, schemaHash [32]byte) (Artifact, error)

// NewBundleSealer builds a Sealer that emits versioned signed bundles for
// one recipient. Manifest stream/domain come from the exporter.
func NewBundleSealer(exp *Exporter, signer *SignerKey, recipient [keyIDSize]byte) Sealer {
	limits := exp.Limits()
	return func(batches []Batch, first, last uint64, schemaEpoch uint64, schemaHash [32]byte) (Artifact, error) {
		txIDs := make([]ids.TxID, len(batches))
		for i, b := range batches {
			txIDs[i] = b.TxID
		}
		manifest := Manifest{
			SourceDomain: exp.Domain(),
			Stream:       exp.Status().Stream,
			SeqFirst:     first,
			SeqLast:      last,
			TxIDs:        txIDs,
			SchemaEpoch:  schemaEpoch,
			SchemaHash:   schemaHash,
		}
		sealed, err := SealBatches(signer, recipient, manifest, batches, limits)
		if err != nil {
			return Artifact{}, err
		}
		return Artifact{
			Name: fmt.Sprintf("bundle-%s-%020d-%020d.spb", exp.Status().Stream, first, last),
			Data: sealed,
		}, nil
	}
}

// PublishPending drains unpublished outbox events in export-seq order,
// packing contiguous same-schema runs into bundles (bounded by limits) and
// publishing each with retry. Transport failures stop the drain (the events
// stay queued for the next run); bundle validation failures mark their
// events failed and continue past the poison. It returns published bundles.
func PublishPending(ctx context.Context, exp *Exporter, outbox *Outbox, seal Sealer, pub Publisher, policy RetryPolicy) (int, error) {
	return PublishPendingWithFiles(ctx, exp, outbox, seal, pub, policy, nil)
}

// PublishPendingWithFiles drains like PublishPending and additionally emits
// recipient-sealed object chunks for file metadata in each published run.
// A nil files publisher disables chunk emission (metadata-only export).
// Chunk emission runs after its bundle publishes: metadata always crosses
// first, and objects whose bytes are missing journal as pending for later
// drains instead of failing their bundle.
func PublishPendingWithFiles(ctx context.Context, exp *Exporter, outbox *Outbox, seal Sealer, pub Publisher, policy RetryPolicy, files *FilePublisher) (int, error) {
	if files != nil {
		if err := files.RetryPending(ctx, pub, policy); err != nil {
			return 0, err
		}
	}
	pending := outbox.Pending(0)
	published := 0
	stream := exp.Status().Stream
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		run, rest := packRun(pending, exp.Limits())
		batches := runBatches(run)
		artifact, err := seal(batches, run[0].Seq, run[len(run)-1].Seq, run[0].SchemaEpoch, run[0].SchemaHash)
		if err != nil {
			// Poison: validation failed on journaled events. Quarantine
			// the run as failed and continue with the rest.
			for _, ev := range run {
				_ = outbox.MarkFailed(ev.Seq, err)
			}
			pending = rest
			continue
		}
		if err := PublishWithRetry(ctx, pub, artifact, policy); err != nil {
			for _, ev := range run {
				_ = outbox.MarkFailed(ev.Seq, err)
			}
			return published, err
		}
		for _, ev := range run {
			if err := outbox.MarkPublished(ev.Seq); err != nil {
				return published, err
			}
		}
		published++
		if files != nil {
			if err := files.ExportRun(ctx, batches, stream, pub, policy); err != nil {
				return published, err
			}
		}
		pending = rest
	}
	return published, nil
}

func runBatches(run []*Event) []Batch {
	out := make([]Batch, len(run))
	for i, ev := range run {
		out[i] = ev.Batch
		// Bundle contiguity runs on export numbering, not origin seqs.
		out[i].Sequence = ev.Seq
	}
	return out
}

// packRun takes the longest leading run of pending events sharing one
// schema identity and contiguous export seqs, bounded by limits.
func packRun(pending []*Event, limits Limits) (run, rest []*Event) {
	limits = limits.withDefaults()
	var encoded int
	for i, ev := range pending {
		if i > 0 {
			prev := pending[i-1]
			if ev.Seq != prev.Seq+1 || ev.SchemaEpoch != prev.SchemaEpoch || ev.SchemaHash != prev.SchemaHash {
				return pending[:i], pending[i:]
			}
		}
		section, err := encodeBatches([]Batch{ev.Batch})
		if err != nil || len(run)+1 > limits.MaxTransactions || encoded+len(section) > limits.MaxPayloadBytes {
			if len(run) == 0 {
				// Single over-limit event still goes out alone so the
				// sealer reports the precise violation.
				return pending[:1], pending[1:]
			}
			return pending[:i], pending[i:]
		}
		encoded += len(section)
		run = append(run, ev)
	}
	return pending, nil
}
