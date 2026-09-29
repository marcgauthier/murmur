package bridge

import (
	"context"
	"fmt"
	"io"
	"os"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/objectstore"
)

// FilePublisher emits recipient-sealed object chunks for file metadata
// carried in published bundles. Plaintext streams from the Low object store
// (verified on read) and is re-encrypted per chunk under the bridge
// recipient key; the Low at-rest key never crosses the domain boundary.
//
// Metadata always publishes before its bytes are attempted: objects whose
// bytes are missing or unreadable stay journaled in the outbox and retry on
// later drains, while their metadata waits (pending) on High.
type FilePublisher struct {
	// Objects reads Low object bytes. Nil disables chunk emission:
	// metadata still publishes and every object stays pending.
	Objects *objectstore.Store
	Signer  *SignerKey
	// Recipient is High's bridge decryption identity.
	Recipient [keyIDSize]byte
	Outbox    *Outbox
	Limits    Limits
}

// fileTarget is one object referenced by file put-records.
type fileTarget struct {
	digest [32]byte
	size   int64
}

// ExportRun publishes chunks for every object referenced by the run's file
// put-records. Unknown or unreadable objects journal as pending; transport
// failures abort the drain for retry.
func (p *FilePublisher) ExportRun(ctx context.Context, batches []Batch, stream string, pub Publisher, policy RetryPolicy) error {
	if p == nil {
		return nil
	}
	targets, err := fileTargets(batches)
	if err != nil {
		return err
	}
	for _, t := range targets {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.exportObject(ctx, t, stream, pub, policy); err != nil {
			return err
		}
	}
	return nil
}

// RetryPending re-attempts every journaled object, in digest order. Missing
// objects stay journaled silently; anything else aborts loudly.
func (p *FilePublisher) RetryPending(ctx context.Context, pub Publisher, policy RetryPolicy) error {
	if p == nil {
		return nil
	}
	for _, pf := range p.Outbox.PendingFiles() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.exportObject(ctx, fileTarget{digest: pf.Digest, size: pf.Size}, pf.Stream, pub, policy); err != nil {
			return err
		}
	}
	return nil
}

// fileTargets collects distinct (digest, size) pairs from file put-records.
func fileTargets(batches []Batch) ([]fileTarget, error) {
	seen := make(map[[32]byte]int64)
	var order [][32]byte
	for _, b := range batches {
		for _, rec := range b.Records {
			if rec.Table != db.BridgeFileTableName || rec.Op != RecordPut {
				continue
			}
			digest, size, err := fileRecordObject(rec)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[digest]; !ok {
				seen[digest] = size
				order = append(order, digest)
				continue
			}
			if seen[digest] != size {
				return nil, fmt.Errorf("bridge: file object %x has conflicting sizes", digest[:8])
			}
		}
	}
	out := make([]fileTarget, 0, len(order))
	for _, d := range order {
		out = append(out, fileTarget{digest: d, size: seen[d]})
	}
	return out, nil
}

// fileRecordObject extracts the referenced object identity from a file
// put-record's columns.
func fileRecordObject(rec Record) ([32]byte, int64, error) {
	var digest [32]byte
	var size int64
	var haveDigest, haveSize bool
	for _, c := range rec.Columns {
		switch c.Column {
		case db.BridgeFileColDigest:
			if c.Value.Type != codec.TypeBlob || len(c.Value.B) != 32 {
				return digest, 0, fmt.Errorf("bridge: file record has malformed digest")
			}
			copy(digest[:], c.Value.B)
			haveDigest = true
		case db.BridgeFileColSize:
			if c.Value.Type != codec.TypeInteger || c.Value.I < 0 {
				return digest, 0, fmt.Errorf("bridge: file record has malformed size")
			}
			size, haveSize = c.Value.I, true
		}
	}
	if !haveDigest || !haveSize {
		return digest, 0, fmt.Errorf("bridge: file record lacks object identity")
	}
	return digest, size, nil
}

// exportObject streams one object into sealed chunk artifacts. A missing
// object (or a nil object store) journals as pending without failing;
// corrupt bytes fail loudly and stay pending for operator repair.
func (p *FilePublisher) exportObject(ctx context.Context, t fileTarget, stream string, pub Publisher, policy RetryPolicy) error {
	limits := p.Limits.withDefaults()
	if p.Objects == nil {
		return p.Outbox.AddPendingFile(t.digest, stream, t.size)
	}
	if !p.Objects.Has(t.digest) {
		return p.Outbox.AddPendingFile(t.digest, stream, t.size)
	}
	var objDigest objectstore.Digest
	copy(objDigest[:], t.digest[:])
	pr, pw := io.Pipe()
	readDone := make(chan error, 1)
	go func() {
		_, err := p.Objects.Read(ctx, objDigest, pw)
		_ = pw.CloseWithError(err)
		readDone <- err
	}()
	// failPending unblocks the reader, journals the retry, and reports.
	failPending := func(err error) error {
		_ = pr.CloseWithError(err)
		<-readDone
		_ = p.Outbox.AddPendingFile(t.digest, stream, t.size)
		return err
	}
	chunkSize := limits.MaxFileChunkBytes
	count := uint32(1)
	if t.size > 0 {
		count = uint32((t.size + int64(chunkSize) - 1) / int64(chunkSize))
	}
	var streamed int64
	buf := make([]byte, chunkSize)
	for index := uint32(0); index < count; index++ {
		if err := ctx.Err(); err != nil {
			return failPending(err)
		}
		want := chunkSize
		if rem := t.size - streamed; rem < int64(want) {
			want = int(rem)
		}
		if _, err := io.ReadFull(pr, buf[:want]); err != nil {
			if os.IsNotExist(err) {
				return failPending(fmt.Errorf("bridge: file object %x missing: %w", t.digest[:8], err))
			}
			return failPending(fmt.Errorf("bridge: file object %x unreadable: %w", t.digest[:8], err))
		}
		sealed, err := SealFileChunk(p.Signer, p.Recipient, stream, t.digest, index, count, uint64(t.size), buf[:want], limits)
		if err != nil {
			return failPending(err)
		}
		artifact := Artifact{Name: FileChunkArtifactName(stream, t.digest, index, count), Data: sealed}
		if err := ValidateArtifact(artifact, limits); err != nil {
			return failPending(err)
		}
		if err := PublishWithRetry(ctx, pub, artifact, policy); err != nil {
			return failPending(err)
		}
		streamed += int64(want)
	}
	if err := <-readDone; err != nil {
		_ = p.Outbox.AddPendingFile(t.digest, stream, t.size)
		return fmt.Errorf("bridge: file object %x unreadable: %w", t.digest[:8], err)
	}
	// The stream must end exactly at the declared size: short reads hide
	// truncation, trailing bytes hide substitution.
	var extra [1]byte
	if n, _ := pr.Read(extra[:]); n != 0 {
		_ = p.Outbox.AddPendingFile(t.digest, stream, t.size)
		return fmt.Errorf("bridge: file object %x longer than declared", t.digest[:8])
	}
	if streamed != t.size {
		_ = p.Outbox.AddPendingFile(t.digest, stream, t.size)
		return fmt.Errorf("bridge: file object %x streamed %d of %d bytes", t.digest[:8], streamed, t.size)
	}
	_ = pr.Close()
	return p.Outbox.RemovePendingFile(t.digest)
}

// fileRecordName extracts the file name from a put-record (used by import).
func fileRecordName(rec Record) (string, error) {
	for _, c := range rec.Columns {
		if c.Column == db.BridgeFileColName {
			if c.Value.Type != codec.TypeText || c.Value.S == "" {
				return "", fmt.Errorf("bridge: file record has malformed name")
			}
			return c.Value.S, nil
		}
	}
	return "", fmt.Errorf("bridge: file record lacks a name")
}
