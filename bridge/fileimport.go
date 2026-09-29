package bridge

import (
	"bytes"
	"context"
	"fmt"
	"io"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/objectstore"
)

// High-side file object installation.
//
// A complete staged chunk set installs once its file metadata has imported:
// the importer matches the staged digest against High's visible file
// metadata (either arrival order works), streams the unsealed plaintext into
// High's object store — re-encrypting under High's own storage key — and
// verifies the replicated digest before discarding staging. Availability
// flips only on verified install, so High peers fetching from this node
// always find bytes behind advertised metadata.

// importReadyFiles installs every complete staged object whose metadata is
// present. Poison (identity conflicts, undecryptable or digest-mismatched
// bytes) quarantines per object without blocking other imports; only
// infrastructural failures abort the pass.
func (im *Importer) importReadyFiles(ctx context.Context, in *Inbox) (int, error) {
	if !im.db.BridgeFilesEnabled() {
		return 0, nil
	}
	installed := 0
	for _, rf := range in.ReadyFiles() {
		if err := ctx.Err(); err != nil {
			return installed, err
		}
		match, err := im.findFileByDigest(ctx, rf.Digest)
		if err != nil {
			return installed, err
		}
		if match == nil {
			continue // metadata not yet imported; stays staged
		}
		if match.Available {
			// Duplicate transfer: bytes already present.
			if err := in.FileApplied(rf.Digest); err != nil {
				return installed, err
			}
			installed++
			continue
		}
		if match.Size != int64(rf.TotalLen) {
			_ = in.QuarantineFile(rf.Digest, fmt.Sprintf("staged length %d != metadata size %d", rf.TotalLen, match.Size))
			continue
		}
		if err := im.installFileObject(ctx, in, rf, *match); err != nil {
			if qerr := in.QuarantineFile(rf.Digest, err.Error()); qerr != nil {
				return installed, qerr
			}
			continue
		}
		if err := in.FileApplied(rf.Digest); err != nil {
			return installed, err
		}
		installed++
	}
	return installed, nil
}

// findFileByDigest scans High's visible file metadata for a digest.
func (im *Importer) findFileByDigest(ctx context.Context, digest [32]byte) (*db.FileStatus, error) {
	var want objectstore.Digest
	copy(want[:], digest[:])
	listed, err := im.db.ListFiles(ctx, "", 0)
	if err != nil {
		return nil, err
	}
	for i := range listed {
		if listed[i].Digest == want {
			cp := listed[i]
			return &cp, nil
		}
	}
	return nil, nil
}

// filePutRecord converts a validated file put-record to the root import
// shape. prepareFileRecord guarantees the shape; this re-checks defensively
// because applyBundleResolved may see records from other paths.
func filePutRecord(rec Record) (db.BridgeFilePut, error) {
	var out db.BridgeFilePut
	name, err := fileRecordName(rec)
	if err != nil {
		return out, err
	}
	digest, size, err := fileRecordObject(rec)
	if err != nil {
		return out, err
	}
	var objDigest objectstore.Digest
	copy(objDigest[:], digest[:])
	return db.BridgeFilePut{Row: rec.Row, Name: name, Digest: objDigest, Size: size}, nil
}

// installFileObject streams one staged object into High's store and
// verifies the replicated digest. Plaintext exists only in bounded chunk
// buffers; it is never staged to disk.
func (im *Importer) installFileObject(ctx context.Context, in *Inbox, rf ReadyFile, match db.FileStatus) error {
	chunks, err := in.openReadyFile(rf.Digest)
	if err != nil {
		return err
	}
	var total uint64
	for _, c := range chunks {
		total += uint64(len(c.Bytes))
	}
	if total != rf.TotalLen || total != uint64(match.Size) {
		return fmt.Errorf("staged bytes %d != object length %d", total, match.Size)
	}
	readers := make([]io.Reader, 0, len(chunks))
	for _, c := range chunks {
		readers = append(readers, bytes.NewReader(c.Bytes))
	}
	gotDigest, gotSize, err := im.db.BridgePutFileObject(ctx, io.MultiReader(readers...))
	if err != nil {
		return err
	}
	var want objectstore.Digest
	copy(want[:], rf.Digest[:])
	if gotDigest != want || gotSize != match.Size {
		return fmt.Errorf("installed digest/length does not match replicated metadata")
	}
	return nil
}
