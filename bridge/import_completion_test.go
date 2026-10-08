package bridge

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func TestDrainReturnsProgressReconciliationErrors(t *testing.T) {
	for _, failure := range []string{"database-read", "inbox-write"} {
		t.Run(failure, func(t *testing.T) {
		high := openTypedContactDBAt(t, t.TempDir(), db.NewNodeID())
			_, _, trust := inboxKeys(t, "progress")
			inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
			if err != nil {
				t.Fatal(err)
			}
			if err := inbox.SyncAuthoritativeProgress("progress", 0); err != nil {
				t.Fatal(err)
			}
			if failure == "database-read" {
				if err := high.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := high.CompleteBridgeImport("progress", 1, nil); err != nil {
					t.Fatal(err)
				}
				// A file where the journal directory belongs guarantees an
				// actual write error even when the test runs as root.
				blocked := filepath.Join(t.TempDir(), "blocked")
				if err := os.WriteFile(blocked, nil, 0600); err != nil {
					t.Fatal(err)
				}
				inbox.dir = blocked
			}
			importer, err := NewImporter(high)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := importer.Drain(context.Background(), inbox); err == nil || n != 0 {
				t.Fatalf("failed reconciliation acknowledged: %d, %v", n, err)
			}
		})
	}
}

func TestImportCompletionStorageFailureRecovery(t *testing.T) {
	for _, mode := range []string{"rows", "files", "mixed", "mixed-file-failure", "rows-interrupted-completion"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			dir, node := t.TempDir(), ids.NewNodeID()
			fs := &failStorage{}
			open := func() *db.DB {
				return openTypedContactDBWithFaults(t, dir, node, fs, true)
			}
			high := open()
			batch := putBatch(1, ids.NewRowID(), ColumnValue{Column: "name", Value: codec.Text("imported")})
			if mode == "files" {
				batch.Records = nil
			}
			hasFiles := mode == "files" || mode == "mixed" || mode == "mixed-file-failure"
			if hasFiles {
				digest := sha256.Sum256([]byte("file payload"))
				batch.Records = append(batch.Records, Record{Table: db.BridgeFileTableName, Row: ids.NewRowID(), Op: RecordPut, Columns: []ColumnValue{
					{Column: db.BridgeFileColName, Value: codec.Text("imported.txt")},
					{Column: db.BridgeFileColDigest, Value: codec.Blob(digest[:])},
					{Column: db.BridgeFileColSize, Value: codec.Int(12)},
				}})
			}
			batch = encodeTypedContactsBatches(t, []Batch{batch})[0]
			bundle := &Bundle{Manifest: Manifest{BundleID: ids.NewTxID(), SourceDomain: ids.NewDBID(), Stream: "completion", SeqFirst: 1, SeqLast: 1}, Batches: []Batch{batch}}
			importer, err := NewImporter(high)
			if err != nil {
				t.Fatal(err)
			}
			// Arm a real filesystem fault only after row/file effects have
			// committed, or between the row and file commits for mixed input.
			if mode == "mixed-file-failure" {
				importer.beforeFileApply = fs.arm
			} else if mode == "rows-interrupted-completion" {
				importer.beforeCompletionError = func() error { return errors.New("interrupted before completion") }
			} else {
				importer.beforeCompletion = fs.arm
			}
			if err := importer.ApplyBundle(ctx, bundle); err == nil {
				t.Fatal("storage failure acknowledged as successful import")
			}
			fs.disarm()
			_ = high.Close()
			high = open()
			if mode != "files" {
				if got := queryNames(t, high); len(got) != 1 || got["imported"] != -1 {
					t.Fatalf("committed row did not survive failed completion: %v", got)
				}
				if has, err := high.HasTransactionReceipt(sourceReceiptID(bundle, batch)); err != nil || !has {
					t.Fatalf("atomic row receipt = %v, %v", has, err)
				}
			}
			completed, err := high.HasTransactionReceipt(bundle.Manifest.BundleID)
			if err != nil {
				t.Fatal(err)
			}
			// A failed fsync has an uncertain outcome: restart may recover
			// the whole completion batch or none, never just some receipts.
			if seq, ok, err := high.BridgeStreamProgress("completion"); err != nil || ok != completed || ok && seq != 1 {
				t.Fatalf("partial completion after restart: receipt=%v progress=%d, %v, %v", completed, seq, ok, err)
			}
			if (mode == "mixed-file-failure" || mode == "rows-interrupted-completion") && completed {
				t.Fatal("completion published before it was attempted")
			}
			if mode == "files" {
				if has, err := high.HasTransactionReceipt(batch.TxID); err != nil || has != completed {
					t.Fatalf("partial file completion: bundle=%v source=%v, %v", completed, has, err)
				}
			}
			importer, err = NewImporter(high)
			if err != nil {
				t.Fatal(err)
			}
			logSequence := func() uint64 {
				seq, err := high.BridgeLogSource().ScanLog(ctx, node, 1, 100, 16<<20, func(*codec.MutationBatch) error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				return seq
			}
			before := logSequence()
			if err := importer.ApplyBundle(ctx, bundle); err != nil {
				t.Fatalf("retry after restart: %v", err)
			}
			if !hasFiles && logSequence() != before {
				t.Fatal("completion repair created a fresh row transaction")
			}
			for _, id := range []ids.TxID{bundle.Manifest.BundleID, batch.TxID} {
				if has, err := high.HasTransactionReceipt(id); err != nil || !has {
					t.Fatalf("repaired receipt %s = %v, %v", id, has, err)
				}
			}
			if seq, ok, err := high.BridgeStreamProgress("completion"); err != nil || !ok || seq != 1 {
				t.Fatalf("repaired progress = %d, %v, %v", seq, ok, err)
			}
			if hasFiles {
				if files, err := high.ListFiles(ctx, "", 0); err != nil || len(files) != 1 || files[0].Name != "imported.txt" || files[0].Size != 12 {
					t.Fatalf("retry skipped file metadata: %+v, %v", files, err)
				}
			}
		})
	}
}
