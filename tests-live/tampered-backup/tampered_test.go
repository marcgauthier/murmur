// Tampered-backup acceptance: corrupt archives fail closed.
//
// A valid backup is taken through the library; byte flips in a payload
// copy (gzip header) and in a metadata copy (repacked archive with a
// corrupted backup-metadata.json) must both fail restore with integrity
// errors while leaving the target directory untouched (no extracted
// files, no restore intent). The pristine backup still restores with
// byte-identical content (positive control).
package tamperedbackup_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

type tamperedTypedRecord struct {
	ID    ids.RowID `rime:"primary"`
	Name  string
	Count int64
	Tags  []string
	Peak  int64
	Floor float64
}

func typedDefinition(t *testing.T) db.TableDefinition {
	t.Helper()
	definition, err := db.Model[tamperedTypedRecord](db.ModelOptions{
		Name: "live_typed_records", TableID: 901,
		RecordOptions: db.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Name": 2, "Count": 3, "Tags": 4, "Peak": 5, "Floor": 6},
			MergePolicies: map[string]db.RecordMergePolicy{"Count": db.RecordMergeCounter, "Tags": db.RecordMergeORSet, "Peak": db.RecordMergeMax, "Floor": db.RecordMergeMin},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func TestTamperedBackupsFailClosed(t *testing.T) {
	ctx := context.Background()
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "tampered-backup", NumNodes: 1, AwaitUnlock: true, TypedRecords: true,
	})

	const rows = 30
	for i := 0; i < rows; i++ {
		if err := cluster.TypedInsert(0, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	names, err := cluster.TypedNames(0)
	if err != nil || len(names) != rows {
		t.Fatalf("seeded count = %d, %v; want %d", len(names), err, rows)
	}
	wantDigest := namesDigest(names)

	cluster.StopNode(0)
	node := cluster.Nodes[0]

	backupDir := t.TempDir()
	offline := openNodeDir(t, ctx, cluster, node.Dir, node.NodeID.String())
	if _, err := offline.Backup(ctx, backup.Config{Destination: mustLocalDest(t, backupDir)}); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := offline.Close(); err != nil {
		t.Fatalf("close offline: %v", err)
	}
	archive := singleArchive(t, backupDir)
	raw, err := os.ReadFile(filepath.Join(backupDir, archive))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pristine backup %s (%d bytes)", archive, len(raw))

	// Payload tamper: flip the gzip magic so the stream is rejected
	// before a single entry is extracted.
	payloadDir := t.TempDir()
	flipped := bytes.Clone(raw)
	flipped[0] ^= 0xFF
	flipped[1] ^= 0xFF
	if err := os.WriteFile(filepath.Join(payloadDir, archive), flipped, 0600); err != nil {
		t.Fatal(err)
	}
	payloadTarget := t.TempDir()
	err = tryRestore(ctx, t, payloadDir, archive, payloadTarget)
	if err == nil {
		t.Fatal("payload-tampered restore succeeded, want integrity failure")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "gzip") {
		t.Fatalf("payload-tampered restore err = %v, want a gzip integrity error", err)
	}
	t.Logf("payload tamper rejected: %v", err)
	assertTargetUntouched(t, payloadTarget)

	// Metadata tamper: repack the archive with a corrupted
	// backup-metadata.json entry (invalid JSON).
	metaDir := t.TempDir()
	repackWithCorruptMetadata(t, filepath.Join(backupDir, archive), filepath.Join(metaDir, archive))
	metaTarget := t.TempDir()
	err = tryRestore(ctx, t, metaDir, archive, metaTarget)
	if err == nil {
		t.Fatal("metadata-tampered restore succeeded, want integrity failure")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "metadata") {
		t.Fatalf("metadata-tampered restore err = %v, want a metadata integrity error", err)
	}
	t.Logf("metadata tamper rejected: %v", err)
	assertTargetUntouched(t, metaTarget)

	// Late-payload flip: corruption surfacing mid-extraction must still
	// fail the restore and must never publish a restore intent.
	lateDir := t.TempDir()
	late := bytes.Clone(raw)
	late[len(late)/2] ^= 0xFF
	if err := os.WriteFile(filepath.Join(lateDir, archive), late, 0600); err != nil {
		t.Fatal(err)
	}
	lateTarget := t.TempDir()
	if err := tryRestore(ctx, t, lateDir, archive, lateTarget); err == nil {
		t.Fatal("late-payload-tampered restore succeeded, want failure")
	} else {
		t.Logf("late payload tamper rejected: %v", err)
	}
	if _, err := os.Stat(filepath.Join(lateTarget, backup.RestoreIntentFileName)); !os.IsNotExist(err) {
		t.Fatalf("restore intent present after failed restore (err=%v)", err)
	}

	// Positive control: the pristine backup restores with identical data.
	pristineTarget := t.TempDir()
	fresh := db.NewNodeID()
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, backupDir),
		BackupName:  archive,
		TargetPath:  pristineTarget,
		FreshNodeID: fresh.String(),
		Mode:        backup.RestoreClone,
	}); err != nil {
		t.Fatalf("pristine restore: %v", err)
	}
	restored := openNodeDir(t, ctx, cluster, pristineTarget, fresh.String())
	defer restored.Close()
	n, err := countRows(ctx, t, restored)
	if err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Fatalf("restored count = %d, want %d", n, rows)
	}
	if got := offlineDigest(ctx, t, restored); got != wantDigest {
		t.Fatalf("restored digest %s != pre-backup %s", got, wantDigest)
	}
	t.Logf("pristine backup restored: %d rows digest %s", n, wantDigest)
}

func tryRestore(ctx context.Context, t *testing.T, srcDir, name, target string) error {
	t.Helper()
	_, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      mustLocalDest(t, srcDir),
		BackupName:  name,
		TargetPath:  target,
		FreshNodeID: db.NewNodeID().String(),
		Mode:        backup.RestoreClone,
	})
	return err
}

// assertTargetUntouched requires that a failed restore extracted no
// content: no regular files anywhere under target and no restore intent.
// (Empty data/keys/files directories created up front are allowed.)
func assertTargetUntouched(t *testing.T, target string) {
	t.Helper()
	var files []string
	err := filepath.Walk(target, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(target, path)
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("failed restore touched target dir: %v", files)
	}
}

// repackWithCorruptMetadata copies the archive entry by entry, flipping a
// byte in backup-metadata.json so it is no longer valid JSON.
func repackWithCorruptMetadata(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	gr, err := gzip.NewReader(in)
	if err != nil {
		t.Fatal(err)
	}
	defer gr.Close()
	tr := tar.NewReader(gr)

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	gw := gzip.NewWriter(out)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	corrupted := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read entry: %v", err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if hdr.Name == "backup-metadata.json" {
			i := bytes.IndexByte(body, '{')
			if i < 0 {
				t.Fatal("metadata has no JSON object opener")
			}
			body[i] = 'X'
			corrupted = true
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatalf("write body: %v", err)
		}
	}
	if !corrupted {
		t.Fatal("backup-metadata.json not found in archive")
	}
}

func singleArchive(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tar.gz") {
			names = append(names, e.Name())
		}
	}
	if len(names) != 1 {
		t.Fatalf("backup dir holds %v, want exactly one archive", names)
	}
	return names[0]
}

func mustLocalDest(t *testing.T, dir string) *backup.LocalDestination {
	t.Helper()
	d, err := backup.NewLocalDestination(dir)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func keyBytes(t *testing.T, keyHex string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func offlineConfig(t *testing.T, nodeDir string, cluster *harness.Cluster, nodeID string) db.Config {
	t.Helper()
	node, err := db.ParseNodeID(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	return db.Config{
		OriginSigning: cluster.OriginSigning(node),
		Path:          nodeDir,
		NodeID:        node,
		DBID:          cluster.DBID,
		Schema:        db.SchemaConfig{Version: 1},
		Tables:        []db.TableDefinition{typedDefinition(t)},
		Spool:         db.DefaultSpoolConfig(),
		// The daemon unlocks with key_id "remote-unlock-key" (see
		// handleAdminUnlock); the offline open must use the same ID.
		Encryption: db.EncryptionConfig{Key: keyBytes(t, cluster.Nodes[0].KeyHex), KeyID: "remote-unlock-key"},
	}
}

func openNodeDir(t *testing.T, ctx context.Context, cluster *harness.Cluster, nodeDir, nodeID string) *db.DB {
	t.Helper()
	handle, err := db.Open(ctx, offlineConfig(t, nodeDir, cluster, nodeID))
	if err != nil {
		t.Fatalf("open node dir: %v", err)
	}
	return handle
}

func countRows(ctx context.Context, t *testing.T, handle *db.DB) (int, error) {
	t.Helper()
	return handle.Count(ctx, tamperedTypedRecord{})
}

// offlineDigest uses the same canonical unique-name digest as TypedNames.
func offlineDigest(ctx context.Context, t *testing.T, handle *db.DB) string {
	t.Helper()
	var records []tamperedTypedRecord
	if err := handle.Find(ctx, &records); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(records))
	for _, row := range records {
		names = append(names, row.Name)
	}
	return namesDigest(names)
}

func namesDigest(names []string) string {
	sort.Strings(names)
	h := sha256.New()
	for _, name := range names {
		fmt.Fprintf(h, "%s\n", name)
	}
	return hex.EncodeToString(h.Sum(nil))
}
