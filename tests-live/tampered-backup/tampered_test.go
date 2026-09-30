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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/backup"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "tb_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func TestTamperedBackupsFailClosed(t *testing.T) {
	ctx := context.Background()
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "tampered-backup",
		NumNodes:    1,
		AwaitUnlock: true,
		Schema:      schemaConfig(),
	})

	const rows = 30
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("%032x", 9000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO tb_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	if n, err := cluster.QueryRowCount(0, "tb_rows"); err != nil || n != rows {
		t.Fatalf("seeded count = %d, %v; want %d", n, err, rows)
	}
	wantDigest, err := cluster.ComputeTableDigest(0, "tb_rows", "id")
	if err != nil {
		t.Fatalf("pre-backup digest: %v", err)
	}

	cluster.StopNode(0)
	node := cluster.Nodes[0]

	backupDir := t.TempDir()
	offline := openNodeDir(t, ctx, cluster, node.PebbleDir, node.NodeID.String())
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

func offlineConfig(t *testing.T, pebbleDir string, cluster *harness.Cluster, nodeID string) db.Config {
	t.Helper()
	node, err := db.ParseNodeID(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	return db.Config{
		Path:   pebbleDir,
		NodeID: node,
		DBID:   cluster.DBID,
		Schema: *schemaConfig(),
		Pebble: db.DefaultPebbleConfig(),
		// The daemon unlocks with key_id "remote-unlock-key" (see
		// handleAdminUnlock); the offline open must use the same ID.
		Encryption: db.EncryptionConfig{Key: keyBytes(t, cluster.Nodes[0].KeyHex), KeyID: "remote-unlock-key"},
	}
}

func openNodeDir(t *testing.T, ctx context.Context, cluster *harness.Cluster, pebbleDir, nodeID string) *db.DB {
	t.Helper()
	handle, err := db.Open(ctx, offlineConfig(t, pebbleDir, cluster, nodeID))
	if err != nil {
		t.Fatalf("open node dir: %v", err)
	}
	return handle
}

func countRows(ctx context.Context, t *testing.T, handle *db.DB) (int, error) {
	t.Helper()
	rows, err := handle.QueryContext(ctx, "SELECT count(*) FROM tb_rows")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("no count row")
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		return 0, err
	}
	return n, rows.Err()
}

// offlineDigest mirrors harness.ComputeTableDigest exactly, including its
// JSON value rendering: scanned cells take the same marshal/unmarshal
// round trip the daemon HTTP path applies before %v formatting.
func offlineDigest(ctx context.Context, t *testing.T, handle *db.DB) string {
	t.Helper()
	rows, err := handle.QueryContext(ctx, "SELECT * FROM tb_rows ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols := rows.Columns()
	h := sha256.New()
	for rows.Next() {
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for _, cell := range dest {
			raw, err := json.Marshal(cell)
			if err != nil {
				t.Fatal(err)
			}
			var v any
			if err := json.Unmarshal(raw, &v); err != nil {
				t.Fatal(err)
			}
			h.Write([]byte(fmt.Sprintf("%v:", v)))
		}
		h.Write([]byte("\n"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}
