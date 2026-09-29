package backup

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// skipIfRoot skips permission-bit tests that root would bypass.
func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("permission test requires a non-root user")
	}
}

// TestDiscardLogger pins the no-op logger so the fail-closed default path
// stays covered.
func TestDiscardLogger(t *testing.T) {
	var l discardLogger
	l.Debug("d", "k", 1)
	l.Info("i")
	l.Warn("w")
	l.Error("e")
}

// TestWorkerStartStop covers the worker lifecycle guards: disabled workers
// stay stopped, double start/stop are idempotent, and a nil destination
// fails fast.
func TestWorkerStartStop(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)

	disabled := NewWorker(ScheduleConfig{Enabled: false}, db)
	if err := disabled.Start(ctx); err != nil {
		t.Fatal(err)
	}
	disabled.Stop() // never started: no-op
	disabled.Stop() // idempotent

	noDest := NewWorker(ScheduleConfig{Enabled: true}, db)
	if err := noDest.Start(ctx); err == nil {
		t.Fatal("nil destination accepted")
	}

	dest, err := NewLocalDestination(filepath.Join(t.TempDir(), "backups"))
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker(ScheduleConfig{Enabled: true, Interval: time.Hour, Destination: dest}, db)
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatalf("second Start = %v", err)
	}
	w.Stop()
	w.Stop()
}

func pollWorker(t *testing.T, w *Worker, wantErr bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		done := !w.lastBackup.IsZero()
		lerr := w.lastErr
		w.mu.Unlock()
		if done && (lerr != nil) == wantErr {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker did not run a scheduled cycle in time")
}

// TestWorkerScheduledCycle proves the background loop performs periodic
// backups until stopped.
func TestWorkerScheduledCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := setupMockDB(t)
	dest, err := NewLocalDestination(filepath.Join(t.TempDir(), "backups"))
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker(ScheduleConfig{
		Enabled: true, Interval: 10 * time.Millisecond, Destination: dest,
	}, db)
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pollWorker(t, w, false)
	w.Stop()
	backups, err := dest.ListBackups(ctx, db.dbID)
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) == 0 {
		t.Fatal("scheduled loop produced no backup")
	}
}

type errDestination struct{ err error }

func (d errDestination) WriteBackup(context.Context, string, io.Reader, int64) error {
	return d.err
}
func (d errDestination) ReadBackup(context.Context, string) (io.ReadCloser, error) {
	return nil, d.err
}
func (d errDestination) ListBackups(context.Context, string) ([]BackupInfo, error) {
	return nil, d.err
}
func (d errDestination) DeleteBackup(context.Context, string) error { return d.err }
func (d errDestination) Type() string                               { return "err" }

// TestWorkerScheduledFailure proves a failing scheduled backup records
// lastErr and the loop keeps running until stopped.
func TestWorkerScheduledFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := setupMockDB(t)
	w := NewWorker(ScheduleConfig{
		Enabled: true, Interval: 10 * time.Millisecond,
		Destination: errDestination{errors.New("boom")},
	}, db)
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	pollWorker(t, w, true)
	w.Stop()
}

// TestHTTPSDestinationValidation pins constructor validation and defaults.
func TestHTTPSDestinationValidation(t *testing.T) {
	if _, err := NewHTTPSDestination(HTTPSOptions{}); err == nil {
		t.Fatal("empty BaseURL accepted")
	}
	if _, err := NewHTTPSDestination(HTTPSOptions{BaseURL: "ftp://x/y"}); err == nil {
		t.Fatal("non-http URL accepted")
	}
	h, err := NewHTTPSDestination(HTTPSOptions{BaseURL: "https://example.com/b/"})
	if err != nil {
		t.Fatal(err)
	}
	if h.Type() != "https" {
		t.Fatalf("Type = %q", h.Type())
	}
	if h.Client.Timeout != 30*time.Minute {
		t.Fatalf("default timeout = %v", h.Client.Timeout)
	}
	if h.BaseURL != "https://example.com/b" {
		t.Fatalf("BaseURL = %q, want trailing slash trimmed", h.BaseURL)
	}
}

// TestHTTPSDeleteBackup covers authorized deletes and server failures.
func TestHTTPSDeleteBackup(t *testing.T) {
	ctx := context.Background()
	var gotAuth, gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotKey = r.Header.Get("X-API-Key")
		switch {
		case strings.HasSuffix(r.URL.Path, "gone.tar.gz"):
			w.WriteHeader(http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "empty.tar.gz"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "bad.tar.gz"):
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	h, err := NewHTTPSDestination(HTTPSOptions{
		BaseURL: srv.URL, AuthBearer: "tok", Headers: map[string]string{"X-API-Key": "k"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a.tar.gz", "gone.tar.gz", "empty.tar.gz"} {
		if err := h.DeleteBackup(ctx, name); err != nil {
			t.Fatalf("DeleteBackup(%s) = %v", name, err)
		}
	}
	if gotAuth != "Bearer tok" || gotKey != "k" {
		t.Fatalf("auth headers = %q/%q", gotAuth, gotKey)
	}
	if err := h.DeleteBackup(ctx, "bad.tar.gz"); err == nil {
		t.Fatal("500 delete succeeded")
	}
	dead, err := NewHTTPSDestination(HTTPSOptions{BaseURL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := dead.DeleteBackup(ctx, "x"); err == nil {
		t.Fatal("delete to dead server succeeded")
	}
}

// TestHTTPSErrors covers the write/read/list failure branches.
func TestHTTPSErrors(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "denied.tar.gz"):
			w.WriteHeader(http.StatusUnauthorized)
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusInternalServerError)
		case strings.HasSuffix(r.URL.Path, "missing.tar.gz"):
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/" && r.URL.Query().Get("dbid") == "badjson":
			_, _ = w.Write([]byte("{oops"))
		case r.URL.Path == "/":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	h, err := NewHTTPSDestination(HTTPSOptions{BaseURL: srv.URL, AuthBearer: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.WriteBackup(ctx, "denied.tar.gz", strings.NewReader("x"), 1); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("denied write = %v, want unauthenticated", err)
	}
	if err := h.WriteBackup(ctx, "other.tar.gz", strings.NewReader("x"), 1); err == nil {
		t.Fatal("500 write succeeded")
	}
	if _, err := h.ReadBackup(ctx, "missing.tar.gz"); !errors.Is(err, ErrDestinationNotFound) {
		t.Fatalf("missing read = %v, want not-found", err)
	}
	if _, err := h.ReadBackup(ctx, "other.tar.gz"); err == nil {
		t.Fatal("500 read succeeded")
	}
	if _, err := h.ListBackups(ctx, "db"); err == nil {
		t.Fatal("500 list succeeded")
	}
	if _, err := h.ListBackups(ctx, "badjson"); err == nil {
		t.Fatal("malformed list succeeded")
	}
	dead, err := NewHTTPSDestination(HTTPSOptions{BaseURL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := dead.WriteBackup(ctx, "x", strings.NewReader("x"), 1); err == nil {
		t.Fatal("write to dead server succeeded")
	}
	if _, err := dead.ReadBackup(ctx, "x"); err == nil {
		t.Fatal("read from dead server succeeded")
	}
	if _, err := dead.ListBackups(ctx, "x"); err == nil {
		t.Fatal("list on dead server succeeded")
	}
}

// TestLocalDestinationErrors covers local validation, missing/denied files,
// unreadable directories, and cancelled or broken streams.
func TestLocalDestinationErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := NewLocalDestination(""); err == nil {
		t.Fatal("empty dir accepted")
	}
	dest, err := NewLocalDestination(filepath.Join(t.TempDir(), "backups"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dest.ReadBackup(ctx, "missing.tar.gz"); !errors.Is(err, ErrDestinationNotFound) {
		t.Fatalf("missing read = %v, want not-found", err)
	}

	gone, err := NewLocalDestination(filepath.Join(t.TempDir(), "gone"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(gone.Dir); err != nil {
		t.Fatal(err)
	}
	if _, err := gone.ListBackups(ctx, "db"); err == nil {
		t.Fatal("list of missing dir succeeded")
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := dest.WriteBackup(cancelled, "x.tar.gz", strings.NewReader("x"), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled write = %v, want context.Canceled", err)
	}
	if err := dest.WriteBackup(ctx, "x.tar.gz", errReader{errors.New("boom")}, 0); err == nil {
		t.Fatal("broken stream write succeeded")
	}
}

// TestLocalReadDenied proves an unreadable archive surfaces a non-NotFound
// error rather than a missing-backup report.
func TestLocalReadDenied(t *testing.T) {
	skipIfRoot(t)
	ctx := context.Background()
	dest, err := NewLocalDestination(filepath.Join(t.TempDir(), "backups"))
	if err != nil {
		t.Fatal(err)
	}
	deniedPath := filepath.Join(dest.Dir, "denied.tar.gz")
	if err := os.WriteFile(deniedPath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(deniedPath, 0); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(deniedPath, 0o600) }()
	if _, err := dest.ReadBackup(ctx, "denied.tar.gz"); err == nil || errors.Is(err, ErrDestinationNotFound) {
		t.Fatalf("denied read = %v, want non-NotFound error", err)
	}
}

// TestLocalListSkips proves non-archives and unreadable archives are
// skipped or fall back to filename matching.
func TestLocalListSkips(t *testing.T) {
	ctx := context.Background()
	dest, err := NewLocalDestination(filepath.Join(t.TempDir(), "backups"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dest.Dir, "subdir"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest.Dir, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest.Dir, "db9-junk.tar.gz"), []byte("not a gzip"), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := dest.ListBackups(ctx, "nomatch")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("filtered list = %+v, want empty", items)
	}
	items, err = dest.ListBackups(ctx, "db9")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Name != "db9-junk.tar.gz" {
		t.Fatalf("fallback list = %+v, want filename match", items)
	}
}

func objectName(t *testing.T, gen uint32) string {
	t.Helper()
	digest := strings.Repeat("ab", 32)
	if gen <= 1 {
		return digest + ".spfo"
	}
	return digest + ".g2.spfo"
}

// TestLinkFilesTreeBranches covers skips, the copy fallback, and marker handling.
func TestLinkFilesTreeBranches(t *testing.T) {
	t.Run("missing-objects", func(t *testing.T) {
		n, b, err := linkFilesTree(t.TempDir(), filepath.Join(t.TempDir(), "staging"))
		if err != nil || n != 0 || b != 0 {
			t.Fatalf("= %d/%d/%v, want 0/0/nil", n, b, err)
		}
	})
	t.Run("skips-and-links", func(t *testing.T) {
		root := t.TempDir()
		objects := filepath.Join(root, "objects")
		if err := os.MkdirAll(filepath.Join(objects, "subdir"), 0o750); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{".hidden", "tmp_part", "foreign.txt"} {
			if err := os.WriteFile(filepath.Join(objects, name), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		obj := objectName(t, 1)
		if err := os.WriteFile(filepath.Join(objects, obj), []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "generation"), []byte("1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		n, b, err := linkFilesTree(root, filepath.Join(t.TempDir(), "staging"))
		if err != nil || n != 1 || b != int64(len("payload")) {
			t.Fatalf("= %d/%d/%v", n, b, err)
		}
	})
	t.Run("copy-fallback", func(t *testing.T) {
		root := t.TempDir()
		objects := filepath.Join(root, "objects")
		if err := os.MkdirAll(objects, 0o750); err != nil {
			t.Fatal(err)
		}
		obj := objectName(t, 1)
		if err := os.WriteFile(filepath.Join(objects, obj), []byte("payload"), 0o600); err != nil {
			t.Fatal(err)
		}
		// Pre-existing destination breaks the hard link, forcing a copy.
		staging := filepath.Join(t.TempDir(), "staging")
		if err := os.MkdirAll(filepath.Join(staging, "objects"), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(staging, "objects", obj), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		n, _, err := linkFilesTree(root, staging)
		if err != nil || n != 1 {
			t.Fatalf("= %d/%v", n, err)
		}
		got, err := os.ReadFile(filepath.Join(staging, "objects", obj))
		if err != nil || string(got) != "payload" {
			t.Fatalf("staged = %q/%v", got, err)
		}
	})
	t.Run("marker-copy-fails", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "objects"), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "generation"), []byte("1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		staging := filepath.Join(t.TempDir(), "staging")
		if err := os.MkdirAll(filepath.Join(staging, "generation"), 0o750); err != nil {
			t.Fatal(err)
		}
		if _, _, err := linkFilesTree(root, staging); err == nil {
			t.Fatal("marker copy onto a directory succeeded")
		}
	})
	t.Run("objects-unreadable", func(t *testing.T) {
		skipIfRoot(t)
		root := t.TempDir()
		objects := filepath.Join(root, "objects")
		if err := os.MkdirAll(objects, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(objects, 0); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Chmod(objects, 0o750) }()
		if _, _, err := linkFilesTree(root, filepath.Join(t.TempDir(), "staging")); err == nil {
			t.Fatal("unreadable objects dir succeeded")
		}
	})
}

// TestReadStagedGeneration pins marker parsing including malformed input.
func TestReadStagedGeneration(t *testing.T) {
	dir := t.TempDir()
	if gen, ok := readStagedGeneration(dir); !ok || gen != 1 {
		t.Fatalf("absent = %d/%v, want 1/true", gen, ok)
	}
	if err := os.WriteFile(filepath.Join(dir, "generation"), []byte("3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if gen, ok := readStagedGeneration(dir); !ok || gen != 3 {
		t.Fatalf("valid = %d/%v, want 3/true", gen, ok)
	}
	for _, bad := range []string{"abc", "0", ""} {
		if err := os.WriteFile(filepath.Join(dir, "generation"), []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if gen, ok := readStagedGeneration(dir); ok || gen != 0 {
			t.Fatalf("malformed %q = %d/%v, want 0/false", bad, gen, ok)
		}
	}
}

// TestStagedUniformBranches covers marker errors, missing staging,
// generation mixes, and directory skips.
func TestStagedUniformBranches(t *testing.T) {
	if _, err := stagedUniform(t.TempDir(), 0, false); err == nil {
		t.Fatal("bad marker accepted")
	}
	if ok, err := stagedUniform(t.TempDir(), 1, true); err != nil || !ok {
		t.Fatalf("missing staging = %v/%v", ok, err)
	}
	dir := t.TempDir()
	objects := filepath.Join(dir, "objects")
	if err := os.MkdirAll(filepath.Join(objects, "subdir"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, objectName(t, 1)), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ok, err := stagedUniform(dir, 1, true); err != nil || !ok {
		t.Fatalf("uniform = %v/%v", ok, err)
	}
	if err := os.WriteFile(filepath.Join(objects, objectName(t, 2)), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ok, err := stagedUniform(dir, 1, true); err != nil || ok {
		t.Fatalf("mixed = %v/%v, want false/nil", ok, err)
	}
}

// TestStagedUniformUnreadable proves an unreadable staging tree fails
// verification instead of reporting uniform.
func TestStagedUniformUnreadable(t *testing.T) {
	skipIfRoot(t)
	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.MkdirAll(filepath.Join(locked, "objects"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(locked, "objects"), 0); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(filepath.Join(locked, "objects"), 0o750) }()
	if _, err := stagedUniform(locked, 1, true); err == nil {
		t.Fatal("unreadable staging accepted")
	}
}

// TestSnapshotFilesRacedRotation proves a persistent generation mix fails
// loudly after bounded retries instead of archiving mixed state.
func TestSnapshotFilesRacedRotation(t *testing.T) {
	root := t.TempDir()
	objects := filepath.Join(root, "objects")
	if err := os.MkdirAll(objects, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, objectName(t, 1)), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, objectName(t, 2)), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "generation"), []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := snapshotFiles(root, filepath.Join(t.TempDir(), "staging"), discardLogger{}); err == nil {
		t.Fatal("mixed snapshot succeeded")
	}
}

// TestSnapshotFilesStagingUnwritable covers staging cleanup failures.
func TestSnapshotFilesStagingUnwritable(t *testing.T) {
	skipIfRoot(t)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "objects"), 0o750); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	staging := filepath.Join(parent, "staging")
	if err := os.MkdirAll(staging, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "junk"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(parent, 0o755) }()
	if _, _, err := snapshotFiles(root, staging, discardLogger{}); err == nil {
		t.Fatal("unwritable staging succeeded")
	}
}

// TestWriteRestoreIntentErrors covers intent persistence failures.
func TestWriteRestoreIntentErrors(t *testing.T) {
	intent := &RestoreIntent{Version: 1, FreshNodeID: "n"}
	if err := WriteRestoreIntent(filepath.Join(t.TempDir(), "missing"), intent); err == nil {
		t.Fatal("intent into missing dir succeeded")
	}
}

func TestLocalDestinationDirNoGroupOther(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dest")
	d, err := NewLocalDestination(dir)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(d.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		t.Fatalf("destination dir mode=%04o, want no group/other bits", fi.Mode().Perm())
	}
}
