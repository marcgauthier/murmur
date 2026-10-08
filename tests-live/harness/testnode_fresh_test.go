package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A bare `go test ./tests-live/<scenario>` must rebuild the fixture binary
// when sources changed, and only then: silent reuse of a stale binary once
// masked a real fix during a red/green verification.
func TestTestNodeStaleReason(t *testing.T) {
	const tags = ""
	old := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	mk := func(t *testing.T, root, name string, mod time.Time) string {
		t.Helper()
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mod, mod); err != nil {
			t.Fatal(err)
		}
		return p
	}
	fresh := func(t *testing.T) (root, bin string) {
		t.Helper()
		root = t.TempDir()
		bin = mk(t, root, "tests-live/bin/testnode", old)
		mk(t, root, "tests-live/bin/testnode.tags", old)
		if err := os.WriteFile(bin+".tags", []byte(testNodeBuildStamp(tags, false)), 0o644); err != nil {
			t.Fatal(err)
		}
		mk(t, root, "db.go", old)
		mk(t, root, "go.mod", old)
		return root, bin
	}

	t.Run("missing binary rebuilds", func(t *testing.T) {
		root, bin := fresh(t)
		if err := os.Remove(bin); err != nil {
			t.Fatal(err)
		}
		if got := testNodeStaleReason(bin, root, tags, false); got != "binary missing" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("tags mismatch rebuilds", func(t *testing.T) {
		root, bin := fresh(t)
		if got := testNodeStaleReason(bin, root, "other-tags", false); got != "build tags changed or unknown" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("newer source rebuilds", func(t *testing.T) {
		root, bin := fresh(t)
		mk(t, root, "replication/manager.go", time.Now())
		got := testNodeStaleReason(bin, root, tags, false)
		if !strings.HasPrefix(got, "source newer than binary: ") {
			t.Fatalf("got %q", got)
		}
		if !strings.Contains(got, "manager.go") {
			t.Fatalf("reason should name the file, got %q", got)
		}
	})

	t.Run("fresh tree reuses", func(t *testing.T) {
		root, bin := fresh(t)
		if got := testNodeStaleReason(bin, root, tags, false); got != "" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("non-sources and VCS ignored", func(t *testing.T) {
		root, bin := fresh(t)
		now := time.Now()
		mk(t, root, "README.md", now)
		mk(t, root, ".git/refs/heads/main", now)
		mk(t, root, "tests-live/failures/note.md", now)
		if got := testNodeStaleReason(bin, root, tags, false); got != "" {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("race mismatch rebuilds", func(t *testing.T) {
		root, bin := fresh(t)
		if got := testNodeStaleReason(bin, root, tags, true); got != "build tags changed or unknown" {
			t.Fatalf("got %q", got)
		}
	})
}
