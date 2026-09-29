package benchmark

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	replicateddb "github.com/marcgauthier/spedsql"
)

// benchTemplate is one populated, closed store per dataset size, built
// lazily on first use and copied per benchmark. Populating once (instead
// of per benchmark) keeps large datasets feasible: the 100K template
// builds in about a minute, 1M in several minutes, and every benchmark
// after that starts from a fast directory copy.
type benchTemplate struct {
	dir  string
	node replicateddb.NodeID
	dbid replicateddb.DBID
	ids  []replicateddb.RowID
}

var (
	templatesMu   sync.Mutex
	templates     = map[int]*benchTemplate{}
	templateDirs  []string
	templateBuilt = map[int]bool{}
)

// TestMain runs the suite, then removes lazily built dataset templates.
func TestMain(m *testing.M) {
	code := m.Run()
	templatesMu.Lock()
	dirs := templateDirs
	templatesMu.Unlock()
	for _, dir := range dirs {
		_ = os.RemoveAll(dir)
	}
	os.Exit(code)
}

// templateFor returns the shared template for n rows, building it once.
func templateFor(b testing.TB, n int) *benchTemplate {
	b.Helper()
	templatesMu.Lock()
	defer templatesMu.Unlock()
	if t, ok := templates[n]; ok {
		return t
	}
	dir, err := os.MkdirTemp("", "bench-template-*")
	if err != nil {
		b.Fatal(err)
	}
	templateDirs = append(templateDirs, dir)
	node := replicateddb.NewNodeID()
	db, err := replicateddb.Open(context.Background(), benchConfig(dir, node, replicateddb.NewDBID()))
	if err != nil {
		b.Fatal(err)
	}
	ids := populate(b, db, n)
	dbid := db.DBID()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	t := &benchTemplate{dir: dir, node: node, dbid: dbid, ids: ids}
	templates[n] = t
	templateBuilt[n] = true
	return t
}

// openTemplateDB copies the n-row template to a fresh dir and opens it.
// The copy keeps the template's node identity (single-node benchmarks
// only); multi-node benchmarks populate live with distinct identities.
func openTemplateDB(b testing.TB, n int) (*replicateddb.DB, []replicateddb.RowID) {
	b.Helper()
	tmpl := templateFor(b, n)
	dest := b.TempDir()
	if err := copyDir(tmpl.dir, dest); err != nil {
		b.Fatal(err)
	}
	db, err := replicateddb.Open(context.Background(), benchConfig(dest, tmpl.node, tmpl.dbid))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db, tmpl.ids
}

// copyDir recursively copies src into dst (dst must exist).
func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		info, err := d.Info()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		syncErr := out.Sync()
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if syncErr != nil {
			return syncErr
		}
		return closeErr
	})
}
