package objectstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestOpenLayoutErrors covers layout validation: empty roots, file-blocked
// object paths, unwritable parents, and malformed generation files.
func TestOpenLayoutErrors(t *testing.T) {
	if _, _, err := openLayout(""); err == nil {
		t.Fatal("empty root accepted")
	}
	blocked := t.TempDir()
	if err := os.WriteFile(filepath.Join(blocked, "objects"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openLayout(blocked); err == nil {
		t.Fatal("file-blocked objects path accepted")
	}
	parent := filepath.Join(t.TempDir(), "ro")
	if err := os.MkdirAll(parent, 0o555); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openLayout(filepath.Join(parent, "root")); err == nil {
		t.Fatal("unwritable parent accepted")
	}
	badGen := t.TempDir()
	if err := os.MkdirAll(filepath.Join(badGen, "objects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badGen, "generation"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(badGen, rotKey(t)); err == nil {
		t.Fatal("malformed generation file accepted")
	}
	if _, err := New(t.TempDir(), []byte("short")); err == nil {
		t.Fatal("short key accepted")
	}
	if _, err := New("", rotKey(t)); err == nil {
		t.Fatal("empty root accepted")
	}
}

// TestOpenGenerationsValidation covers key-ring validation and missing-key
// refusal for multi-generation opens.
func TestOpenGenerationsValidation(t *testing.T) {
	if _, err := OpenGenerations(t.TempDir(), nil); err == nil {
		t.Fatal("empty ring accepted")
	}
	if _, err := OpenGenerations(t.TempDir(), map[uint32][]byte{0: rotKey(t)}); err == nil {
		t.Fatal("generation 0 accepted")
	}
	if _, err := OpenGenerations(t.TempDir(), map[uint32][]byte{1: []byte("short")}); err == nil {
		t.Fatal("short key accepted")
	}
	// Generation file says 2 but only the gen-1 key is supplied.
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "objects"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "generation"), []byte("2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenGenerations(root, map[uint32][]byte{1: rotKey(t)}); err == nil {
		t.Fatal("missing current-generation key accepted")
	}
}

// TestOpenGenerationsMissingDiskKey proves an on-disk generation without a
// supplied key refuses loudly, using an interrupted rotation fixture.
func TestOpenGenerationsMissingDiskKey(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	oldKey, newKey := rotKey(t), rotKey(t)
	st, err := New(root, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	rotPut(t, ctx, st, 100)
	rotPut(t, ctx, st, 200)
	cctx, cancel := context.WithCancel(ctx)
	calls := 0
	_ = st.RotateKey(cctx, newKey, func(done, total int) {
		calls++
		cancel()
	})
	st.Close()
	if calls == 0 {
		t.Fatal("rotation made no progress")
	}
	// Mixed generations on disk: the new key alone is insufficient.
	if _, err := OpenGenerations(root, map[uint32][]byte{2: newKey}); err == nil {
		t.Fatal("missing on-disk generation key accepted")
	}
}

// TestOpenWithPreviousValidation pins key-size validation.
func TestOpenWithPreviousValidation(t *testing.T) {
	if _, err := OpenWithPrevious(t.TempDir(), []byte("short"), rotKey(t)); err == nil {
		t.Fatal("short current key accepted")
	}
	if _, err := OpenWithPrevious(t.TempDir(), rotKey(t), []byte("short")); err == nil {
		t.Fatal("short previous key accepted")
	}
}

// TestOpenWithPreviousFresh proves the previous key is accepted and ignored
// when no older generation exists yet.
func TestOpenWithPreviousFresh(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st, err := OpenWithPrevious(root, rotKey(t), rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if gen := st.CurrentGeneration(); gen != 1 {
		t.Fatalf("generation = %d", gen)
	}
	d, data := rotPut(t, ctx, st, 64)
	if got := rotRead(t, ctx, st, d); !bytes.Equal(got, data) {
		t.Fatal("bytes differ")
	}
}

// TestOpenWithPreviousRotated proves opening a rotated store with both keys.
func TestOpenWithPreviousRotated(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	oldKey, newKey := rotKey(t), rotKey(t)
	st, err := New(root, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	d, data := rotPut(t, ctx, st, 64)
	if err := st.RotateKey(ctx, newKey, nil); err != nil {
		t.Fatal(err)
	}
	st.Close()
	rec, err := OpenWithPrevious(root, newKey, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()
	if gen := rec.CurrentGeneration(); gen != 2 {
		t.Fatalf("generation = %d", gen)
	}
	if got := rotRead(t, ctx, rec, d); !bytes.Equal(got, data) {
		t.Fatal("rotated bytes differ")
	}
}

// TestRotateKeyValidation pins key-size and context validation.
func TestRotateKeyValidation(t *testing.T) {
	ctx := context.Background()
	st, err := New(t.TempDir(), rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.RotateKey(ctx, []byte("short"), nil); err == nil {
		t.Fatal("short key accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := st.RotateKey(cancelled, rotKey(t), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled rotation = %v", err)
	}
}

// TestStoreClosedOps proves every operation fails closed after Close.
func TestStoreClosedOps(t *testing.T) {
	ctx := context.Background()
	st, err := New(t.TempDir(), rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	d, _ := rotPut(t, ctx, st, 16)
	st.Close()
	if _, err := st.keyCopyFor(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("keyCopyFor = %v", err)
	}
	if _, _, _, err := st.resolve(d); !errors.Is(err, ErrClosed) {
		t.Fatalf("resolve = %v", err)
	}
	if _, err := st.Serve(d); !errors.Is(err, ErrClosed) {
		t.Fatalf("Serve = %v", err)
	}
	if _, err := st.List(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("List = %v", err)
	}
}

// TestStoreMissingObject proves unknown digests fail lookups.
func TestStoreMissingObject(t *testing.T) {
	st, err := New(t.TempDir(), rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var missing Digest
	missing[0] = 0x42
	if _, _, _, err := st.resolve(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resolve = %v, want not-exist", err)
	}
	if _, err := st.Serve(missing); err == nil {
		t.Fatal("Serve(missing) succeeded")
	}
	if _, err := st.keyCopyFor(99); err == nil {
		t.Fatal("keyCopyFor(unknown gen) succeeded")
	}
}

// TestListSkipsAndMalformed proves inventory skips foreign files and fails
// closed on malformed object names.
func TestListSkipsAndMalformed(t *testing.T) {
	ctx := context.Background()
	st, err := New(t.TempDir(), rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	d, _ := rotPut(t, ctx, st, 16)
	objects := filepath.Join(st.root, "objects")
	if err := os.MkdirAll(filepath.Join(objects, "subdir"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	infos, err := st.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Digest != d {
		t.Fatalf("List = %+v", infos)
	}
	if err := os.WriteFile(filepath.Join(objects, "junk.spfo"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := st.List(ctx); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("List with malformed name = %v", err)
	}
}

// TestParseObjectNameTable pins every object-name shape.
func TestParseObjectNameTable(t *testing.T) {
	digest := strings.Repeat("ab", 32)
	for _, tc := range []struct {
		name string
		want bool
		gen  uint32
	}{
		{digest + ".spfo", true, 1},
		{digest + ".g2.spfo", true, 2},
		{digest + ".g10.spfo", true, 10},
		{"notes.txt", false, 0},
		{digest + ".txt.spfo", false, 0},
		{digest + ".gx.spfo", false, 0},
		{digest + ".g1.spfo", false, 0},
		{"short.spfo", false, 0},
		{strings.Repeat("zz", 32) + ".spfo", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, gen, ok := parseObjectName(tc.name)
			if ok != tc.want || (ok && gen != tc.gen) {
				t.Fatalf("= %v/%d, want %v/%d", ok, gen, tc.want, tc.gen)
			}
			if ok && d.String() != digest {
				t.Fatalf("digest = %s", d.String())
			}
		})
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// TestPutCancelledAndBroken proves Put honors cancellation and propagates
// reader failures.
func TestPutCancelledAndBroken(t *testing.T) {
	ctx := context.Background()
	st, err := New(t.TempDir(), rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.Put(cancelled, bytes.NewReader([]byte("x"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Put = %v", err)
	}
	if _, err := st.Put(ctx, errReader{errors.New("boom")}); err == nil {
		t.Fatal("broken-reader Put succeeded")
	}
}

// TestWriteGenerationMissingRoot covers durable-publish setup failure.
func TestWriteGenerationMissingRoot(t *testing.T) {
	if err := writeGeneration(filepath.Join(t.TempDir(), "missing"), 2); err == nil {
		t.Fatal("write into missing root succeeded")
	}
}

// TestReadGenerationUnreadable proves a non-NotFound generation read error
// surfaces instead of defaulting to generation 1.
func TestReadGenerationUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission test requires a non-root user")
	}
	root := t.TempDir()
	p := filepath.Join(root, "generation")
	if err := os.WriteFile(p, []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(p, 0o600) }()
	if _, err := readGeneration(root); err == nil {
		t.Fatal("unreadable generation file accepted")
	}
}
