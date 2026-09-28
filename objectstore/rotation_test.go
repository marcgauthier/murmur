package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func rotKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	return key
}

func rotPut(t *testing.T, ctx context.Context, st *Store, size int) (Digest, []byte) {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	info, err := st.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return info.Digest, data
}

func rotRead(t *testing.T, ctx context.Context, st *Store, digest Digest) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := st.Read(ctx, digest, &buf); err != nil {
		t.Fatalf("read %s: %v", digest, err)
	}
	return buf.Bytes()
}

func TestRotateKey(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	oldKey, newKey := rotKey(t), rotKey(t)
	st, err := New(root, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	d1, data1 := rotPut(t, ctx, st, 100)
	d2, data2 := rotPut(t, ctx, st, 200_000)
	var progressed int
	if err := st.RotateKey(ctx, newKey, func(done, total int) { progressed = done }); err != nil {
		t.Fatal(err)
	}
	if progressed != 2 {
		t.Fatalf("progress reached %d objects, want 2", progressed)
	}
	if gen := st.CurrentGeneration(); gen != 2 {
		t.Fatalf("current generation %d, want 2", gen)
	}
	// Old-generation files are gone; new ones verify.
	if _, err := os.Stat(filepath.Join(root, "objects", d1.String()+".spfo")); !os.IsNotExist(err) {
		t.Fatal("gen-1 file survives rotation")
	}
	if _, err := os.Stat(filepath.Join(root, "objects", d1.String()+".g2.spfo")); err != nil {
		t.Fatalf("gen-2 file missing: %v", err)
	}
	if got := rotRead(t, ctx, st, d1); !bytes.Equal(got, data1) {
		t.Fatal("rotated bytes differ")
	}
	if got := rotRead(t, ctx, st, d2); !bytes.Equal(got, data2) {
		t.Fatal("rotated large bytes differ")
	}
	raw, err := os.ReadFile(filepath.Join(root, generationFile))
	if err != nil || string(raw) != "2\n" {
		t.Fatalf("generation file %q err=%v", raw, err)
	}
	// The old key alone no longer decrypts (generation-2 files only).
	st.Close()
	stOld, err := New(root, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	defer stOld.Close()
	if _, err := stOld.Read(ctx, d1, io.Discard); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("read with retired key: got %v, want ErrInvalidObject", err)
	}
	stNew, err := New(root, newKey)
	if err != nil {
		t.Fatal(err)
	}
	defer stNew.Close()
	if got := rotRead(t, ctx, stNew, d1); !bytes.Equal(got, data1) {
		t.Fatal("reopened rotated bytes differ")
	}
}

func TestRotateKeyEmpty(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st, err := New(root, rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	newKey := rotKey(t)
	if err := st.RotateKey(ctx, newKey, nil); err != nil {
		t.Fatal(err)
	}
	if gen := st.CurrentGeneration(); gen != 2 {
		t.Fatalf("generation %d, want 2", gen)
	}
	d, data := rotPut(t, ctx, st, 10)
	if got := rotRead(t, ctx, st, d); !bytes.Equal(got, data) {
		t.Fatal("post-rotation bytes differ")
	}
}

func TestRotateKeyInterrupted(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	oldKey, newKey := rotKey(t), rotKey(t)
	st, err := New(root, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	d1, data1 := rotPut(t, ctx, st, 100)
	d2, data2 := rotPut(t, ctx, st, 200)
	d3, data3 := rotPut(t, ctx, st, 300)

	// Cancel after the first rewrite: mixed generations on disk, the
	// generation file still at 1.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	calls := 0
	err = st.RotateKey(cctx, newKey, func(done, total int) {
		calls++
		if done == 1 {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted rotation: got %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("progress calls %d, want 1", calls)
	}
	st.Close()

	// Either key alone refuses the mixed state loudly.
	if _, err := New(root, oldKey); err == nil {
		t.Fatal("single-key open of mixed generations succeeded")
	}
	if _, err := New(root, newKey); err == nil {
		t.Fatal("single-key open of mixed generations succeeded")
	}
	// Missing one generation key refuses too.
	if _, err := OpenGenerations(root, map[uint32][]byte{1: oldKey}); err == nil {
		t.Fatal("recovery open without the new key succeeded")
	}
	// Both keys complete the rotation automatically.
	rec, err := OpenGenerations(root, map[uint32][]byte{1: oldKey, 2: newKey})
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()
	if gen := rec.CurrentGeneration(); gen != 2 {
		t.Fatalf("recovered generation %d, want 2", gen)
	}
	for d, want := range map[Digest][]byte{d1: data1, d2: data2, d3: data3} {
		if got := rotRead(t, ctx, rec, d); !bytes.Equal(got, want) {
			t.Fatalf("recovered bytes differ for %s", d)
		}
		if _, err := os.Stat(filepath.Join(root, "objects", d.String()+".spfo")); !os.IsNotExist(err) {
			t.Fatalf("gen-1 file for %s survives recovery", d)
		}
	}
	// Afterwards the new key alone suffices.
	rec.Close()
	stNew, err := New(root, newKey)
	if err != nil {
		t.Fatal(err)
	}
	defer stNew.Close()
	if got := rotRead(t, ctx, stNew, d1); !bytes.Equal(got, data1) {
		t.Fatal("post-recovery bytes differ")
	}
}

func TestRotateKeyTwice(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st, err := New(root, rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	d, data := rotPut(t, ctx, st, 50)
	k2, k3 := rotKey(t), rotKey(t)
	if err := st.RotateKey(ctx, k2, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.RotateKey(ctx, k3, nil); err != nil {
		t.Fatal(err)
	}
	if gen := st.CurrentGeneration(); gen != 3 {
		t.Fatalf("generation %d, want 3", gen)
	}
	if _, err := os.Stat(filepath.Join(root, "objects", d.String()+".g3.spfo")); err != nil {
		t.Fatalf("gen-3 file missing: %v", err)
	}
	if got := rotRead(t, ctx, st, d); !bytes.Equal(got, data) {
		t.Fatal("twice-rotated bytes differ")
	}
	entries, err := os.ReadDir(filepath.Join(root, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("%d files remain, want 1", len(entries))
	}
}

func TestRotateKeyConcurrentReads(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	st, err := New(root, rotKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	d, data := rotPut(t, ctx, st, 300_000)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				var buf bytes.Buffer
				if _, err := st.Read(ctx, d, &buf); err != nil {
					t.Errorf("concurrent read: %v", err)
					return
				}
				if !bytes.Equal(buf.Bytes(), data) {
					t.Error("concurrent read bytes differ")
					return
				}
			}
		}()
	}
	if err := st.RotateKey(ctx, rotKey(t), nil); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
