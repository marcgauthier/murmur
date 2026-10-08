package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestPutReadStreamsAuthenticatedImmutableObject(t *testing.T) {
	root := t.TempDir()
	key := bytes.Repeat([]byte{0x6d}, 32)
	store, err := New(root, key)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	plain := bytes.Repeat([]byte("distinct-file-marker/"), (3*ChunkSize)/21+5)
	info, err := store.Put(context.Background(), bytes.NewReader(plain))
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(plain)
	if info.Digest != want || info.Length != int64(len(plain)) || !store.Has(info.Digest) {
		t.Fatalf("stored info=%+v, want digest %x length %d", info, want, len(plain))
	}
	entries, err := os.ReadDir(filepath.Join(root, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].IsDir() {
		t.Fatalf("object directory entries=%v, want one immutable object", entries)
	}
	encoded, err := os.ReadFile(filepath.Join(root, "objects", entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("distinct-file-marker")) {
		t.Fatal("object payload was persisted in plaintext")
	}
	var got bytes.Buffer
	readInfo, err := store.Read(context.Background(), info.Digest, &got)
	if err != nil {
		t.Fatal(err)
	}
	if readInfo != info || !bytes.Equal(got.Bytes(), plain) {
		t.Fatalf("round trip info=%+v length=%d, want info=%+v length=%d", readInfo, got.Len(), info, len(plain))
	}
	second, err := store.Put(context.Background(), bytes.NewReader(plain))
	if err != nil || second != info {
		t.Fatalf("idempotent put info=%+v err=%v", second, err)
	}
	entries, err = os.ReadDir(filepath.Join(root, "objects"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("idempotent put created extra objects: entries=%d err=%v", len(entries), err)
	}
}

func TestInventoryCollectionHonorsReferencesPinsAndRetention(t *testing.T) {
	store, err := New(t.TempDir(), bytes.Repeat([]byte{0x55}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	put := func(value string) Digest {
		t.Helper()
		info, err := store.Put(context.Background(), bytes.NewBufferString(value))
		if err != nil {
			t.Fatal(err)
		}
		return info.Digest
	}
	pinned, referenced, young := put("pinned"), put("referenced"), put("young")
	pin, err := store.Pin(pinned)
	if err != nil {
		t.Fatal(err)
	}
	infos, err := store.List(context.Background())
	if err != nil || len(infos) != 3 {
		t.Fatalf("inventory size=%d err=%v, want 3", len(infos), err)
	}
	if removed, err := store.Collect([]Digest{referenced}, time.Hour); err != nil || len(removed) != 0 {
		t.Fatalf("young-object collection removed=%v err=%v", removed, err)
	}
	if removed, err := store.Collect([]Digest{referenced, young}, 0); err != nil || len(removed) != 0 {
		t.Fatalf("pinned/reference collection removed=%v err=%v", removed, err)
	}
	if !store.Has(pinned) || !store.Has(referenced) || !store.Has(young) {
		t.Fatal("collection removed a pinned or referenced object")
	}
	if err := pin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pin.Close(); err != nil {
		t.Fatal(err)
	}
	removed, err := store.Collect([]Digest{referenced, young}, 0)
	if err != nil || len(removed) != 1 || removed[0] != pinned {
		t.Fatalf("unpin collection removed=%v err=%v, want only pinned object", removed, err)
	}
	if store.Has(pinned) || !store.Has(referenced) || !store.Has(young) {
		t.Fatal("collection did not respect the current reference set")
	}
}

func TestActiveReadPinsObjectAgainstCollection(t *testing.T) {
	store, err := New(t.TempDir(), bytes.Repeat([]byte{0x56}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	plain := bytes.Repeat([]byte("stream-pin"), ChunkSize/10+10)
	info, err := store.Put(context.Background(), bytes.NewReader(plain))
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		_, err := store.Read(context.Background(), info.Digest, blockingWriter{started: started, release: release})
		readDone <- err
	}()
	<-started
	removed, err := store.Collect(nil, 0)
	if err != nil || len(removed) != 0 || !store.Has(info.Digest) {
		close(release)
		t.Fatalf("collection during read removed=%v err=%v", removed, err)
	}
	close(release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	removed, err = store.Collect(nil, 0)
	if err != nil || len(removed) != 1 || removed[0] != info.Digest {
		t.Fatalf("post-read collection removed=%v err=%v", removed, err)
	}
}

func TestPinConcurrentWithCollect(t *testing.T) {
	store, err := New(t.TempDir(), bytes.Repeat([]byte{0x57}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	plain := []byte("concurrent pin retention")
	type result struct {
		mode int
		ok   bool
		err  error
	}
	var successes [3]int
	for round := 0; round < 512; round++ {
		info, err := store.Put(context.Background(), bytes.NewReader(plain))
		if err != nil {
			t.Fatal(err)
		}
		start, stop := make(chan struct{}), make(chan struct{})
		collected := make(chan error, 1)
		go func() {
			<-start
			for {
				if _, err := store.Collect(nil, 0); err != nil {
					collected <- err
					return
				}
				select {
				case <-stop:
					collected <- nil
					return
				default:
					runtime.Gosched()
				}
			}
		}()
		// No publisher runs during acquisition: a successful handle cannot
		// hide a collection error behind recreation of the same digest.
		checkRetained := func() error {
			if _, err := store.Collect(nil, 0); err != nil {
				return err
			}
			var got bytes.Buffer
			if _, err := store.Read(context.Background(), info.Digest, &got); err != nil {
				return fmt.Errorf("retained object is unreadable: %v", err)
			}
			if !bytes.Equal(got.Bytes(), plain) {
				return fmt.Errorf("retained object plaintext differs")
			}
			return nil
		}
		results := make(chan result, 12)
		for worker := 0; worker < 12; worker++ {
			go func(mode int) {
				<-start
				var err error
				switch mode {
				case 0:
					pin, pinErr := store.Pin(info.Digest)
					if pinErr != nil {
						err = pinErr
						break
					}
					err = checkRetained()
					_ = pin.Close()
					results <- result{mode: mode, ok: true, err: err}
					return
				case 1:
					served, serveErr := store.Serve(info.Digest)
					if serveErr != nil {
						err = serveErr
						break
					}
					err = checkRetained()
					if err == nil {
						var header [headerLen]byte
						_, err = served.ReadAt(header[:], 0)
					}
					_ = served.Close()
					results <- result{mode: mode, ok: true, err: err}
					return
				case 2:
					var got bytes.Buffer
					_, err = store.Read(context.Background(), info.Digest, &got)
					if err == nil && !bytes.Equal(got.Bytes(), plain) {
						err = fmt.Errorf("concurrent read plaintext differs")
					}
				}
				if errors.Is(err, os.ErrNotExist) {
					// GC may win before this operation acquires a pin.
					results <- result{mode: mode}
				} else {
					results <- result{mode: mode, ok: err == nil, err: err}
				}
			}(worker % 3)
		}
		close(start)
		var firstErr error
		for worker := 0; worker < 12; worker++ {
			r := <-results
			if r.ok {
				successes[r.mode]++
			}
			if r.err != nil && firstErr == nil {
				firstErr = fmt.Errorf("round %d mode %d: %w", round, r.mode, r.err)
			}
		}
		close(stop)
		if err := <-collected; err != nil {
			t.Fatal(err)
		}
		if firstErr != nil {
			t.Fatal(firstErr)
		}
		// Every successful operation has closed its pin. The digest must
		// become collectible again, including after nested Read pins.
		if _, err := store.Collect(nil, 0); err != nil {
			t.Fatal(err)
		}
		if store.Has(info.Digest) {
			t.Fatalf("round %d: object retained after all handles closed", round)
		}
	}
	for mode, count := range successes {
		if count == 0 {
			t.Fatalf("mode %d never acquired an object", mode)
		}
	}
}

type blockingWriter struct {
	started chan struct{}
	release chan struct{}
}

func (w blockingWriter) Write(p []byte) (int, error) {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	<-w.release
	return len(p), nil
}

func TestReadRejectsTamperedAndTruncatedObjects(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, bytes.Repeat([]byte{0x21}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	plain := bytes.Repeat([]byte("authenticated-chunk"), ChunkSize/19+2)
	info, err := store.Put(context.Background(), bytes.NewReader(plain))
	if err != nil {
		t.Fatal(err)
	}
	path := store.objectPath(info.Digest)

	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tampered := append([]byte(nil), original...)
	tampered[headerLen+4] ^= 0x40
	if err := os.WriteFile(path, tampered, 0600); err != nil {
		t.Fatal(err)
	}
	var dst bytes.Buffer
	if _, err := store.Read(context.Background(), info.Digest, &dst); !errors.Is(err, ErrInvalidObject) || dst.Len() != 0 {
		t.Fatalf("tampered object err=%v bytes=%d, want authentication error and no unauthenticated chunk", err, dst.Len())
	}

	if err := os.WriteFile(path, original[:len(original)-1], 0600); err != nil {
		t.Fatal(err)
	}
	dst.Reset()
	if _, err := store.Read(context.Background(), info.Digest, &dst); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("truncated object err=%v, want invalid object", err)
	}
}

func TestWrongKeyAndClosedStoreFailClosed(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, bytes.Repeat([]byte{0x31}, 32))
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Put(context.Background(), bytes.NewBufferString("private"))
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := New(root, bytes.Repeat([]byte{0x32}, 32))
	if err != nil {
		t.Fatal(err)
	}
	var dst bytes.Buffer
	if _, err := wrong.Read(context.Background(), info.Digest, &dst); !errors.Is(err, ErrInvalidObject) || dst.Len() != 0 {
		t.Fatalf("wrong-key read err=%v bytes=%d", err, dst.Len())
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), bytes.NewReader(nil)); !errors.Is(err, ErrClosed) {
		t.Fatalf("put after close err=%v, want closed error", err)
	}
	if _, err := wrong.Read(context.Background(), info.Digest, io.Discard); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("second wrong-key read err=%v", err)
	}
	_ = wrong.Close()
}

func TestReadHonorsCancellation(t *testing.T) {
	store, err := New(t.TempDir(), bytes.Repeat([]byte{0x44}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	info, err := store.Put(context.Background(), bytes.NewReader(bytes.Repeat([]byte("x"), 4*ChunkSize)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Read(ctx, info.Digest, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read err=%v", err)
	}
}
