package crypto

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func testFS(t *testing.T, base vfs.FS) (*EncryptedFS, string) {
	t.Helper()
	dir := t.TempDir()
	reg, err := OpenRegistry(dir, testProvider(), testDBID(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reg.Close)
	var dbID [16]byte
	copy(dbID[:], reg.dbID[:])
	efs, err := NewEncryptedFS(FSOptions{Base: base, Registry: reg, DBID: dbID})
	if err != nil {
		t.Fatal(err)
	}
	return efs, dir
}

func writeFileSync(t *testing.T, efs *EncryptedFS, name string, data []byte) {
	t.Helper()
	f, err := efs.Create(name, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func readFileAll(t *testing.T, efs *EncryptedFS, name string) []byte {
	t.Helper()
	f, err := efs.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, fi.Size())
	if _, err := io.ReadFull(sectionReaderAdapter{f}, out); err != nil {
		t.Fatal(err)
	}
	return out
}

type sectionReaderAdapter struct{ f vfs.File }

func (s sectionReaderAdapter) Read(p []byte) (int, error) { return s.f.Read(p) }

func TestEncryptedFileRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, 100, 65536, 200000} {
		efs, dir := testFS(t, vfs.Default)
		name := filepath.Join(dir, "f")
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		writeFileSync(t, efs, name, data)
		got := readFileAll(t, efs, name)
		if !bytes.Equal(got, data) {
			t.Fatalf("size %d: mismatch", size)
		}
		// No plaintext on disk (for sizes big enough to matter).
		if size >= 100 {
			raw, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(raw, data[:min(64, len(data))]) && size > 64 {
				t.Fatalf("size %d: plaintext found on disk", size)
			}
			_ = raw
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestEncryptedFileAllAlgorithms(t *testing.T) {
	algs := []AlgorithmID{
		AlgorithmAES128GCM, AlgorithmAES192GCM, AlgorithmAES256GCM,
		AlgorithmChaCha20Poly1305, AlgorithmXChaCha20Poly1305,
		AlgorithmAEGIS128L, AlgorithmAEGIS256,
	}
	for _, alg := range algs {
		t.Run(alg.String(), func(t *testing.T) {
			efs, dir := testFS(t, vfs.Default)
			ctx := bgCtx()
			if err := efs.Registry().SetDefaultAlgorithm(ctx, alg); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(dir, "f")
			data := make([]byte, 150000)
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
			writeFileSync(t, efs, name, data)
			// Overwrite the middle via OpenReadWrite + Sync.
			f, err := efs.OpenReadWrite(name, "")
			if err != nil {
				t.Fatal(err)
			}
			patch := bytesRepeat(0xcc, 70000)
			if _, err := f.WriteAt(patch, 10000); err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			f.Close()
			copy(data[10000:], patch)
			if got := readFileAll(t, efs, name); !bytes.Equal(got, data) {
				t.Fatal("overwrite mismatch")
			}
			if err := efs.verifyImage(name); err != nil {
				t.Fatalf("verify: %v", err)
			}
		})
	}
}

func TestEncryptedFileMemFS(t *testing.T) {
	base := vfs.NewMem()
	efs, _ := testFS(t, base)
	writeFileSync(t, efs, "f", bytesRepeat(0xab, 100000))
	if got := readFileAll(t, efs, "f"); !bytes.Equal(got, bytesRepeat(0xab, 100000)) {
		t.Fatal("memfs mismatch")
	}
	fi, err := efs.Stat("f")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 100000 {
		t.Fatalf("stat size %d", fi.Size())
	}
}

func TestEncryptedFileTamper(t *testing.T) {
	efs, dir := testFS(t, vfs.Default)
	name := filepath.Join(dir, "f")
	data := make([]byte, 200000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	writeFileSync(t, efs, name, data)
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	// Flip bytes across every region: both header slots, first record
	// envelope, record bodies, checkpoint area, tail.
	positions := []int{0, 100, 255, 256, 300, 511, 512, 600, 1000, len(raw) / 2, len(raw) - 100, len(raw) - 1}
	for _, pos := range positions {
		bad := append([]byte(nil), raw...)
		bad[pos] ^= 0x01
		if err := os.WriteFile(name, bad, 0o600); err != nil {
			t.Fatal(err)
		}
		f, err := efs.Open(name)
		if err != nil {
			if !errors.Is(err, ErrAuth) && !errors.Is(err, ErrCorrupt) {
				t.Fatalf("pos %d: wrong error %v", pos, err)
			}
			continue
		}
		// Open may succeed when the flip lands in an unread region; a full
		// read must catch it.
		_, rerr := io.ReadAll(sectionReaderAdapter{f})
		f.Close()
		if rerr == nil {
			// The flip may have landed in padding between records... there
			// is no padding; every byte is covered. Unless it hit the
			// INACTIVE header slot (valid stale slot is fine to ignore?
			// No: any flip must fail). Check which slot is active.
			t.Fatalf("pos %d: tamper undetected", pos)
		}
		if !errors.Is(rerr, ErrAuth) && !errors.Is(rerr, ErrCorrupt) && rerr != io.EOF {
			t.Fatalf("pos %d: wrong read error %v", pos, rerr)
		}
	}
}

func TestEncryptedFileTruncate(t *testing.T) {
	efs, dir := testFS(t, vfs.Default)
	name := filepath.Join(dir, "f")
	data := bytesRepeat(0x5a, 100000)
	writeFileSync(t, efs, name, data)
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	// Nonzero truncation is rejected.
	for _, n := range []int{1, 100, 511, 512, 1000, len(raw) - 1} {
		if err := os.WriteFile(name, raw[:n], 0o600); err != nil {
			t.Fatal(err)
		}
		f, err := efs.Open(name)
		if err == nil {
			_, rerr := io.ReadAll(sectionReaderAdapter{f})
			f.Close()
			err = rerr
		}
		if !errors.Is(err, ErrAuth) && !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncate %d: expected auth/corrupt, got %v", n, err)
		}
	}
	// Zero-byte files open as empty (crash-safe creation window).
	if err := os.WriteFile(name, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := efs.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := f.Stat()
	if fi.Size() != 0 {
		t.Fatalf("size %d", fi.Size())
	}
	f.Close()
}

func TestEncryptedFileSplice(t *testing.T) {
	efs, dir := testFS(t, vfs.Default)
	a := filepath.Join(dir, "a")
	b := filepath.Join(dir, "b")
	writeFileSync(t, efs, a, bytesRepeat(0x11, 100000))
	writeFileSync(t, efs, b, bytesRepeat(0x22, 100000))
	rawA, _ := os.ReadFile(a)
	rawB, _ := os.ReadFile(b)
	// Splice B's first record bytes into A past the headers.
	bad := append([]byte(nil), rawA...)
	copy(bad[512:1024], rawB[512:1024])
	if err := os.WriteFile(a, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := efs.Open(a)
	if err == nil {
		_, rerr := io.ReadAll(sectionReaderAdapter{f})
		f.Close()
		err = rerr
	}
	if !errors.Is(err, ErrAuth) && !errors.Is(err, ErrCorrupt) {
		t.Fatalf("splice undetected: %v", err)
	}
}

func TestEncryptedFileRollback(t *testing.T) {
	efs, dir := testFS(t, vfs.Default)
	name := filepath.Join(dir, "f")
	// One chunk, committed twice with different content: the stale first
	// record and the committed second record have equal length.
	writeFileSync(t, efs, name, bytesRepeat(0x01, 1000))
	f, err := efs.OpenReadWrite(name, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytesRepeat(0x02, 1000), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	recs := scanRecords(t, raw)
	var datas [][2]int // (offset, length) of data records
	for _, r := range recs {
		if r.typ == recordTypeData {
			datas = append(datas, [2]int{r.off, r.length})
		}
	}
	if len(datas) != 2 || datas[0][1] != datas[1][1] {
		t.Fatalf("layout: %+v", datas)
	}
	// Replay the older valid record over the committed one. Both seals are
	// genuine; the checkpoint-committed recordSeq must reject the replay.
	bad := append([]byte(nil), raw...)
	copy(bad[datas[1][0]:], raw[datas[0][0]:datas[0][0]+datas[0][1]])
	if err := os.WriteFile(name, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	rf, err := efs.Open(name)
	if err == nil {
		_, rerr := io.ReadAll(sectionReaderAdapter{rf})
		rf.Close()
		err = rerr
	}
	if !errors.Is(err, ErrAuth) && !errors.Is(err, ErrCorrupt) {
		t.Fatalf("record replay undetected: %v", err)
	}
}

// testRecord is a parsed (not verified) record layout.
type testRecord struct {
	typ    byte
	off    int
	length int
}

// scanRecords parses record envelopes to locate record boundaries.
func scanRecords(t *testing.T, raw []byte) []testRecord {
	t.Helper()
	var out []testRecord
	off := headerTotalLen
	for off < len(raw) {
		if off+2 > len(raw) {
			t.Fatalf("truncated at %d", off)
		}
		typ := raw[off]
		var fixed int
		switch typ {
		case recordTypeData:
			fixed = dataFixedLen
		case recordTypeCheckpoint:
			fixed = ckptFixedLen
		default:
			t.Fatalf("type %d at %d", typ, off)
		}
		nonceLen := int(raw[off+1+fixed])
		sealedOff := off + 1 + fixed + 1 + nonceLen
		sealedLen := int(binaryLittleUint32(raw[sealedOff:]))
		length := 1 + fixed + 1 + nonceLen + 4 + sealedLen
		out = append(out, testRecord{typ: typ, off: off, length: length})
		off += length
	}
	return out
}

func binaryLittleUint32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func TestEncryptedFileRemoveKeepsKeyUntilRetired(t *testing.T) {
	efs, dir := testFS(t, vfs.Default)
	m, err := NewManager(ManagerOptions{FS: efs, Roots: []string{dir}})
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "f")
	writeFileSync(t, efs, name, bytesRepeat(0x9c, 1000))
	if n := efs.Registry().KeyCount(); n != 1 {
		t.Fatalf("keys %d", n)
	}
	old := keyIDs(t, efs, name)[0]
	if _, err := m.RotateDataKey(bgCtx()); err != nil {
		t.Fatal(err)
	}
	if err := efs.Remove(name); err != nil {
		t.Fatal(err)
	}
	// Removal alone never retires keys (checkpoints may pin them).
	if n := efs.Registry().KeyCount(); n != 2 {
		t.Fatalf("keys %d after remove", n)
	}
	// Nothing references the old key: it retires; the active key stays.
	if err := m.RetireUnreferencedKeys(bgCtx()); err != nil {
		t.Fatal(err)
	}
	if n := efs.Registry().KeyCount(); n != 1 {
		t.Fatalf("keys %d after retire", n)
	}
	var oldID [KeyIDLen]byte
	copy(oldID[:], old)
	if _, _, err := efs.Registry().GetKey(bgCtx(), oldID); err == nil {
		t.Fatal("expected old key to retire")
	}
}

func TestEncryptedFileDoubleWriteOpen(t *testing.T) {
	efs, dir := testFS(t, vfs.Default)
	name := filepath.Join(dir, "f")
	f, err := efs.Create(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := efs.OpenReadWrite(name, ""); err == nil {
		t.Fatal("expected double-write-open error")
	}
}

func TestEncryptedFileStat(t *testing.T) {
	efs, dir := testFS(t, vfs.Default)
	name := filepath.Join(dir, "f")
	writeFileSync(t, efs, name, bytesRepeat(0xdd, 5000))
	fi, err := efs.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 5000 {
		t.Fatalf("stat size %d", fi.Size())
	}
	// Non-content files fall back to the base stat.
	regPath := filepath.Join(dir, "KEYREGISTRY")
	rfi, err := efs.Stat(regPath)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(regPath)
	if rfi.Size() != int64(len(raw)) {
		t.Fatal("registry stat should be physical")
	}
}
