package crypto

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func testManager(t *testing.T, opt ManagerOptions) (*Manager, *EncryptedFS, string) {
	t.Helper()
	efs, dir := testFS(t, vfs.Default)
	opt.FS = efs
	if opt.Roots == nil {
		opt.Roots = []string{dir}
	}
	m, err := NewManager(opt)
	if err != nil {
		t.Fatal(err)
	}
	m.CleanupStaging()
	return m, efs, dir
}

func TestManagerRotate(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	m, _, _ := testManager(t, ManagerOptions{
		MinRotationInterval: time.Hour,
		Now:                 func() time.Time { return now },
	})
	g, err := m.RotateDataKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if g != 2 {
		t.Fatalf("gen %d", g)
	}
	if _, err := m.RotateDataKey(ctx); !errors.Is(err, ErrRotationTooSoon) {
		t.Fatalf("expected too-soon, got %v", err)
	}
	now = now.Add(2 * time.Hour)
	if g, err := m.RotateDataKey(ctx); err != nil || g != 3 {
		t.Fatalf("gen %d, err %v", g, err)
	}
	st := m.Status()
	if st.Generation != 3 || st.LastRotation.IsZero() {
		t.Fatalf("status %+v", st)
	}
}

func keyIDs(t *testing.T, efs *EncryptedFS, names ...string) []string {
	t.Helper()
	var out []string
	for _, n := range names {
		id, known, err := efs.peekKeyID(n)
		if err != nil || !known {
			t.Fatalf("%s: %v %v", n, known, err)
		}
		out = append(out, string(id[:]))
	}
	return out
}

func TestManagerRewrite(t *testing.T) {
	ctx := context.Background()
	m, efs, dir := testManager(t, ManagerOptions{})
	a := filepath.Join(dir, "a")
	b := filepath.Join(dir, "b")
	da, db := bytesRepeat(0x11, 90000), bytesRepeat(0x22, 3000)
	writeFileSync(t, efs, a, da)
	writeFileSync(t, efs, b, db)
	before := keyIDs(t, efs, a, b)
	if len(before) != 2 || before[0] != before[1] {
		t.Fatalf("new files must share the active key: %+v", before)
	}
	// Rotate, then rewrite: both files move to the new active key and the
	// old key retires (no other references).
	if _, err := m.RotateDataKey(ctx); err != nil {
		t.Fatal(err)
	}
	rewrote, skipped, err := m.RewriteEncryptedFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rewrote != 2 || skipped != 0 {
		t.Fatalf("rewrote %d skipped %d", rewrote, skipped)
	}
	if got := readFileAll(t, efs, a); !bytes.Equal(got, da) {
		t.Fatal("a changed")
	}
	if got := readFileAll(t, efs, b); !bytes.Equal(got, db) {
		t.Fatal("b changed")
	}
	after := keyIDs(t, efs, a, b)
	if after[0] == before[0] || after[1] == before[1] {
		t.Fatal("files not moved to the new key")
	}
	if efs.Registry().KeyCount() != 1 {
		t.Fatalf("registry holds %d keys", efs.Registry().KeyCount())
	}
	if err := efs.verifyImage(a); err != nil {
		t.Fatal(err)
	}
	st := m.Status()
	if st.LastRewriteFiles != 2 || st.LastRewrite.IsZero() {
		t.Fatalf("status %+v", st)
	}
	if m.HasJournal() {
		t.Fatal("journal remains")
	}
}

func TestManagerRewriteSkipsBusy(t *testing.T) {
	ctx := context.Background()
	m, efs, dir := testManager(t, ManagerOptions{})
	a := filepath.Join(dir, "a")
	writeFileSync(t, efs, a, bytesRepeat(0x44, 1000))
	w, err := efs.OpenReadWrite(a, "")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	rewrote, skipped, err := m.RewriteEncryptedFiles(ctx)
	if err == nil {
		t.Fatal("expected busy-file error")
	}
	if rewrote != 0 || skipped != 1 {
		t.Fatalf("rewrote %d skipped %d", rewrote, skipped)
	}
}

func TestManagerExpiryTick(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	m, efs, dir := testManager(t, ManagerOptions{
		MaxKeyLifetime:  time.Hour,
		MaxFilesPerTick: 10,
		Now:             func() time.Time { return now },
	})
	efs.Registry().now = func() time.Time { return now }
	old := filepath.Join(dir, "old")
	writeFileSync(t, efs, old, bytesRepeat(0x55, 5000))
	oldKey := keyIDs(t, efs, old)[0]
	// Age past the lifetime, rotate, then add a fresh file on the new key.
	now = now.Add(2 * time.Hour)
	if _, err := m.RotateDataKey(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := filepath.Join(dir, "fresh")
	writeFileSync(t, efs, fresh, bytesRepeat(0x66, 5000))
	freshKey := keyIDs(t, efs, fresh)[0]
	if freshKey == oldKey {
		t.Fatal("fresh file not on new key")
	}

	checked, rewrote, skipped, err := m.RunExpiryTick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if checked != 1 || rewrote != 1 || skipped != 0 {
		t.Fatalf("checked %d rewrote %d skipped %d", checked, rewrote, skipped)
	}
	// Only the expired-key file was touched.
	if got := keyIDs(t, efs, old)[0]; got == oldKey {
		t.Fatal("old file not re-encrypted")
	}
	if got := keyIDs(t, efs, fresh)[0]; got != freshKey {
		t.Fatal("fresh file touched")
	}
	if got := readFileAll(t, efs, old); !bytes.Equal(got, bytesRepeat(0x55, 5000)) {
		t.Fatal("old content changed")
	}
	st := m.Status()
	if st.FilesRewrote != 1 || st.WorkerErrors != 0 {
		t.Fatalf("status %+v", st)
	}
	if st.DataKeyExpiry.IsZero() || st.KeysActive != 1 {
		t.Fatalf("status %+v", st)
	}
}

func TestManagerExpiryRotationOnCreate(t *testing.T) {
	now := time.Now()
	m, efs, dir := testManager(t, ManagerOptions{
		MaxKeyLifetime: time.Hour,
		Now:            func() time.Time { return now },
	})
	efs.Registry().now = func() time.Time { return now }
	efs.SetRotationCheck(m.MaybeRotateOnExpiry)
	writeFileSync(t, efs, filepath.Join(dir, "a"), bytesRepeat(0x11, 100))
	gen1 := m.reg.Generation()
	// Expire the active key; the next create rotates first.
	now = now.Add(2 * time.Hour)
	writeFileSync(t, efs, filepath.Join(dir, "b"), bytesRepeat(0x22, 100))
	if gen := m.reg.Generation(); gen != gen1+1 {
		t.Fatalf("gen %d, want %d", gen, gen1+1)
	}
	ids := keyIDs(t, efs, filepath.Join(dir, "a"), filepath.Join(dir, "b"))
	if ids[0] == ids[1] {
		t.Fatal("b not on the rotated key")
	}
}

func TestManagerRetireKeepsPinned(t *testing.T) {
	ctx := context.Background()
	m, efs, dir := testManager(t, ManagerOptions{})
	live := filepath.Join(dir, "live")
	writeFileSync(t, efs, live, bytesRepeat(0x11, 1000))
	oldKey := keyIDs(t, efs, live)[0]
	// Checkpoint the file aside (simulated hardlink copy), rotate, rewrite.
	ckptDir := filepath.Join(dir, "ckpt")
	if err := os.MkdirAll(ckptDir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(live)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ckptDir, "live"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := efs.Registry().Pin(ctx, ckptDir, "checkpoint"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RotateDataKey(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.RewriteEncryptedFiles(ctx); err != nil {
		t.Fatal(err)
	}
	// The old key survives: the checkpoint pins it.
	if _, _, err := efs.Registry().GetKey(ctx, strToKeyID(oldKey)); err != nil {
		t.Fatalf("pinned key retired: %v", err)
	}
	inv, err := m.BuildInventory()
	if err != nil {
		t.Fatal(err)
	}
	if inv[oldKey] == nil || inv[oldKey].Checkpoints != 1 {
		t.Fatalf("inventory %+v", inv)
	}
	// Release the checkpoint: the key retires.
	if err := efs.Registry().Unpin(ctx, ckptDir); err != nil {
		t.Fatal(err)
	}
	if err := m.RetireUnreferencedKeys(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := efs.Registry().GetKey(ctx, strToKeyID(oldKey)); err == nil {
		t.Fatal("expected unpinned key to retire")
	}
}

func strToKeyID(s string) [KeyIDLen]byte {
	var id [KeyIDLen]byte
	copy(id[:], s)
	return id
}

func TestManagerResumeRewrite(t *testing.T) {
	ctx := context.Background()
	m, efs, dir := testManager(t, ManagerOptions{})
	a := filepath.Join(dir, "a")
	content := bytesRepeat(0x99, 5000)
	writeFileSync(t, efs, a, content)
	if _, err := m.RotateDataKey(ctx); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash before any file completed: pending journal plus an
	// orphan staging temp.
	j := &rewriteJournal{Version: 1, StartedAt: time.Now().UTC(), Files: []rewriteFileEntry{{Path: a, State: "pending"}}}
	if err := m.writeJournal(j); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(a+".rewrite-orphan", []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !m.HasJournal() {
		t.Fatal("journal missing")
	}
	if err := m.ResumeRewrite(ctx); err != nil {
		t.Fatal(err)
	}
	if m.HasJournal() {
		t.Fatal("journal remains after resume")
	}
	if got := readFileAll(t, efs, a); !bytes.Equal(got, content) {
		t.Fatal("content changed across resume")
	}
	if _, err := os.Stat(a + ".rewrite-orphan"); !os.IsNotExist(err) {
		t.Fatal("orphan temp remains")
	}
}

func TestManagerRewriteStagingTamper(t *testing.T) {
	m, efs, dir := testManager(t, ManagerOptions{})
	a := filepath.Join(dir, "a")
	writeFileSync(t, efs, a, bytesRepeat(0x77, 8000))
	// Build a valid rewrite staging image by hand, tamper with it, and
	// require verification to fail.
	tmp := a + ".rewrite-test"
	w, err := efs.Create(tmp, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytesRepeat(0x77, 8000)); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	raw, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), raw...)
	bad[len(bad)/2] ^= 0x01
	if err := os.WriteFile(tmp, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := efs.verifyImage(tmp); !errors.Is(err, ErrAuth) && !errors.Is(err, ErrCorrupt) {
		t.Fatalf("staging tamper undetected: %v", err)
	}
	_ = m
}
