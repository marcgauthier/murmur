package crypto

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testDBID(t *testing.T) [16]byte {
	t.Helper()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	return id
}

func testProvider() *MapProvider {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i + 1)
	}
	return &MapProvider{Keys: map[string][]byte{"s1": k}, CurrentID: "s1"}
}

func TestRegistryRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbID := testDBID(t)
	r, err := OpenRegistry(dir, testProvider(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	// Fresh registries start at generation 1 with one active key.
	active, err := r.ActiveKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(active.Key) != 32 || r.Generation() != 1 || r.KeyCount() != 1 {
		t.Fatalf("fresh state: %+v", active)
	}
	gen, id, err := r.NewGeneration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gen != 2 {
		t.Fatalf("gen %d", gen)
	}
	got, alg, err := r.GetKey(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 32 || alg != DefaultAlgorithm {
		t.Fatal("key mismatch")
	}
	if active.ID == id {
		t.Fatal("generation did not mint a new key")
	}
	// Old keys serve reads until retired.
	if _, _, err := r.GetKey(ctx, active.ID); err != nil {
		t.Fatalf("old key unreadable: %v", err)
	}
	if err := r.RemoveKey(ctx, active.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.GetKey(ctx, active.ID); err == nil {
		t.Fatal("expected unknown-key error")
	}
	r.Close()

	// Reopen: generation 2 and its key survive.
	r2, err := OpenRegistry(dir, testProvider(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.Generation() != 2 {
		t.Fatalf("gen %d", r2.Generation())
	}
	got2, _, err := r2.GetKey(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != string(got) {
		t.Fatal("key did not survive reopen")
	}
}

func TestRegistryWrongKey(t *testing.T) {
	dir := t.TempDir()
	dbID := testDBID(t)
	r, err := OpenRegistry(dir, testProvider(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()

	wrong := &MapProvider{Keys: map[string][]byte{"s1": bytesRepeat(0xee, 32)}, CurrentID: "s1"}
	if _, err := OpenRegistry(dir, wrong, dbID); !errors.Is(err, ErrAuth) {
		t.Fatalf("expected auth error, got %v", err)
	}
}

func TestRegistryDBIDMismatch(t *testing.T) {
	dir := t.TempDir()
	r, err := OpenRegistry(dir, testProvider(), testDBID(t))
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if _, err := OpenRegistry(dir, testProvider(), testDBID(t)); !errors.Is(err, ErrAuth) {
		t.Fatalf("expected auth error on dbid mismatch, got %v", err)
	}
}

func TestRegistryTamper(t *testing.T) {
	dir := t.TempDir()
	dbID := testDBID(t)
	r, err := OpenRegistry(dir, testProvider(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()

	raw, err := os.ReadFile(filepath.Join(dir, RegistryFileName))
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte in every region: magic, version, key id, nonce, sealed
	// length, sealed payload head/middle/tail.
	positions := map[string]int{
		"magic":      0,
		"version":    4,
		"keyid":      10,
		"nonce":      11,
		"sealedlen":  10 + 12,
		"sealedhead": 10 + 12 + 4,
		"sealedmid":  len(raw) / 2,
		"sealedtail": len(raw) - 1,
	}
	for name, pos := range positions {
		bad := append([]byte(nil), raw...)
		bad[pos] ^= 0x01
		if err := os.WriteFile(filepath.Join(dir, RegistryFileName), bad, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err = OpenRegistry(dir, testProvider(), dbID)
		if !errors.Is(err, ErrAuth) && !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: expected auth/corrupt error, got %v", name, err)
		}
	}
	// Truncation is rejected too.
	for _, n := range []int{0, 3, 10, len(raw) - 1} {
		if err := os.WriteFile(filepath.Join(dir, RegistryFileName), raw[:n], 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenRegistry(dir, testProvider(), dbID); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncate %d: expected corrupt error, got %v", n, err)
		}
	}
}

func TestRegistryRewrap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbID := testDBID(t)
	p := testProvider()
	r, err := OpenRegistry(dir, p, dbID)
	if err != nil {
		t.Fatal(err)
	}
	active, err := r.ActiveKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Rotate the provider to a new storage key.
	p.Keys["s2"] = bytesRepeat(0x7a, 32)
	p.CurrentID = "s2"
	newID, err := r.Rewrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if newID != "s2" {
		t.Fatalf("storage id %q", newID)
	}
	r.Close()

	// Old key alone cannot open; new key can, with data keys intact.
	oldOnly := &MapProvider{Keys: map[string][]byte{"s1": p.Keys["s1"]}, CurrentID: "s1"}
	if _, err := OpenRegistry(dir, oldOnly, dbID); err == nil {
		t.Fatal("expected failure with old key only")
	}
	r2, err := OpenRegistry(dir, p, dbID)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	got, _, err := r2.GetKey(ctx, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(active.Key) {
		t.Fatal("data key changed across rewrap")
	}
	// No staging artifacts remain.
	for _, name := range []string{registryIntent, registryBakName} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s remains: %v", name, err)
		}
	}
}

func TestRegistryRewrapWith(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbID := testDBID(t)
	r, err := OpenRegistry(dir, testProvider(), dbID)
	if err != nil {
		t.Fatal(err)
	}
	active, err := r.ActiveKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Direct-material rotation (no provider change).
	mat := KeyMaterial{ID: "direct-v2", Key: bytesRepeat(0x3c, 32)}
	if _, err := r.RewrapWith(ctx, mat); err != nil {
		t.Fatal(err)
	}
	r.Close()
	p2 := &MapProvider{Keys: map[string][]byte{"direct-v2": bytesRepeat(0x3c, 32)}, CurrentID: "direct-v2"}
	r2, err := OpenRegistry(dir, p2, dbID)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if got, _, err := r2.GetKey(ctx, active.ID); err != nil || string(got) != string(active.Key) {
		t.Fatalf("rewrap-with broke data keys: %v", err)
	}
}

// stageIntent writes a valid rotation intent + backup by hand to simulate a
// crash mid-rotation.
func stageIntent(t *testing.T, r *Registry, dir string, newID string, newKey []byte) {
	t.Helper()
	ctx := context.Background()
	r.mu.Lock()
	defer r.mu.Unlock()
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer zeroMaterial(material)
	r.storageKeyID = newID
	r.seq++
	intent, err := r.sealLocked(newKey, material)
	r.seq--
	r.storageKeyID = "s1"
	if err != nil {
		t.Fatal(err)
	}
	cur, err := os.ReadFile(filepath.Join(dir, RegistryFileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, registryBakName), cur, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, registryIntent), intent, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryIntentRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbID := testDBID(t)
	p := testProvider()
	r, err := OpenRegistry(dir, p, dbID)
	if err != nil {
		t.Fatal(err)
	}
	active, err := r.ActiveKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.Keys["s2"] = bytesRepeat(0x7a, 32)
	stageIntent(t, r, dir, "s2", p.Keys["s2"])
	r.Close()

	// Next open completes the rotation forward under the new key.
	p.CurrentID = "s2"
	r2, err := OpenRegistry(dir, p, dbID)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if r2.StorageKeyID() != "s2" {
		t.Fatalf("storage id %q", r2.StorageKeyID())
	}
	got, _, err := r2.GetKey(ctx, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(active.Key) {
		t.Fatal("data key changed across intent recovery")
	}
}

func TestRegistryIntentTamper(t *testing.T) {
	dir := t.TempDir()
	dbID := testDBID(t)
	p := testProvider()
	r, err := OpenRegistry(dir, p, dbID)
	if err != nil {
		t.Fatal(err)
	}
	p.Keys["s2"] = bytesRepeat(0x7a, 32)
	stageIntent(t, r, dir, "s2", p.Keys["s2"])
	// Flip one byte in the staged intent.
	intent, err := os.ReadFile(filepath.Join(dir, registryIntent))
	if err != nil {
		t.Fatal(err)
	}
	intent[len(intent)-1] ^= 0x01
	if err := os.WriteFile(filepath.Join(dir, registryIntent), intent, 0o600); err != nil {
		t.Fatal(err)
	}
	r.Close()

	p.CurrentID = "s2"
	if _, err := OpenRegistry(dir, p, dbID); !errors.Is(err, ErrAuth) {
		t.Fatalf("expected auth error on tampered intent, got %v", err)
	}
}

func TestRegistrySnapshotTamper(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbID := testDBID(t)
	p := testProvider()
	r, err := OpenRegistry(dir, p, dbID)
	if err != nil {
		t.Fatal(err)
	}
	p.Keys["s2"] = bytesRepeat(0x7a, 32)
	stageIntent(t, r, dir, "s2", p.Keys["s2"])
	cur, err := os.ReadFile(filepath.Join(dir, registryBakName))
	if err != nil {
		t.Fatal(err)
	}
	cur[len(cur)/2] ^= 0x01
	if err := os.WriteFile(filepath.Join(dir, registryBakName), cur, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.VerifyStagingArtifacts(ctx); !errors.Is(err, ErrAuth) {
		t.Fatalf("expected auth error on tampered snapshot, got %v", err)
	}
	r.Close()
	p.CurrentID = "s2"
	if _, err := OpenRegistry(dir, p, dbID); !errors.Is(err, ErrAuth) {
		t.Fatalf("expected auth error on open with tampered snapshot, got %v", err)
	}
}

func TestRegistryGenerations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	r, err := OpenRegistry(dir, testProvider(), testDBID(t))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if g := r.Generation(); g != 1 {
		t.Fatalf("gen %d", g)
	}
	g, _, err := r.NewGeneration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if g != 2 {
		t.Fatalf("gen %d", g)
	}
	if err := r.SetDefaultAlgorithm(ctx, AlgorithmAEGIS128L); err != nil {
		t.Fatal(err)
	}
	if r.DefaultAlgorithm() != AlgorithmAEGIS128L {
		t.Fatal("default algorithm not set")
	}
	// Expire keys created before now+1h: both generations expire.
	n, err := r.ExpireBefore(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expired %d", n)
	}
	for _, k := range r.Keys() {
		if k.State != KeyExpired {
			t.Fatal("state not expired")
		}
		if k.Key != nil {
			t.Fatal("metadata must not carry material")
		}
		if _, _, err := r.GetKey(ctx, k.ID); err != nil {
			t.Fatalf("expired key unreadable: %v", err)
		}
	}
}

func TestRegistryPins(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	r, err := OpenRegistry(dir, testProvider(), testDBID(t))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.Pin(ctx, "/tmp/ckpt-1", "checkpoint"); err != nil {
		t.Fatal(err)
	}
	if err := r.Pin(ctx, "/tmp/ckpt-1", "checkpoint"); err != nil {
		t.Fatal(err)
	}
	if pins := r.Pins(); len(pins) != 1 || pins[0].Kind != "checkpoint" {
		t.Fatalf("pins %+v", pins)
	}
	if err := r.Unpin(ctx, "/tmp/ckpt-1"); err != nil {
		t.Fatal(err)
	}
	if pins := r.Pins(); len(pins) != 0 {
		t.Fatalf("pins %+v", pins)
	}
}
