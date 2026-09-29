package crypto

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Registry file layout:
//
//	magic[4]="NMKR" | version u16 | storageKeyID len u16 | storageKeyID
//	| nonce[12] | sealedLen u32 | sealed payload (AES-256-GCM)
//
// The wrap key (KEK) is HKDF(storage key, dbID, storageKeyID); the AAD covers
// the whole envelope, so any byte flip fails authentication. Payload:
//
//	seq u64 | dbID[16] | generation u64 | defaultAlg u16 | keyCount u32
//	per key: keyID[16] | alg u16 | generation u64 | createdAt u64
//	         | state u8 | keyLen u16 | key bytes
//	pinCount u32 | per pin: pathLen u16 | path | kindLen u16 | kind
//
// Each generation has exactly one data key; new files use the active
// generation's key. Key metadata is cached in memory, but key MATERIAL is
// always re-read from disk and unwrapped on demand, so unwrapped data keys
// exist only while referenced by files or operations. The KEK itself is
// cached (re-derived only when the storage key id changes) so KMS-style
// providers are not hit per file open.
const (
	registryMagic   = "NMKR"
	registryVersion = 1

	// RegistryFileName is the registry file name inside the keys directory.
	RegistryFileName = "KEYREGISTRY"
	registryBakName  = "KEYREGISTRY.bak"
	registryIntent   = "KEYREGISTRY.intent"
)

// KeyState tracks data-key lifecycle.
type KeyState uint8

const (
	// KeyActive is usable for new writes.
	KeyActive KeyState = 1
	// KeyExpired is retained for reads until its files are re-encrypted.
	KeyExpired KeyState = 2
)

// KeyIDLen is the length of random key ids.
const KeyIDLen = 16

// StoredKey is one registry entry. Key holds material only when the entry
// was returned by a material-bearing call (ActiveKey, GetKey).
type StoredKey struct {
	ID         [KeyIDLen]byte
	Alg        AlgorithmID
	Generation uint64
	CreatedAt  time.Time
	State      KeyState
	Key        []byte // 32 bytes, or nil for metadata-only entries
}

// Pin registers a checkpoint/backup directory whose file headers pin keys.
type Pin struct {
	Path string
	Kind string // "checkpoint" or "backup"
}

// Registry persists per-generation data keys wrapped by the storage key. All
// mutations are atomic (temp file + fsync + rename + dir fsync). It is safe
// for concurrent use.
type Registry struct {
	mu       sync.Mutex
	dir      string
	path     string
	provider KeyProvider
	dbID     [16]byte

	// Cached metadata (single-owner dir, always coherent under mu).
	seq          uint64
	storageKeyID string
	generation   uint64
	defaultAlg   AlgorithmID
	keys         map[string]*StoredKey // string(keyID) -> metadata (Key nil)
	pins         []Pin
	diskBytes    int64

	// Cached KEK (zeroed on Close/replace).
	kek   []byte
	kekID string

	// now injects time for key creation (tests).
	now func() time.Time
}

// OpenRegistry opens or creates the registry in dir. The provider must serve
// the storage key; dbID binds the registry to one database.
func OpenRegistry(dir string, provider KeyProvider, dbID [16]byte) (*Registry, error) {
	if provider == nil {
		return nil, fmt.Errorf("crypto: registry requires a storage-key provider")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("crypto: registry dir: %w", err)
	}
	r := &Registry{
		dir:        dir,
		path:       filepath.Join(dir, RegistryFileName),
		provider:   provider,
		dbID:       dbID,
		keys:       make(map[string]*StoredKey),
		defaultAlg: DefaultAlgorithm,
		now:        time.Now,
	}
	ctx := context.Background()
	// Complete or clean up any interrupted storage-key rotation first.
	if err := r.recoverStaging(ctx); err != nil {
		return nil, err
	}
	// Sweep crashed temp files.
	if matches, err := filepath.Glob(filepath.Join(dir, "KEYREGISTRY*.tmp-*")); err == nil {
		for _, p := range matches {
			_ = os.Remove(p)
		}
	}
	raw, err := os.ReadFile(r.path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("crypto: read registry: %w", err)
		}
		mat, err := provider.Current(ctx)
		if err != nil {
			return nil, fmt.Errorf("crypto: storage key: %w", err)
		}
		defer Zero(mat.Key)
		r.storageKeyID = mat.ID
		r.generation = 1
		kek, err := deriveKEK(mat.Key, r.dbID[:], []byte(mat.ID))
		if err != nil {
			return nil, err
		}
		r.kek, r.kekID = kek, mat.ID
		id, key, err := newDataKey()
		if err != nil {
			return nil, err
		}
		r.keys[string(id[:])] = &StoredKey{
			ID: id, Alg: DefaultAlgorithm, Generation: 1,
			CreatedAt: r.now().UTC(), State: KeyActive,
		}
		if err := r.persistLocked(mat.Key, map[string][]byte{string(id[:]): key}); err != nil {
			delete(r.keys, string(id[:]))
			Zero(key)
			return nil, err
		}
		Zero(key)
		return r, nil
	}
	st, err := os.Stat(r.path)
	if err == nil {
		r.diskBytes = st.Size()
	}
	mat, payload, err := r.openEnvelope(ctx, raw)
	if err != nil {
		return nil, err
	}
	defer Zero(mat.Key)
	if err := r.decodePayload(payload, dbID[:]); err != nil {
		return nil, err
	}
	r.storageKeyID = mat.ID
	kek, err := deriveKEK(mat.Key, r.dbID[:], []byte(mat.ID))
	if err != nil {
		return nil, err
	}
	r.kek, r.kekID = kek, mat.ID
	return r, nil
}

func newDataKey() ([KeyIDLen]byte, []byte, error) {
	var id [KeyIDLen]byte
	if _, err := rand.Read(id[:]); err != nil {
		return id, nil, fmt.Errorf("crypto: rand: %w", err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return id, nil, fmt.Errorf("crypto: rand: %w", err)
	}
	return id, key, nil
}

// recoverStaging completes an interrupted Rewrap or removes stale artifacts.
func (r *Registry) recoverStaging(ctx context.Context) error {
	intentPath := filepath.Join(r.dir, registryIntent)
	intentRaw, err := os.ReadFile(intentPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("crypto: read rotation intent: %w", err)
	}
	if err == nil {
		// An intent exists: verify and complete it (forward recovery).
		mat, _, err := r.openEnvelope(ctx, intentRaw)
		if err != nil {
			return fmt.Errorf("crypto: rotation intent invalid, refusing to open: %w", err)
		}
		Zero(mat.Key)
		if err := r.verifyBackup(ctx); err != nil {
			return err
		}
		if err := durableRename(intentPath, r.path); err != nil {
			return fmt.Errorf("crypto: complete rotation: %w", err)
		}
		_ = os.Remove(filepath.Join(r.dir, registryBakName))
		return nil
	}
	// No intent: a leftover backup is stale garbage from a completed
	// rotation; verify opportunistically, then remove.
	if err := r.verifyBackup(ctx); err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(r.dir, registryBakName))
	return nil
}

// verifyBackup fails closed when a pre-rotation snapshot is present but
// tampered. A missing backup, or one wrapped by an unavailable historical
// key, is not an error here (the registry itself is authoritative).
func (r *Registry) verifyBackup(ctx context.Context) error {
	bakPath := filepath.Join(r.dir, registryBakName)
	raw, err := os.ReadFile(bakPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("crypto: read rotation snapshot: %w", err)
	}
	_, _, err = r.openEnvelope(ctx, raw)
	if err != nil {
		if isKeyUnavailable(err) {
			return nil
		}
		return fmt.Errorf("crypto: rotation snapshot invalid: %w", err)
	}
	return nil
}

// VerifyStagingArtifacts verifies rotation intent/snapshot seals without
// mutating anything. Used by recovery paths and tamper tests.
func (r *Registry) VerifyStagingArtifacts(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	intentPath := filepath.Join(r.dir, registryIntent)
	if raw, err := os.ReadFile(intentPath); err == nil {
		mat, _, err := r.openEnvelope(ctx, raw)
		if err != nil {
			return fmt.Errorf("crypto: rotation intent invalid: %w", err)
		}
		Zero(mat.Key)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("crypto: read rotation intent: %w", err)
	}
	return r.verifyBackup(ctx)
}

// openEnvelope parses and authenticates one registry blob.
func (r *Registry) openEnvelope(ctx context.Context, raw []byte) (KeyMaterial, []byte, error) {
	off := 0
	need := func(n int) ([]byte, error) {
		if len(raw)-off < n {
			return nil, fmt.Errorf("%w: registry truncated", ErrCorrupt)
		}
		b := raw[off : off+n]
		off += n
		return b, nil
	}
	magic, err := need(4)
	if err != nil {
		return KeyMaterial{}, nil, err
	}
	if string(magic) != registryMagic {
		return KeyMaterial{}, nil, fmt.Errorf("%w: bad registry magic", ErrCorrupt)
	}
	ver, err := need(2)
	if err != nil {
		return KeyMaterial{}, nil, err
	}
	if binary.LittleEndian.Uint16(ver) != registryVersion {
		return KeyMaterial{}, nil, fmt.Errorf("%w: registry version %d", ErrCorrupt, binary.LittleEndian.Uint16(ver))
	}
	idLenRaw, err := need(2)
	if err != nil {
		return KeyMaterial{}, nil, err
	}
	idLen := int(binary.LittleEndian.Uint16(idLenRaw))
	if idLen > 256 {
		return KeyMaterial{}, nil, fmt.Errorf("%w: registry key id too long", ErrCorrupt)
	}
	idRaw, err := need(idLen)
	if err != nil {
		return KeyMaterial{}, nil, err
	}
	nonce, err := need(12)
	if err != nil {
		return KeyMaterial{}, nil, err
	}
	sealedLenRaw, err := need(4)
	if err != nil {
		return KeyMaterial{}, nil, err
	}
	sealedLen := int(binary.LittleEndian.Uint32(sealedLenRaw))
	sealed, err := need(sealedLen)
	if err != nil {
		return KeyMaterial{}, nil, err
	}
	if off != len(raw) {
		return KeyMaterial{}, nil, fmt.Errorf("%w: registry trailing bytes", ErrCorrupt)
	}
	mat, err := r.provider.Lookup(ctx, string(idRaw))
	if err != nil {
		return KeyMaterial{}, nil, fmt.Errorf("crypto: storage key %q unavailable: %w", string(idRaw), err)
	}
	kek, err := deriveKEK(mat.Key, r.dbID[:], idRaw)
	if err != nil {
		Zero(mat.Key)
		return KeyMaterial{}, nil, err
	}
	defer Zero(kek)
	aead, err := AlgorithmAES256GCM.NewAEAD(kek)
	if err != nil {
		Zero(mat.Key)
		return KeyMaterial{}, nil, err
	}
	// AAD is the envelope exactly as sealed: everything before sealedLen.
	aad := raw[:off-sealedLen-4]
	payload, err := aead.Open(nil, nonce, sealed, aad)
	if err != nil {
		Zero(mat.Key)
		return KeyMaterial{}, nil, fmt.Errorf("%w: registry seal: %v", ErrAuth, err)
	}
	return mat, payload, nil
}

func isKeyUnavailable(err error) bool {
	return err != nil && bytes.Contains([]byte(err.Error()), []byte("unavailable"))
}

// decodePayload parses a verified payload into the metadata cache (no key
// material is retained).
func (r *Registry) decodePayload(payload, dbID []byte) error {
	off := 0
	need := func(n int) ([]byte, error) {
		if len(payload)-off < n {
			return nil, fmt.Errorf("%w: registry payload truncated", ErrCorrupt)
		}
		b := payload[off : off+n]
		off += n
		return b, nil
	}
	seqRaw, err := need(8)
	if err != nil {
		return err
	}
	r.seq = binary.LittleEndian.Uint64(seqRaw)
	idRaw, err := need(16)
	if err != nil {
		return err
	}
	if !bytes.Equal(idRaw, dbID) {
		return fmt.Errorf("%w: registry belongs to another database", ErrAuth)
	}
	genRaw, err := need(8)
	if err != nil {
		return err
	}
	r.generation = binary.LittleEndian.Uint64(genRaw)
	algRaw, err := need(2)
	if err != nil {
		return err
	}
	r.defaultAlg = AlgorithmID(binary.LittleEndian.Uint16(algRaw))
	if _, err := r.defaultAlg.KeySize(); err != nil {
		return fmt.Errorf("%w: registry default algorithm: %v", ErrCorrupt, err)
	}
	countRaw, err := need(4)
	if err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(countRaw)
	if count > 10_000_000 {
		return fmt.Errorf("%w: registry key count absurd", ErrCorrupt)
	}
	keys := make(map[string]*StoredKey, count)
	for i := uint32(0); i < count; i++ {
		id, err := need(KeyIDLen)
		if err != nil {
			return err
		}
		alg, err := need(2)
		if err != nil {
			return err
		}
		gen, err := need(8)
		if err != nil {
			return err
		}
		ts, err := need(8)
		if err != nil {
			return err
		}
		st, err := need(1)
		if err != nil {
			return err
		}
		kl, err := need(2)
		if err != nil {
			return err
		}
		keyLen := int(binary.LittleEndian.Uint16(kl))
		if keyLen != 32 {
			return fmt.Errorf("%w: registry key length %d", ErrCorrupt, keyLen)
		}
		if _, err := need(keyLen); err != nil {
			return err
		}
		aid := AlgorithmID(binary.LittleEndian.Uint16(alg))
		if _, err := aid.KeySize(); err != nil {
			return fmt.Errorf("%w: registry key algorithm: %v", ErrCorrupt, err)
		}
		var kid [KeyIDLen]byte
		copy(kid[:], id)
		keys[string(kid[:])] = &StoredKey{
			ID:         kid,
			Alg:        aid,
			Generation: binary.LittleEndian.Uint64(gen),
			CreatedAt:  time.Unix(int64(binary.LittleEndian.Uint64(ts)), 0).UTC(),
			State:      KeyState(st[0]),
		}
	}
	// Pins (absent in first-generation payloads).
	var pins []Pin
	if off < len(payload) {
		pinCountRaw, err := need(4)
		if err != nil {
			return err
		}
		pinCount := binary.LittleEndian.Uint32(pinCountRaw)
		if pinCount > 1_000_000 {
			return fmt.Errorf("%w: registry pin count absurd", ErrCorrupt)
		}
		for i := uint32(0); i < pinCount; i++ {
			plRaw, err := need(2)
			if err != nil {
				return err
			}
			pl := int(binary.LittleEndian.Uint16(plRaw))
			if pl > 4096 {
				return fmt.Errorf("%w: pin path too long", ErrCorrupt)
			}
			pb, err := need(pl)
			if err != nil {
				return err
			}
			klRaw, err := need(2)
			if err != nil {
				return err
			}
			kl := int(binary.LittleEndian.Uint16(klRaw))
			if kl > 64 {
				return fmt.Errorf("%w: pin kind too long", ErrCorrupt)
			}
			kb, err := need(kl)
			if err != nil {
				return err
			}
			pins = append(pins, Pin{Path: string(bytes.Clone(pb)), Kind: string(bytes.Clone(kb))})
		}
	}
	if off != len(payload) {
		return fmt.Errorf("%w: registry payload trailing bytes", ErrCorrupt)
	}
	r.keys = keys
	r.pins = pins
	return nil
}

// encodePayload renders the registry payload at the current seq. material
// supplies the sealed key bytes (never cached).
func (r *Registry) encodePayload(material map[string][]byte) ([]byte, error) {
	var payload bytes.Buffer
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], r.seq)
	payload.Write(tmp[:])
	payload.Write(r.dbID[:])
	binary.LittleEndian.PutUint64(tmp[:], r.generation)
	payload.Write(tmp[:])
	var tmp2 [2]byte
	binary.LittleEndian.PutUint16(tmp2[:], uint16(r.defaultAlg))
	payload.Write(tmp2[:])
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(r.keys)))
	payload.Write(tmp4[:])
	for _, k := range r.keys {
		mat, ok := material[string(k.ID[:])]
		if !ok || len(mat) != 32 {
			return nil, fmt.Errorf("crypto: missing material for key %x", k.ID[:4])
		}
		payload.Write(k.ID[:])
		binary.LittleEndian.PutUint16(tmp2[:], uint16(k.Alg))
		payload.Write(tmp2[:])
		binary.LittleEndian.PutUint64(tmp[:], k.Generation)
		payload.Write(tmp[:])
		binary.LittleEndian.PutUint64(tmp[:], uint64(k.CreatedAt.Unix()))
		payload.Write(tmp[:])
		payload.Write([]byte{byte(k.State)})
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(mat)))
		payload.Write(tmp2[:])
		payload.Write(mat)
	}
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(r.pins)))
	payload.Write(tmp4[:])
	for _, p := range r.pins {
		if len(p.Path) > 4096 || len(p.Kind) > 64 {
			return nil, fmt.Errorf("crypto: pin too long")
		}
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(p.Path)))
		payload.Write(tmp2[:])
		payload.WriteString(p.Path)
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(p.Kind)))
		payload.Write(tmp2[:])
		payload.WriteString(p.Kind)
	}
	return payload.Bytes(), nil
}

// sealLocked renders a full registry blob at the current seq.
func (r *Registry) sealLocked(storageKey []byte, material map[string][]byte) ([]byte, error) {
	if len(r.storageKeyID) > 256 {
		return nil, fmt.Errorf("crypto: storage key id too long")
	}
	payload, err := r.encodePayload(material)
	if err != nil {
		return nil, err
	}
	kek, err := deriveKEK(storageKey, r.dbID[:], []byte(r.storageKeyID))
	if err != nil {
		return nil, err
	}
	defer Zero(kek)
	aead, err := AlgorithmAES256GCM.NewAEAD(kek)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("crypto: rand: %w", err)
	}
	var env bytes.Buffer
	var tmp2 [2]byte
	env.WriteString(registryMagic)
	binary.LittleEndian.PutUint16(tmp2[:], registryVersion)
	env.Write(tmp2[:])
	binary.LittleEndian.PutUint16(tmp2[:], uint16(len(r.storageKeyID)))
	env.Write(tmp2[:])
	env.WriteString(r.storageKeyID)
	env.Write(nonce)
	sealed := aead.Seal(nil, nonce, payload, env.Bytes())
	Zero(payload)
	var out bytes.Buffer
	out.Write(env.Bytes())
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(sealed)))
	out.Write(tmp4[:])
	out.Write(sealed)
	return out.Bytes(), nil
}

// persistLocked seals and atomically installs the registry. Callers must
// hold r.mu and pass the storage key (never stored) plus the full key
// material map.
func (r *Registry) persistLocked(storageKey []byte, material map[string][]byte) error {
	r.seq++
	sealed, err := r.sealLocked(storageKey, material)
	if err != nil {
		r.seq--
		return err
	}
	if err := atomicWriteFile(r.path, sealed); err != nil {
		return err
	}
	r.diskBytes = int64(len(sealed))
	return nil
}

// loadMaterialLocked re-reads the registry file, verifies its seal with the
// cached KEK (re-resolving the storage key when the id changed), and returns
// all key material. Callers hold r.mu.
func (r *Registry) loadMaterialLocked(ctx context.Context) (map[string][]byte, error) {
	raw, err := os.ReadFile(r.path)
	if err != nil {
		return nil, fmt.Errorf("crypto: read registry: %w", err)
	}
	storageID, payload, err := r.openPayloadWithCache(ctx, raw)
	if err != nil {
		return nil, err
	}
	_ = storageID
	return parseMaterial(payload)
}

// openPayloadWithCache verifies a registry blob using the cached KEK when
// its storage id matches, else resolves the key via the provider.
func (r *Registry) openPayloadWithCache(ctx context.Context, raw []byte) (string, []byte, error) {
	off := 0
	need := func(n int) ([]byte, error) {
		if len(raw)-off < n {
			return nil, fmt.Errorf("%w: registry truncated", ErrCorrupt)
		}
		b := raw[off : off+n]
		off += n
		return b, nil
	}
	magic, err := need(4)
	if err != nil {
		return "", nil, err
	}
	if string(magic) != registryMagic {
		return "", nil, fmt.Errorf("%w: bad registry magic", ErrCorrupt)
	}
	ver, err := need(2)
	if err != nil {
		return "", nil, err
	}
	if binary.LittleEndian.Uint16(ver) != registryVersion {
		return "", nil, fmt.Errorf("%w: registry version", ErrCorrupt)
	}
	idLenRaw, err := need(2)
	if err != nil {
		return "", nil, err
	}
	idLen := int(binary.LittleEndian.Uint16(idLenRaw))
	if idLen > 256 {
		return "", nil, fmt.Errorf("%w: registry key id too long", ErrCorrupt)
	}
	idRaw, err := need(idLen)
	if err != nil {
		return "", nil, err
	}
	nonce, err := need(12)
	if err != nil {
		return "", nil, err
	}
	sealedLenRaw, err := need(4)
	if err != nil {
		return "", nil, err
	}
	sealedLen := int(binary.LittleEndian.Uint32(sealedLenRaw))
	sealed, err := need(sealedLen)
	if err != nil {
		return "", nil, err
	}
	if off != len(raw) {
		return "", nil, fmt.Errorf("%w: registry trailing bytes", ErrCorrupt)
	}
	kek := r.kek
	if r.kekID != string(idRaw) || len(kek) != 32 {
		mat, err := r.provider.Lookup(ctx, string(idRaw))
		if err != nil {
			return "", nil, fmt.Errorf("crypto: storage key %q unavailable: %w", string(idRaw), err)
		}
		defer Zero(mat.Key)
		fresh, err := deriveKEK(mat.Key, r.dbID[:], idRaw)
		if err != nil {
			return "", nil, err
		}
		Zero(r.kek)
		r.kek, r.kekID = fresh, string(idRaw)
		kek = fresh
	}
	aead, err := AlgorithmAES256GCM.NewAEAD(kek)
	if err != nil {
		return "", nil, err
	}
	payload, err := aead.Open(nil, nonce, sealed, raw[:off-sealedLen-4])
	if err != nil {
		return "", nil, fmt.Errorf("%w: registry seal: %v", ErrAuth, err)
	}
	return string(idRaw), payload, nil
}

// parseMaterial extracts key material from a verified payload.
func parseMaterial(payload []byte) (map[string][]byte, error) {
	off := 0
	need := func(n int) ([]byte, error) {
		if len(payload)-off < n {
			return nil, fmt.Errorf("%w: registry payload truncated", ErrCorrupt)
		}
		b := payload[off : off+n]
		off += n
		return b, nil
	}
	if _, err := need(8 + 16 + 8 + 2); err != nil {
		return nil, err
	}
	countRaw, err := need(4)
	if err != nil {
		return nil, err
	}
	count := binary.LittleEndian.Uint32(countRaw)
	out := make(map[string][]byte, count)
	for i := uint32(0); i < count; i++ {
		id, err := need(KeyIDLen)
		if err != nil {
			return nil, err
		}
		if _, err := need(2 + 8 + 8 + 1); err != nil {
			return nil, err
		}
		kl, err := need(2)
		if err != nil {
			return nil, err
		}
		if binary.LittleEndian.Uint16(kl) != 32 {
			return nil, fmt.Errorf("%w: registry key length", ErrCorrupt)
		}
		key, err := need(32)
		if err != nil {
			return nil, err
		}
		out[string(id)] = bytes.Clone(key)
	}
	return out, nil
}

// atomicWriteFile writes data to a temp file, fsyncs, renames over path, and
// fsyncs the directory. The temp pattern derives from the target name.
func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("crypto: temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("crypto: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("crypto: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("crypto: close temp: %w", err)
	}
	if err := durableRename(tmpName, path); err != nil {
		return err
	}
	return nil
}

// durableRename renames oldpath to newpath and fsyncs the directory.
func durableRename(oldpath, newpath string) error {
	if err := os.Rename(oldpath, newpath); err != nil {
		return fmt.Errorf("crypto: rename: %w", err)
	}
	if err := syncDir(filepath.Dir(newpath)); err != nil {
		return err
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("crypto: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("crypto: sync dir: %w", err)
	}
	return nil
}

// NewGeneration starts a new data-key generation: one fresh random key that
// becomes the active key for new files. It returns the generation number
// and the new key id.
func (r *Registry) NewGeneration(ctx context.Context) (uint64, [KeyIDLen]byte, error) {
	var zero [KeyIDLen]byte
	r.mu.Lock()
	defer r.mu.Unlock()
	mat, err := r.provider.Current(ctx)
	if err != nil {
		return 0, zero, fmt.Errorf("crypto: storage key: %w", err)
	}
	defer Zero(mat.Key)
	r.storageKeyID = mat.ID
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return 0, zero, err
	}
	defer zeroMaterial(material)
	id, key, err := newDataKey()
	if err != nil {
		return 0, zero, err
	}
	r.generation++
	material[string(id[:])] = key
	r.keys[string(id[:])] = &StoredKey{
		ID: id, Alg: r.defaultAlg, Generation: r.generation,
		CreatedAt: r.now().UTC(), State: KeyActive,
	}
	if err := r.persistLocked(mat.Key, material); err != nil {
		r.generation--
		delete(r.keys, string(id[:]))
		return 0, zero, err
	}
	return r.generation, id, nil
}

func zeroMaterial(m map[string][]byte) {
	for _, v := range m {
		Zero(v)
	}
}

// GetKey returns a copy of the key material and algorithm for id. Material
// is re-read from disk and unwrapped on every call.
func (r *Registry) GetKey(ctx context.Context, id [KeyIDLen]byte) ([]byte, AlgorithmID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, ok := r.keys[string(id[:])]
	if !ok {
		return nil, AlgorithmUnknown, fmt.Errorf("%w: unknown data key", ErrAuth)
	}
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return nil, AlgorithmUnknown, err
	}
	defer zeroMaterial(material)
	key, ok := material[string(id[:])]
	if !ok {
		return nil, AlgorithmUnknown, fmt.Errorf("%w: unknown data key", ErrAuth)
	}
	return bytes.Clone(key), meta.Alg, nil
}

// ActiveKey returns the active generation's key metadata and material.
func (r *Registry) ActiveKey(ctx context.Context) (StoredKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.activeKeyLocked(ctx)
}

func (r *Registry) activeKeyLocked(ctx context.Context) (StoredKey, error) {
	var active *StoredKey
	for _, k := range r.keys {
		if k.Generation == r.generation {
			active = k
			break
		}
	}
	if active == nil {
		return StoredKey{}, fmt.Errorf("%w: no active data key", ErrCorrupt)
	}
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return StoredKey{}, err
	}
	defer zeroMaterial(material)
	key, ok := material[string(active.ID[:])]
	if !ok {
		return StoredKey{}, fmt.Errorf("%w: no active data key", ErrCorrupt)
	}
	out := *active
	out.Key = bytes.Clone(key)
	return out, nil
}

// RemoveKey deletes id from the registry (after its references are gone).
func (r *Registry) RemoveKey(ctx context.Context, id [KeyIDLen]byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.keys[string(id[:])]; !ok {
		return nil
	}
	mat, err := r.provider.Current(ctx)
	if err != nil {
		return fmt.Errorf("crypto: storage key: %w", err)
	}
	defer Zero(mat.Key)
	r.storageKeyID = mat.ID
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return err
	}
	defer zeroMaterial(material)
	k := r.keys[string(id[:])]
	delete(r.keys, string(id[:]))
	delete(material, string(id[:]))
	if err := r.persistLocked(mat.Key, material); err != nil {
		r.keys[string(id[:])] = k
		return err
	}
	return nil
}

// ExpireBefore marks active keys created before t as expired. It returns the
// number newly expired.
func (r *Registry) ExpireBefore(ctx context.Context, t time.Time) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, k := range r.keys {
		if k.State == KeyActive && k.CreatedAt.Before(t) {
			k.State = KeyExpired
			n++
		}
	}
	if n == 0 {
		return 0, nil
	}
	mat, err := r.provider.Current(ctx)
	if err != nil {
		return 0, fmt.Errorf("crypto: storage key: %w", err)
	}
	defer Zero(mat.Key)
	r.storageKeyID = mat.ID
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return 0, err
	}
	defer zeroMaterial(material)
	if err := r.persistLocked(mat.Key, material); err != nil {
		return 0, err
	}
	return n, nil
}

// Keys returns metadata snapshots (never material).
func (r *Registry) Keys() []StoredKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]StoredKey, 0, len(r.keys))
	for _, k := range r.keys {
		out = append(out, *k)
	}
	return out
}

// Generation returns the active data-key generation.
func (r *Registry) Generation() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generation
}

// ActiveKeyID returns the active generation's key id.
func (r *Registry) ActiveKeyID() ([KeyIDLen]byte, error) {
	var zero [KeyIDLen]byte
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range r.keys {
		if k.Generation == r.generation {
			return k.ID, nil
		}
	}
	return zero, fmt.Errorf("%w: no active data key", ErrCorrupt)
}

// ActiveKeyMeta returns the active key's metadata (no material).
func (r *Registry) ActiveKeyMeta() (StoredKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, k := range r.keys {
		if k.Generation == r.generation {
			out := *k
			return out, nil
		}
	}
	return StoredKey{}, fmt.Errorf("%w: no active data key", ErrCorrupt)
}

// DefaultAlgorithm returns the algorithm for new files.
func (r *Registry) DefaultAlgorithm() AlgorithmID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.defaultAlg
}

// SetDefaultAlgorithm changes the algorithm for new files only.
func (r *Registry) SetDefaultAlgorithm(ctx context.Context, alg AlgorithmID) error {
	if _, err := alg.KeySize(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.defaultAlg == alg {
		return nil
	}
	mat, err := r.provider.Current(ctx)
	if err != nil {
		return fmt.Errorf("crypto: storage key: %w", err)
	}
	defer Zero(mat.Key)
	r.storageKeyID = mat.ID
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return err
	}
	defer zeroMaterial(material)
	prev := r.defaultAlg
	r.defaultAlg = alg
	if err := r.persistLocked(mat.Key, material); err != nil {
		r.defaultAlg = prev
		return err
	}
	return nil
}

// RebindDBID moves the registry to a new database identity for coordinated
// reseed: the KEK re-derives under the new DBID and the sealed payload is
// re-written with the new DBID, atomically installed. Key material is
// preserved (re-wrapped, not rotated). The caller must have opened the
// registry under the source DBID; files must already be rebound (registry
// persists last) so a crash always retries to convergence.
func (r *Registry) RebindDBID(ctx context.Context, newDBID [16]byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dbID == newDBID {
		return nil
	}
	mat, err := r.provider.Current(ctx)
	if err != nil {
		return fmt.Errorf("crypto: storage key: %w", err)
	}
	defer Zero(mat.Key)
	r.storageKeyID = mat.ID
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return err
	}
	defer zeroMaterial(material)
	prev := r.dbID
	r.dbID = newDBID
	if err := r.persistLocked(mat.Key, material); err != nil {
		r.dbID = prev
		return err
	}
	return nil
}

// StorageKeyID returns the storage key id the registry is wrapped under.
func (r *Registry) StorageKeyID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.storageKeyID
}

// KeyCount returns the number of stored keys.
func (r *Registry) KeyCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

// DiskBytes returns the last persisted registry size.
func (r *Registry) DiskBytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.diskBytes
}

// Pin registers a checkpoint/backup directory whose files pin keys.
func (r *Registry) Pin(ctx context.Context, path, kind string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pins {
		if p.Path == path {
			return nil
		}
	}
	mat, err := r.provider.Current(ctx)
	if err != nil {
		return fmt.Errorf("crypto: storage key: %w", err)
	}
	defer Zero(mat.Key)
	r.storageKeyID = mat.ID
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return err
	}
	defer zeroMaterial(material)
	r.pins = append(r.pins, Pin{Path: path, Kind: kind})
	if err := r.persistLocked(mat.Key, material); err != nil {
		r.pins = r.pins[:len(r.pins)-1]
		return err
	}
	return nil
}

// Unpin releases a checkpoint/backup directory.
func (r *Registry) Unpin(ctx context.Context, path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	idx := -1
	for i, p := range r.pins {
		if p.Path == path {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil
	}
	mat, err := r.provider.Current(ctx)
	if err != nil {
		return fmt.Errorf("crypto: storage key: %w", err)
	}
	defer Zero(mat.Key)
	r.storageKeyID = mat.ID
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		return err
	}
	defer zeroMaterial(material)
	keep := r.pins
	r.pins = append(keep[:idx], keep[idx+1:]...)
	if err := r.persistLocked(mat.Key, material); err != nil {
		r.pins = keep
		return err
	}
	return nil
}

// Pins returns the registered checkpoint/backup directories.
func (r *Registry) Pins() []Pin {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Pin(nil), r.pins...)
}

// Rewrap re-encrypts the registry under the provider's current storage key
// (storage-key rotation). Data files are untouched. Crash-safe: an intent
// file completes the rotation on the next open.
func (r *Registry) Rewrap(ctx context.Context) (string, error) {
	mat, err := r.provider.Current(ctx)
	if err != nil {
		return "", fmt.Errorf("crypto: storage key: %w", err)
	}
	defer Zero(mat.Key)
	return r.rewrapWith(ctx, mat)
}

// RewrapWith re-encrypts the registry under explicit material (direct-key
// rotation path). The material is copied, never retained.
func (r *Registry) RewrapWith(ctx context.Context, mat KeyMaterial) (string, error) {
	cp := KeyMaterial{ID: mat.ID, Key: bytes.Clone(mat.Key)}
	defer Zero(cp.Key)
	return r.rewrapWith(ctx, cp)
}

func (r *Registry) rewrapWith(ctx context.Context, mat KeyMaterial) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if mat.ID == r.storageKeyID {
		material, err := r.loadMaterialLocked(ctx)
		if err != nil {
			return "", err
		}
		defer zeroMaterial(material)
		if err := r.persistLocked(mat.Key, material); err != nil {
			return "", err
		}
		Zero(r.kek)
		kek, err := deriveKEK(mat.Key, r.dbID[:], []byte(mat.ID))
		if err != nil {
			return "", err
		}
		r.kek, r.kekID = kek, mat.ID
		return r.storageKeyID, nil
	}
	prevID := r.storageKeyID
	cur, err := os.ReadFile(r.path)
	if err != nil {
		return "", fmt.Errorf("crypto: read registry: %w", err)
	}
	bakPath := filepath.Join(r.dir, registryBakName)
	if err := atomicWriteFile(bakPath, cur); err != nil {
		return "", err
	}
	material, err := r.loadMaterialLocked(ctx)
	if err != nil {
		_ = os.Remove(bakPath)
		return "", err
	}
	defer zeroMaterial(material)
	r.storageKeyID = mat.ID
	r.seq++
	intent, err := r.sealLocked(mat.Key, material)
	if err != nil {
		r.storageKeyID = prevID
		r.seq--
		_ = os.Remove(bakPath)
		return "", err
	}
	intentPath := filepath.Join(r.dir, registryIntent)
	if err := atomicWriteFile(intentPath, intent); err != nil {
		r.storageKeyID = prevID
		r.seq--
		_ = os.Remove(bakPath)
		return "", err
	}
	if err := durableRename(intentPath, r.path); err != nil {
		r.storageKeyID = prevID
		return "", fmt.Errorf("crypto: commit rotation: %w", err)
	}
	r.diskBytes = int64(len(intent))
	_ = os.Remove(bakPath)
	Zero(r.kek)
	kek, err := deriveKEK(mat.Key, r.dbID[:], []byte(mat.ID))
	if err != nil {
		return "", err
	}
	r.kek, r.kekID = kek, mat.ID
	return r.storageKeyID, nil
}

// ReplaceProvider swaps the storage-key provider. Call it after RewrapWith
// with explicit material so later persists (rotations, algorithm changes)
// re-wrap under the new key instead of the stale provider's current key.
// The next Open uses the application's configured provider/material.
func (r *Registry) ReplaceProvider(p KeyProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p != nil {
		r.provider = p
	}
}

// Close zeroes cached secrets. The registry must not be used after.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	Zero(r.kek)
	r.kek = nil
	r.keys = nil
}
