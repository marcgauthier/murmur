package bridge

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/compression"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

// Bundle suite and framing. The suite versions the whole construction
// (signature, recipient wrap, AEAD, compression, canonical encoding);
// receivers reject unknown suites.
const (
	// BundleMagic prefixes every bundle.
	BundleMagic = "SPB1"
	// SuiteV1 is Ed25519 signatures, X25519+HKDF recipient wrap of a fresh
	// XChaCha20-Poly1305 content key, XChaCha20-Poly1305 payload, and codec-id-prefixed
	// compression (compression.Codec; default deflate) over the canonical encoding below.
	SuiteV1 = 1
	SuiteV2 = 2
)

// Wire sizes.
const (
	suiteSize       = 2
	keyIDSize       = 32
	nonceSize       = 24
	wrappedKeySize  = 32 + 16 // content key sealed with the KEK
	signatureSize   = 64
	sealedLenSize   = 8
	headerSize      = 4 + suiteSize + keyIDSize + keyIDSize + keyIDSize + nonceSize + nonceSize + wrappedKeySize + signatureSize + sealedLenSize
	maxNameLen      = 256
	maxTxPerBundle  = 1 << 20 // absolute decode cap; Limits.MaxTransactions binds tighter
	bundleWrapInfo  = "spedsql-bridge-wrap/v1"
	bundleWrapSaltC = "spedsql-bridge-wrap-salt/v1"
)

// SignerKey is a bridge export-signing identity. KeyIDs are raw Ed25519
// public keys (self-identifying). Bridge keys are separate from at-rest,
// QUIC, and database credentials by construction: they live only here.
type SignerKey struct {
	ID   [keyIDSize]byte
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
}

// GenerateSigningKey creates a fresh export-signing key.
func GenerateSigningKey() (*SignerKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	var id [keyIDSize]byte
	copy(id[:], pub)
	return &SignerKey{ID: id, pub: pub, priv: priv}, nil
}

// Public returns the Ed25519 public key for trust-store registration.
func (k *SignerKey) Public() ed25519.PublicKey { return append(ed25519.PublicKey(nil), k.pub...) }

// RecipientKey is a bridge recipient (X25519) identity.
type RecipientKey struct {
	ID   [keyIDSize]byte
	pub  [keyIDSize]byte
	priv [keyIDSize]byte
}

// GenerateRecipientKey creates a fresh recipient identity.
func GenerateRecipientKey() (*RecipientKey, error) {
	var priv [keyIDSize]byte
	if _, err := io.ReadFull(rand.Reader, priv[:]); err != nil {
		return nil, err
	}
	pub, err := curve25519.X25519(priv[:], curve25519.Basepoint)
	if err != nil {
		return nil, err
	}
	var k RecipientKey
	k.priv = priv
	copy(k.pub[:], pub)
	k.ID = k.pub
	return &k, nil
}

// Public returns the X25519 public key shared with exporters.
func (k *RecipientKey) Public() [keyIDSize]byte { return k.pub }

// TrustStore holds trusted exporter signing keys, their authorized source
// streams, and the receiver's own decryption keys (current plus explicitly
// retained previous keys). Key rotation adds the successor before retiring
// the predecessor; removal drops pending bundles signed under it, so
// callers retain replaced keys until in-flight bundles drain. It is safe for
// concurrent use.
type TrustStore struct {
	mu         sync.RWMutex
	signers    map[[keyIDSize]byte]ed25519.PublicKey
	streams    map[[keyIDSize]byte]map[string]bool
	recipients map[[keyIDSize]byte][keyIDSize]byte
	maxKeys    int
}

// NewTrustStore creates an empty store.
func NewTrustStore() *TrustStore {
	return &TrustStore{
		signers:    make(map[[keyIDSize]byte]ed25519.PublicKey),
		streams:    make(map[[keyIDSize]byte]map[string]bool),
		recipients: make(map[[keyIDSize]byte][keyIDSize]byte),
		maxKeys:    64,
	}
}

// AddSigner trusts pub for exactly the given source streams.
func (t *TrustStore) AddSigner(pub [keyIDSize]byte, streams ...string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.signers) >= t.maxKeys {
		if _, ok := t.signers[pub]; !ok {
			return fmt.Errorf("bridge: trust store holds %d keys", t.maxKeys)
		}
	}
	t.signers[pub] = append(ed25519.PublicKey(nil), pub[:]...)
	set := make(map[string]bool, len(streams))
	for _, s := range streams {
		set[s] = true
	}
	t.streams[pub] = set
	return nil
}

// RemoveSigner drops a signing key and its stream authorizations.
func (t *TrustStore) RemoveSigner(pub [keyIDSize]byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.signers, pub)
	delete(t.streams, pub)
}

// AddRecipient registers one of the receiver's own decryption keys.
func (t *TrustStore) AddRecipient(k *RecipientKey) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.recipients) >= t.maxKeys {
		if _, ok := t.recipients[k.ID]; !ok {
			return fmt.Errorf("bridge: trust store holds %d keys", t.maxKeys)
		}
	}
	t.recipients[k.ID] = k.priv
	return nil
}

// RemoveRecipient drops a decryption key.
func (t *TrustStore) RemoveRecipient(id [keyIDSize]byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.recipients, id)
}

// SignerIdentity reports one trusted signer's public key identity and its
// authorized source streams.
type SignerIdentity struct {
	ID      string   `json:"id"`
	Streams []string `json:"streams"`
}

// Identities lists trusted signer identities and own recipient key IDs
// for diagnostics. Only public identifiers leave the store: private key
// material is never exposed.
func (t *TrustStore) Identities() (signers []SignerIdentity, recipients []string) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for id, set := range t.streams {
		var streams []string
		for s := range set {
			streams = append(streams, s)
		}
		sort.Strings(streams)
		signers = append(signers, SignerIdentity{ID: hex.EncodeToString(id[:]), Streams: streams})
	}
	sort.Slice(signers, func(i, j int) bool { return signers[i].ID < signers[j].ID })
	for id := range t.recipients {
		recipients = append(recipients, hex.EncodeToString(id[:]))
	}
	sort.Strings(recipients)
	return signers, recipients
}

func (t *TrustStore) signer(id [keyIDSize]byte) (ed25519.PublicKey, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	pub, ok := t.signers[id]
	return pub, ok
}

func (t *TrustStore) authorized(stream string, id [keyIDSize]byte) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.streams[id][stream]
}

func (t *TrustStore) recipient(id [keyIDSize]byte) ([keyIDSize]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	priv, ok := t.recipients[id]
	return priv, ok
}

// Manifest is the authenticated routing/integrity metadata. It travels
// inside the encrypted payload; the signature covers the sealed payload, so
// tampering with sequence, schema, transaction, or recipient data fails.
type Manifest struct {
	BundleID      ids.TxID
	SourceDomain  ids.DBID
	Stream        string
	SeqFirst      uint64
	SeqLast       uint64
	TxIDs         []ids.TxID
	SchemaEpoch   uint64
	SchemaHash    [32]byte
	PayloadDigest [32]byte // SHA-256 over the canonical batch section
}

// Bundle is one opened bundle: authenticated manifest plus logical batches.
type Bundle struct {
	Manifest Manifest
	Batches  []Batch
	Signer   [keyIDSize]byte
}

// SealBatches builds a versioned, signed, recipient-encrypted bundle carrying
// batches. manifest supplies routing identity (a zero BundleID is generated);
// batches must form exactly the contiguous range
// [SeqFirst..SeqLast] with matching transaction identities in order.
func SealBatches(signer *SignerKey, recipient [keyIDSize]byte, manifest Manifest, batches []Batch, limits Limits) ([]byte, error) {
	if signer == nil {
		return nil, fmt.Errorf("bridge: seal requires a signing key")
	}
	if err := checkManifest(manifest, batches, limits); err != nil {
		return nil, err
	}
	if manifest.BundleID.IsZero() {
		manifest.BundleID = ids.NewTxID()
	}
	batchSection, err := encodeBatches(batches)
	if err != nil {
		return nil, err
	}
	manifest.PayloadDigest = sha256.Sum256(batchSection)
	payload, err := encodePayload(manifest, batchSection)
	if err != nil {
		return nil, err
	}
	compressed, err := compressPayload(payload, limits)
	if err != nil {
		return nil, err
	}
	return sealEnvelope(signer, recipient, compressed, limits)
}

func checkManifest(m Manifest, batches []Batch, limits Limits) error {
	if m.SourceDomain.IsZero() {
		return fmt.Errorf("bridge: manifest requires a source domain")
	}
	if m.Stream == "" || len(m.Stream) > maxNameLen {
		return fmt.Errorf("bridge: invalid manifest stream")
	}
	if len(batches) == 0 {
		return fmt.Errorf("bridge: bundle carries no batches")
	}
	if len(batches) > limits.MaxTransactions {
		return fmt.Errorf("bridge: %d batches exceed limit %d", len(batches), limits.MaxTransactions)
	}
	if m.SeqLast < m.SeqFirst || m.SeqLast-m.SeqFirst+1 != uint64(len(batches)) {
		return fmt.Errorf("bridge: sequence range [%d..%d] does not match %d batches",
			m.SeqFirst, m.SeqLast, len(batches))
	}
	if len(m.TxIDs) != len(batches) {
		return fmt.Errorf("bridge: %d manifest transactions != %d batches", len(m.TxIDs), len(batches))
	}
	for i, b := range batches {
		if err := b.Validate(limits); err != nil {
			return err
		}
		if b.Sequence != m.SeqFirst+uint64(i) {
			return fmt.Errorf("bridge: batch %d sequence %d breaks contiguity", i, b.Sequence)
		}
		if b.TxID != m.TxIDs[i] {
			return fmt.Errorf("bridge: batch %d transaction identity mismatch", i)
		}
	}
	return nil
}

// OpenBundle verifies and opens a bundle: framing/suite/recipient/signer
// checks, signature verification, recipient unwrap, bounded AEAD open,
// bounded decompression, canonical decode, digest and consistency checks,
// and signer/stream authorization.
func OpenBundle(data []byte, trust *TrustStore, limits Limits) (*Bundle, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("bridge: bundle of %d bytes is truncated", len(data))
	}
	compressed, signerID, err := openEnvelope(data, trust, limits)
	if err != nil {
		return nil, err
	}
	payload, err := decompressPayload(compressed, limits)
	if err != nil {
		return nil, err
	}
	manifest, batchSection, err := decodePayload(payload, limits)
	if err != nil {
		return nil, err
	}
	if digest := sha256.Sum256(batchSection); digest != manifest.PayloadDigest {
		return nil, fmt.Errorf("bridge: payload digest mismatch")
	}
	batches, err := decodeBatches(batchSection, limits)
	if err != nil {
		return nil, err
	}
	if err := checkManifest(manifest, batches, limits); err != nil {
		return nil, err
	}
	if !trust.authorized(manifest.Stream, signerID) {
		return nil, fmt.Errorf("bridge: signer is not authorized for stream %q", manifest.Stream)
	}
	return &Bundle{Manifest: manifest, Batches: batches, Signer: signerID}, nil
}

// signedBytes reconstructs the signed region: header prefix (everything
// before the signature), the sealed length, and the sealed payload.
func signedBytes(headerPrefix, sealedLen, sealed []byte) []byte {
	out := make([]byte, 0, len(headerPrefix)+len(sealedLen)+len(sealed))
	out = append(out, headerPrefix...)
	out = append(out, sealedLen...)
	out = append(out, sealed...)
	return out
}

// openEnvelope verifies framing, suite, signer/recipient identity, signature,
// and payload authentication, returning the sealed plaintext and the signer
// ID. Stream authorization stays with the caller: bundles authorize the
// manifest stream, file chunks the chunk stream.
func openEnvelope(data []byte, trust *TrustStore, limits Limits) ([]byte, [keyIDSize]byte, error) {
	var signerID [keyIDSize]byte
	if len(data) < headerSize {
		return nil, signerID, fmt.Errorf("bridge: bundle of %d bytes is truncated", len(data))
	}
	if len(data) > limits.MaxBundleBytes {
		return nil, signerID, fmt.Errorf("bridge: bundle of %d bytes exceeds limit %d", len(data), limits.MaxBundleBytes)
	}
	off := 0
	magic := string(data[off : off+4])
	off += 4
	suite := binary.BigEndian.Uint16(data[off : off+2])
	off += 2
	if magic != BundleMagic {
		return nil, signerID, fmt.Errorf("bridge: bad magic %q", magic)
	}
	if suite != SuiteV2 {
		return nil, signerID, fmt.Errorf("bridge: unsupported suite %d", suite)
	}
	var recipientID, ephPub [keyIDSize]byte
	copy(signerID[:], data[off:off+keyIDSize])
	off += keyIDSize
	copy(recipientID[:], data[off:off+keyIDSize])
	off += keyIDSize
	copy(ephPub[:], data[off:off+keyIDSize])
	off += keyIDSize
	contentNonce := append([]byte(nil), data[off:off+nonceSize]...)
	off += nonceSize
	wrapNonce := append([]byte(nil), data[off:off+nonceSize]...)
	off += nonceSize
	wrapped := append([]byte(nil), data[off:off+wrappedKeySize]...)
	off += wrappedKeySize
	sig := append([]byte(nil), data[off:off+signatureSize]...)
	off += signatureSize
	sealedLen := binary.BigEndian.Uint64(data[off : off+sealedLenSize])
	off += sealedLenSize
	if uint64(len(data)-off) != sealedLen {
		return nil, signerID, fmt.Errorf("bridge: sealed length %d != %d trailing bytes", sealedLen, len(data)-off)
	}
	sealed := data[off:]

	signerPub, ok := trust.signer(signerID)
	if !ok {
		return nil, signerID, fmt.Errorf("bridge: unknown signer")
	}
	recipPriv, ok := trust.recipient(recipientID)
	if !ok {
		return nil, signerID, fmt.Errorf("bridge: bundle is not for a known recipient")
	}
	signed := signedBytes(data[:off-signatureSize-sealedLenSize], data[off-sealedLenSize:off], sealed)
	if !ed25519.Verify(signerPub, signed, sig) {
		return nil, signerID, fmt.Errorf("bridge: signature verification failed")
	}
	contentKey, err := unwrapContentKey(recipPriv, ephPub, recipientID, wrapNonce, wrapped)
	if err != nil {
		return nil, signerID, fmt.Errorf("bridge: recipient unwrap failed: %w", err)
	}
	aad := data[:off-signatureSize-sealedLenSize]
	aead, err := chacha20poly1305.NewX(contentKey[:])
	if err != nil {
		return nil, signerID, err
	}
	plaintext, err := aead.Open(nil, contentNonce, sealed, aad)
	if err != nil {
		return nil, signerID, fmt.Errorf("bridge: payload authentication failed")
	}
	return plaintext, signerID, nil
}

func sealEnvelope(signer *SignerKey, recipient [keyIDSize]byte, compressed []byte, limits Limits) ([]byte, error) {
	var contentKey [32]byte
	if _, err := io.ReadFull(rand.Reader, contentKey[:]); err != nil {
		return nil, err
	}
	var ephPriv [keyIDSize]byte
	if _, err := io.ReadFull(rand.Reader, ephPriv[:]); err != nil {
		return nil, err
	}
	ephPub, err := curve25519.X25519(ephPriv[:], curve25519.Basepoint)
	if err != nil {
		return nil, err
	}
	var ephArr [keyIDSize]byte
	copy(ephArr[:], ephPub)
	var contentNonce, wrapNonce [nonceSize]byte
	if _, err := io.ReadFull(rand.Reader, contentNonce[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(rand.Reader, wrapNonce[:]); err != nil {
		return nil, err
	}
	wrapped, err := wrapContentKey(ephPriv, ephArr, recipient, wrapNonce, contentKey)
	if err != nil {
		return nil, err
	}

	header := make([]byte, 0, headerSize)
	header = append(header, BundleMagic...)
	var tmp [8]byte
	binary.BigEndian.PutUint16(tmp[:2], SuiteV2)
	header = append(header, tmp[:2]...)
	header = append(header, signer.ID[:]...)
	header = append(header, recipient[:]...)
	header = append(header, ephArr[:]...)
	header = append(header, contentNonce[:]...)
	header = append(header, wrapNonce[:]...)
	header = append(header, wrapped...)

	aead, err := chacha20poly1305.NewX(contentKey[:])
	if err != nil {
		return nil, err
	}
	sealed := aead.Seal(nil, contentNonce[:], compressed, header)
	binary.BigEndian.PutUint64(tmp[:], uint64(len(sealed)))
	sig := ed25519.Sign(signer.priv, signedBytes(header, tmp[:], sealed))

	out := make([]byte, 0, len(header)+signatureSize+sealedLenSize+len(sealed))
	out = append(out, header...)
	out = append(out, sig...)
	out = append(out, tmp[:]...)
	out = append(out, sealed...)
	if len(out) > limits.MaxBundleBytes {
		return nil, fmt.Errorf("bridge: encoded bundle of %d bytes exceeds limit %d", len(out), limits.MaxBundleBytes)
	}
	return out, nil
}

func wrapContentKey(ephPriv, ephPub, recipient [keyIDSize]byte, wrapNonce [nonceSize]byte, contentKey [32]byte) ([]byte, error) {
	shared, err := curve25519.X25519(ephPriv[:], recipient[:])
	if err != nil {
		return nil, err
	}
	kek := bridgeKEK(shared, ephPub, recipient)
	aead, err := chacha20poly1305.NewX(kek)
	if err != nil {
		return nil, err
	}
	var aad bytes.Buffer
	aad.WriteString(BundleMagic)
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], SuiteV2)
	aad.Write(tmp[:])
	aad.Write(ephPub[:])
	aad.Write(recipient[:])
	return aead.Seal(nil, wrapNonce[:], contentKey[:], aad.Bytes()), nil
}

func unwrapContentKey(recipPriv, ephPub, recipient [keyIDSize]byte, wrapNonce, wrapped []byte) ([32]byte, error) {
	var zero [32]byte
	shared, err := curve25519.X25519(recipPriv[:], ephPub[:])
	if err != nil {
		return zero, err
	}
	kek := bridgeKEK(shared, ephPub, recipient)
	aead, err := chacha20poly1305.NewX(kek)
	if err != nil {
		return zero, err
	}
	var aad bytes.Buffer
	aad.WriteString(BundleMagic)
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], SuiteV2)
	aad.Write(tmp[:])
	aad.Write(ephPub[:])
	aad.Write(recipient[:])
	opened, err := aead.Open(nil, wrapNonce, wrapped, aad.Bytes())
	if err != nil {
		return zero, err
	}
	if len(opened) != 32 {
		return zero, fmt.Errorf("bridge: wrapped key of %d bytes", len(opened))
	}
	var key [32]byte
	copy(key[:], opened)
	return key, nil
}

func bridgeKEK(shared []byte, ephPub, recipient [keyIDSize]byte) []byte {
	var salt bytes.Buffer
	salt.WriteString(bundleWrapSaltC)
	salt.Write(ephPub[:])
	salt.Write(recipient[:])
	r := hkdf.New(sha256.New, shared, salt.Bytes(), []byte(bundleWrapInfo))
	kek := make([]byte, 32)
	_, _ = io.ReadFull(r, kek)
	return kek
}

// compressPayload prefixes the compressed bytes with the one-byte codec
// id. The result is sealed and signed by the envelope, so the id is
// authenticated and cannot be swapped.
func compressPayload(raw []byte, limits Limits) ([]byte, error) {
	cd := limits.Codec
	if cd == nil {
		cd = compression.Deflate
	}
	out, err := cd.Compress([]byte{cd.ID()}, raw)
	if err != nil {
		return nil, fmt.Errorf("bridge: compress: %w", err)
	}
	return out, nil
}

func decompressPayload(compressed []byte, limits Limits) ([]byte, error) {
	if len(compressed) == 0 {
		return nil, fmt.Errorf("bridge: empty compressed payload")
	}
	user := limits.Codecs
	if limits.Codec != nil {
		user = append([]compression.Codec{limits.Codec}, user...)
	}
	reg, err := compression.NewRegistry(user...)
	if err != nil {
		return nil, fmt.Errorf("bridge: %w", err)
	}
	cd, err := reg.Get(compressed[0])
	if err != nil {
		return nil, fmt.Errorf("bridge: payload codec: %w", err)
	}
	out, err := cd.Decompress(nil, compressed[1:], limits.MaxPayloadBytes)
	if err != nil {
		return nil, fmt.Errorf("bridge: decompress: %w", err)
	}
	if len(out) > limits.MaxPayloadBytes {
		return nil, fmt.Errorf("bridge: decompressed %d bytes exceed limit %d", len(out), limits.MaxPayloadBytes)
	}
	return out, nil
}

// Canonical payload encoding: bundleID[16], sourceDomain[16],
// streamLen u16 + stream, seqFirst u64, seqLast u64, txCount u32 +
// txIDs[16]*, schemaEpoch u64, schemaHash[32], digest[32],
// batchSectionLen u64 + batchSection.

func encodePayload(m Manifest, batchSection []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(m.BundleID[:])
	buf.Write(m.SourceDomain[:])
	if len(m.Stream) > maxNameLen {
		return nil, fmt.Errorf("bridge: stream name too long")
	}
	var tmp [8]byte
	binary.BigEndian.PutUint16(tmp[:2], uint16(len(m.Stream)))
	buf.Write(tmp[:2])
	buf.WriteString(m.Stream)
	binary.BigEndian.PutUint64(tmp[:], m.SeqFirst)
	buf.Write(tmp[:])
	binary.BigEndian.PutUint64(tmp[:], m.SeqLast)
	buf.Write(tmp[:])
	binary.BigEndian.PutUint32(tmp[:4], uint32(len(m.TxIDs)))
	buf.Write(tmp[:4])
	for _, id := range m.TxIDs {
		buf.Write(id[:])
	}
	binary.BigEndian.PutUint64(tmp[:], m.SchemaEpoch)
	buf.Write(tmp[:])
	buf.Write(m.SchemaHash[:])
	buf.Write(m.PayloadDigest[:])
	binary.BigEndian.PutUint64(tmp[:], uint64(len(batchSection)))
	buf.Write(tmp[:])
	buf.Write(batchSection)
	return buf.Bytes(), nil
}

func decodePayload(payload []byte, limits Limits) (Manifest, []byte, error) {
	var m Manifest
	r := bytes.NewReader(payload)
	readN := func(n int) ([]byte, error) {
		b := make([]byte, n)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, fmt.Errorf("bridge: truncated payload")
		}
		return b, nil
	}
	b, err := readN(16)
	if err != nil {
		return m, nil, err
	}
	copy(m.BundleID[:], b)
	b, err = readN(16)
	if err != nil {
		return m, nil, err
	}
	copy(m.SourceDomain[:], b)
	var u16 [2]byte
	if _, err := io.ReadFull(r, u16[:]); err != nil {
		return m, nil, fmt.Errorf("bridge: truncated payload")
	}
	streamLen := int(binary.BigEndian.Uint16(u16[:]))
	if streamLen == 0 || streamLen > maxNameLen {
		return m, nil, fmt.Errorf("bridge: invalid stream length %d", streamLen)
	}
	sb, err := readN(streamLen)
	if err != nil {
		return m, nil, err
	}
	m.Stream = string(sb)
	var u64 [8]byte
	if _, err := io.ReadFull(r, u64[:]); err != nil {
		return m, nil, fmt.Errorf("bridge: truncated payload")
	}
	m.SeqFirst = binary.BigEndian.Uint64(u64[:])
	if _, err := io.ReadFull(r, u64[:]); err != nil {
		return m, nil, fmt.Errorf("bridge: truncated payload")
	}
	m.SeqLast = binary.BigEndian.Uint64(u64[:])
	var u32 [4]byte
	if _, err := io.ReadFull(r, u32[:]); err != nil {
		return m, nil, fmt.Errorf("bridge: truncated payload")
	}
	txCount := int(binary.BigEndian.Uint32(u32[:]))
	if txCount < 0 || txCount > maxTxPerBundle || txCount > limits.MaxTransactions {
		return m, nil, fmt.Errorf("bridge: invalid transaction count %d", txCount)
	}
	m.TxIDs = make([]ids.TxID, txCount)
	for i := range m.TxIDs {
		b, err := readN(16)
		if err != nil {
			return m, nil, err
		}
		copy(m.TxIDs[i][:], b)
	}
	if _, err := io.ReadFull(r, u64[:]); err != nil {
		return m, nil, fmt.Errorf("bridge: truncated payload")
	}
	m.SchemaEpoch = binary.BigEndian.Uint64(u64[:])
	b, err = readN(32)
	if err != nil {
		return m, nil, err
	}
	copy(m.SchemaHash[:], b)
	b, err = readN(32)
	if err != nil {
		return m, nil, err
	}
	copy(m.PayloadDigest[:], b)
	if _, err := io.ReadFull(r, u64[:]); err != nil {
		return m, nil, fmt.Errorf("bridge: truncated payload")
	}
	sectionLen := binary.BigEndian.Uint64(u64[:])
	if sectionLen > uint64(len(payload)) {
		return m, nil, fmt.Errorf("bridge: invalid batch section length %d", sectionLen)
	}
	section, err := readN(int(sectionLen))
	if err != nil {
		return m, nil, err
	}
	if r.Len() != 0 {
		return m, nil, fmt.Errorf("bridge: %d trailing payload bytes", r.Len())
	}
	return m, section, nil
}

// Batch section: count u32 + batches of
// txID[16], origin[16], seq u64, hlc u64, recCount u32 +
// records of tableLen u16 + table, row[16], op u8, colCount u32 +
// columns of colLen u16 + column + codec value.

func encodeBatches(batches []Batch) ([]byte, error) {
	var buf bytes.Buffer
	var tmp [8]byte
	binary.BigEndian.PutUint32(tmp[:4], uint32(len(batches)))
	buf.Write(tmp[:4])
	for _, b := range batches {
		buf.Write(b.TxID[:])
		buf.Write(b.Origin[:])
		binary.BigEndian.PutUint64(tmp[:], b.Sequence)
		buf.Write(tmp[:])
		binary.BigEndian.PutUint64(tmp[:], b.HLC)
		buf.Write(tmp[:])
		binary.BigEndian.PutUint32(tmp[:4], uint32(len(b.Records)))
		buf.Write(tmp[:4])
		for _, r := range b.Records {
			if len(r.Table) == 0 || len(r.Table) > maxNameLen {
				return nil, fmt.Errorf("bridge: invalid table name")
			}
			binary.BigEndian.PutUint16(tmp[:2], uint16(len(r.Table)))
			buf.Write(tmp[:2])
			buf.WriteString(r.Table)
			buf.Write(r.Row[:])
			buf.WriteByte(byte(r.Op))
			binary.BigEndian.PutUint32(tmp[:4], uint32(len(r.Columns)))
			buf.Write(tmp[:4])
			for _, c := range r.Columns {
				if len(c.Column) == 0 || len(c.Column) > maxNameLen {
					return nil, fmt.Errorf("bridge: invalid column name")
				}
				binary.BigEndian.PutUint16(tmp[:2], uint16(len(c.Column)))
				buf.Write(tmp[:2])
				buf.WriteString(c.Column)
				buf.Write(codec.AppendValue(nil, c.Value))
				buf.WriteByte(byte(c.Policy))
				buf.Write(codec.EncodeCRDTRecords(nil, c.Records))
			}
		}
	}
	return buf.Bytes(), nil
}

func decodeBatches(section []byte, limits Limits) ([]Batch, error) {
	r := bytes.NewReader(section)
	var u32 [4]byte
	if _, err := io.ReadFull(r, u32[:]); err != nil {
		return nil, fmt.Errorf("bridge: truncated batch section")
	}
	count := int(binary.BigEndian.Uint32(u32[:]))
	if count < 0 || count > maxTxPerBundle || count > limits.MaxTransactions {
		return nil, fmt.Errorf("bridge: invalid batch count %d", count)
	}
	batches := make([]Batch, 0, count)
	for i := 0; i < count; i++ {
		var b Batch
		fixed := make([]byte, 16+16+8+8+4)
		if _, err := io.ReadFull(r, fixed); err != nil {
			return nil, fmt.Errorf("bridge: truncated batch %d", i)
		}
		copy(b.TxID[:], fixed[0:16])
		copy(b.Origin[:], fixed[16:32])
		b.Sequence = binary.BigEndian.Uint64(fixed[32:40])
		b.HLC = binary.BigEndian.Uint64(fixed[40:48])
		recCount := int(binary.BigEndian.Uint32(fixed[48:52]))
		if recCount < 0 || recCount > limits.MaxTransactions {
			return nil, fmt.Errorf("bridge: invalid record count %d", recCount)
		}
		for j := 0; j < recCount; j++ {
			var rec Record
			var u16 [2]byte
			if _, err := io.ReadFull(r, u16[:]); err != nil {
				return nil, fmt.Errorf("bridge: truncated record %d/%d", i, j)
			}
			tableLen := int(binary.BigEndian.Uint16(u16[:]))
			if tableLen == 0 || tableLen > maxNameLen {
				return nil, fmt.Errorf("bridge: invalid table length %d", tableLen)
			}
			name := make([]byte, tableLen)
			if _, err := io.ReadFull(r, name); err != nil {
				return nil, fmt.Errorf("bridge: truncated record %d/%d", i, j)
			}
			rec.Table = string(name)
			if _, err := io.ReadFull(r, rec.Row[:]); err != nil {
				return nil, fmt.Errorf("bridge: truncated record %d/%d", i, j)
			}
			op, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("bridge: truncated record %d/%d", i, j)
			}
			rec.Op = RecordOp(op)
			if _, err := io.ReadFull(r, u32[:]); err != nil {
				return nil, fmt.Errorf("bridge: truncated record %d/%d", i, j)
			}
			colCount := int(binary.BigEndian.Uint32(u32[:]))
			if colCount < 0 || colCount > limits.MaxTransactions {
				return nil, fmt.Errorf("bridge: invalid column count %d", colCount)
			}
			for k := 0; k < colCount; k++ {
				if _, err := io.ReadFull(r, u16[:]); err != nil {
					return nil, fmt.Errorf("bridge: truncated column %d/%d/%d", i, j, k)
				}
				colLen := int(binary.BigEndian.Uint16(u16[:]))
				if colLen == 0 || colLen > maxNameLen {
					return nil, fmt.Errorf("bridge: invalid column length %d", colLen)
				}
				cname := make([]byte, colLen)
				if _, err := io.ReadFull(r, cname); err != nil {
					return nil, fmt.Errorf("bridge: truncated column %d/%d/%d", i, j, k)
				}
				rest, err := io.ReadAll(r)
				if err != nil {
					return nil, fmt.Errorf("bridge: truncated value %d/%d/%d", i, j, k)
				}
				v, remaining, err := codec.ConsumeValue(rest, limits.MaxPayloadBytes)
				if err != nil {
					return nil, fmt.Errorf("bridge: invalid value %d/%d/%d: %w", i, j, k, err)
				}
				if len(remaining) < 1 {
					return nil, fmt.Errorf("bridge: missing merge policy")
				}
				policy := schema.MergePolicy(remaining[0])
				records, next, err := codec.ConsumeCRDTRecords(remaining[1:], codec.Limits{MaxValueBytes: limits.MaxPayloadBytes, MaxMutations: 100000})
				if err != nil {
					return nil, err
				}
				r.Reset(next)
				rec.Columns = append(rec.Columns, ColumnValue{Column: string(cname), Value: v, Policy: policy, Records: records})
			}
			b.Records = append(b.Records, rec)
		}
		batches = append(batches, b)
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("bridge: %d trailing batch bytes", r.Len())
	}
	return batches, nil
}
