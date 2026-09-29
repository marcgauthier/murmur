package bridge

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
)

// OutboxKey is one named 256-bit journal encryption key. The ID is stored
// in plaintext alongside each file it protects; the key itself never
// touches the journal and must be re-supplied on every open.
type OutboxKey struct {
	ID  string
	Key [32]byte
}

// outboxEnvelopeVersion versions the encrypted event file format.
const outboxEnvelopeVersion = 2

type outboxEnvelope struct {
	V     int    `json:"v"`
	Key   string `json:"key_id"`
	Nonce string `json:"nonce"`
	Data  string `json:"data"`
}

// isOutboxEnvelope reports whether raw looks like a versioned encrypted
// envelope rather than a legacy plaintext event.
func isOutboxEnvelope(raw []byte) bool {
	var peek struct {
		V int `json:"v"`
	}
	if err := json.Unmarshal(raw, &peek); err != nil {
		return false
	}
	return peek.V == outboxEnvelopeVersion
}

func validateOutboxKey(k OutboxKey) error {
	if k.ID == "" || len(k.ID) > 64 {
		return fmt.Errorf("bridge: outbox key id must be 1..64 characters")
	}
	var zero [32]byte
	if k.Key == zero {
		return fmt.Errorf("bridge: outbox key %q is all zero", k.ID)
	}
	return nil
}

// sealOutboxEvent encrypts one marshalled event file for seq under key.
// The sequence and key identity bind as associated data so ciphertext
// cannot move between files or keys unnoticed.
func sealOutboxEvent(plaintext []byte, seq uint64, key OutboxKey) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key.Key[:])
	if err != nil {
		return nil, err
	}
	var nonce [chacha20poly1305.NonceSizeX]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, err
	}
	aad := outboxAAD(seq, key.ID)
	sealed := aead.Seal(nil, nonce[:], plaintext, aad)
	env := outboxEnvelope{V: outboxEnvelopeVersion, Key: key.ID,
		Nonce: base64.StdEncoding.EncodeToString(nonce[:]),
		Data:  base64.StdEncoding.EncodeToString(sealed)}
	return json.Marshal(env)
}

// openOutboxEventSeq authenticates and decrypts the event file at seq,
// resolving the key by the envelope's identity. Unknown key IDs, wrong
// keys, and any tampering — including ciphertext transplanted from another
// file — fail closed.
func openOutboxEventSeq(raw []byte, seq uint64, ring []OutboxKey) ([]byte, error) {
	var env outboxEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("bridge: outbox envelope corrupt: %w", err)
	}
	if env.V != outboxEnvelopeVersion {
		return nil, fmt.Errorf("bridge: outbox envelope version %d", env.V)
	}
	var key *OutboxKey
	for i := range ring {
		if ring[i].ID == env.Key {
			key = &ring[i]
			break
		}
	}
	if key == nil {
		return nil, fmt.Errorf("bridge: outbox key %q is not available", env.Key)
	}
	nonce, err := base64.StdEncoding.DecodeString(env.Nonce)
	if err != nil || len(nonce) != chacha20poly1305.NonceSizeX {
		return nil, fmt.Errorf("bridge: outbox envelope has a bad nonce")
	}
	sealed, err := base64.StdEncoding.DecodeString(env.Data)
	if err != nil {
		return nil, fmt.Errorf("bridge: outbox envelope corrupt: %w", err)
	}
	aead, err := chacha20poly1305.NewX(key.Key[:])
	if err != nil {
		return nil, err
	}
	plaintext, err := aead.Open(nil, nonce, sealed, outboxAAD(seq, env.Key))
	if err != nil {
		return nil, fmt.Errorf("bridge: outbox event authentication failed")
	}
	return plaintext, nil
}

func outboxAAD(seq uint64, keyID string) []byte {
	return []byte(fmt.Sprintf("spedsql-outbox-v2\x00%s\x00%020d", keyID, seq))
}
