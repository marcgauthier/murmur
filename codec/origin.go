package codec

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/marcgauthier/murmur/ids"
)

const OriginSignatureVersion uint16 = 1
const originEnvelopeSize = 16 + 2 + 32 + ed25519.SignatureSize

var (
	ErrOriginUnsigned  = errors.New("origin: missing or unsupported signature")
	ErrOriginUnknown   = errors.New("origin: untrusted origin")
	ErrOriginSignature = errors.New("origin: invalid signature or identity")
	ErrOriginDigest    = errors.New("origin: mutation digest mismatch")
	ErrOriginConflict  = errors.New("origin: transaction identity conflicts with retained history")
)

// MutationDigest hashes the canonical mutation count and ordered mutations.
func MutationDigest(b *MutationBatch) [32]byte {
	h := sha256.New()
	raw := make([]byte, 0, MutationHeaderSize+binary.MaxVarintLen64+1)
	raw = binary.BigEndian.AppendUint32(raw, uint32(len(b.Mutations)))
	_, _ = h.Write(raw)
	for i := range b.Mutations {
		m := &b.Mutations[i]
		raw = raw[:0]
		raw = binary.BigEndian.AppendUint32(raw, m.TableID)
		raw = append(raw, m.RowID[:]...)
		raw = binary.BigEndian.AppendUint32(raw, m.ColumnID)
		raw = binary.BigEndian.AppendUint32(raw, uint32(m.Flags))
		switch m.Value.Type {
		case TypeBlob:
			raw = append(raw, byte(TypeBlob))
			raw = binary.AppendUvarint(raw, uint64(len(m.Value.B)))
			_, _ = h.Write(raw)
			_, _ = h.Write(m.Value.B)
		case TypeText:
			raw = append(raw, byte(TypeText))
			raw = binary.AppendUvarint(raw, uint64(len(m.Value.S)))
			_, _ = h.Write(raw)
			_, _ = h.Write([]byte(m.Value.S))
		default:
			raw = AppendValue(raw, m.Value)
			_, _ = h.Write(raw)
		}
		if b.ProtocolVersion >= 5 {
			_, _ = h.Write([]byte{byte(m.Policy)})
			_, _ = h.Write(EncodeCRDTRecords(nil, m.Records))
		}
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// OriginSigningBytes is independent of transport framing and compression.
func OriginSigningBytes(b *MutationBatch) []byte {
	out := []byte("murmur/origin-transaction/v1")
	out = append(out, b.DBID[:]...)
	out = append(out, b.OriginNode[:]...)
	out = binary.BigEndian.AppendUint64(out, b.Sequence)
	out = append(out, b.TxID[:]...)
	out = binary.BigEndian.AppendUint64(out, b.HLC)
	out = binary.BigEndian.AppendUint64(out, b.SchemaEpoch)
	out = append(out, b.SchemaHash[:]...)
	return append(out, b.MutationDigest[:]...)
}

func SignOrigin(b *MutationBatch, dbid ids.DBID, key ed25519.PrivateKey) error {
	if b == nil || len(key) != ed25519.PrivateKeySize || dbid.IsZero() || b.OriginNode.IsZero() || b.Sequence == 0 || b.TxID.IsZero() || b.HLC == 0 || len(b.Mutations) == 0 {
		return fmt.Errorf("%w: cannot sign invalid transaction", ErrOriginSignature)
	}
	b.DBID = dbid
	b.SignatureVersion = OriginSignatureVersion
	b.MutationDigest = MutationDigest(b)
	copy(b.OriginSignature[:], ed25519.Sign(key, OriginSigningBytes(b)))
	return nil
}

func VerifyOriginIdentity(b *MutationBatch, dbid ids.DBID, key ed25519.PublicKey) error {
	if b == nil || b.SignatureVersion != OriginSignatureVersion {
		return ErrOriginUnsigned
	}
	if len(key) != ed25519.PublicKeySize || dbid.IsZero() || b.DBID != dbid || b.OriginNode.IsZero() || b.TxID.IsZero() || b.Sequence == 0 || b.HLC == 0 {
		return ErrOriginSignature
	}
	if !ed25519.Verify(key, OriginSigningBytes(b), b.OriginSignature[:]) {
		return ErrOriginSignature
	}
	return nil
}

func VerifyOrigin(b *MutationBatch, dbid ids.DBID, key ed25519.PublicKey) error {
	if err := VerifyOriginIdentity(b, dbid, key); err != nil {
		return err
	}
	if len(b.Mutations) == 0 || MutationDigest(b) != b.MutationDigest {
		return ErrOriginDigest
	}
	return nil
}

func appendOriginEnvelope(dst []byte, b *MutationBatch) []byte {
	dst = append(dst, b.DBID[:]...)
	dst = binary.BigEndian.AppendUint16(dst, b.SignatureVersion)
	dst = append(dst, b.MutationDigest[:]...)
	return append(dst, b.OriginSignature[:]...)
}

func consumeOriginEnvelope(src []byte, b *MutationBatch) {
	copy(b.DBID[:], src[:16])
	b.SignatureVersion = binary.BigEndian.Uint16(src[16:18])
	copy(b.MutationDigest[:], src[18:50])
	copy(b.OriginSignature[:], src[50:114])
}
