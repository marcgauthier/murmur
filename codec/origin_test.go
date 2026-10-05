package codec

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/marcgauthier/murmur/ids"
)

func originVector() (*MutationBatch, ed25519.PrivateKey) {
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	b := &MutationBatch{ProtocolVersion: 4, DBID: ids.DBID{1}, OriginNode: ids.NodeID{2}, Sequence: 3, TxID: ids.TxID{4}, HLC: 5, SchemaEpoch: 6, SchemaHash: [32]byte{7}, Mutations: []Mutation{{TableID: 8, RowID: ids.RowID{9}, ColumnID: 10, Value: Text("hello")}}}
	if err := SignOrigin(b, b.DBID, key); err != nil {
		panic(err)
	}
	return b, key
}

func TestOriginCanonicalVector(t *testing.T) {
	b, key := originVector()
	const wantMutation = "00000008090000000000000000000000000000000000000a00000000030568656c6c6f"
	raw := EncodeBatch(nil, b)[BatchHeaderSize:]
	if hex.EncodeToString(raw) != wantMutation {
		t.Fatalf("canonical mutation bytes = %x", raw)
	}
	const wantDigest = "84473b617373265e5822fc5c232ced7232394431644383c9655a0fee1fbc7456"
	if hex.EncodeToString(b.MutationDigest[:]) != wantDigest {
		t.Fatalf("canonical digest = %x", b.MutationDigest)
	}
	const wantSigning = "6d75726d75722f6f726967696e2d7472616e73616374696f6e2f76310100000000000000000000000000000002000000000000000000000000000000000000000000000304000000000000000000000000000000000000000000000500000000000000060700000000000000000000000000000000000000000000000000000000000000"
	prefix, err := hex.DecodeString(wantSigning)
	if err != nil {
		t.Fatal(err)
	}
	got := OriginSigningBytes(b)
	if hex.EncodeToString(got[:len(got)-32]) != hex.EncodeToString(prefix) {
		t.Fatalf("canonical signing prefix = %x", got[:len(got)-32])
	}
	if hex.EncodeToString(b.OriginSignature[:]) != "73deb7b4a6084eb9d1bf8aa42975a6fe735e90bd17c80ef32e5e2d2a03e0ddf9df59ed413864bd82447579d7c24b39d29fbd0a3bd6840338b64005760bbcb605" {
		t.Fatalf("canonical signature = %x", b.OriginSignature)
	}
	if err := VerifyOrigin(b, b.DBID, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
	decoded, rest, err := DecodeBatch(EncodeBatch(nil, b), DefaultLimits())
	if err != nil || len(rest) != 0 {
		t.Fatalf("decode: %v", err)
	}
	if err := VerifyOrigin(decoded, b.DBID, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
}

func TestOriginTamperingAndCrossDatabase(t *testing.T) {
	original, key := originVector()
	pub := key.Public().(ed25519.PublicKey)
	cases := map[string]func(*MutationBatch){
		"DBID":        func(b *MutationBatch) { b.DBID[0]++ },
		"Origin":      func(b *MutationBatch) { b.OriginNode[0]++ },
		"Sequence":    func(b *MutationBatch) { b.Sequence++ },
		"TxID":        func(b *MutationBatch) { b.TxID[0]++ },
		"HLC":         func(b *MutationBatch) { b.HLC++ },
		"SchemaEpoch": func(b *MutationBatch) { b.SchemaEpoch++ },
		"SchemaHash":  func(b *MutationBatch) { b.SchemaHash[0]++ },
		"Digest":      func(b *MutationBatch) { b.MutationDigest[0]++ },
		"Signature":   func(b *MutationBatch) { b.OriginSignature[0]++ },
		"Unsigned":    func(b *MutationBatch) { b.SignatureVersion = 0 },
		"Value":       func(b *MutationBatch) { b.Mutations[0].Value = Text("evil") },
		"Row":         func(b *MutationBatch) { b.Mutations[0].RowID[0]++ },
		"Flags":       func(b *MutationBatch) { b.Mutations[0].Flags++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			b := *original
			b.Mutations = append([]Mutation(nil), original.Mutations...)
			mutate(&b)
			if err := VerifyOrigin(&b, original.DBID, pub); err == nil {
				t.Fatal("tampering accepted")
			}
		})
	}
	if err := VerifyOrigin(original, ids.DBID{99}, pub); !errors.Is(err, ErrOriginSignature) {
		t.Fatalf("cross DB: %v", err)
	}
	if err := VerifyOrigin(original, original.DBID, ed25519.NewKeyFromSeed(make([]byte, 32)).Public().(ed25519.PublicKey)[:31]); !errors.Is(err, ErrOriginSignature) {
		t.Fatalf("bad public key length: %v", err)
	}
}

func TestOriginMutationOrderAndChunkIdentity(t *testing.T) {
	b, key := originVector()
	b.Mutations = append(b.Mutations, Mutation{TableID: 8, RowID: ids.RowID{9}, ColumnID: 10, Value: Text("last")})
	if err := SignOrigin(b, b.DBID, key); err != nil {
		t.Fatal(err)
	}
	b.Mutations[0], b.Mutations[1] = b.Mutations[1], b.Mutations[0]
	if err := VerifyOrigin(b, b.DBID, key.Public().(ed25519.PublicKey)); !errors.Is(err, ErrOriginDigest) {
		t.Fatal(err)
	}
	b.Mutations[0], b.Mutations[1] = b.Mutations[1], b.Mutations[0]
	frames, err := EncodeTransactionChunks(b, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	c, err := DecodeTransactionChunk(frames[0], 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyOriginIdentity(c.OriginBatch(), b.DBID, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
	c.HLC++
	if err := VerifyOriginIdentity(c.OriginBatch(), b.DBID, key.Public().(ed25519.PublicKey)); err == nil {
		t.Fatal("chunk identity tampering accepted")
	}
}

func BenchmarkOrigin(b *testing.B) {
	for _, size := range []int{32, 64 << 10, 1 << 20} {
		b.Run(fmtSize(size), func(b *testing.B) {
			tx, key := originVector()
			tx.Mutations[0].Value = Blob(make([]byte, size))
			pub := key.Public().(ed25519.PublicKey)
			_ = SignOrigin(tx, tx.DBID, key)
			b.Run("sign", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					if err := SignOrigin(tx, tx.DBID, key); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("verify", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					if err := VerifyOrigin(tx, tx.DBID, pub); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("forward", func(b *testing.B) {
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					_ = EncodeBatch(nil, tx)
				}
			})
		})
	}
}
func fmtSize(n int) string {
	if n == 32 {
		return "32B"
	}
	if n == 64<<10 {
		return "64KiB"
	}
	return "1MiB"
}
