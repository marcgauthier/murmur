package codec

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
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

func originV5Vector() (*MutationBatch, ed25519.PrivateKey) {
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	b := &MutationBatch{ProtocolVersion: 5, DBID: ids.DBID{1}, OriginNode: ids.NodeID{2}, Sequence: 3, TxID: ids.TxID{4}, HLC: 5, SchemaEpoch: 6, SchemaHash: [32]byte{7}, Mutations: []Mutation{
		{TableID: 8, RowID: ids.RowID{9}, ColumnID: 10, Value: Text("hello"), Policy: schema.LWW},
		{TableID: 8, RowID: ids.RowID{10}, ColumnID: 11, Value: Blob([]byte{1, 2, 3}), Policy: schema.OR_SET, Records: []CRDTRecord{{Key: []byte("k1"), Data: []byte("d1")}, {Key: []byte("k2"), Data: []byte("d2")}}},
		{TableID: 8, RowID: ids.RowID{11}, ColumnID: ColumnTombstone, Value: Null(), Flags: FlagTombstone, Policy: schema.LWW},
	}}
	if err := SignOrigin(b, b.DBID, key); err != nil {
		panic(err)
	}
	return b, key
}

func cloneBatch(b *MutationBatch) *MutationBatch {
	out := *b
	out.Mutations = make([]Mutation, len(b.Mutations))
	for i := range b.Mutations {
		out.Mutations[i] = b.Mutations[i]
		// Deep-copy record bytes: tamper cases mutate them in place and
		// must never pollute the shared original.
		records := make([]CRDTRecord, len(b.Mutations[i].Records))
		for j := range b.Mutations[i].Records {
			records[j].Key = append([]byte(nil), b.Mutations[i].Records[j].Key...)
			records[j].Data = append([]byte(nil), b.Mutations[i].Records[j].Data...)
		}
		out.Mutations[i].Records = records
	}
	return &out
}

// Inserting a duplicate mutation must invalidate the signature: the digest
// covers the count and every mutation in order.
func TestOriginDuplicateMutationRejected(t *testing.T) {
	original, key := originV5Vector()
	pub := key.Public().(ed25519.PublicKey)
	b := cloneBatch(original)
	b.Mutations = append(b.Mutations, original.Mutations[0])
	if err := VerifyOrigin(b, original.DBID, pub); !errors.Is(err, ErrOriginDigest) {
		t.Fatalf("duplicate mutation: %v", err)
	}
}

// Policy and CRDT records are covered by the digest at protocol v5 (the
// production floor): flipping, reordering, dropping, or adding any of them
// must invalidate the signature.
func TestOriginV5PolicyAndRecordsTamperingRejected(t *testing.T) {
	original, key := originV5Vector()
	pub := key.Public().(ed25519.PublicKey)
	cases := map[string]func(*MutationBatch){
		"policy flip":      func(b *MutationBatch) { b.Mutations[1].Policy = schema.PN_COUNTER },
		"policy on LWW":    func(b *MutationBatch) { b.Mutations[0].Policy = schema.MAX },
		"records reorder":  func(b *MutationBatch) { r := b.Mutations[1].Records; r[0], r[1] = r[1], r[0] },
		"record key flip":  func(b *MutationBatch) { b.Mutations[1].Records[0].Key[0]++ },
		"record data flip": func(b *MutationBatch) { b.Mutations[1].Records[1].Data[0]++ },
		"record drop":      func(b *MutationBatch) { b.Mutations[1].Records = b.Mutations[1].Records[:1] },
		"record append":    func(b *MutationBatch) { b.Mutations[1].Records = append(b.Mutations[1].Records, CRDTRecord{Key: []byte("k3")}) },
		"records on LWW":   func(b *MutationBatch) { b.Mutations[0].Records = []CRDTRecord{{Key: []byte("x")}} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			b := cloneBatch(original)
			mutate(b)
			if err := VerifyOrigin(b, original.DBID, pub); !errors.Is(err, ErrOriginDigest) {
				t.Fatalf("%s: %v", name, err)
			}
		})
	}
	if err := VerifyOrigin(original, original.DBID, pub); err != nil {
		t.Fatalf("valid v5 batch: %v", err)
	}
}

// nil and empty values canonicalize identically: same digest bytes, same
// decoded form, interchangeable under one signature.
func TestOriginNilEmptyValueCanonicalization(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	pub := key.Public().(ed25519.PublicKey)
	mkbatch := func(v Value) *MutationBatch {
		return &MutationBatch{ProtocolVersion: 5, DBID: ids.DBID{1}, OriginNode: ids.NodeID{2}, Sequence: 3, TxID: ids.TxID{4}, HLC: 5, SchemaEpoch: 1, Mutations: []Mutation{{TableID: 8, RowID: ids.RowID{9}, ColumnID: 10, Value: v}}}
	}
	nilBatch, emptyBatch := mkbatch(Blob(nil)), mkbatch(Blob([]byte{}))
	if MutationDigest(nilBatch) != MutationDigest(emptyBatch) {
		t.Fatal("nil and empty blob digests differ")
	}
	if err := SignOrigin(nilBatch, nilBatch.DBID, key); err != nil {
		t.Fatal(err)
	}
	// Same envelope verifies on the empty-valued twin.
	emptyBatch.MutationDigest, emptyBatch.SignatureVersion, emptyBatch.OriginSignature =
		nilBatch.MutationDigest, nilBatch.SignatureVersion, nilBatch.OriginSignature
	if err := VerifyOrigin(emptyBatch, nilBatch.DBID, pub); err != nil {
		t.Fatalf("canonical twin rejected: %v", err)
	}
	for name, b := range map[string]*MutationBatch{"nil": nilBatch, "empty": emptyBatch} {
		decoded, rest, err := DecodeBatch(EncodeBatch(nil, b), DefaultLimits())
		if err != nil || len(rest) != 0 {
			t.Fatalf("%s round-trip: %v", name, err)
		}
		if MutationDigest(decoded) != MutationDigest(b) {
			t.Fatalf("%s digest changed across round-trip", name)
		}
		if err := VerifyOrigin(decoded, nilBatch.DBID, pub); err != nil {
			t.Fatalf("%s decoded rejected: %v", name, err)
		}
	}
}

// A v5 batch exercising every signed dimension must survive
// encode/decode with identical digest and signature input.
func TestOriginV5RoundTripVector(t *testing.T) {
	original, key := originV5Vector()
	pub := key.Public().(ed25519.PublicKey)
	decoded, rest, err := DecodeBatch(EncodeBatch(nil, original), DefaultLimits())
	if err != nil || len(rest) != 0 {
		t.Fatalf("decode: %v", err)
	}
	if MutationDigest(decoded) != original.MutationDigest {
		t.Fatal("digest changed across round-trip")
	}
	if hex.EncodeToString(OriginSigningBytes(decoded)) != hex.EncodeToString(OriginSigningBytes(original)) {
		t.Fatal("signature input changed across round-trip")
	}
	if err := VerifyOrigin(decoded, original.DBID, pub); err != nil {
		t.Fatalf("decoded batch rejected: %v", err)
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
