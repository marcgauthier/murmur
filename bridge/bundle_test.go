package bridge

import (
	"reflect"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

type bundleFixture struct {
	signer    *SignerKey
	recipient *RecipientKey
	trust     *TrustStore
	limits    Limits
	domain    ids.DBID
}

func newBundleFixture(t *testing.T) *bundleFixture {
	t.Helper()
	signer, err := GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := GenerateRecipientKey()
	if err != nil {
		t.Fatal(err)
	}
	trust := NewTrustStore()
	if err := trust.AddSigner(signer.ID, "stream-a"); err != nil {
		t.Fatal(err)
	}
	if err := trust.AddRecipient(recipient); err != nil {
		t.Fatal(err)
	}
	return &bundleFixture{
		signer:    signer,
		recipient: recipient,
		trust:     trust,
		limits:    Limits{}.withDefaults(),
		domain:    ids.NewDBID(),
	}
}

func sampleBatches(first uint64, n int) []Batch {
	out := make([]Batch, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, Batch{
			TxID:     ids.NewTxID(),
			Origin:   ids.NewNodeID(),
			Sequence: first + uint64(i),
			HLC:      100 + uint64(i),
			Records: []Record{{
				Table: "contacts", Row: ids.NewRowID(), Op: RecordPut,
				Columns: []ColumnValue{
					{Column: "name", Value: codec.Text("ann")},
					{Column: "score", Value: codec.Int(10)},
					{Column: "meta", Value: codec.Blob([]byte{0x1, 0x2})},
					{Column: "nick", Value: codec.Value{}},
				},
			}},
		})
	}
	return out
}

func (f *bundleFixture) manifest(batches []Batch) Manifest {
	txIDs := make([]ids.TxID, len(batches))
	for i, b := range batches {
		txIDs[i] = b.TxID
	}
	return Manifest{
		SourceDomain: f.domain,
		Stream:       "stream-a",
		SeqFirst:     batches[0].Sequence,
		SeqLast:      batches[len(batches)-1].Sequence,
		TxIDs:        txIDs,
		SchemaEpoch:  3,
		SchemaHash:   [32]byte{0xab},
	}
}

func TestBundleRoundTrip(t *testing.T) {
	f := newBundleFixture(t)
	batches := sampleBatches(10, 3)
	// Mix in a row delete.
	batches[1].Records = append(batches[1].Records, Record{
		Table: "contacts", Row: ids.NewRowID(), Op: RecordDelete,
	})
	sealed, err := SealBatches(f.signer, f.recipient.Public(), f.manifest(batches), batches, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenBundle(sealed, f.trust, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	m := opened.Manifest
	if m.BundleID.IsZero() || m.SourceDomain != f.domain || m.Stream != "stream-a" ||
		m.SeqFirst != 10 || m.SeqLast != 12 || m.SchemaEpoch != 3 || m.SchemaHash != [32]byte{0xab} {
		t.Fatalf("manifest = %+v", m)
	}
	if opened.Signer != f.signer.ID {
		t.Fatal("signer mismatch")
	}
	if len(opened.Batches) != 3 {
		t.Fatalf("batches = %d", len(opened.Batches))
	}
	for i, b := range opened.Batches {
		want := batches[i]
		if b.TxID != want.TxID || b.Origin != want.Origin || b.Sequence != want.Sequence || b.HLC != want.HLC {
			t.Fatalf("batch %d identity drift: %+v", i, b)
		}
		if !reflect.DeepEqual(b.Records, want.Records) {
			t.Fatalf("batch %d records drift", i)
		}
	}
}

func TestBundleTamper(t *testing.T) {
	f := newBundleFixture(t)
	batches := sampleBatches(1, 2)
	sealed, err := SealBatches(f.signer, f.recipient.Public(), f.manifest(batches), batches, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	// Offsets span every framing field and the sealed payload.
	probes := []int{0, 3, 4, 5, 6, 40, 70, 100, 130, 160, 190, 230, 260, 262, 268, 269, len(sealed) / 2, len(sealed) - 1}
	for _, off := range probes {
		mut := append([]byte(nil), sealed...)
		mut[off] ^= 0xff
		if _, err := OpenBundle(mut, f.trust, f.limits); err == nil {
			t.Fatalf("tamper at offset %d accepted", off)
		}
	}
	// Truncation and trailing garbage fail.
	for _, mut := range [][]byte{sealed[:len(sealed)-10], append(append([]byte(nil), sealed...), 0x00)} {
		if _, err := OpenBundle(mut, f.trust, f.limits); err == nil {
			t.Fatal("malformed framing accepted")
		}
	}
}

func TestBundleTrust(t *testing.T) {
	f := newBundleFixture(t)
	batches := sampleBatches(5, 1)
	sealed, err := SealBatches(f.signer, f.recipient.Public(), f.manifest(batches), batches, f.limits)
	if err != nil {
		t.Fatal(err)
	}

	// Unknown signer / unauthorized stream / unknown recipient.
	empty := NewTrustStore()
	if err := empty.AddRecipient(f.recipient); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBundle(sealed, empty, f.limits); err == nil {
		t.Fatal("unknown signer accepted")
	}
	wrongStream := NewTrustStore()
	if err := wrongStream.AddSigner(f.signer.ID, "other-stream"); err != nil {
		t.Fatal(err)
	}
	if err := wrongStream.AddRecipient(f.recipient); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBundle(sealed, wrongStream, f.limits); err == nil {
		t.Fatal("unauthorized stream accepted")
	}
	otherRecip, err := GenerateRecipientKey()
	if err != nil {
		t.Fatal(err)
	}
	wrongRecip := NewTrustStore()
	if err := wrongRecip.AddSigner(f.signer.ID, "stream-a"); err != nil {
		t.Fatal(err)
	}
	if err := wrongRecip.AddRecipient(otherRecip); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBundle(sealed, wrongRecip, f.limits); err == nil {
		t.Fatal("wrong recipient accepted")
	}

	// Rotation: the successor verifies while the retained predecessor
	// still opens pending bundles; retiring it closes that window.
	next, err := GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.trust.AddSigner(next.ID, "stream-a"); err != nil {
		t.Fatal(err)
	}
	b2 := sampleBatches(6, 1)
	sealed2, err := SealBatches(next, f.recipient.Public(), f.manifest(b2), b2, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBundle(sealed, f.trust, f.limits); err != nil {
		t.Fatalf("retained signer rejected: %v", err)
	}
	if _, err := OpenBundle(sealed2, f.trust, f.limits); err != nil {
		t.Fatalf("successor rejected: %v", err)
	}
	f.trust.RemoveSigner(f.signer.ID)
	if _, err := OpenBundle(sealed, f.trust, f.limits); err == nil {
		t.Fatal("retired signer still accepted")
	}
	if _, err := OpenBundle(sealed2, f.trust, f.limits); err != nil {
		t.Fatalf("successor rejected after rotation: %v", err)
	}
}

func TestBundleBounds(t *testing.T) {
	f := newBundleFixture(t)
	batches := sampleBatches(1, 2)
	sealed, err := SealBatches(f.signer, f.recipient.Public(), f.manifest(batches), batches, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	tight := f.limits
	tight.MaxBundleBytes = len(sealed) - 1
	if _, err := OpenBundle(sealed, f.trust, tight); err == nil {
		t.Fatal("oversized bundle accepted")
	}
	// Decompression bombs fail: seal big, open small.
	big := f.limits
	bigBatch := sampleBatches(1, 1)
	bigBatch[0].Records[0].Columns = append(bigBatch[0].Records[0].Columns,
		ColumnValue{Column: "pad", Value: codec.Blob(make([]byte, 1<<20))})
	sealedBig, err := SealBatches(f.signer, f.recipient.Public(), f.manifest(bigBatch), bigBatch, big)
	if err != nil {
		t.Fatal(err)
	}
	small := f.limits
	small.MaxPayloadBytes = 1024
	if _, err := OpenBundle(sealedBig, f.trust, small); err == nil {
		t.Fatal("decompression bomb accepted")
	}
}

func TestSealValidation(t *testing.T) {
	f := newBundleFixture(t)
	batches := sampleBatches(1, 2)
	m := f.manifest(batches)
	seal := func(mm Manifest, bb []Batch, l Limits) error {
		_, err := SealBatches(f.signer, f.recipient.Public(), mm, bb, l)
		return err
	}
	if err := seal(m, nil, f.limits); err == nil {
		t.Fatal("empty batches sealed")
	}
	gapped := append([]Batch(nil), batches...)
	gapped[1].Sequence = 99
	if err := seal(m, gapped, f.limits); err == nil {
		t.Fatal("non-contiguous batches sealed")
	}
	swapped := m
	swapped.TxIDs = []ids.TxID{batches[1].TxID, batches[0].TxID}
	if err := seal(swapped, batches, f.limits); err == nil {
		t.Fatal("txid mismatch sealed")
	}
	noDomain := m
	noDomain.SourceDomain = ids.DBID{}
	if err := seal(noDomain, batches, f.limits); err == nil {
		t.Fatal("missing domain sealed")
	}
	tight := f.limits
	tight.MaxTransactions = 1
	if err := seal(m, batches, tight); err == nil {
		t.Fatal("over-limit batches sealed")
	}
	if _, err := SealBatches(nil, f.recipient.Public(), m, batches, f.limits); err == nil {
		t.Fatal("nil signer sealed")
	}
}

// sealCrafted bypasses SealBatches validation to exercise OpenBundle's
// independent checks (white-box forgery).
func sealCrafted(t *testing.T, f *bundleFixture, m Manifest, batches []Batch) []byte {
	t.Helper()
	section, err := encodeBatches(batches)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := encodePayload(m, section)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := compressPayload(payload, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealEnvelope(f.signer, f.recipient.Public(), compressed, f.limits)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

func TestOpenRejectsForgedConsistency(t *testing.T) {
	f := newBundleFixture(t)
	batches := sampleBatches(1, 2)

	// Wrong payload digest (valid signature over the bytes, stale digest).
	m := f.manifest(batches)
	m.PayloadDigest = [32]byte{0x1}
	if _, err := OpenBundle(sealCrafted(t, f, m, batches), f.trust, f.limits); err == nil {
		t.Fatal("forged digest accepted")
	}
	// Manifest range disagrees with the batches it carries.
	m2 := f.manifest(batches)
	m2.SeqLast = 99
	if _, err := OpenBundle(sealCrafted(t, f, m2, batches), f.trust, f.limits); err == nil {
		t.Fatal("forged range accepted")
	}
}
