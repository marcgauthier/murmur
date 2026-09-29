package bridge

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
)

func testOutboxKey(id string, seed byte) OutboxKey {
	var key [32]byte
	for i := range key {
		key[i] = seed + byte(i)
	}
	return OutboxKey{ID: id, Key: key}
}

func appendMarker(t *testing.T, o *Outbox, origin ids.NodeID, originSeq uint64, marker string) uint64 {
	t.Helper()
	seq, err := o.Append(Batch{TxID: ids.NewTxID(), Origin: origin, Sequence: originSeq, HLC: originSeq,
		Records: []Record{{Table: "contacts", Row: ids.NewRowID(), Op: RecordPut,
			Columns: []ColumnValue{{Column: "name", Value: codec.Text(marker)}}}}},
		origin, originSeq, 1, [32]byte{0x1})
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func journalText(t *testing.T, dir string) string {
	t.Helper()
	var sb strings.Builder
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sb.Write(raw)
		sb.WriteByte('\n')
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return sb.String()
}

type envelopePeek struct {
	V   int    `json:"v"`
	Key string `json:"key_id"`
}

func envelopeOf(t *testing.T, dir string, seq uint64) envelopePeek {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "events", eventName(seq)))
	if err != nil {
		t.Fatal(err)
	}
	var env envelopePeek
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	return env
}

const encryptMarker = "s3cr3t-payload-marker-0123456789abcdef"

func TestOutboxEncryptedRoundTrip(t *testing.T) {
	dir := t.TempDir()
	k1 := testOutboxKey("k1", 0x11)
	o, err := OpenOutboxEncrypted(dir, Limits{}, k1)
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	appendMarker(t, o, origin, 1, encryptMarker)
	appendMarker(t, o, origin, 2, "plain-bob")
	if env := envelopeOf(t, dir, 1); env.V != 2 || env.Key != "k1" {
		t.Fatalf("envelope = %+v", env)
	}
	// No record payload or table name rests in plaintext anywhere.
	dump := journalText(t, dir)
	for _, marker := range []string{encryptMarker, "plain-bob", "contacts", "BatchB64", "batch"} {
		if strings.Contains(dump, marker) {
			t.Fatalf("journal contains plaintext %q", marker)
		}
	}
	// State rewrites stay sealed and decode identically.
	if err := o.MarkFailed(1, errTestBoom); err != nil {
		t.Fatal(err)
	}
	if err := o.MarkPublished(2); err != nil {
		t.Fatal(err)
	}
	if dump := journalText(t, dir); strings.Contains(dump, encryptMarker) {
		t.Fatal("rewrite leaked plaintext")
	}
	o2, err := OpenOutboxEncrypted(dir, Limits{}, k1)
	if err != nil {
		t.Fatal(err)
	}
	pending := o2.Pending(0)
	if len(pending) != 1 || pending[0].Seq != 1 {
		t.Fatalf("pending = %+v", pending)
	}
	got := pending[0].Batch.Records[0].Columns[0].Value.S
	if got != encryptMarker {
		t.Fatalf("recovered value = %q", got)
	}
	if pending[0].LastError != errTestBoom.Error() || pending[0].Attempts != 1 {
		t.Fatalf("recovered failure state = %+v", pending[0])
	}
}

var errTestBoom = errBoom{}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

func TestOutboxWrongKeyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	k1 := testOutboxKey("k1", 0x11)
	o, err := OpenOutboxEncrypted(dir, Limits{}, k1)
	if err != nil {
		t.Fatal(err)
	}
	appendMarker(t, o, ids.NewNodeID(), 1, encryptMarker)

	otherID := testOutboxKey("k2", 0x22)
	if _, err := OpenOutboxEncrypted(dir, Limits{}, otherID); err == nil {
		t.Fatal("unknown key id opened the journal")
	}
	sameID := testOutboxKey("k1", 0xFF)
	if _, err := OpenOutboxEncrypted(dir, Limits{}, sameID); err == nil {
		t.Fatal("wrong key bytes opened the journal")
	}
	// The journal itself is untouched and still opens with the right key.
	if _, err := OpenOutboxEncrypted(dir, Limits{}, k1); err != nil {
		t.Fatalf("reopen: %v", err)
	}
}

func TestOutboxTamperFailsClosed(t *testing.T) {
	build := func(t *testing.T) string {
		dir := t.TempDir()
		o, err := OpenOutboxEncrypted(dir, Limits{}, testOutboxKey("k1", 0x11))
		if err != nil {
			t.Fatal(err)
		}
		origin := ids.NewNodeID()
		appendMarker(t, o, origin, 1, encryptMarker)
		appendMarker(t, o, origin, 2, "second-row-marker")
		return dir
	}
	rewrite := func(t *testing.T, dir string, seq uint64, mutate func(*outboxEnvelope)) {
		path := filepath.Join(dir, "events", eventName(seq))
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var env outboxEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatal(err)
		}
		mutate(&env)
		raw, err = json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	flip := func(s string) string {
		b := []byte(s)
		if b[4] == 'A' {
			b[4] = 'B'
		} else {
			b[4] = 'A'
		}
		return string(b)
	}
	t.Run("ciphertext", func(t *testing.T) {
		dir := build(t)
		rewrite(t, dir, 1, func(env *outboxEnvelope) { env.Data = flip(env.Data) })
		if _, err := OpenOutboxEncrypted(dir, Limits{}, testOutboxKey("k1", 0x11)); err == nil {
			t.Fatal("tampered ciphertext opened")
		}
	})
	t.Run("nonce", func(t *testing.T) {
		dir := build(t)
		rewrite(t, dir, 2, func(env *outboxEnvelope) { env.Nonce = flip(env.Nonce) })
		if _, err := OpenOutboxEncrypted(dir, Limits{}, testOutboxKey("k1", 0x11)); err == nil {
			t.Fatal("tampered nonce opened")
		}
	})
	t.Run("transplant", func(t *testing.T) {
		dir := build(t)
		// Move event 2's ciphertext into event 1's file: the sequence
		// binding must reject it.
		raw2, err := os.ReadFile(filepath.Join(dir, "events", eventName(2)))
		if err != nil {
			t.Fatal(err)
		}
		var env2 outboxEnvelope
		if err := json.Unmarshal(raw2, &env2); err != nil {
			t.Fatal(err)
		}
		rewrite(t, dir, 1, func(env *outboxEnvelope) {
			env.Data, env.Nonce = env2.Data, env2.Nonce
		})
		if _, err := OpenOutboxEncrypted(dir, Limits{}, testOutboxKey("k1", 0x11)); err == nil {
			t.Fatal("transplanted ciphertext opened")
		}
	})
}

func TestOutboxKeyRotation(t *testing.T) {
	dir := t.TempDir()
	k1, k2 := testOutboxKey("k1", 0x11), testOutboxKey("k2", 0x22)
	o, err := OpenOutboxEncrypted(dir, Limits{}, k1)
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	appendMarker(t, o, origin, 1, encryptMarker)
	if err := o.RotateKey(k2); err != nil {
		t.Fatal(err)
	}
	if ids := o.KeyIDs(); len(ids) != 2 || ids[0] != "k1" || ids[1] != "k2" {
		t.Fatalf("ring = %v", ids)
	}
	appendMarker(t, o, origin, 2, "second-row-marker")
	if env := envelopeOf(t, dir, 1); env.Key != "k1" {
		t.Fatalf("event 1 envelope = %+v", env)
	}
	if env := envelopeOf(t, dir, 2); env.Key != "k2" {
		t.Fatalf("event 2 envelope = %+v", env)
	}
	// Both keys open the mixed journal after restart.
	o2, err := OpenOutboxEncrypted(dir, Limits{}, k1, k2)
	if err != nil {
		t.Fatal(err)
	}
	if got := o2.Pending(0); len(got) != 2 {
		t.Fatalf("pending = %d", len(got))
	}
	// Rekey converges the journal onto the successor; the predecessor
	// can then retire without losing pending exports.
	if err := o2.Rekey("k2"); err != nil {
		t.Fatal(err)
	}
	if env := envelopeOf(t, dir, 1); env.Key != "k2" {
		t.Fatalf("rekeyed event 1 envelope = %+v", env)
	}
	o3, err := OpenOutboxEncrypted(dir, Limits{}, k2)
	if err != nil {
		t.Fatal(err)
	}
	if got := o3.Pending(0); len(got) != 2 || got[0].Batch.Records[0].Columns[0].Value.S != encryptMarker {
		t.Fatalf("rekeyed pending = %+v", got)
	}
	if err := o3.DropKey("k1"); err == nil {
		t.Fatal("dropped an absent key")
	}
	if err := o3.DropKey("k2"); err == nil {
		t.Fatal("dropped the last key")
	}
	if _, err := OpenOutboxEncrypted(dir, Limits{}, k1); err == nil {
		t.Fatal("retired key opened the rekeyed journal")
	}
}

func TestOutboxKeyValidation(t *testing.T) {
	if _, err := OpenOutboxEncrypted(t.TempDir(), Limits{}); err == nil {
		t.Fatal("keyless encrypted open succeeded")
	}
	k1 := testOutboxKey("k1", 0x11)
	if _, err := OpenOutboxEncrypted(t.TempDir(), Limits{}, k1, k1); err == nil {
		t.Fatal("duplicate key ids accepted")
	}
	bad := OutboxKey{ID: "", Key: k1.Key}
	if _, err := OpenOutboxEncrypted(t.TempDir(), Limits{}, bad); err == nil {
		t.Fatal("empty key id accepted")
	}
	var zero [32]byte
	if _, err := OpenOutboxEncrypted(t.TempDir(), Limits{}, OutboxKey{ID: "z", Key: zero}); err == nil {
		t.Fatal("zero key accepted")
	}
	o, err := OpenOutboxEncrypted(t.TempDir(), Limits{}, k1)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.RotateKey(OutboxKey{}); err == nil {
		t.Fatal("rotated in an invalid key")
	}
	if err := o.Rekey("ghost"); err == nil {
		t.Fatal("rekeyed to an absent key")
	}
}

func TestOutboxLegacyInterop(t *testing.T) {
	// Plaintext journals keep working unencrypted...
	plainDir := t.TempDir()
	o, err := OpenOutbox(plainDir, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	appendMarker(t, o, ids.NewNodeID(), 1, encryptMarker)
	if _, err := OpenOutbox(plainDir, Limits{}); err != nil {
		t.Fatalf("legacy reopen: %v", err)
	}
	// ...but refuse to open once encryption is configured.
	if _, err := OpenOutboxEncrypted(plainDir, Limits{}, testOutboxKey("k1", 0x11)); err == nil {
		t.Fatal("plaintext journal opened with keys configured")
	}
	// Encrypted journals refuse to open without keys.
	encDir := t.TempDir()
	enc, err := OpenOutboxEncrypted(encDir, Limits{}, testOutboxKey("k1", 0x11))
	if err != nil {
		t.Fatal(err)
	}
	appendMarker(t, enc, ids.NewNodeID(), 1, encryptMarker)
	if _, err := OpenOutbox(encDir, Limits{}); err == nil {
		t.Fatal("encrypted journal opened without keys")
	}
}

func TestOutboxEncryptedPublishes(t *testing.T) {
	dir := t.TempDir()
	k1 := testOutboxKey("k1", 0x11)
	o, err := OpenOutboxEncrypted(dir, Limits{}, k1)
	if err != nil {
		t.Fatal(err)
	}
	origin := ids.NewNodeID()
	appendMarker(t, o, origin, 1, encryptMarker)
	signer, recip, trust := inboxKeys(t, "s")
	lowDB := stubDB{ids.NewDBID()}
	exp, err := NewExporter(lowDB, Config{Role: RoleLowExporter, Domain: lowDB.id, Stream: "s"}, &stubPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	pending := o.Pending(0)
	if len(pending) != 1 {
		t.Fatalf("pending = %d", len(pending))
	}
	seal := NewBundleSealer(exp, signer, recip.Public())
	artifact, err := seal([]Batch{pending[0].Batch}, 1, 1, pending[0].SchemaEpoch, pending[0].SchemaHash)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := OpenBundle(artifact.Data, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	got := bundle.Batches[0].Records[0].Columns[0].Value.S
	if got != encryptMarker {
		t.Fatalf("published value = %q", got)
	}
}
