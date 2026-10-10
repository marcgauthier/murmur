package bridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/schema"
)

func openTypedContactDBAt(t *testing.T, dir string, node db.NodeID) *db.DB {
	return openTypedContactDBWithFaults(t, dir, node, nil, false)
}

func insertTypedContact(t *testing.T, database *db.DB, id ids.RowID, name string, score int64) {
	t.Helper()
	if err := database.InsertItem(context.Background(), &typedBridgeContactRecord{ID: id, Name: &name, Score: &score}); err != nil {
		t.Fatal(err)
	}
}

func defineExpandedContact(t *testing.T) db.TableDefinition {
	t.Helper()
	definition, err := db.Model[typedBridgeContactExpanded](db.ModelOptions{
		Name: "contacts", TableID: 90,
		RecordOptions: db.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Name": 2, "Score": 3, "Email": 4},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func defineBaseContact(t *testing.T) db.TableDefinition {
	t.Helper()
	definition, err := db.Model[typedBridgeContactRecord](db.ModelOptions{
		Name: "contacts", TableID: 90,
		RecordOptions: db.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Name": 2, "Score": 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func defineTypedNope(t *testing.T) db.TableDefinition {
	t.Helper()
	definition, err := db.Model[typedBridgeNopeRecord](db.ModelOptions{
		Name: "nope", TableID: 91,
		RecordOptions: db.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "C": 2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func defineContactRatio(t *testing.T) db.TableDefinition {
	t.Helper()
	definition, err := db.Model[typedBridgeContactRatioRecord](db.ModelOptions{
		Name: "contacts", TableID: 90,
		RecordOptions: db.RecordOptions{
			FieldIDs:      map[string]uint32{"ID": 1, "Name": 2, "Score": 3, "Ratio": 4},
			MergePolicies: map[string]db.RecordMergePolicy{"Ratio": db.RecordMergeMax},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func openTypedContactDBWithFaults(t *testing.T, dir string, node db.NodeID, faults *failStorage, files bool) *db.DB {
	t.Helper()
	definition := defineBaseContact(t)
	spoolCfg := db.DefaultSpoolConfig()
	if faults != nil {
		spoolCfg.Faults = faults.hooks()
	}
	fileCfg := db.FilesConfig{}
	if files {
		fileCfg = db.FilesConfig{Enabled: true, ObjectKey: bytes.Repeat([]byte{0x45}, 32)}
	}
	database, err := db.Open(context.Background(), db.Config{
		Path:          dir,
		NodeID:        node,
		OriginSigning: testidentity.Config(node),
		Spool:         spoolCfg,
		Encryption:    db.EncryptionConfig{Key: bytes.Repeat([]byte{0x44}, 32), KeyID: "typed-bridge-import-test"},
		Tables:        []db.TableDefinition{definition},
		Files:         fileCfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func sealTypedContacts(t *testing.T, signer *SignerKey, recipient *RecipientKey, manifest Manifest, batches []Batch) Artifact {
	t.Helper()
	encoded := encodeTypedContactsBatches(t, batches)
	return sealForInboxWithManifest(t, signer, recipient, manifest, encoded)
}

func sealTypedContactsForInbox(t *testing.T, signer *SignerKey, recipient *RecipientKey, stream string, first uint64, batches []Batch) Artifact {
	t.Helper()
	for i := range batches {
		batches[i].Sequence = first + uint64(i)
	}
	txIDs := make([]ids.TxID, len(batches))
	for i, batch := range batches {
		txIDs[i] = batch.TxID
	}
	sum := sha256.Sum256([]byte("bridge-test-source:" + stream))
	var source ids.DBID
	copy(source[:], sum[:len(source)])
	manifest := Manifest{
		SourceDomain: source, Stream: stream,
		SeqFirst: first, SeqLast: first + uint64(len(batches)) - 1,
		TxIDs: txIDs, SchemaEpoch: 1,
	}
	return sealTypedContacts(t, signer, recipient, manifest, batches)
}

func encodeTypedContactsBatches(t *testing.T, batches []Batch) []Batch {
	t.Helper()
	compiled, err := recordcodec.Compile(reflect.TypeFor[typedBridgeContactExpanded](), recordcodec.CompileOptions{
		TableID: 90, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2, "Score": 3, "Email": 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	nopeCompiled, err := recordcodec.Compile(reflect.TypeFor[typedBridgeNopeRecord](), recordcodec.CompileOptions{
		TableID: 91, PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "C": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded := append([]Batch(nil), batches...)
	for bi := range encoded {
		encoded[bi].Records = append([]Record(nil), batches[bi].Records...)
		for ri := range encoded[bi].Records {
			rec := &encoded[bi].Records[ri]
			if rec.Op != RecordPut {
				continue
			}
			if rec.Table == "nope" {
				value := &typedBridgeNopeRecord{ID: rec.Row}
				includeValue := false
				for _, column := range rec.Columns {
					if !strings.EqualFold(column.Column, "C") {
						t.Fatalf("unexpected nope field %q", column.Column)
					}
					includeValue = true
					if column.Value.Type == codec.TypeNull {
						value.C = nil
					} else if column.Value.Type == codec.TypeText {
						text := column.Value.S
						value.C = &text
					} else {
						t.Fatalf("nope field fixture has type %v", column.Value.Type)
					}
				}
				columns := make([]ColumnValue, 0, 2)
				for _, field := range []struct {
					name string
					id   uint32
				}{{"ID", 1}, {"C", 2}} {
					if field.name == "C" && !includeValue {
						continue
					}
					payload, err := recordcodec.EncodeField(nopeCompiled, field.id, value, nil, recordcodec.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					columns = append(columns, ColumnValue{Column: field.name, Policy: schema.LWW, Value: codec.Blob(payload)})
				}
				rec.Columns = columns
				continue
			}
			if rec.Table != "contacts" {
				continue
			}
			contact := &typedBridgeContactExpanded{ID: rec.Row}
			include := map[string]bool{"ID": true}
			primaryNull := false
			var passthrough []ColumnValue
			for _, column := range rec.Columns {
				switch strings.ToLower(column.Column) {
				case "id":
					if column.Value.Type == codec.TypeNull {
						primaryNull = true
					}
				case "name":
					if column.Value.Type == codec.TypeNull {
						contact.Name, include["Name"] = nil, true
						continue
					}
					if column.Value.Type != codec.TypeText {
						t.Fatalf("name fixture has type %v", column.Value.Type)
					}
					name := column.Value.S
					contact.Name, include["Name"] = &name, true
				case "score":
					if column.Value.Type != codec.TypeInteger {
						t.Fatalf("score fixture has type %v", column.Value.Type)
					}
					value := column.Value.I
					contact.Score, include["Score"] = &value, true
				case "email":
					if column.Value.Type == codec.TypeNull {
						contact.Email, include["Email"] = nil, true
						continue
					}
					if column.Value.Type != codec.TypeText {
						t.Fatalf("email fixture has type %v", column.Value.Type)
					}
					email := column.Value.S
					contact.Email, include["Email"] = &email, true
				case "ratio":
					column.Column, column.Policy = "Ratio", schema.MAX
					passthrough = append(passthrough, column)
				default:
					t.Fatalf("unexpected contact field %q", column.Column)
				}
			}
			columns := make([]ColumnValue, 0, len(include))
			for _, field := range []struct {
				name string
				id   uint32
			}{{"ID", 1}, {"Name", 2}, {"Score", 3}, {"Email", 4}} {
				if !include[field.name] {
					continue
				}
				if field.name == "ID" && primaryNull {
					columns = append(columns, ColumnValue{Column: "ID", Policy: schema.LWW, Value: codec.Null()})
					continue
				}
				payload, err := recordcodec.EncodeField(compiled, field.id, contact, nil, recordcodec.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				columns = append(columns, ColumnValue{Column: field.name, Policy: schema.LWW, Value: codec.Blob(payload)})
			}
			rec.Columns = append(columns, passthrough...)
		}
	}
	return encoded
}

func typedContactValue(t *testing.T, database *db.DB, key ids.RowID) (*typedBridgeContactRecord, bool) {
	t.Helper()
	value := typedBridgeContactRecord{ID: key}
	if err := database.GetItem(context.Background(), &value); err != nil {
		if errors.Is(err, rime.ErrNotFound) {
			return nil, false
		}
		t.Fatal(err)
	}
	return &value, true
}

func typedContactInsert(t *testing.T, database *db.DB, key ids.RowID, name string, score int64) {
	t.Helper()
	if err := database.InsertItem(context.Background(), &typedBridgeContactRecord{ID: key, Name: &name, Score: &score}); err != nil {
		t.Fatal(err)
	}
}

func typedContactSetName(t *testing.T, database *db.DB, key ids.RowID, name string) {
	t.Helper()
	if err := database.Update(context.Background(), &typedBridgeContactRecord{ID: key}, db.Set("Name", &name)); err != nil {
		t.Fatal(err)
	}
}
