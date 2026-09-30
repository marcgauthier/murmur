package bridge

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func TestLowImportHighOverrideAndExplicitRelease(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recipient, trust := inboxKeys(t, "owned")
	inbox, err := OpenInbox(t.TempDir(), trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	first := sealForInbox(t, signer, recipient, "owned", 1, []Batch{putBatch(1, row,
		ColumnValue{Column: "name", Value: codec.Text("low-1")},
		ColumnValue{Column: "score", Value: codec.Int(1)},
	)})
	if err := inbox.Receive(first); err != nil {
		t.Fatal(err)
	}
	importer, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := importer.Drain(ctx, inbox); err != nil || n != 1 {
		t.Fatalf("initial import = %d, %v", n, err)
	}
	rowPolicy, ok, err := high.BridgeRowProvenance("contacts", row)
	if err != nil || !ok || rowPolicy.Owner != db.BridgeOwnerLow || rowPolicy.FirstSeq != 1 || rowPolicy.LastSeq != 1 {
		t.Fatalf("row provenance = %+v, present=%v, err=%v", rowPolicy, ok, err)
	}
	if _, err := high.ExecContext(ctx, `UPDATE contacts SET name=? WHERE id=?`, "high", row[:]); err != nil {
		t.Fatal(err)
	}
	fieldPolicy, ok, err := high.BridgeFieldProvenance("contacts", row, "name")
	if err != nil || !ok || fieldPolicy.Owner != db.BridgeOwnerHigh || fieldPolicy.OverrideTxID.IsZero() {
		t.Fatalf("High override = %+v, present=%v, err=%v", fieldPolicy, ok, err)
	}
	second := sealForInbox(t, signer, recipient, "owned", 2, []Batch{putBatch(2, row,
		ColumnValue{Column: "name", Value: codec.Text("low-2")},
		ColumnValue{Column: "score", Value: codec.Int(2)},
	)})
	if err := inbox.Receive(second); err != nil {
		t.Fatal(err)
	}
	if n, err := importer.Drain(ctx, inbox); err != nil || n != 1 {
		t.Fatalf("protected update = %d, %v", n, err)
	}
	var name string
	var score int64
	if err := high.QueryRowContext(ctx, `SELECT name, score FROM contacts WHERE id=?`, row[:]).Scan(&name, &score); err != nil {
		t.Fatal(err)
	}
	if name != "high" || score != 2 {
		t.Fatalf("Low update bypassed ownership: name=%q score=%d", name, score)
	}
	if err := high.ReleaseBridgeOwnership(ctx, "contacts", row, "name"); err != nil {
		t.Fatal(err)
	}
	third := sealForInbox(t, signer, recipient, "owned", 3, []Batch{putBatch(3, row,
		ColumnValue{Column: "name", Value: codec.Text("low-3")},
	)})
	if err := inbox.Receive(third); err != nil {
		t.Fatal(err)
	}
	if n, err := importer.Drain(ctx, inbox); err != nil || n != 1 {
		t.Fatalf("released update = %d, %v", n, err)
	}
	if err := high.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id=?`, row[:]).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "low-3" {
		t.Fatalf("explicit release did not restore Low ownership: %q", name)
	}
}

func TestHighCreatedCollisionAndProtectedDeleteResolution(t *testing.T) {
	ctx := context.Background()
	high := openHighDB(t)
	signer, recipient, trust := inboxKeys(t, "policy")
	if err := trust.AddSigner(signer.ID, "policy", "delete", "delete-accept"); err != nil {
		t.Fatal(err)
	}
	inboxDir := t.TempDir()
	inbox, err := OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	importer, err := NewImporter(high)
	if err != nil {
		t.Fatal(err)
	}
	collisionRow := ids.NewRowID()
	if _, err := high.ExecContext(ctx, `INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)`, collisionRow[:], "high-created", 5); err != nil {
		t.Fatal(err)
	}
	collision := sealForInbox(t, signer, recipient, "policy", 1, []Batch{putBatch(1, collisionRow, ColumnValue{Column: "name", Value: codec.Text("low")})})
	if err := inbox.Receive(collision); err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, inbox); !errors.Is(err, ErrIdentityCollision) {
		t.Fatalf("collision error = %v", err)
	}
	if got := inbox.Progress()[0]; got.Applied != 0 || got.QuarantineTotal != 1 {
		t.Fatalf("collision was not quarantined: %+v", got)
	}

	// Use a fresh stream for the imported row and its protected delete.
	stream := "delete"
	row := ids.NewRowID()
	put := sealForInbox(t, signer, recipient, stream, 1, []Batch{putBatch(1, row, ColumnValue{Column: "name", Value: codec.Text("low")})})
	if err := inbox.Receive(put); err != nil {
		t.Fatal(err)
	}
	if n, err := importer.Drain(ctx, inbox); err != nil || n != 1 {
		t.Fatalf("seed import = %d, %v", n, err)
	}
	if _, err := high.ExecContext(ctx, `UPDATE contacts SET name=? WHERE id=?`, "high", row[:]); err != nil {
		t.Fatal(err)
	}
	del := sealForInbox(t, signer, recipient, stream, 2, []Batch{delBatch(2, row)})
	if err := inbox.Receive(del); err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, inbox); !errors.Is(err, ErrProtectedDelete) {
		t.Fatalf("protected delete = %v", err)
	}
	progress := inbox.Progress()
	var held *SchemaHold
	for i := range progress {
		if progress[i].Stream == stream && len(progress[i].Holds) > 0 {
			held = &progress[i].Holds[0]
		}
	}
	if held == nil || held.Kind != "policy" || held.Resolution != "" {
		t.Fatalf("delete was not durably held: %+v", progress)
	}
	inbox, err = OpenInbox(inboxDir, trust, Limits{}.withDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if inbox.PolicyResolution(stream, 2) != "" {
		t.Fatal("unresolved ownership hold gained a decision after restart")
	}
	if err := importer.ResolvePolicyHold(ctx, inbox, stream, 2, ResolutionKeepHigh); err != nil {
		t.Fatal(err)
	}
	var highName string
	if err := high.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id=?`, row[:]).Scan(&highName); err != nil || highName != "high" {
		t.Fatalf("keep-high resolution lost the row: name=%q err=%v", highName, err)
	}

	acceptStream := "delete-accept"
	acceptRow := ids.NewRowID()
	seed := sealForInbox(t, signer, recipient, acceptStream, 1, []Batch{putBatch(1, acceptRow, ColumnValue{Column: "name", Value: codec.Text("low")})})
	if err := inbox.Receive(seed); err != nil {
		t.Fatal(err)
	}
	if n, err := importer.Drain(ctx, inbox); err != nil || n != 1 {
		t.Fatalf("accept seed = %d, %v", n, err)
	}
	if _, err := high.ExecContext(ctx, `UPDATE contacts SET name=? WHERE id=?`, "high", acceptRow[:]); err != nil {
		t.Fatal(err)
	}
	acceptDelete := sealForInbox(t, signer, recipient, acceptStream, 2, []Batch{delBatch(2, acceptRow)})
	if err := inbox.Receive(acceptDelete); err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Drain(ctx, inbox); !errors.Is(err, ErrProtectedDelete) {
		t.Fatalf("accept path protected delete = %v", err)
	}
	if err := importer.ResolvePolicyHold(ctx, inbox, acceptStream, 2, ResolutionAcceptLowDelete); err != nil {
		t.Fatal(err)
	}
	if err := high.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id=?`, acceptRow[:]).Scan(new(string)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("accepted Low delete should remove the row; err=%v", err)
	}
	policy, ok, err := high.BridgeFieldProvenance("contacts", acceptRow, "name")
	if err != nil || !ok || policy.Owner != db.BridgeOwnerLow {
		t.Fatalf("delete resolution did not release field: %+v %v %v", policy, ok, err)
	}
}
