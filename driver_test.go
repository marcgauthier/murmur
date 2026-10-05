package murmur

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

func openDriverTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := openSignedFixture(context.Background(), testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestDriverConnectorRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openDriverTestDB(t)
	sqldb := sql.OpenDB(NewConnector(db))
	defer sqldb.Close()

	id1, id2 := NewRowID(), NewRowID()
	if _, err := sqldb.ExecContext(ctx,
		`INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)`,
		id1[:], "ann", "111", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx,
		`INSERT INTO contacts (id, name) VALUES (?, ?)`, id2[:], "bob"); err != nil {
		t.Fatal(err)
	}

	var (
		name  sql.NullString
		phone sql.NullString
		score sql.NullInt64
		gotID []byte
	)
	if err := sqldb.QueryRowContext(ctx,
		`SELECT id, name, phone, score FROM contacts WHERE name = ?`, "ann").Scan(
		&gotID, &name, &phone, &score); err != nil {
		t.Fatal(err)
	}
	if string(gotID) != string(id1[:]) || !name.Valid || name.String != "ann" ||
		!phone.Valid || phone.String != "111" || !score.Valid || score.Int64 != 10 {
		t.Fatalf("unexpected row: id=%x name=%+v phone=%+v score=%+v", gotID, name, phone, score)
	}
	if err := sqldb.QueryRowContext(ctx,
		`SELECT phone, score FROM contacts WHERE name = ?`, "bob").Scan(&phone, &score); err != nil {
		t.Fatal(err)
	}
	if phone.Valid || score.Valid {
		t.Fatalf("want NULL phone/score, got %+v %+v", phone, score)
	}

	// Durable through the normal path: visible to the direct API and counted.
	rows := queryAll(t, db, `SELECT id FROM contacts`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows via direct API, got %d", len(rows))
	}

	// Closing the sql.DB must not close the underlying *DB.
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE contacts SET phone = ? WHERE name = ?`, "222", "ann"); err != nil {
		t.Fatalf("direct API after sql.DB close: %v", err)
	}
}

func TestDriverRegisteredOpen(t *testing.T) {
	ctx := context.Background()
	db := openDriverTestDB(t)

	const handle = "driver-test-registered"
	RegisterDriverDB(handle, db)
	defer UnregisterDriverDB(handle)

	// Test opening with DriverName ("murmur")
	sqldb, err := sql.Open(DriverName, handle)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()
	if err := sqldb.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	id := NewRowID()
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, id[:]); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := sqldb.QueryRowContext(ctx, `SELECT COUNT(*) FROM contacts`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("want 1 row, got %d", count)
	}

	// Test opening with LegacyDriverName ("replicateddb")
	sqldbLegacy, err := sql.Open(LegacyDriverName, handle)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldbLegacy.Close()
	if err := sqldbLegacy.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	// sql.Open resolves the connector eagerly, so an unknown handle fails here.
	if bad, err := sql.Open(DriverName, "driver-test-missing"); err == nil {
		_ = bad.Close()
		t.Fatalf("want ErrDriverNotRegistered, got open success")
	} else if !errors.Is(err, ErrDriverNotRegistered) {
		t.Fatalf("want ErrDriverNotRegistered, got %v", err)
	}
}

func TestDriverTxCommitRollback(t *testing.T) {
	ctx := context.Background()
	db := openDriverTestDB(t)
	sqldb := sql.OpenDB(NewConnector(db))
	defer sqldb.Close()

	committed := NewRowID()
	tx, err := sqldb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, committed[:], "cid"); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := tx.QueryRowContext(ctx, `SELECT name FROM contacts WHERE id = ?`, committed[:]).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "cid" {
		t.Fatalf("want cid inside tx, got %q", name)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := queryAll(t, db, `SELECT id FROM contacts WHERE id = ?`, committed[:]); len(got) != 1 {
		t.Fatalf("want committed row visible, got %d", len(got))
	}

	rolledBack := NewRowID()
	tx, err = sqldb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, rolledBack[:]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := queryAll(t, db, `SELECT id FROM contacts WHERE id = ?`, rolledBack[:]); len(got) != 0 {
		t.Fatalf("want rolled-back row absent, got %d", len(got))
	}
}

func TestDriverWriteQueryRejected(t *testing.T) {
	ctx := context.Background()
	db := openDriverTestDB(t)
	sqldb := sql.OpenDB(NewConnector(db))
	defer sqldb.Close()

	id := NewRowID()
	rows, err := sqldb.QueryContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, id[:])
	if !errors.Is(err, ErrDriverWriteQuery) {
		if err == nil {
			_ = rows.Close()
		}
		t.Fatalf("want ErrDriverWriteQuery, got %v", err)
	}
	if got := queryAll(t, db, `SELECT id FROM contacts`); len(got) != 0 {
		t.Fatalf("rejected write must not land, got %d rows", len(got))
	}

	// Writes with rows work inside an explicit transaction.
	tx, err := sqldb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	qrows, err := tx.QueryContext(ctx, `SELECT id FROM contacts WHERE id = ?`, id[:])
	if err != nil {
		t.Fatal(err)
	}
	_ = qrows.Close()
	if _, err := tx.ExecContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, id[:]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := queryAll(t, db, `SELECT id FROM contacts`); len(got) != 1 {
		t.Fatalf("want 1 row after tx commit, got %d", len(got))
	}
}

func TestDriverConnCloseReleasesTx(t *testing.T) {
	ctx := context.Background()
	db := openDriverTestDB(t)

	conn, err := NewConnector(db).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	txConn, ok := conn.(driver.ConnBeginTx)
	if !ok {
		t.Fatal("connector conn lacks BeginTx")
	}
	dtx, err := txConn.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare(`INSERT INTO contacts (id) VALUES (?)`)
	if err != nil {
		t.Fatal(err)
	}
	id := NewRowID()
	if _, err := stmt.(driver.StmtExecContext).ExecContext(ctx,
		[]driver.NamedValue{{Ordinal: 1, Value: append([]byte(nil), id[:]...)}}); err != nil {
		t.Fatal(err)
	}
	// Drop the connection without commit: the tx must roll back and the
	// second BeginTx below must fail (driverTx was detached by Close).
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := dtx.Rollback(); !errors.Is(err, ErrTxDone) {
		t.Fatalf("want ErrTxDone after conn close, got %v", err)
	}

	// The serialized write coordinator must be free again.
	done := make(chan error, 1)
	go func() {
		probe := NewRowID()
		_, err := db.ExecContext(context.Background(), `INSERT INTO contacts (id) VALUES (?)`, probe[:])
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("write coordinator still held after conn close")
	}
	if got := queryAll(t, db, `SELECT id FROM contacts WHERE id = ?`, id[:]); len(got) != 0 {
		t.Fatalf("want uncommitted row absent, got %d", len(got))
	}

	var validator driver.Validator = conn.(driver.Validator)
	if validator.IsValid() {
		t.Fatal("closed conn must not be valid")
	}
}
