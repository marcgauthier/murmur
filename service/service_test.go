package service

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
)

var testToken = bytes.Repeat([]byte{0x5a}, 32)

func testSchema() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "items",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
			{Name: "ratio", Type: schema.ColReal, Nullable: true},
			{Name: "meta", Type: schema.ColBlob, Nullable: true},
		},
	}}
}

func openTestDB(t *testing.T) *db.DB {
	t.Helper()
	database, err := db.Open(context.Background(), db.Config{
		Path:   t.TempDir(),
		NodeID: db.NewNodeID(),
		Schema: db.SchemaConfig{Version: 1, Tables: testSchema()},
		Pebble: db.DefaultPebbleConfig(),
		Encryption: db.EncryptionConfig{
			Key:   bytes.Repeat([]byte{0x3a}, 32),
			KeyID: "test-key",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func testServer(t *testing.T, database *db.DB, cfg Config) (*httptest.Server, *Client) {
	t.Helper()
	cfg.BearerToken = testToken
	h, err := New(database, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	client, err := NewClient(srv.URL, testToken, &tls.Config{RootCAs: pool})
	if err != nil {
		t.Fatal(err)
	}
	return srv, client
}

func TestServiceAuth(t *testing.T) {
	database := openTestDB(t)
	h, err := New(database, Config{BearerToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(database, Config{BearerToken: []byte("short")}); err == nil {
		t.Fatal("short token accepted")
	}
	if _, err := New(nil, Config{BearerToken: testToken}); err == nil {
		t.Fatal("nil database accepted")
	}

	// Plain HTTP is refused even with the right token.
	plain := httptest.NewServer(h)
	defer plain.Close()
	req, _ := http.NewRequest(http.MethodGet, plain.URL+"/v1/status", nil)
	req.Header.Set("Authorization", "Bearer "+string(testToken))
	resp, err := plain.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("plain HTTP status = %d, want 403", resp.StatusCode)
	}

	// TLS without/with wrong token is unauthorized.
	tlsSrv := httptest.NewTLSServer(h)
	defer tlsSrv.Close()
	for _, tc := range []struct {
		name  string
		token string
	}{
		{"missing", ""},
		{"wrong", string(bytes.Repeat([]byte{0x41}, 32))},
		{"short", "short"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, tlsSrv.URL+"/v1/status", nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			resp, err := tlsSrv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", resp.StatusCode)
			}
		})
	}
}

func TestServiceQueryExecRoundTrip(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	_, client := testServer(t, database, Config{})

	id1, id2 := db.NewRowID(), db.NewRowID()
	if _, err := client.Exec(ctx, `INSERT INTO items (id, name, score, ratio, meta) VALUES (?, ?, ?, ?, ?)`,
		id1[:], "ann", int64(10), 2.5, []byte{0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Exec(ctx, `INSERT INTO items (id) VALUES (?)`, id2[:]); err != nil {
		t.Fatal(err)
	}

	cols, rows, err := client.Query(ctx, `SELECT id, name, score, ratio, meta FROM items ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	// NULL name sorts first in SQLite.
	if len(cols) != 5 || cols[0] != "id" || cols[4] != "meta" {
		t.Fatalf("columns = %v", cols)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0][1] != nil || rows[0][2] != nil || rows[0][3] != nil || rows[0][4] != nil {
		t.Fatalf("NULL row decoded as %v", rows[0])
	}
	got := rows[1]
	if !bytes.Equal(got[0].([]byte), id1[:]) {
		t.Fatalf("id = %x", got[0])
	}
	if got[1] != "ann" || got[2] != int64(10) || got[3] != 2.5 {
		t.Fatalf("row = %v", got)
	}
	if !bytes.Equal(got[4].([]byte), []byte{0x01, 0x02}) {
		t.Fatalf("blob = %v", got[4])
	}

	st, err := client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.State != "ready" || st.NodeID != database.NodeID().String() || st.DBID == "" {
		t.Fatalf("status = %+v", st)
	}
	if st.LocalSeq != 2 {
		t.Fatalf("LocalSeq = %d, want 2", st.LocalSeq)
	}
}

func TestServiceQueryRejectsWrite(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	_, client := testServer(t, database, Config{})

	id := db.NewRowID()
	if _, _, err := client.Query(ctx, `INSERT INTO items (id) VALUES (?)`, id[:]); err == nil {
		t.Fatal("write via query endpoint succeeded")
	}
	// Exec of a read works (implicit-transaction read path).
	if _, err := client.Exec(ctx, `SELECT COUNT(*) FROM items`); err != nil {
		t.Fatalf("exec read: %v", err)
	}
}

func TestServiceRowCap(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	_, client := testServer(t, database, Config{MaxRows: 1})

	for i := 0; i < 2; i++ {
		id := db.NewRowID()
		if _, err := client.Exec(ctx, `INSERT INTO items (id) VALUES (?)`, id[:]); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := client.Query(ctx, `SELECT id FROM items`); err == nil {
		t.Fatal("over-cap query succeeded")
	}
}

func TestServiceSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	database := openTestDB(t)
	_, client := testServer(t, database, Config{})

	id1 := db.NewRowID()
	if _, err := client.Exec(ctx, `INSERT INTO items (id, name) VALUES (?, ?)`, id1[:], "ann"); err != nil {
		t.Fatal(err)
	}
	events, errCh := client.Subscribe(ctx, `SELECT id, name FROM items ORDER BY name`, SubscribeOptions{})

	// Initial event carries current state.
	var initial Event
	select {
	case initial = <-events:
	case err := <-errCh:
		t.Fatalf("subscribe failed: %v", err)
	case <-ctx.Done():
		t.Fatal("no initial event")
	}
	if initial.Type != "initial" || len(initial.Rows) != 1 {
		t.Fatalf("initial = %+v", initial)
	}

	// A committed write produces an update.
	id2 := db.NewRowID()
	if _, err := client.Exec(ctx, `INSERT INTO items (id, name) VALUES (?, ?)`, id2[:], "bob"); err != nil {
		t.Fatal(err)
	}
	var updated Event
	select {
	case updated = <-events:
	case err := <-errCh:
		t.Fatalf("stream failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no update event")
	}
	if updated.Type != "update" || len(updated.Rows) != 2 {
		t.Fatalf("updated = %+v", updated)
	}

	// Cancel ends the stream.
	cancel()
	select {
	case _, ok := <-events:
		if ok {
			// Drain any race-buffered event, then expect closure.
			select {
			case <-events:
			case <-time.After(5 * time.Second):
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not end after cancel")
	}
}

func TestServiceSubscribeRejectsWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	database := openTestDB(t)
	_, client := testServer(t, database, Config{})

	_, errCh := client.Subscribe(ctx, `DELETE FROM items`, SubscribeOptions{})
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("write subscription accepted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no rejection for write subscription")
	}
}

func TestClientRejectsHTTP(t *testing.T) {
	if _, err := NewClient("http://localhost:9", testToken, nil); err == nil {
		t.Fatal("plain-http client accepted")
	}
	if _, err := NewClient("https://localhost:9", nil, nil); err == nil {
		t.Fatal("tokenless client accepted")
	}
}

func TestValueCodec(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
	}{
		{"nil", nil},
		{"int64", int64(-42)},
		{"int", int(7)},
		{"uint64", uint64(9)},
		{"float", 1.25},
		{"text", "hi"},
		{"blob", []byte{0x00, 0xff}},
		{"bool", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := ToValue(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			back, err := v.Any()
			if err != nil {
				t.Fatal(err)
			}
			switch want := tc.in.(type) {
			case nil:
				if back != nil {
					t.Fatalf("back = %v", back)
				}
			case []byte:
				if !bytes.Equal(back.([]byte), want) {
					t.Fatalf("back = %v", back)
				}
			case int:
				if back != int64(want) {
					t.Fatalf("back = %v", back)
				}
			case uint64:
				if back != int64(want) {
					t.Fatalf("back = %v", back)
				}
			case float32:
				if back != float64(want) {
					t.Fatalf("back = %v", back)
				}
			default:
				if back != tc.in {
					t.Fatalf("back = %v (%T), want %v", back, back, tc.in)
				}
			}
		})
	}
	if _, err := ToValue(uint64(1) << 63); err == nil {
		t.Fatal("uint64 overflow accepted")
	}
	if _, err := ToValue(complex(1, 2)); err == nil {
		t.Fatal("complex accepted")
	}
	if _, err := (Value{Type: "blob", B: "!!!"}).Any(); err == nil {
		t.Fatal("bad blob accepted")
	}
	if _, err := (Value{Type: "wat"}).Any(); err == nil {
		t.Fatal("unknown type accepted")
	}
}
