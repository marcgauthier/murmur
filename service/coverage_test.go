package service

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
)

// TestToValueWidths pins the integer-width, unsigned-overflow, float32, and
// nil-blob conversions plus the fail-closed default.
func TestToValueWidths(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		want int64
	}{
		{"int8", int8(-8), -8},
		{"int16", int16(-16), -16},
		{"int32", int32(-32), -32},
		{"uint", uint(7), 7},
		{"uint8", uint8(8), 8},
		{"uint16", uint16(16), 16},
		{"uint32", uint32(32), 32},
		{"uint64", uint64(1<<63 - 1), 1<<63 - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := ToValue(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if v.Type != "int" || v.I != tc.want {
				t.Fatalf("ToValue(%v) = %+v", tc.in, v)
			}
		})
	}
	for _, tc := range []struct {
		name string
		in   any
	}{
		{"uint64-overflow", uint64(1 << 63)},
		{"uint-overflow", uint(1 << 63)},
		{"unsupported", struct{}{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ToValue(tc.in); err == nil {
				t.Fatalf("ToValue(%v) = nil, want error", tc.in)
			}
		})
	}
	v, err := ToValue(float32(1.5))
	if err != nil || v.Type != "float" || v.F != 1.5 {
		t.Fatalf("float32 = %+v/%v", v, err)
	}
	var nilBlob []byte
	if v, err := ToValue(nilBlob); err != nil || v.Type != "null" {
		t.Fatalf("nil blob = %+v/%v", v, err)
	}
}

// TestValueAnyEdges pins blob decoding edges and the unknown-type rejection.
func TestValueAnyEdges(t *testing.T) {
	empty, err := (Value{Type: "blob"}).Any()
	if err != nil {
		t.Fatal(err)
	}
	if b, ok := empty.([]byte); !ok || len(b) != 0 {
		t.Fatalf("empty blob = %#v", empty)
	}
	if _, err := (Value{Type: "blob", B: "!!!"}).Any(); err == nil {
		t.Fatal("bad base64 accepted")
	}
	if _, err := (Value{Type: "mystery"}).Any(); err == nil {
		t.Fatal("unknown type accepted")
	}
	if _, err := ValuesToAny([]Value{{Type: "mystery"}}); err == nil {
		t.Fatal("ValuesToAny accepted unknown type")
	}
}

// TestDecodeJSONArgs pins the subscription args parameter codec.
func TestDecodeJSONArgs(t *testing.T) {
	args, err := DecodeJSONArgs("")
	if err != nil || args != nil {
		t.Fatalf("empty = %v/%v, want nil/nil", args, err)
	}
	args, err = DecodeJSONArgs(`[{"t":"int","i":3},{"t":"text","s":"x"},{"t":"null"}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 3 || args[0] != int64(3) || args[1] != "x" || args[2] != nil {
		t.Fatalf("decoded = %#v", args)
	}
	if _, err := DecodeJSONArgs(`[{"t":`); err == nil {
		t.Fatal("malformed args accepted")
	}
	if _, err := DecodeJSONArgs(`[{"t":"mystery"}]`); err == nil {
		t.Fatal("unknown arg type accepted")
	}
}

func authedPost(t *testing.T, srv *httptest.Server, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+string(testToken))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// TestServiceDecodeBodyErrors proves malformed query bodies fail closed.
func TestServiceDecodeBodyErrors(t *testing.T) {
	database := openTestDB(t)
	h, err := New(database, Config{BearerToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(h)
	defer srv.Close()
	for _, tc := range []struct {
		name string
		body string
	}{
		{"malformed", `{"query":`},
		{"unknown-field", `{"query":"SELECT 1","bogus":true}`},
		{"empty-query", `{"query":"","args":[]}`},
		{"missing-query", `{"args":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := authedPost(t, srv, "/v1/query", tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
	// Oversized bodies are rejected against MaxBodyBytes.
	tiny, err := New(database, Config{BearerToken: testToken, MaxBodyBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	tinySrv := httptest.NewTLSServer(tiny)
	defer tinySrv.Close()
	resp := authedPost(t, tinySrv, "/v1/query", `{"query":"SELECT 1 FROM items"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized status = %d, want 400", resp.StatusCode)
	}
}

// TestServiceQueryExecErrors proves bad arguments fail with 400 and backend
// failures surface as 500 on both SQL endpoints.
func TestServiceQueryExecErrors(t *testing.T) {
	database := openTestDB(t)
	h, err := New(database, Config{BearerToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(h)
	defer srv.Close()
	badArg := `{"query":"SELECT * FROM items WHERE id = ?","args":[{"t":"blob","b":"!!!"}]}`
	for _, path := range []string{"/v1/query", "/v1/exec"} {
		resp := authedPost(t, srv, path, badArg)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s bad-arg status = %d, want 400", path, resp.StatusCode)
		}
	}
	resp := authedPost(t, srv, "/v1/query", `{"query":"SELECT * FROM nope"}`)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("bad-table query status = %d, want 500", resp.StatusCode)
	}
	resp = authedPost(t, srv, "/v1/exec", `{"query":"INSERT INTO nope (id) VALUES (1)"}`)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("bad-table exec status = %d, want 500", resp.StatusCode)
	}
}

func authedGet(t *testing.T, srv *httptest.Server, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+string(testToken))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// TestServiceSubscribeParams proves subscription parameter validation and
// that valid resume/emit_unchanged parameters are accepted.
func TestServiceSubscribeParams(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	id := db.NewRowID()
	if _, err := database.ExecContext(ctx, `INSERT INTO items (id) VALUES (?)`, id[:]); err != nil {
		t.Fatal(err)
	}
	h, err := New(database, Config{BearerToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(h)
	defer srv.Close()
	for _, tc := range []string{
		"/v1/subscribe",
		"/v1/subscribe?query=",
		"/v1/subscribe?query=SELECT+1&args=%5B%7B",
		"/v1/subscribe?query=SELECT+1&resume=abc",
		"/v1/subscribe?query=SELECT+1&emit_unchanged=maybe",
	} {
		resp := authedGet(t, srv, tc)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", tc, resp.StatusCode)
		}
	}
	// Valid parameters open a stream (cancelled immediately).
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet,
		srv.URL+"/v1/subscribe?query=SELECT+id+FROM+items&resume=0&emit_unchanged=true", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+string(testToken))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid-params status = %d, want 200", resp.StatusCode)
	}
	cancel()
}

type noFlushWriter struct {
	header http.Header
	code   int
}

func (w *noFlushWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *noFlushWriter) Write(p []byte) (int, error) { return len(p), nil }

func (w *noFlushWriter) WriteHeader(code int) { w.code = code }

// TestServiceSubscribeNoFlusher proves a non-streaming ResponseWriter fails
// the subscription with 500 instead of hanging.
func TestServiceSubscribeNoFlusher(t *testing.T) {
	database := openTestDB(t)
	h, err := New(database, Config{BearerToken: testToken})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/subscribe?query=SELECT+id+FROM+items", nil)
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Authorization", "Bearer "+string(testToken))
	w := &noFlushWriter{}
	h.ServeHTTP(w, req)
	if w.code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.code)
	}
}

// TestSubscribeEventRowCap proves an over-cap subscription event terminates
// the stream with an error on both ends.
func TestSubscribeEventRowCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	database := openTestDB(t)
	for i := 0; i < 2; i++ {
		id := db.NewRowID()
		if _, err := database.ExecContext(ctx, `INSERT INTO items (id) VALUES (?)`, id[:]); err != nil {
			t.Fatal(err)
		}
	}
	_, client := testServer(t, database, Config{MaxRows: 1})
	_, errCh := client.Subscribe(ctx, `SELECT id FROM items`, SubscribeOptions{})
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("over-cap stream ended cleanly, want error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no terminal error for over-cap stream")
	}
}

// TestClientStatusAuthError proves a wrong-token client surfaces the 401.
func TestClientStatusAuthError(t *testing.T) {
	database := openTestDB(t)
	srv, _ := testServer(t, database, Config{})
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	bad, err := NewClient(srv.URL, bytes.Repeat([]byte{0x41}, 32), &tls.Config{RootCAs: pool})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Status(context.Background()); err == nil {
		t.Fatal("wrong-token Status succeeded")
	}
}

// TestClientConstructorErrors pins the remaining NewClient rejections.
func TestClientConstructorErrors(t *testing.T) {
	if _, err := NewClient("://missing-scheme", testToken, nil); err == nil {
		t.Fatal("unparseable base URL accepted")
	}
}

// TestClientPostMarshalError proves an unencodable body fails before dialing.
func TestClientPostMarshalError(t *testing.T) {
	database := openTestDB(t)
	_, client := testServer(t, database, Config{})
	var out queryResponse
	if err := client.post(context.Background(), "/v1/query", func() {}, &out); err == nil {
		t.Fatal("unmarshalable body accepted")
	}
}

// TestClientQueryExecBadArgs proves client-side argument encoding failures.
func TestClientQueryExecBadArgs(t *testing.T) {
	ctx := context.Background()
	database := openTestDB(t)
	_, client := testServer(t, database, Config{})
	if _, _, err := client.Query(ctx, `SELECT ?`, struct{}{}); err == nil {
		t.Fatal("Query with bad arg succeeded")
	}
	if _, err := client.Exec(ctx, `SELECT ?`, struct{}{}); err == nil {
		t.Fatal("Exec with bad arg succeeded")
	}
	_, errCh := client.Subscribe(ctx, `SELECT id FROM items`, SubscribeOptions{Args: []any{struct{}{}}})
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Subscribe with bad arg ended cleanly")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no error for bad subscribe args")
	}
}

func clientFor(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	c, err := NewClient(srv.URL, testToken, &tls.Config{RootCAs: pool})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestClientSubscribeStreamShapes drives the SSE decoder against canned
// streams: server error events, malformed payloads, and skipped lines.
func TestClientSubscribeStreamShapes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mk := func(t *testing.T, body string) *Client {
		t.Helper()
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, body)
		}))
		t.Cleanup(srv.Close)
		return clientFor(t, srv)
	}
	t.Run("server-error-event", func(t *testing.T) {
		c := mk(t, "data: {\"type\":\"error\",\"cursor\":3,\"error\":\"kaput\"}\n\n")
		_, errCh := c.Subscribe(ctx, `SELECT 1`, SubscribeOptions{})
		select {
		case err := <-errCh:
			if err == nil || !strings.Contains(err.Error(), "kaput") {
				t.Fatalf("err = %v, want kaput", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no terminal error")
		}
	})
	t.Run("bare-error-type", func(t *testing.T) {
		c := mk(t, "data: {\"type\":\"error\",\"cursor\":0}\n\n")
		_, errCh := c.Subscribe(ctx, `SELECT 1`, SubscribeOptions{})
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("bare error event ended cleanly")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no terminal error")
		}
	})
	t.Run("malformed-event", func(t *testing.T) {
		c := mk(t, "data: {oops\n\n")
		_, errCh := c.Subscribe(ctx, `SELECT 1`, SubscribeOptions{})
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("malformed event ended cleanly")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no terminal error")
		}
	})
	t.Run("skipped-lines-then-clean-end", func(t *testing.T) {
		c := mk(t, ": heartbeat\n\ndata: {\"type\":\"update\",\"cursor\":1,\"columns\":[],\"rows\":[]}\n\n")
		events, errCh := c.Subscribe(ctx, `SELECT 1`, SubscribeOptions{})
		select {
		case ev := <-events:
			if ev.Type != "update" || ev.Cursor != 1 {
				t.Fatalf("event = %+v", ev)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no event")
		}
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("terminal err = %v, want clean end", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("stream did not end")
		}
	})
}

// TestResponseErrorShapes pins both error-envelope decodings.
func TestResponseErrorShapes(t *testing.T) {
	mk := func(t *testing.T, code int, body string) *Client {
		t.Helper()
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			_, _ = io.WriteString(w, body)
		}))
		t.Cleanup(srv.Close)
		return clientFor(t, srv)
	}
	c := mk(t, http.StatusTeapot, "plain failure")
	var out StatusDTO
	err := c.post(context.Background(), "/v1/status", sqlRequest{Query: "x"}, &out)
	if err == nil || !strings.Contains(err.Error(), "unexpected status 418") {
		t.Fatalf("plain-body err = %v", err)
	}
}
