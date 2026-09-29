package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/admin"
	"github.com/marcgauthier/spedsql/schema"
)

func TestAuthenticatedTLSUnlockLifecycle(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	path := t.TempDir()
	nodeID, dbID := db.NewNodeID(), db.NewDBID()
	seed, err := db.Open(context.Background(), db.Config{
		Path: path, NodeID: nodeID, DBID: dbID,
		Schema:     db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "items", Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}}}}},
		Pebble:     db.DefaultPebbleConfig(),
		Encryption: db.EncryptionConfig{Algorithm: db.AES256GCM, KeyAlgorithm: db.AES256GCM, Key: key, KeyID: "admin-test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	var openCalls atomic.Int32
	h, err := admin.New(admin.Config{
		BearerToken: []byte("separate-admin-token-with-32-bytes"),
		Open: func(ctx context.Context, material db.KeyMaterial) (*db.DB, error) {
			openCalls.Add(1)
			return db.Open(ctx, db.Config{
				Path: path, NodeID: nodeID, DBID: dbID,
				Schema:     db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "items", Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}}}}},
				Pebble:     db.DefaultPebbleConfig(),
				Encryption: db.EncryptionConfig{Algorithm: db.AES256GCM, KeyAlgorithm: db.AES256GCM, Key: material.Key, KeyID: material.ID},
			})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	server := httptest.NewTLSServer(h)
	defer server.Close()
	client := server.Client()
	request := func(method, path string, body []byte, token string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	readStatus := func() (bool, string) {
		t.Helper()
		resp := request(http.MethodGet, "/v1/status", nil, "separate-admin-token-with-32-bytes")
		defer resp.Body.Close()
		var status struct {
			Unlocked bool   `json:"unlocked"`
			State    string `json:"state"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			t.Fatal(err)
		}
		return status.Unlocked, status.State
	}
	if unlocked, _ := readStatus(); unlocked || openCalls.Load() != 0 {
		t.Fatalf("database opened before unlock: unlocked=%v openCalls=%d", unlocked, openCalls.Load())
	}
	resp := request(http.MethodGet, "/v1/status", nil, "wrong-admin-token-xxxxxxxxxxxxxxxx")
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized || openCalls.Load() != 0 {
		t.Fatalf("bad admin token status=%d openCalls=%d", resp.StatusCode, openCalls.Load())
	}

	unlock := func(k []byte) int {
		t.Helper()
		body, err := json.Marshal(map[string]any{"key_id": "admin-test", "algorithm": string(db.AES256GCM), "key": k})
		if err != nil {
			t.Fatal(err)
		}
		resp := request(http.MethodPost, "/v1/unlock", body, "separate-admin-token-with-32-bytes")
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if code := unlock(make([]byte, 32)); code != http.StatusUnauthorized {
		t.Fatalf("wrong key unlock status=%d, want 401", code)
	}
	if unlocked, _ := readStatus(); unlocked {
		t.Fatal("failed key verification published an unlocked runtime")
	}
	if code := unlock(key); code != http.StatusNoContent {
		t.Fatalf("valid key unlock status=%d, want 204", code)
	}
	if unlocked, state := readStatus(); !unlocked || state != "ready" {
		t.Fatalf("unlocked status = %v/%q, want true/ready", unlocked, state)
	}
	resp = request(http.MethodPost, "/v1/lock", nil, "separate-admin-token-with-32-bytes")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("lock status=%d, want 204", resp.StatusCode)
	}
	if unlocked, _ := readStatus(); unlocked {
		t.Fatal("database remained unlocked after lock")
	}
}

func TestHandlerRejectsPlainHTTP(t *testing.T) {
	h, err := admin.New(admin.Config{BearerToken: []byte("separate-admin-token-with-32-bytes"), Open: func(context.Context, db.KeyMaterial) (*db.DB, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("plain HTTP status=%d, want 403", resp.Code)
	}
}
