package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestClient_Operations(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "healthy", "version": "0.1.0"})
	})

	mux.HandleFunc("/v1/admin/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"admin": true, "connections": 5})
	})

	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["query"] == "SELECT error" {
			http.Error(w, "syntax error", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(QueryResult{
			Columns: []string{"id", "val"},
			Rows:    [][]string{{"1", "a"}},
		})
	})

	mux.HandleFunc("/v1/exec", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["query"] == "INVALID" {
			http.Error(w, "exec failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ExecResult{
			RowsAffected: 1,
			LastInsertID: 42,
		})
	})

	mux.HandleFunc("/v1/debug/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"node_id": "peer-1", "addr": "127.0.0.1:7443"},
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx := context.Background()

	// 1. NewClient Options
	cli, err := NewClient(ClientOptions{
		BaseURL:  ts.URL,
		Timeout:  2 * time.Second,
		Insecure: true,
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// 2. Status
	status, err := cli.Status(ctx)
	if err != nil || status["status"] != "healthy" {
		t.Errorf("Status failed: %v, status: %v", err, status)
	}

	// 3. AdminStatus
	adminStatus, err := cli.AdminStatus(ctx)
	if err != nil || adminStatus["admin"] != true {
		t.Errorf("AdminStatus failed: %v, status: %v", err, adminStatus)
	}

	// 4. Query Success & Error
	qRes, err := cli.Query(ctx, "SELECT 1")
	if err != nil || len(qRes.Rows) != 1 {
		t.Errorf("Query failed: %v, res: %v", err, qRes)
	}
	_, err = cli.Query(ctx, "SELECT error")
	if err == nil {
		t.Errorf("expected Query error, got nil")
	}

	// 5. Exec Success & Error
	eRes, err := cli.Exec(ctx, "INSERT INTO t VALUES (1)")
	if err != nil || eRes.RowsAffected != 1 {
		t.Errorf("Exec failed: %v, res: %v", err, eRes)
	}
	_, err = cli.Exec(ctx, "INVALID")
	if err == nil {
		t.Errorf("expected Exec error, got nil")
	}

	// 6. DebugPeers
	peers, err := cli.DebugPeers(ctx)
	if err != nil || len(peers) != 1 {
		t.Errorf("DebugPeers failed: %v, peers: %v", err, peers)
	}
}

func TestClient_ErrorsAndFallbacks(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "fallback"})
	})
	mux.HandleFunc("/v1/admin/status", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	mux.HandleFunc("/v1/debug/peers", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx := context.Background()
	cli, err := NewClient(ClientOptions{BaseURL: ts.URL})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// AdminStatus fallback to /v1/status
	adminStatus, err := cli.AdminStatus(ctx)
	if err != nil || adminStatus["status"] != "fallback" {
		t.Errorf("expected fallback to /v1/status, got %v, err: %v", adminStatus, err)
	}

	// DebugPeers error
	_, err = cli.DebugPeers(ctx)
	if err == nil {
		t.Errorf("expected error from DebugPeers, got nil")
	}

	// Status error (non-200)
	failTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer failTs.Close()

	failCli, err := NewClient(ClientOptions{BaseURL: failTs.URL})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if _, err := failCli.Status(ctx); err == nil {
		t.Errorf("expected Status error on 500")
	}
	if _, err := failCli.Query(ctx, "SELECT 1"); err == nil {
		t.Errorf("expected Query error on 500")
	}
	if _, err := failCli.Exec(ctx, "INSERT 1"); err == nil {
		t.Errorf("expected Exec error on 500")
	}

	// JSON decode errors (server returns 200 with invalid JSON body)
	corruptTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer corruptTs.Close()

	corruptCli, _ := NewClient(ClientOptions{BaseURL: corruptTs.URL})
	if _, err := corruptCli.Status(ctx); err == nil {
		t.Errorf("expected Status decode error")
	}
	if _, err := corruptCli.AdminStatus(ctx); err == nil {
		t.Errorf("expected AdminStatus decode error")
	}
	if _, err := corruptCli.Query(ctx, "SELECT 1"); err == nil {
		t.Errorf("expected Query decode error")
	}
	if _, err := corruptCli.Exec(ctx, "INSERT 1"); err == nil {
		t.Errorf("expected Exec decode error")
	}
	if _, err := corruptCli.DebugPeers(ctx); err == nil {
		t.Errorf("expected DebugPeers decode error")
	}
}

func TestClient_TLSAndOptions(t *testing.T) {
	// BaseURL without protocol prefix
	cli, err := NewClient(ClientOptions{BaseURL: "127.0.0.1:9999"})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if cli.baseURL != "https://127.0.0.1:9999" {
		t.Errorf("expected https prefix added, got: %s", cli.baseURL)
	}

	// Invalid CA File
	_, err = NewClient(ClientOptions{BaseURL: "localhost:8080", CAFile: "/nonexistent/ca.pem"})
	if err == nil {
		t.Errorf("expected error for nonexistent CAFile")
	}

	// Non-PEM CA File
	tmpCA, err := os.CreateTemp("", "bad_ca_*.pem")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	defer os.Remove(tmpCA.Name())
	_, _ = tmpCA.WriteString("NOT A REAL PEM CERTIFICATE")
	_ = tmpCA.Close()

	_, err = NewClient(ClientOptions{BaseURL: "localhost:8080", CAFile: tmpCA.Name()})
	if err == nil {
		t.Errorf("expected error for non-PEM CAFile")
	}

	// Invalid Cert/Key files
	_, err = NewClient(ClientOptions{BaseURL: "localhost:8080", CertFile: "/nonexistent/cert.pem", KeyFile: "/nonexistent/key.pem"})
	if err == nil {
		t.Errorf("expected error for nonexistent Cert/Key files")
	}
}
