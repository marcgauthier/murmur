// Package admin provides an optional TLS-only administrative unlock handler.
// It is independent of replication peer authentication and is not started by
// the embedded database unless an application explicitly mounts it.
package admin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	db "github.com/marcgauthier/spedsql"
)

const maxUnlockBody = 64 << 10

// OpenFunc verifies key material and opens the embedded database. The DB's
// workers are started by db.Open only after its key provider has been checked.
type OpenFunc func(context.Context, db.KeyMaterial) (*db.DB, error)

// Config defines the separate administrative bearer policy and open action.
type Config struct {
	BearerToken []byte
	Open        OpenFunc
}

// Handler serves the optional administrative lifecycle endpoints. All
// endpoints require TLS and the configured bearer token.
type Handler struct {
	token []byte
	open  OpenFunc

	mu      sync.Mutex
	runtime *db.DB
}

// New validates the administrative credentials and open callback.
func New(cfg Config) (*Handler, error) {
	if len(cfg.BearerToken) < 32 {
		return nil, errors.New("admin: bearer token must contain at least 32 bytes")
	}
	if cfg.Open == nil {
		return nil, errors.New("admin: open callback is required")
	}
	return &Handler{token: append([]byte(nil), cfg.BearerToken...), open: cfg.Open}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil {
		http.Error(w, "TLS required", http.StatusForbidden)
		return
	}
	provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if len(provided) != len(h.token) || subtle.ConstantTimeCompare([]byte(provided), h.token) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/unlock":
		h.unlock(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/lock":
		h.lock(w)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
		h.status(w)
	default:
		http.NotFound(w, r)
	}
}

type unlockRequest struct {
	KeyID     string `json:"key_id"`
	Algorithm string `json:"algorithm"`
	Key       []byte `json:"key"`
}

func (h *Handler) unlock(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUnlockBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var req unlockRequest
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid unlock request", http.StatusBadRequest)
		return
	}
	defer zero(req.Key)
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "invalid unlock request", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.KeyID) == "" || strings.TrimSpace(req.Algorithm) == "" || len(req.Key) != 32 {
		http.Error(w, "invalid unlock request", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.runtime != nil {
		http.Error(w, "already unlocked", http.StatusConflict)
		return
	}
	material := db.KeyMaterial{ID: req.KeyID, Algorithm: req.Algorithm, Key: append([]byte(nil), req.Key...)}
	runtime, err := h.open(r.Context(), material)
	zero(material.Key)
	if err != nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		http.Error(w, "unlock failed", http.StatusUnauthorized)
		return
	}
	if runtime == nil {
		http.Error(w, "unlock failed", http.StatusUnauthorized)
		return
	}
	h.runtime = runtime
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) lock(w http.ResponseWriter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.runtime == nil {
		http.Error(w, "already locked", http.StatusConflict)
		return
	}
	runtime := h.runtime
	h.runtime = nil
	if err := runtime.Close(); err != nil {
		http.Error(w, "close failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type statusResponse struct {
	Unlocked bool   `json:"unlocked"`
	State    string `json:"state"`
}

func (h *Handler) status(w http.ResponseWriter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	response := statusResponse{}
	if h.runtime != nil {
		response.Unlocked = true
		response.State = h.runtime.Status().State.String()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// Close locks the service and closes any open database instance.
func (h *Handler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	zero(h.token)
	h.token = nil
	if h.runtime == nil {
		return nil
	}
	runtime := h.runtime
	h.runtime = nil
	return runtime.Close()
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
