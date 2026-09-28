package service

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	db "github.com/nomadsql/replicateddb"
)

const (
	defaultMaxRows      = 10_000
	defaultMaxBodyBytes = 1 << 20
)

// Config authenticates and bounds the service handler.
type Config struct {
	// BearerToken authorizes every request. It must hold at least 32 bytes
	// and is separate from inter-node QUIC credentials.
	BearerToken []byte
	// MaxRows caps rows per query/subscription event. Zero selects 10,000.
	MaxRows int
	// MaxBodyBytes caps JSON request bodies. Zero selects 1 MiB.
	MaxBodyBytes int64
}

// Handler serves the versioned service endpoints over an application-owned
// TLS listener. All endpoints require TLS and the Bearer [REDACTED]
type Handler struct {
	database *db.DB
	token    []byte
	maxRows  int
	maxBody  int64
}

// New validates cfg and binds the handler to an open database. The handler
// never closes db; its lifetime stays with the application.
func New(database *db.DB, cfg Config) (*Handler, error) {
	if database == nil {
		return nil, errors.New("service: database is required")
	}
	if len(cfg.BearerToken) < 32 {
		return nil, errors.New("service: Bearer [REDACTED] must contain at least 32 bytes")
	}
	maxRows := cfg.MaxRows
	if maxRows <= 0 {
		maxRows = defaultMaxRows
	}
	maxBody := cfg.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}
	return &Handler{
		database: database,
		token:    append([]byte(nil), cfg.BearerToken...),
		maxRows:  maxRows,
		maxBody:  maxBody,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil {
		writeError(w, http.StatusForbidden, "TLS required")
		return
	}
	provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if len(provided) != len(h.token) || subtle.ConstantTimeCompare([]byte(provided), h.token) != 1 {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
		h.status(w)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/query":
		h.query(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/exec":
		h.exec(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/subscribe":
		h.subscribe(w, r)
	default:
		http.NotFound(w, r)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// StatusDTO is the redacted status surface: identities and counters only,
// never key material.
type StatusDTO struct {
	State          string `json:"state"`
	NodeID         string `json:"node_id"`
	DBID           string `json:"db_id"`
	HLC            uint64 `json:"hlc"`
	LocalSeq       uint64 `json:"local_seq"`
	SchemaEpoch    uint64 `json:"schema_epoch"`
	PeerCount      int    `json:"peer_count"`
	ConnectedPeers int    `json:"connected_peers"`
	UptimeMillis   int64  `json:"uptime_millis"`
}

func (h *Handler) status(w http.ResponseWriter) {
	st := h.database.Status()
	writeJSON(w, StatusDTO{
		State:          st.State.String(),
		NodeID:         st.NodeID.String(),
		DBID:           st.DBID.String(),
		HLC:            st.HLC,
		LocalSeq:       st.LocalSeq,
		SchemaEpoch:    st.SchemaEpoch,
		PeerCount:      st.PeerCount,
		ConnectedPeers: st.ConnectedPeers,
		UptimeMillis:   st.Uptime.Milliseconds(),
	})
}

type sqlRequest struct {
	Query string  `json:"query"`
	Args  []Value `json:"args"`
}

func (h *Handler) decodeBody(w http.ResponseWriter, r *http.Request) (*sqlRequest, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBody)
	var req sqlRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request: %v", err))
		return nil, false
	}
	if req.Query == "" {
		writeError(w, http.StatusBadRequest, "query is required")
		return nil, false
	}
	return &req, true
}

type queryResponse struct {
	Columns []string  `json:"columns"`
	Rows    [][]Value `json:"rows"`
}

func (h *Handler) query(w http.ResponseWriter, r *http.Request) {
	req, ok := h.decodeBody(w, r)
	if !ok {
		return
	}
	if !db.IsReadOnlyStatement(req.Query) {
		writeError(w, http.StatusBadRequest, "query endpoint accepts read-only statements; use exec")
		return
	}
	args, err := ValuesToAny(req.Args)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := h.database.QueryContext(r.Context(), req.Query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()
	cols := rows.Columns()
	raw := make([][]any, 0, 64)
	for rows.Next() {
		if len(raw) >= h.maxRows {
			writeError(w, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("result exceeds %d rows", h.maxRows))
			return
		}
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		raw = append(raw, dest)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	enc, err := MarshalRows(raw)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if enc == nil {
		enc = [][]Value{}
	}
	writeJSON(w, queryResponse{Columns: cols, Rows: enc})
}

type execResponse struct {
	LastInsertID int64 `json:"last_insert_id"`
	RowsAffected int64 `json:"rows_affected"`
}

func (h *Handler) exec(w http.ResponseWriter, r *http.Request) {
	req, ok := h.decodeBody(w, r)
	if !ok {
		return
	}
	args, err := ValuesToAny(req.Args)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := h.database.ExecContext(r.Context(), req.Query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	lastID, _ := res.LastInsertId()
	affected, _ := res.RowsAffected()
	writeJSON(w, execResponse{LastInsertID: lastID, RowsAffected: affected})
}

// streamEvent is the SSE payload for one subscription event.
type streamEvent struct {
	Type    string    `json:"type"`
	Cursor  uint64    `json:"cursor"`
	Columns []string  `json:"columns,omitempty"`
	Rows    [][]Value `json:"rows,omitempty"`
	Error   string    `json:"error,omitempty"`
}

func (h *Handler) subscribe(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	if query == "" {
		writeError(w, http.StatusBadRequest, "query is required")
		return
	}
	args, err := DecodeJSONArgs(r.URL.Query().Get("args"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var opts db.SubscriptionOptions
	if resume := r.URL.Query().Get("resume"); resume != "" {
		cursor, err := strconv.ParseUint(resume, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid resume cursor")
			return
		}
		opts.ResumeFromCursor = cursor
	}
	if eu := r.URL.Query().Get("emit_unchanged"); eu != "" {
		emit, err := strconv.ParseBool(eu)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid emit_unchanged")
			return
		}
		opts.EmitUnchanged = emit
	}
	sub, err := h.database.SubscribeWithOptions(r.Context(), query, opts, args...)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, db.ErrReadOnlyRequired) || errors.Is(err, db.ErrSubscriptionExpired) {
			code = http.StatusBadRequest
		}
		writeError(w, code, err.Error())
		return
	}
	defer sub.Close()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return
			}
			out := streamEvent{Type: string(ev.Type), Cursor: ev.Cursor}
			if ev.Err != nil {
				out.Type = "error"
				out.Error = ev.Err.Error()
				_, _ = w.Write([]byte("data: "))
				_ = enc.Encode(out)
				_, _ = w.Write([]byte("\n"))
				flusher.Flush()
				return
			}
			if len(ev.Rows) > h.maxRows {
				out.Type = "error"
				out.Error = fmt.Sprintf("event exceeds %d rows", h.maxRows)
				_, _ = w.Write([]byte("data: "))
				_ = enc.Encode(out)
				_, _ = w.Write([]byte("\n"))
				flusher.Flush()
				return
			}
			out.Columns = ev.Columns
			raw := make([][]any, len(ev.Rows))
			for i, row := range ev.Rows {
				raw[i] = row.Values
			}
			encRows, err := MarshalRows(raw)
			if err != nil {
				out = streamEvent{Type: "error", Cursor: ev.Cursor, Error: err.Error()}
				_, _ = w.Write([]byte("data: "))
				_ = enc.Encode(out)
				_, _ = w.Write([]byte("\n"))
				flusher.Flush()
				return
			}
			out.Rows = encRows
			_, _ = w.Write([]byte("data: "))
			_ = enc.Encode(out)
			_, _ = w.Write([]byte("\n"))
			flusher.Flush()
		}
	}
}
