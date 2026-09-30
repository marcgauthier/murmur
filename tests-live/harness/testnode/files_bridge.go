package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/bridge"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

// FilesConfigFile mirrors db.FilesConfig in node JSON configuration.
type FilesConfigFile struct {
	Enabled         bool         `json:"enabled"`
	ObjectKeyHex    string       `json:"object_key_hex,omitempty"`
	FetchAddr       string       `json:"fetch_addr,omitempty"`
	FetchPeers      []PeerConfig `json:"fetch_peers,omitempty"`
	FetchIntervalMs int64        `json:"fetch_interval_ms,omitempty"`
	FetchTimeoutMs  int64        `json:"fetch_timeout_ms,omitempty"`
	MaxFileBytes    int64        `json:"max_file_bytes,omitempty"`
}

// BridgeConfigFile configures one-directional bridge behavior for a node:
// a Low exporter drains an outbox into a shared staging directory, and a
// High importer drains that directory into its inbox and database.
type BridgeConfigFile struct {
	Role             string `json:"role,omitempty"` // "", "low-exporter", "high-importer"
	Stream           string `json:"stream,omitempty"`
	StagingDir       string `json:"staging_dir,omitempty"`
	OutboxDir        string `json:"outbox_dir,omitempty"`
	InboxDir         string `json:"inbox_dir,omitempty"`
	SignerKeyFile    string `json:"signer_key_file,omitempty"`    // low-exporter: signer private (64B)
	RecipientPubFile string `json:"recipient_pub_file,omitempty"` // low-exporter: recipient public (32B)
	RecipientKeyFile string `json:"recipient_key_file,omitempty"` // high-importer: recipient private (32B)
	SignerPubFile    string `json:"signer_pub_file,omitempty"`    // high-importer: trusted signer public (32B)
	// SignerPubFiles trusts additional signer public keys (rotation
	// overlap); SignerPubFile stays the primary.
	SignerPubFiles []string `json:"signer_pub_files,omitempty"`
}

// bridgeRuntime holds the initialized Low or High bridge stack for a node.
type bridgeRuntime struct {
	mu        sync.Mutex
	role      string
	stream    string
	staging   string
	outbox    *bridge.Outbox
	capturer  *bridge.Capturer
	exporter  *bridge.Exporter
	seal      bridge.Sealer
	publisher bridge.Publisher
	files     *bridge.FilePublisher
	trust     *bridge.TrustStore
	inbox     *bridge.Inbox
	importer  *bridge.Importer
	inboxDir  string
}

func (d *NodeDaemon) databaseOrLocked(w http.ResponseWriter) *db.DB {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return nil
	}
	return d.database
}

// --- files endpoints ---

func (d *NodeDaemon) handleFilesUpload(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Name string `json:"name"`
		Data string `json:"data_base64"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 256<<20)).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
		return
	}
	raw, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad base64: %v", err), http.StatusBadRequest)
		return
	}
	info, err := database.UploadFile(r.Context(), req.Name, bytes.NewReader(raw))
	if err != nil {
		http.Error(w, fmt.Sprintf("upload error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"name":   info.Name,
		"digest": hex.EncodeToString(info.Digest[:]),
		"size":   info.Size,
		"chunks": info.Chunks,
	})
}

func (d *NodeDaemon) handleFilesDownload(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}
	reader, err := database.OpenFile(r.Context(), name)
	if err != nil {
		http.Error(w, fmt.Sprintf("open error: %v", err), http.StatusNotFound)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	if _, err := io.Copy(w, reader); err != nil {
		log.Printf("[SPEDSQL] file download %q interrupted: %v", name, err)
	}
}

func fileStatusJSON(st db.FileStatus) map[string]any {
	return map[string]any{
		"name":      st.Name,
		"digest":    hex.EncodeToString(st.Digest[:]),
		"size":      st.Size,
		"chunks":    st.Chunks,
		"exists":    st.Exists,
		"deleted":   st.Deleted,
		"available": st.Available,
	}
}

func (d *NodeDaemon) handleFilesStatus(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}
	st, err := database.FileStatus(r.Context(), name)
	if err != nil {
		http.Error(w, fmt.Sprintf("status error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(fileStatusJSON(st))
}

func (d *NodeDaemon) handleFilesSearch(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	q := r.URL.Query()
	prefix, substr, limit := q.Get("prefix"), q.Get("substr"), 100
	var out []db.FileStatus
	var err error
	if substr != "" {
		out, err = database.SearchFiles(r.Context(), substr, limit)
	} else {
		out, err = database.ListFiles(r.Context(), prefix, limit)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("search error: %v", err), http.StatusInternalServerError)
		return
	}
	items := make([]any, 0, len(out))
	for _, st := range out {
		items = append(items, fileStatusJSON(st))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"files": items})
}

func (d *NodeDaemon) handleFilesDelete(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		var req struct {
			Name string `json:"name"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
		name = req.Name
	}
	if name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}
	if err := database.DeleteFile(r.Context(), name); err != nil {
		http.Error(w, fmt.Sprintf("delete error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"deleted": name})
}

func (d *NodeDaemon) handleFilesFetchStats(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	st := database.FileFetchStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"pending":       st.Pending,
		"in_flight":     st.InFlight,
		"completed":     st.Completed,
		"failed":        st.Failed,
		"bytes_fetched": st.BytesFetched,
		"last_error":    st.LastError,
	})
}

func (d *NodeDaemon) handleFilesFetch(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "missing name", http.StatusBadRequest)
		return
	}
	if err := database.FetchFile(r.Context(), req.Name); err != nil {
		http.Error(w, fmt.Sprintf("fetch error: %v", err), http.StatusInternalServerError)
		return
	}
	st, err := database.FileStatus(r.Context(), req.Name)
	if err != nil {
		http.Error(w, fmt.Sprintf("status error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(fileStatusJSON(st))
}

// --- bridge endpoints ---

// initBridge builds the Low exporter or High importer stack once the
// database is open. Empty role disables the bridge. Callers hold d.mu.
func (d *NodeDaemon) initBridge(database *db.DB) error {
	cfg := d.cfg.Bridge
	if cfg == nil || cfg.Role == "" {
		return nil
	}
	rt := &bridgeRuntime{role: cfg.Role, stream: cfg.Stream, staging: cfg.StagingDir}
	switch cfg.Role {
	case "low-exporter":
		if cfg.Stream == "" || cfg.StagingDir == "" || cfg.OutboxDir == "" {
			return fmt.Errorf("bridge low-exporter requires stream, staging_dir, outbox_dir")
		}
		signerRaw, err := os.ReadFile(cfg.SignerKeyFile)
		if err != nil {
			return fmt.Errorf("read signer key: %w", err)
		}
		signer, err := bridge.ParseSignerKey(signerRaw)
		if err != nil {
			return fmt.Errorf("parse signer key: %w", err)
		}
		recipientRaw, err := os.ReadFile(cfg.RecipientPubFile)
		if err != nil {
			return fmt.Errorf("read recipient public key: %w", err)
		}
		if len(recipientRaw) != 32 {
			return fmt.Errorf("recipient public key must be 32 bytes, got %d", len(recipientRaw))
		}
		var recipient [32]byte
		copy(recipient[:], recipientRaw)
		outbox, err := bridge.OpenOutbox(cfg.OutboxDir, bridge.Limits{})
		if err != nil {
			return fmt.Errorf("open outbox: %w", err)
		}
		dirPub, err := bridge.NewDirPublisher(cfg.StagingDir, bridge.Limits{})
		if err != nil {
			return fmt.Errorf("open staging publisher: %w", err)
		}
		schema, err := database.BridgeSchema()
		if err != nil {
			return fmt.Errorf("bridge schema: %w", err)
		}
		capturer, err := bridge.NewCapturer(database.BridgeLogSource(), schema, outbox, 64, 4<<20)
		if err != nil {
			return fmt.Errorf("new capturer: %w", err)
		}
		exporter, err := bridge.NewExporter(database, bridge.Config{
			Role:   bridge.RoleLowExporter,
			Domain: database.DBID(),
			Stream: cfg.Stream,
		}, dirPub)
		if err != nil {
			return fmt.Errorf("new exporter: %w", err)
		}
		rt.outbox = outbox
		rt.capturer = capturer
		rt.exporter = exporter
		rt.seal = bridge.NewBundleSealer(exporter, signer, recipient)
		rt.publisher = dirPub
		rt.files = &bridge.FilePublisher{
			Objects:   database.BridgeFileObjects(),
			Signer:    signer,
			Recipient: recipient,
			Outbox:    outbox,
		}
	case "high-importer":
		if cfg.Stream == "" || cfg.StagingDir == "" || cfg.InboxDir == "" {
			return fmt.Errorf("bridge high-importer requires stream, staging_dir, inbox_dir")
		}
		recipientRaw, err := os.ReadFile(cfg.RecipientKeyFile)
		if err != nil {
			return fmt.Errorf("read recipient key: %w", err)
		}
		recipient, err := bridge.ParseRecipientKey(recipientRaw)
		if err != nil {
			return fmt.Errorf("parse recipient key: %w", err)
		}
		signerPubRaw, err := os.ReadFile(cfg.SignerPubFile)
		if err != nil {
			return fmt.Errorf("read signer public key: %w", err)
		}
		if len(signerPubRaw) != 32 {
			return fmt.Errorf("signer public key must be 32 bytes, got %d", len(signerPubRaw))
		}
		var signerPub [32]byte
		copy(signerPub[:], signerPubRaw)
		trust := bridge.NewTrustStore()
		if err := trust.AddSigner(signerPub, cfg.Stream); err != nil {
			return fmt.Errorf("trust signer: %w", err)
		}
		for _, extra := range cfg.SignerPubFiles {
			raw, err := os.ReadFile(extra)
			if err != nil {
				return fmt.Errorf("read extra signer public key: %w", err)
			}
			if len(raw) != 32 {
				return fmt.Errorf("extra signer public key must be 32 bytes, got %d", len(raw))
			}
			var pub [32]byte
			copy(pub[:], raw)
			if err := trust.AddSigner(pub, cfg.Stream); err != nil {
				return fmt.Errorf("trust extra signer: %w", err)
			}
		}
		if err := trust.AddRecipient(recipient); err != nil {
			return fmt.Errorf("trust recipient: %w", err)
		}
		inbox, err := bridge.OpenInbox(cfg.InboxDir, trust, bridge.Limits{})
		if err != nil {
			return fmt.Errorf("open inbox: %w", err)
		}
		importer, err := bridge.NewImporter(database)
		if err != nil {
			return fmt.Errorf("new importer: %w", err)
		}
		rt.trust = trust
		rt.inbox = inbox
		rt.importer = importer
		rt.inboxDir = cfg.InboxDir
	default:
		return fmt.Errorf("unknown bridge role %q", cfg.Role)
	}
	d.bridge = rt
	log.Printf("[SPEDSQL] Bridge %s ready (stream %s)", cfg.Role, cfg.Stream)
	return nil
}

func (d *NodeDaemon) bridgeOrError(w http.ResponseWriter, want string) *bridgeRuntime {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return nil
	}
	if d.bridge == nil || d.bridge.role != want {
		http.Error(w, fmt.Sprintf("bridge role %q not configured", want), http.StatusNotFound)
		return nil
	}
	return d.bridge
}

func (d *NodeDaemon) handleBridgeExport(w http.ResponseWriter, r *http.Request) {
	rt := d.bridgeOrError(w, "low-exporter")
	if rt == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var opts struct {
		CaptureOnly bool `json:"capture_only"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&opts)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	captured, err := rt.capturer.CaptureOnce(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("capture error: %v", err), http.StatusInternalServerError)
		return
	}
	published := 0
	if !opts.CaptureOnly {
		published, err = bridge.PublishPendingWithFiles(ctx, rt.exporter, rt.outbox, rt.seal, rt.publisher, bridge.RetryPolicy{MaxAttempts: 3}, rt.files)
		if err != nil {
			http.Error(w, fmt.Sprintf("publish error: %v", err), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"captured":  captured,
		"published": published,
	})
}

func (d *NodeDaemon) handleBridgeImport(w http.ResponseWriter, r *http.Request) {
	rt := d.bridgeOrError(w, "high-importer")
	if rt == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	applied, err := d.bridgeAppliedLocked(rt)
	if err != nil {
		http.Error(w, fmt.Sprintf("cursor error: %v", err), http.StatusInternalServerError)
		return
	}
	entries, err := os.ReadDir(rt.staging)
	if err != nil {
		http.Error(w, fmt.Sprintf("staging error: %v", err), http.StatusInternalServerError)
		return
	}
	var names []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.Contains(name, ".tmp-") || applied[name] {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	received := 0
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(rt.staging, name))
		if err != nil {
			http.Error(w, fmt.Sprintf("read artifact %s: %v", name, err), http.StatusInternalServerError)
			return
		}
		if err := rt.inbox.Receive(bridge.Artifact{Name: name, Data: raw}); err != nil {
			http.Error(w, fmt.Sprintf("receive artifact %s: %v", name, err), http.StatusInternalServerError)
			return
		}
		applied[name] = true
		received++
	}
	if err := d.bridgeSaveAppliedLocked(rt, applied); err != nil {
		http.Error(w, fmt.Sprintf("cursor error: %v", err), http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	imported, err := rt.importer.Drain(ctx, rt.inbox)
	if err != nil {
		http.Error(w, fmt.Sprintf("import error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"received": received,
		"imported": imported,
	})
}

func (d *NodeDaemon) bridgeCursorFile(rt *bridgeRuntime) string {
	return filepath.Join(rt.inboxDir, "applied.json")
}

func (d *NodeDaemon) bridgeAppliedLocked(rt *bridgeRuntime) (map[string]bool, error) {
	applied := make(map[string]bool)
	raw, err := os.ReadFile(d.bridgeCursorFile(rt))
	if err != nil {
		if os.IsNotExist(err) {
			return applied, nil
		}
		return nil, err
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err != nil {
		return nil, err
	}
	for _, n := range names {
		applied[n] = true
	}
	return applied, nil
}

func (d *NodeDaemon) bridgeSaveAppliedLocked(rt *bridgeRuntime, applied map[string]bool) error {
	var names []string
	for n := range applied {
		names = append(names, n)
	}
	sort.Strings(names)
	raw, err := json.Marshal(names)
	if err != nil {
		return err
	}
	return os.WriteFile(d.bridgeCursorFile(rt), raw, 0644)
}

func (d *NodeDaemon) handleBridgeStatus(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.database == nil {
		http.Error(w, "node is locked", http.StatusServiceUnavailable)
		return
	}
	if d.bridge == nil {
		http.Error(w, "bridge not configured", http.StatusNotFound)
		return
	}
	resp := map[string]any{"role": d.bridge.role, "stream": d.bridge.stream}
	if d.bridge.outbox != nil {
		resp["pending_files"] = len(d.bridge.outbox.PendingFiles())
		resp["pending_events"] = len(d.bridge.outbox.Pending(0))
	}
	if d.bridge.inbox != nil {
		resp["progress"] = d.bridge.inbox.Progress()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (d *NodeDaemon) handleBridgeProvenance(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	q := r.URL.Query()
	table, column, rowHex := q.Get("table"), q.Get("column"), q.Get("row")
	if table == "" || column == "" || rowHex == "" {
		http.Error(w, "missing table, column, or row", http.StatusBadRequest)
		return
	}
	rowRaw, err := hex.DecodeString(rowHex)
	if err != nil || len(rowRaw) != 16 {
		http.Error(w, "row must be 16-byte hex", http.StatusBadRequest)
		return
	}
	var row ids.RowID
	copy(row[:], rowRaw)
	pol, ok, err := database.BridgeFieldProvenance(table, row, column)
	if err != nil {
		http.Error(w, fmt.Sprintf("provenance error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	owner := "low"
	if pol.Owner == db.BridgeOwnerHigh {
		owner = "high"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"present": ok,
		"owner":   owner,
		"stream":  pol.Stream,
	})
}

func (d *NodeDaemon) handleBridgeRelease(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Table  string `json:"table"`
		Column string `json:"column"`
		RowHex string `json:"row_hex"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
		return
	}
	rowRaw, err := hex.DecodeString(req.RowHex)
	if err != nil || len(rowRaw) != 16 {
		http.Error(w, "row_hex must be 16-byte hex", http.StatusBadRequest)
		return
	}
	var row ids.RowID
	copy(row[:], rowRaw)
	if err := database.ReleaseBridgeOwnership(r.Context(), req.Table, row, req.Column); err != nil {
		http.Error(w, fmt.Sprintf("release error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"released": true})
}

func (d *NodeDaemon) handleAdminMigrate(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Tables []schema.TableSchema `json:"tables"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
		return
	}
	if err := database.Migrate(r.Context(), req.Tables); err != nil {
		http.Error(w, fmt.Sprintf("migrate error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"migrated": true})
}

func (d *NodeDaemon) handleAdminRotateKey(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		KeyID     string `json:"key_id"`
		KeyHex    string `json:"key_hex"`
		Algorithm string `json:"algorithm"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad json: %v", err), http.StatusBadRequest)
		return
	}
	key, err := hex.DecodeString(req.KeyHex)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad key_hex: %v", err), http.StatusBadRequest)
		return
	}
	if err := database.RotateStorageKey(r.Context(), db.KeyMaterial{ID: req.KeyID, Algorithm: req.Algorithm, Key: key}); err != nil {
		http.Error(w, fmt.Sprintf("rotate error: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"rotated": true})
}

func (d *NodeDaemon) handleAdminEncryptionStatus(w http.ResponseWriter, r *http.Request) {
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(database.EncryptionStatus())
}
