package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"github.com/marcgauthier/murmur/ids"
	"net/http"
)

// The test fixture exposes read-only receipt inspection for negative security assertions.
func (d *NodeDaemon) handleDebugReceipt(w http.ResponseWriter, r *http.Request) {
	id, err := ids.ParseNodeID(r.URL.Query().Get("txid"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "locked", http.StatusServiceUnavailable)
		return
	}
	exists, err := database.HasTransactionReceipt(ids.TxID(id))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]bool{"exists": exists})
}

// Key authorization is confined to the fixture's existing administrative mTLS API.
func (d *NodeDaemon) handleAuthorizeOrigin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID    string `json:"node_id"`
		PublicKey string `json:"public_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	id, err := ids.ParseNodeID(req.NodeID)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	key, err := hex.DecodeString(req.PublicKey)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.database == nil || d.originRegistry == nil {
		http.Error(w, "locked", 503)
		return
	}
	if err := d.originRegistry.Add(id, ed25519.PublicKey(key)); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	w.WriteHeader(http.StatusOK)
}
