package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"math/big"
	"net/http"
)

func (d *NodeDaemon) handleMerge(w http.ResponseWriter, r *http.Request) {
	var req struct{ Table, Column, Row, Operation, Delta, Element, Kind string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	node, err := ids.ParseNodeID(req.Row)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	row := ids.RowID(node)
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "locked", 503)
		return
	}
	tx, err := database.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer tx.Rollback()
	switch req.Operation {
	case "increment":
		delta, ok := new(big.Int).SetString(req.Delta, 10)
		if !ok {
			err = fmt.Errorf("invalid delta")
		} else {
			err = tx.CounterAdd(r.Context(), req.Table, req.Column, row, delta)
		}
	case "add", "remove":
		var e db.SetElement
		switch req.Kind {
		case "string", "":
			e = db.SetString(req.Element)
		case "integer":
			v, ok := new(big.Int).SetString(req.Element, 10)
			if !ok || !v.IsInt64() {
				err = fmt.Errorf("invalid integer")
			} else {
				e = db.SetInt(v.Int64())
			}
		case "null":
			e = db.SetNull()
		case "boolean":
			e = db.SetBool(req.Element == "true")
		default:
			err = fmt.Errorf("invalid kind")
		}
		if err == nil {
			if req.Operation == "add" {
				err = tx.SetAdd(r.Context(), req.Table, req.Column, row, e)
			} else {
				err = tx.SetRemove(r.Context(), req.Table, req.Column, row, e)
			}
		}
	default:
		err = fmt.Errorf("invalid operation")
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	w.WriteHeader(204)
}
func (d *NodeDaemon) handleMergeState(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	database := d.database
	d.mu.Unlock()
	if database == nil {
		http.Error(w, "locked", 503)
		return
	}
	id, err := ids.ParseNodeID(r.URL.Query().Get("row"))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	hashes := make(map[string]string)
	for _, column := range []string{"count", "tags"} {
		records, err := database.CRDTRecords(r.Context(), r.URL.Query().Get("table"), column, ids.RowID(id))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		sum := sha256.Sum256(codec.EncodeCRDTRecords(nil, records))
		hashes[column] = hex.EncodeToString(sum[:])
	}
	json.NewEncoder(w).Encode(hashes)
}
