package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
)

type liveModelHostname string

type liveModelDevice struct {
	ID     ids.RowID         `rime:"ID" json:"id"`
	Name   string            `rime:"primary" json:"name"`
	Site   string            `rime:"index" json:"site"`
	Status int               `rime:"ordered" json:"status"`
	Online bool              `json:"online"`
	Host   liveModelHostname `rime:"prefix" json:"host"`
	Seen   time.Time         `rime:"index,ordered" json:"seen"`
}

func (d *NodeDaemon) handleModels(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.TypedRecords {
		http.Error(w, "typed test API is disabled", http.StatusNotFound)
		return
	}
	database := d.databaseOrLocked(w)
	if database == nil {
		return
	}
	var req struct {
		Operation string            `json:"operation"`
		Row       liveModelDevice   `json:"row"`
		Rows      []liveModelDevice `json:"rows"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var err error
	switch req.Operation {
	case "insert":
		err = database.InsertItem(r.Context(), &req.Row)
	case "read":
		err = database.FindOne(r.Context(), &req.Row, q.Eq("Name", req.Row.Name))
	case "update":
		err = database.Update(r.Context(), &req.Row, db.Set("Status", req.Row.Status), db.Set("Online", req.Row.Online), db.Set("Host", req.Row.Host), db.Set("Seen", req.Row.Seen))
	case "delete":
		err = database.DeleteItem(r.Context(), &req.Row)
	case "insert-many":
		err = database.InsertMany(r.Context(), &req.Rows)
	case "query":
		err = database.Find(r.Context(), &req.Rows, q.Eq("Site", req.Row.Site), q.Gte("Status", req.Row.Status))
	case "query-rich":
		filters := []q.Matcher{q.StartsWith("Host", string(req.Row.Host)), q.EndsWith("Host", ".net"), q.Contains("Host", "router"), q.Like("Host", "router-%.net"), q.Between("Status", req.Row.Status, 5), q.NotIn("Site", "LAB"), q.Between("Seen", req.Row.Seen, req.Row.Seen.Add(time.Second))}
		err = database.Find(r.Context(), &req.Rows, filters...)
		if err == nil {
			var count int
			count, err = database.Count(r.Context(), liveModelDevice{}, filters...)
			if err == nil && count != len(req.Rows) {
				err = fmt.Errorf("count mismatch")
			}
			var exists bool
			if err == nil {
				exists, err = database.Exists(r.Context(), liveModelDevice{}, filters...)
				if err == nil && exists != (count > 0) {
					err = fmt.Errorf("exists mismatch")
				}
			}
		}
	case "rollback":
		err = database.Transaction(r.Context(), func(tx *db.Tx) error {
			if err := tx.InsertItem(&req.Row); err != nil {
				return err
			}
			return fmt.Errorf("intentional rollback")
		})
	default:
		http.Error(w, "unknown model operation", http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"row": req.Row, "rows": req.Rows})
}
