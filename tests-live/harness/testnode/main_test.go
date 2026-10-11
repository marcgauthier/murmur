package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	db "github.com/marcgauthier/murmur"
)

// TestHandleAdminOpenProgress proves the endpoint reports the last open
// attempt while d.mu is held elsewhere: openDatabase keeps d.mu for the
// whole open, so any d.mu dependency here would deadlock slow opens.
func TestHandleAdminOpenProgress(t *testing.T) {
	d := &NodeDaemon{}
	d.openBegin()
	d.recordOpenProgress(db.OpenProgress{
		Phase:            db.OpenRebuilding,
		ProcessedItems:   42,
		RowsInserted:     40,
		RowsSkipped:      2,
		CurrentTable:     "live_typed_records",
		ReplayFilesTotal: 3,
		ReplayFilesDone:  2,
		ReplayBytesTotal: 300,
		ReplayBytesDone:  200,
		ReplayRecords:    1000,
	})

	get := func() map[string]any {
		t.Helper()
		d.mu.Lock()
		defer d.mu.Unlock()
		req := httptest.NewRequest(http.MethodGet, "/v1/admin/open-progress", nil)
		rec := httptest.NewRecorder()
		d.handleAdminOpenProgress(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return body
	}

	body := get()
	if body["opening"] != true || body["done"] != false {
		t.Fatalf("opening/done = %v/%v", body["opening"], body["done"])
	}
	if body["phase"] != "rebuilding" || body["processed_items"] != 42.0 ||
		body["rows_inserted"] != 40.0 || body["rows_skipped"] != 2.0 ||
		body["current_table"] != "live_typed_records" {
		t.Fatalf("progress = %v", body)
	}
	if body["replay_files_total"] != 3.0 || body["replay_files_done"] != 2.0 ||
		body["replay_bytes_total"] != 300.0 || body["replay_bytes_done"] != 200.0 ||
		body["replay_records"] != 1000.0 {
		t.Fatalf("replay = %v", body)
	}

	d.openEnd(nil)
	body = get()
	if body["opening"] != false || body["done"] != true || body["phase"] != "ready" {
		t.Fatalf("terminal = %v", body)
	}

	d.openBegin()
	d.openEnd(errors.New("boom"))
	body = get()
	if body["phase"] != "failed" || body["error"] != "boom" {
		t.Fatalf("failed = %v", body)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/open-progress", nil)
	rec := httptest.NewRecorder()
	d.handleAdminOpenProgress(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", rec.Code)
	}
}
