package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
)

func TestJSONHandlerExportsScalarAndLabeledSeries(t *testing.T) {
	node, dbid, peer := murmur.NewNodeID(), murmur.NewDBID(), murmur.NewNodeID()
	status := murmur.Status{
		State: murmur.StateReady, NodeID: node, DBID: dbid, StateGeneration: 7,
		Metrics: murmur.MetricsSnapshot{
			LocalCommits: 3,
			Scheduler:    murmur.SchedulerSnapshot{Local: murmur.SchedulerClassStats{Acquisitions: 5, ServiceNanos: uint64(2 * time.Second)}},
		},
		Peers: []murmur.PeerDiagnostics{{NodeID: peer, Connected: true, BytesSent: 100}},
	}
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler(func() murmur.Status { return status }).ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	var response Response
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	find := func(name string, labels map[string]string) (float64, bool) {
		for _, sample := range response.Metrics {
			if sample.Name != name || len(sample.Labels) != len(labels) {
				continue
			}
			match := true
			for k, v := range labels {
				if sample.Labels[k] != v {
					match = false
				}
			}
			if match {
				return sample.Value, true
			}
		}
		return 0, false
	}
	for name, want := range map[string]float64{
		"spedsql_state_generation":            7,
		"spedsql_local_commits_total":         3,
		"spedsql_sched_service_seconds_total": 2,
	} {
		labels := map[string]string(nil)
		if name == "spedsql_sched_service_seconds_total" {
			labels = map[string]string{"class": "local"}
		}
		if got, ok := find(name, labels); !ok || got != want {
			t.Errorf("%s = %v, present=%t; want %v", name, got, ok, want)
		}
	}
	if got, ok := find("spedsql_peer_connected", map[string]string{"peer": peer.String()}); !ok || got != 1 {
		t.Errorf("peer connected = %v, present=%t; want 1", got, ok)
	}
	if got, ok := find("spedsql_info", map[string]string{"state": "ready", "node_id": node.String(), "db_id": dbid.String()}); !ok || got != 1 {
		t.Errorf("info = %v, present=%t; want labeled value 1", got, ok)
	}
}

func TestHandlerMethodAndMissingSource(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		src          SnapshotFunc
		want         int
	}{
		{"method", "POST", func() murmur.Status { return murmur.Status{} }, 405},
		{"missing source", "GET", nil, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			Handler(tc.src).ServeHTTP(rec, httptest.NewRequest(tc.method, "/metrics", nil))
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}
