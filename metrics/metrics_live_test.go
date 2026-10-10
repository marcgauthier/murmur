package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/internal/testidentity"
)

type metricsContact struct {
	ID   murmur.RowID `rime:"primary"`
	Name string
}

func TestJSONHandlerLiveDB(t *testing.T) {
	ctx := context.Background()
	definition, err := murmur.Model[metricsContact]()
	if err != nil {
		t.Fatal(err)
	}
	nodeID := murmur.NewNodeID()
	cfg := murmur.Config{
		Path: t.TempDir(), NodeID: nodeID, OriginSigning: testidentity.Config(nodeID),
		Tables:     []murmur.TableDefinition{definition},
		Spool:      murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{Key: bytes.Repeat([]byte{0x3a}, 32), KeyID: "test-key"},
	}
	db, err := murmur.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := murmur.NewRowID()
	if err := db.InsertItem(ctx, &metricsContact{ID: id, Name: "ann"}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	Handler(db.Status).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	var response Response
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	values := make(map[string]float64)
	for _, sample := range response.Metrics {
		if len(sample.Labels) == 0 {
			values[sample.Name] = sample.Value
		}
	}
	if values["spedsql_local_commits_total"] != 1 || values["spedsql_local_commit_mutations_total"] < 1 ||
		values["spedsql_local_seq"] != 1 || values["spedsql_state_generation"] != 1 || values["spedsql_peer_count"] != 0 {
		t.Fatalf("unexpected live metrics: %+v", values)
	}
}
