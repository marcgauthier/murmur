package metrics

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	replicateddb "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/schema"
)

// TestCollectorLiveDB proves the collector serves a real DB snapshot with
// no global registration and no HTTP listener.
func TestCollectorLiveDB(t *testing.T) {
	ctx := context.Background()
	cfg := replicateddb.Config{
		Path:   t.TempDir(),
		NodeID: replicateddb.NewNodeID(),
		Schema: replicateddb.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "contacts",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   bytes.Repeat([]byte{0x3a}, 32),
			KeyID: "test-key",
		},
	}
	db, err := replicateddb.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id := replicateddb.NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
		t.Fatal(err)
	}

	c := NewCollector(db.Status)
	want := `
# HELP spedsql_local_commits_total Durable local commits.
# TYPE spedsql_local_commits_total counter
spedsql_local_commits_total 1
# HELP spedsql_state_generation Durable state generation.
# TYPE spedsql_state_generation gauge
spedsql_state_generation 1
# HELP spedsql_peer_count Known peers (configured plus inbound-discovered).
# TYPE spedsql_peer_count gauge
spedsql_peer_count 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want),
		"spedsql_local_commits_total", "spedsql_state_generation",
		"spedsql_peer_count"); err != nil {
		t.Fatal(err)
	}
}
