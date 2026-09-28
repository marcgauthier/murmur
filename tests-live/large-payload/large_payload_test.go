// Multi-process large-payload acceptance: a 250-row bulk transaction with
// one 1.5 MiB value commits on one spedsql daemon and must replicate
// exactly (row count plus SHA-256) to its mesh peer.
package largepayload_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	db "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/tests-live/harness"
)

func TestBulkTransactionWithLargeValueReplicatesExactly(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "large-payload",
		NumNodes: 2,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "contacts", Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "phone", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		}}}},
	})

	const bulkRows = 250
	const largeValueBytes = 1536 * 1024
	largeValue := strings.Repeat("SPeD-SQL-large-payload-", largeValueBytes/len("SPeD-SQL-large-payload-"))
	largeValue += strings.Repeat("x", largeValueBytes-len(largeValue))
	largeID := ids.NewRowID()
	for i := 0; i < bulkRows; i++ {
		id := ids.NewRowID()
		if err := cluster.ExecSQL(0, `INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)`,
			hex.EncodeToString(id[:]), fmt.Sprintf("bulk-%04d", i), i); err != nil {
			t.Fatalf("insert bulk row %d: %v", i, err)
		}
	}
	if err := cluster.ExecSQL(0, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		hex.EncodeToString(largeID[:]), "large-payload", largeValue); err != nil {
		t.Fatalf("insert large value: %v", err)
	}

	wantDigest := sha256.Sum256([]byte(largeValue))
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		countRes, countErr := cluster.QuerySQL(1, `SELECT count(*) FROM contacts`)
		valueRes, valueErr := cluster.QuerySQL(1, `SELECT phone FROM contacts WHERE id = ?`, hex.EncodeToString(largeID[:]))
		if countErr == nil && valueErr == nil && len(countRes.Rows) == 1 && len(valueRes.Rows) == 1 {
			count, _ := countRes.Rows[0][0].(float64)
			gotValue, _ := valueRes.Rows[0][0].(string)
			if int(count) == bulkRows+1 {
				gotDigest := sha256.Sum256([]byte(gotValue))
				if len(gotValue) != largeValueBytes || gotDigest != wantDigest {
					t.Fatalf("replicated value length/digest = %d/%x, want %d/%x", len(gotValue), gotDigest, largeValueBytes, wantDigest)
				}
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d bulk rows and the large payload on node 2", bulkRows+1)
}
