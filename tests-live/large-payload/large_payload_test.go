// Multi-process large-payload acceptance: 250 typed rows plus one row with
// a 1.5 MiB value commit on one daemon and must replicate exactly (row
// count plus SHA-256) to its mesh peer.
package largepayload_test

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestBulkTransactionWithLargeValueReplicatesExactly(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "large-payload",
		NumNodes:        2,
		TypedRecords:    true,
		TypedContention: true,
	})

	const bulkRows = 250
	const largeValueBytes = 1536 * 1024
	largeValue := strings.Repeat("murmur-large-payload-", largeValueBytes/len("murmur-large-payload-"))
	largeValue += strings.Repeat("x", largeValueBytes-len(largeValue))
	largeID := ids.NewRowID().String()
	for i := 0; i < bulkRows; i++ {
		id := ids.NewRowID().String()
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: id, Name: fmt.Sprintf("bulk-%04d", i), Score: int64(i)}); err != nil {
			t.Fatalf("insert bulk row %d: %v", i, err)
		}
	}
	if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: largeID, Name: "large-payload", Phone: largeValue}); err != nil {
		t.Fatalf("insert large value: %v", err)
	}

	wantDigest := sha256.Sum256([]byte(largeValue))
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		rows, rowsErr := cluster.TypedContentionRows(1)
		got, readErr := cluster.TypedContentionRead(1, largeID)
		if rowsErr == nil && readErr == nil && len(rows) == bulkRows+1 {
			gotDigest := sha256.Sum256([]byte(got.Phone))
			if len(got.Phone) != largeValueBytes || gotDigest != wantDigest {
				t.Fatalf("replicated value length/digest = %d/%x, want %d/%x", len(got.Phone), gotDigest, largeValueBytes, wantDigest)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d bulk rows and the large payload on node 2", bulkRows+1)
}
