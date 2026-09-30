// Multi-process subscription continuity: a server-sent-events subscription
// on one spedsql daemon observes a far-side write while meshed, sees
// nothing across a split, then receives the backlog after healing with no
// reset in between.
package subscribe_test

import (
	"encoding/hex"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func hasBody(ev harness.StreamEvent, body string) bool {
	for _, row := range ev.Rows {
		for _, v := range row {
			if v.S == body {
				return true
			}
		}
	}
	return false
}

func TestSubscriptionContinuityAcrossPartition(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "subscribe",
		NumNodes: 2,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "notes", Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "body", Type: schema.ColText, Nullable: true},
		}}}},
	})

	sub, err := cluster.Subscribe(0, `SELECT body FROM notes`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if ev := harness.NextStreamEvent(t, sub, 10*time.Second); ev.Type != string(db.EventInitial) {
		t.Fatalf("first event = %q", ev.Type)
	}

	// Sanity: a far-side write arrives while meshed.
	id := ids.NewRowID()
	if err := cluster.ExecSQL(1, `INSERT INTO notes (id, body) VALUES (?, ?)`, hex.EncodeToString(id[:]), "one"); err != nil {
		t.Fatal(err)
	}
	sawOne := false
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ev := harness.NextStreamEvent(t, sub, 15*time.Second)
		if ev.Type == string(db.EventReset) {
			t.Fatalf("reset before partition: %s", ev.Error)
		}
		if hasBody(ev, "one") {
			sawOne = true
			break
		}
	}
	if !sawOne {
		t.Fatal("meshed update never arrived")
	}

	// Split: the far-side write must not arrive.
	if err := cluster.RemovePeer(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RemovePeer(1, 0); err != nil {
		t.Fatal(err)
	}
	id2 := ids.NewRowID()
	if err := cluster.ExecSQL(1, `INSERT INTO notes (id, body) VALUES (?, ?)`, hex.EncodeToString(id2[:]), "two"); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-sub.Events():
		if hasBody(ev, "two") {
			t.Fatal("partitioned write leaked across the split")
		}
	case <-time.After(500 * time.Millisecond):
	}

	// Heal: the backlog arrives and the stream continues without reset.
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ev := harness.NextStreamEvent(t, sub, 20*time.Second)
		if ev.Type == string(db.EventReset) {
			t.Fatalf("reset across heal: %s", ev.Error)
		}
		if hasBody(ev, "two") {
			return
		}
	}
	t.Fatal("healed update never arrived")
}
