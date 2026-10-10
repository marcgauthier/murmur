package typedrecords_test

import (
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestModelsApplicationReplicatesAndReopens(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{Name: "models-application", NumNodes: 2, TypedRecords: true})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)
	want := harness.ModelRow{Name: "router-01", Site: "OTT", Status: 3, Online: true}
	inserted, _, err := cluster.ModelOperation(0, "insert", want, nil)
	if err != nil || inserted.ID.IsZero() || inserted.ID[6]>>4 != 5 {
		t.Fatalf("insert=%+v, %v", inserted, err)
	}
	wait := func(node int, status int, online bool) harness.ModelRow {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			row, _, err := cluster.ModelOperation(node, "read", harness.ModelRow{Name: want.Name}, nil)
			if err == nil && row.ID == inserted.ID && row.Status == status && row.Online == online {
				return row
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("node %d did not converge", node)
		return harness.ModelRow{}
	}
	wait(1, 3, true)
	// UUIDv5 identity is derived locally from the key on either process.
	duplicate, _, err := cluster.ModelOperation(1, "insert", want, nil)
	if err == nil || !duplicate.ID.IsZero() {
		t.Fatalf("duplicate accepted: %+v, %v", duplicate, err)
	}
	if _, _, err = cluster.ModelOperation(1, "update", harness.ModelRow{Name: want.Name, Status: 2, Online: false}, nil); err != nil {
		t.Fatal(err)
	}
	wait(0, 2, false)
	_, rows, err := cluster.ModelOperation(0, "query", harness.ModelRow{Site: "OTT", Status: 2}, nil)
	if err != nil || len(rows) != 1 || rows[0].ID != inserted.ID {
		t.Fatalf("query=%+v, %v", rows, err)
	}
	if _, _, err = cluster.ModelOperation(0, "insert-many", harness.ModelRow{}, []harness.ModelRow{{Name: "batch-a", Site: "OTT"}, {Name: "batch-b", Site: "OTT"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, rows, err = cluster.ModelOperation(1, "query", harness.ModelRow{Site: "OTT"}, nil)
		if err == nil && len(rows) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("batch replication=%+v, %v", rows, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cluster.StopNode(1)
	cluster.StartNode(1)
	cluster.WaitNodeReady(1)
	wait(1, 2, false)
	// Existing fixture migration now publishes through MigrateModels and retains
	// the runtime collection while peers adopt an additive schema.
	if err = cluster.MigrateTypedRecords(0); err != nil {
		t.Fatal(err)
	}
	wait(0, 2, false)
	wait(1, 2, false)
	if _, _, err = cluster.ModelOperation(0, "delete", harness.ModelRow{Name: want.Name}, nil); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		_, rows, err = cluster.ModelOperation(1, "query", harness.ModelRow{Site: "OTT", Status: 2}, nil)
		if err == nil && len(rows) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delete replication=%+v, %v", rows, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cluster.StopNode(1)
	cluster.StartNode(1)
	cluster.WaitNodeReady(1)
	_, rows, err = cluster.ModelOperation(1, "query", harness.ModelRow{Site: "OTT", Status: 2}, nil)
	if err != nil || len(rows) != 0 {
		t.Fatalf("tombstone after reopen=%+v, %v", rows, err)
	}
}

func TestModelReadShortcutsRichPredicatesAndAssignmentsReplicate(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{Name: "model-query-shortcuts", NumNodes: 2, TypedRecords: true})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)
	now := time.Now().Round(0)
	inserted, _, err := cluster.ModelOperation(0, "insert", harness.ModelRow{Name: "rich-router", Site: "OTT", Host: "router-01.net", Status: 3, Online: true, Seen: now}, nil)
	if err != nil {
		t.Fatal(err)
	}
	query := harness.ModelRow{Host: "router-", Status: 2, Seen: now.In(time.FixedZone("other", 3600))}
	wait := func(node, status int) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			_, rows, err := cluster.ModelOperation(node, "query-rich", query, nil)
			if err == nil && len(rows) == 1 && rows[0].ID == inserted.ID && rows[0].Status == status {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("node %d rich query did not converge", node)
	}
	wait(1, 3)
	_, _, err = cluster.ModelOperation(1, "update", harness.ModelRow{Name: inserted.Name, Host: "router-02.net", Status: 2, Online: false, Seen: now.In(time.FixedZone("updated", -7200))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wait(0, 2)
	cluster.StopNode(0)
	cluster.StartNode(0)
	cluster.WaitNodeReady(0)
	wait(0, 2)
	got, _, err := cluster.ModelOperation(0, "read", harness.ModelRow{Name: inserted.Name}, nil)
	if err != nil || got.Host != "router-02.net" || got.Online || !got.Seen.Equal(now) {
		t.Fatalf("read after restart=%+v %v", got, err)
	}
}
