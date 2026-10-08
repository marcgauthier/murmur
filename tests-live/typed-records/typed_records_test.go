package typedrecords_test

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestTypedFilesReplicateAndFetchWithoutSQL(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-files", NumNodes: 2, TypedRecords: true,
		Files: &harness.FilesOptions{
			ObjectKeyHex:    strings.Repeat("44", 32),
			MaxFileBytes:    1 << 20,
			FetchIntervalMs: -1,
		},
	})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)
	name := "typed/report.bin"
	want := []byte("file metadata uses Spool while bytes are fetched from the peer")
	if _, err := cluster.UploadFile(0, name, want); err != nil {
		t.Fatalf("typed node upload: %v", err)
	}
	if err := cluster.WaitFileAvailable(1, name, false, 30*time.Second); err != nil {
		t.Fatalf("replicated typed file metadata: %v", err)
	}
	if _, err := cluster.FetchFile(1, name); err != nil {
		t.Fatalf("fetch typed file object: %v", err)
	}
	if err := cluster.WaitFileAvailable(1, name, true, 30*time.Second); err != nil {
		t.Fatalf("typed file object availability: %v", err)
	}
	got, err := cluster.DownloadFile(1, name)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("typed peer download = %q, %v", got, err)
	}
	if err := cluster.DeleteFile(0, name); err != nil {
		t.Fatalf("typed file delete: %v", err)
	}
	if err := cluster.WaitFileDeleted(1, name, 30*time.Second); err != nil {
		t.Fatalf("replicated typed file tombstone: %v", err)
	}

	for _, traversal := range []string{
		"../../../../etc/passwd", "..\\..\\windows\\win.ini",
		"%2e%2e%2fetc%2fpasswd", "/etc/passwd",
		"typed/report.bin/../../etc/passwd", "..",
	} {
		if data, err := cluster.DownloadFile(0, traversal); err == nil && len(data) != 0 {
			t.Fatalf("traversal download %q returned %d bytes", traversal, len(data))
		}
		if status, err := cluster.StatFile(0, traversal); err == nil && (status.Exists || status.Available) {
			t.Fatalf("traversal status %q reports an available object: %+v", traversal, status)
		}
		if hits, err := cluster.SearchFiles(0, "", traversal); err == nil {
			for _, hit := range hits {
				if hit.Name == traversal && hit.Available {
					t.Fatalf("traversal search %q reports an available object", traversal)
				}
			}
		}
		_ = cluster.DeleteFile(0, traversal)
	}

	marker := "typed-file-traversal-marker"
	traversalName := "../../" + marker
	traversalBytes := []byte("typed file names stay opaque object keys")
	if _, err := cluster.UploadFile(0, traversalName, traversalBytes); err != nil {
		t.Fatalf("upload traversal-shaped typed file name: %v", err)
	}
	got, err = cluster.DownloadFile(0, traversalName)
	if err != nil || !bytes.Equal(got, traversalBytes) {
		t.Fatalf("traversal-shaped typed file round-trip = %q, %v", got, err)
	}
	if err := cluster.DeleteFile(0, traversalName); err != nil {
		t.Fatalf("delete traversal-shaped typed file name: %v", err)
	}
	if err := filepath.WalkDir(cluster.RuntimeDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if strings.Contains(entry.Name(), marker) {
			t.Errorf("typed traversal-shaped name escaped into filesystem path: %s", path)
		}
		return nil
	}); err != nil {
		t.Fatalf("scan typed cluster runtime directory: %v", err)
	}
}

func TestTypedNodeLocalRowsPersistOnlyOnTheirNode(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-node-local", NumNodes: 2, TypedRecords: true,
	})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)
	if err := cluster.TypedLocalInsert(0, "private-node-zero"); err != nil {
		t.Fatalf("write node-local row: %v", err)
	}
	if err := cluster.TypedInsert(0, "replication-barrier"); err != nil {
		t.Fatalf("write replicated barrier row: %v", err)
	}
	waitTypedCount(t, cluster, 1, "replication-barrier", 1, 30*time.Second)
	if got, err := cluster.TypedLocalCount(0, "private-node-zero"); err != nil || got != 1 {
		t.Fatalf("local node's private row count = %d, %v; want 1", got, err)
	}
	if got, err := cluster.TypedLocalCount(1, "private-node-zero"); err != nil || got != 0 {
		t.Fatalf("peer received node-local row: count=%d err=%v", got, err)
	}
}

func TestTypedModeRejectsSQLAPIWithoutMutatingRecords(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-sql-isolation", NumNodes: 2, TypedRecords: true,
	})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)

	const marker = "typed-api-sql-isolation"
	if err := cluster.TypedInsert(0, marker); err != nil {
		t.Fatalf("insert typed control row: %v", err)
	}
	waitTypedCount(t, cluster, 1, marker, 1, 30*time.Second)

	var errorResponses []string
	for _, query := range []string{
		"DELETE FROM live_typed_records",
		"INSERT INTO live_typed_records (name) VALUES ('x'); DELETE FROM live_typed_records",
		"INSERT INTO live_typed_records (name) VALUES (x'00000000000000000000000000000000', 'stacked'); DROP TABLE live_typed_records; --",
		"SELECT name FROM live_typed_records WHERE name = ? UNION SELECT secret, secret FROM system_secrets --",
		"INSERT INTO " + strings.Repeat("q", 3000) + " (id) VALUES (1)",
		"SELEC FROM WHERE WHATEVER",
		"SELECT * FROM absent_table WHERE x = 1",
		"SELECT * FROM live_typed_records WHERE name = '" + strings.Repeat("z", 64<<10) + "'",
	} {
		errorResponses = append(errorResponses, postRemovedSQL(t, cluster, "/v1/exec", query))
	}
	for _, query := range []string{
		"SELECT name FROM live_typed_records WHERE name = ?",
		"SELECT id, name FROM live_typed_records WHERE name = ?",
		"SELECT name FROM live_typed_records WHERE name = '' OR '1'='1'",
		"SELECT id, name FROM live_typed_records WHERE name = ? UNION SELECT id, secret FROM system_secrets --",
	} {
		errorResponses = append(errorResponses, postRemovedSQL(t, cluster, "/v1/query", query, "' UNION SELECT 1 --"))
	}
	for _, response := range errorResponses {
		for _, marker := range []string{"goroutine ", "panic:", ".go:", "stack trace", "CREATE TABLE", "key_hex"} {
			if strings.Contains(response, marker) {
				t.Fatalf("SQL API rejection leaked %q: %.300s", marker, response)
			}
		}
	}

	for node := range 2 {
		if got, err := cluster.TypedCount(node, marker); err != nil || got != 1 {
			t.Fatalf("node %d typed control row after rejected SQL = %d, %v; want 1", node, got, err)
		}
	}
}

func postRemovedSQL(t *testing.T, cluster *harness.Cluster, endpoint, query string, args ...any) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"query": query, "args": args})
	if err != nil {
		t.Fatalf("encode removed SQL request: %v", err)
	}
	url := "https://" + cluster.Nodes[0].APIAddr + endpoint
	resp, err := cluster.APIClient().Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST removed SQL endpoint %s: %v", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read removed SQL response: %v", err)
	}
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("removed SQL endpoint %s returned %d, want %d: %s", endpoint, resp.StatusCode, http.StatusGone, body)
	}
	return string(body)
}

func TestTypedCRDTWritesConvergeAfterOfflinePartition(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-offline-crdt", NumNodes: 2, TypedRecords: true, ManualPeers: true,
	})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)

	rowID := ids.NewRowID()
	for node := range 2 {
		if err := cluster.TypedInsertWithID(node, rowID, "offline-shared-row"); err != nil {
			t.Fatalf("node %d local insert while partitioned: %v", node, err)
		}
	}
	if err := cluster.TypedCounterAdd(0, "offline-shared-row", 7); err != nil {
		t.Fatalf("node 0 counter update while partitioned: %v", err)
	}
	if err := cluster.TypedCounterAdd(1, "offline-shared-row", 11); err != nil {
		t.Fatalf("node 1 counter update while partitioned: %v", err)
	}
	if err := cluster.TypedSetAdd(0, "offline-shared-row", "from-node-0"); err != nil {
		t.Fatalf("node 0 set update while partitioned: %v", err)
	}
	if err := cluster.TypedSetAdd(1, "offline-shared-row", "from-node-1"); err != nil {
		t.Fatalf("node 1 set update while partitioned: %v", err)
	}

	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("connect node 0 to node 1: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("connect node 1 to node 0: %v", err)
	}
	for node := range 2 {
		waitTypedCount(t, cluster, node, "offline-shared-row", 1, 30*time.Second)
		waitTypedCounter(t, cluster, node, "offline-shared-row", 18, 30*time.Second)
		waitTypedSet(t, cluster, node, "offline-shared-row", []string{"from-node-0", "from-node-1"}, 30*time.Second)
	}
}

func TestTypedEmptyPeerCatchesUpThroughSnapshot(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-snapshot-catchup", NumNodes: 2, TypedRecords: true, ManualPeers: true,
		Replication: &harness.ReplicationOptions{MinLogRetentionMs: 1, MinRetainedBatches: 1},
	})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)
	for i := range 12 {
		if err := cluster.TypedInsertWithID(0, ids.NewRowID(), "snapshot-row"); err != nil {
			t.Fatalf("source insert %d: %v", i, err)
		}
	}
	// Let the configured log retention interval elapse, then collect old
	// batches before the empty peer is admitted. The receiver must use a
	// snapshot rather than passing only because the source still has the log.
	time.Sleep(1100 * time.Millisecond)
	if err := cluster.TriggerGC(0); err != nil {
		t.Fatalf("collect source log before peer admission: %v", err)
	}
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("connect source to empty peer: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("connect empty peer to source: %v", err)
	}
	waitTypedCount(t, cluster, 1, "snapshot-row", 12, 30*time.Second)
	if got, ok := harness.MetricValueFrom(cluster.FetchPath(1, "/metrics"), "spedsql_repl_snapshots_received_total"); !ok || got < 1 {
		t.Fatalf("typed receiver snapshots received metric = %v (present=%t), want >= 1", got, ok)
	}
}

func TestTypedConcurrentSchemaBranchesMergeAcrossPartition(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-schema-branches", NumNodes: 2, TypedRecords: true, ManualPeers: true,
	})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedInsert(0, "branched-record"); err != nil {
		t.Fatalf("seed shared record: %v", err)
	}
	waitTypedCount(t, cluster, 1, "branched-record", 1, 30*time.Second)

	if err := cluster.RemovePeer(0, 1); err != nil {
		t.Fatalf("partition node 0: %v", err)
	}
	if err := cluster.RemovePeer(1, 0); err != nil {
		t.Fatalf("partition node 1: %v", err)
	}
	if err := cluster.MigrateTypedRecords(0); err != nil {
		t.Fatalf("create Note schema branch: %v", err)
	}
	if err := cluster.MigrateTypedRecordsRegion(1); err != nil {
		t.Fatalf("create Region schema branch: %v", err)
	}
	waitTypedEpoch(t, cluster, 0, 2, 10*time.Second)
	waitTypedEpoch(t, cluster, 1, 2, 10*time.Second)
	branch0, branch1 := cluster.FetchPath(0, "/v1/schema"), cluster.FetchPath(1, "/v1/schema")
	if !strings.Contains(branch0, `"Name":"Note"`) || strings.Contains(branch0, `"Name":"Region"`) ||
		!strings.Contains(branch1, `"Name":"Region"`) || strings.Contains(branch1, `"Name":"Note"`) {
		t.Fatalf("nodes did not hold independent schema branches before reconnect: node0=%s node1=%s", branch0, branch1)
	}
	if err := cluster.TypedSetNote(0, "branched-record", "note-from-node-0"); err != nil {
		t.Fatalf("write Note on branch 0: %v", err)
	}
	if err := cluster.TypedSetRegion(1, "branched-record", "region-from-node-1"); err != nil {
		t.Fatalf("write Region on branch 1: %v", err)
	}

	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("rejoin node 0: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("rejoin node 1: %v", err)
	}
	waitTypedEpoch(t, cluster, 0, 3, 30*time.Second)
	waitTypedEpoch(t, cluster, 1, 3, 30*time.Second)
	for node := range 2 {
		if err := cluster.MigrateTypedRecordsV4(node); err != nil {
			t.Fatalf("bind merged record definition on node %d: %v", node, err)
		}
	}
	for node := range 2 {
		waitTypedBranchValues(t, cluster, node, "branched-record", "note-from-node-0", "region-from-node-1", 30*time.Second)
	}
}

func waitTypedBranchValues(t *testing.T, cluster *harness.Cluster, node int, name, wantNote, wantRegion string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var note, region string
	var lastErr error
	for time.Now().Before(deadline) {
		note, region, lastErr = cluster.TypedBranchValues(node, name)
		if lastErr == nil && note == wantNote && region == wantRegion {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %d merged branch values = (%q,%q), %v; want (%q,%q)", node, note, region, lastErr, wantNote, wantRegion)
}

func TestTypedRecordReplicatesAndRebuildsAcrossRestart(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "typed-records",
		NumNodes:     2,
		TypedRecords: true,
	})
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)
	remoteSchema := cluster.FetchPath(0, "/v1/schema")
	if !strings.Contains(remoteSchema, "live_typed_records") {
		t.Fatalf("remote schema manifest response = %q, want typed table", remoteSchema)
	}

	watch, initial, err := cluster.StartTypedWatch(1, "native-rime-write")
	if err != nil {
		t.Fatalf("start peer typed subscription: %v", err)
	}
	defer watch.Close()
	if initial.Count != 0 {
		t.Fatalf("typed subscription initial count = %d, want 0", initial.Count)
	}
	if err := cluster.TypedInsert(0, "native-rime-write"); err != nil {
		t.Fatalf("typed insert: %v", err)
	}
	if err := cluster.TriggerGC(0); err != nil {
		t.Fatalf("operator-triggered GC through admin API: %v", err)
	}
	update, err := watch.Next()
	if err != nil || update.Type != "update" || update.Count != 1 {
		t.Fatalf("peer typed subscription update = %+v, %v; want one row", update, err)
	}
	waitTypedCount(t, cluster, 1, "native-rime-write", 1, 30*time.Second)
	if err := cluster.TypedExplicitTransaction(0, "explicit-lifecycle"); err != nil {
		t.Fatalf("explicit typed transaction: %v", err)
	}
	waitTypedCount(t, cluster, 1, "explicit-lifecycle", 1, 30*time.Second)
	waitTypedCount(t, cluster, 0, "explicit-lifecycle-rolled-back", 0, 30*time.Second)
	waitTypedCount(t, cluster, 1, "explicit-lifecycle-rolled-back", 0, 30*time.Second)
	if err := cluster.TypedCounterAdd(0, "native-rime-write", 20); err != nil {
		t.Fatalf("typed counter add: %v", err)
	}
	waitTypedCounter(t, cluster, 1, "native-rime-write", 20, 30*time.Second)
	if err := cluster.TypedSetAdd(0, "native-rime-write", "z"); err != nil {
		t.Fatalf("typed set add: %v", err)
	}
	if err := cluster.TypedSetAdd(0, "native-rime-write", "aa"); err != nil {
		t.Fatalf("typed second set add: %v", err)
	}
	if err := cluster.TypedSetRemove(0, "native-rime-write", "z"); err != nil {
		t.Fatalf("typed set remove: %v", err)
	}
	waitTypedSet(t, cluster, 1, "native-rime-write", []string{"aa"}, 30*time.Second)
	if err := cluster.TypedExtremaUpdate(0, "native-rime-write", 18, -3); err != nil {
		t.Fatalf("typed extrema update: %v", err)
	}
	waitTypedExtrema(t, cluster, 1, "native-rime-write", 18, -3, 30*time.Second)

	cluster.StopNode(1)
	cluster.StartNode(1)
	cluster.WaitNodeReady(1)
	waitTypedCount(t, cluster, 1, "native-rime-write", 1, 30*time.Second)
	waitTypedCounter(t, cluster, 1, "native-rime-write", 20, 30*time.Second)
	waitTypedSet(t, cluster, 1, "native-rime-write", []string{"aa"}, 30*time.Second)
	waitTypedExtrema(t, cluster, 1, "native-rime-write", 18, -3, 30*time.Second)
}

func TestTypedSchemaMigrationAdoptsOnOlderPeerAndRetainsNewFields(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "typed-schema-evolution", NumNodes: 2, TypedRecords: true,
	})
	if err := cluster.TypedInsert(0, "schema-evolution-row"); err != nil {
		t.Fatal(err)
	}
	waitTypedCount(t, cluster, 1, "schema-evolution-row", 1, 30*time.Second)
	if err := cluster.MigrateTypedRecords(0); err != nil {
		t.Fatalf("runtime additive typed migration: %v", err)
	}
	waitTypedEpoch(t, cluster, 0, 2, 10*time.Second)
	waitTypedEpoch(t, cluster, 1, 2, 30*time.Second)
	if err := cluster.TypedSetNote(0, "schema-evolution-row", "new-field-survives"); err != nil {
		t.Fatalf("write new typed field: %v", err)
	}
	if err := cluster.TypedRename(1, "schema-evolution-row", "schema-evolution-renamed"); err != nil {
		t.Fatalf("update known field from older typed peer: %v", err)
	}
	waitTypedCount(t, cluster, 0, "schema-evolution-renamed", 1, 30*time.Second)
	waitTypedCount(t, cluster, 1, "schema-evolution-renamed", 1, 30*time.Second)
	waitTypedNote(t, cluster, "schema-evolution-renamed", "new-field-survives", 30*time.Second)

	cluster.StopNode(1)
	cluster.StartNode(1)
	cluster.WaitNodeReady(1)
	waitTypedCount(t, cluster, 1, "schema-evolution-renamed", 1, 30*time.Second)
	waitTypedEpoch(t, cluster, 1, 2, 10*time.Second)
	waitTypedNote(t, cluster, "schema-evolution-renamed", "new-field-survives", 30*time.Second)
}

func waitTypedEpoch(t *testing.T, cluster *harness.Cluster, node int, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, err := cluster.TypedSchemaEpoch(node)
		if err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedSchemaEpoch(node)
	t.Fatalf("node %d typed schema epoch = %d, %v; want %d", node, got, err, want)
}

func waitTypedNote(t *testing.T, cluster *harness.Cluster, name, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, err := cluster.TypedNote(0, name)
		if err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedNote(0, name)
	t.Fatalf("typed Note = %q, %v; want %q", got, err, want)
}

func waitTypedExtrema(t *testing.T, cluster *harness.Cluster, node int, name string, peak int64, floor float64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastPeak int64
	var lastFloor float64
	var lastErr error
	for time.Now().Before(deadline) {
		lastPeak, lastFloor, lastErr = cluster.TypedExtremaValues(node, name)
		if lastErr == nil && lastPeak == peak && lastFloor == floor {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %d extrema for %q = (%d,%g), %v; want (%d,%g)", node, name, lastPeak, lastFloor, lastErr, peak, floor)
}

func waitTypedSet(t *testing.T, cluster *harness.Cluster, node int, name string, want []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []string
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = cluster.TypedSetValues(node, name)
		if lastErr == nil && equalStrings(last, want) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %d typed set for %q = %v, %v; want %v", node, name, last, lastErr, want)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func waitTypedCounter(t *testing.T, cluster *harness.Cluster, node int, name string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = cluster.TypedCounterValue(node, name)
		if lastErr == nil && last == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("node %d typed counter for %q = %d, %v; want %d", node, name, last, lastErr, want)
}

func waitTypedCount(t *testing.T, cluster *harness.Cluster, node int, name string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = cluster.TypedCount(node, name)
		if lastErr == nil && last == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("node %d typed count for %q = %d, %v; want %d", node, name, last, lastErr, want)
}
