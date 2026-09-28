package replicateddb

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fetchReplConfig is a replicating two-node config with files enabled. The
// fetch endpoint listens on an ephemeral port; FetchPeers wires statically.
func fetchReplConfig(path string, node NodeID, dbid DBID, tls *TLSCredential, peers []Peer) Config {
	cfg := replConfig(path, node, dbid, tls, peers)
	cfg.Files.Enabled = true
	cfg.Files.ObjectKey = append([]byte(nil), testObjectKey...)
	cfg.Files.FetchAddr = "127.0.0.1:0"
	cfg.Files.FetchInterval = -1 // manual FetchFile unless a test opts in
	cfg.Files.FetchTimeout = 30 * time.Second
	return cfg
}

func fetchAddr(t *testing.T, db *DB) string {
	t.Helper()
	if db.files == nil || db.files.fetch.server == nil {
		t.Fatal("fetch server not running")
	}
	return db.files.fetch.server.Addr()
}

func waitForFileMeta(t *testing.T, db *DB, name string, timeout time.Duration) FileStatus {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := db.FileStatus(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if st.Exists {
			return st
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q metadata", name)
	return FileStatus{}
}

func waitForFileAvailable(t *testing.T, db *DB, name string, timeout time.Duration) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := db.FileStatus(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if st.Exists && !st.Deleted && st.Available {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q availability", name)
}

func openFetchPair(t *testing.T, ctx context.Context) (dbA, dbB *DB, nodeA, nodeB NodeID, dbid DBID, addrA string) {
	t.Helper()
	nodeA, nodeB = NewNodeID(), NewNodeID()
	dbid = NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dbA, err := Open(ctx, fetchReplConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbA.Close() })
	replA := waitForAddr(t, dbA, 5*time.Second)
	addrA = fetchAddr(t, dbA)

	cfgB := fetchReplConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{replA}}})
	cfgB.Files.FetchPeers = []Peer{{NodeID: nodeA, Addrs: []string{addrA}}}
	dbB, err = Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbB.Close() })
	return dbA, dbB, nodeA, nodeB, dbid, addrA
}

func TestFileFetchEndToEnd(t *testing.T) {
	ctx := context.Background()
	dbA, dbB, _, _, _, _ := openFetchPair(t, ctx)

	data := make([]byte, 200_000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	uploadBytes(t, dbA, "shared/blob", data)

	// Metadata converges through replication; bytes stay on A.
	st := waitForFileMeta(t, dbB, "shared/blob", 20*time.Second)
	if st.Available {
		t.Fatal("B has bytes before any fetch")
	}
	if err := dbB.FetchFile(ctx, "shared/blob"); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	st, err := dbB.FileStatus(ctx, "shared/blob")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Available {
		t.Fatal("fetch succeeded but bytes unavailable")
	}
	r, err := dbB.OpenFile(ctx, "shared/blob")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("fetched bytes differ from upload")
	}
	stats := dbB.FileFetchStats()
	if stats.Completed != 1 || stats.Failed != 0 || stats.BytesFetched != uint64(len(data)) {
		t.Fatalf("fetch stats %+v", stats)
	}
	// Install consumes staging: no residue remains.
	entries, err := os.ReadDir(dbB.files.fetch.staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("%d staging files remain after fetch", len(entries))
	}
}

func TestFileFetchSourceFallback(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB, nodeC, nodeD := NewNodeID(), NewNodeID(), NewNodeID(), NewNodeID()
	dbid := NewDBID()
	otherDB := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB, nodeC, nodeD)

	dbA, err := Open(ctx, fetchReplConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbA.Close() })
	replA := waitForAddr(t, dbA, 5*time.Second)
	fetchA := fetchAddr(t, dbA)

	// C serves a different database: dials succeed, requests refuse.
	cfgC := fetchReplConfig(t.TempDir(), nodeC, otherDB, creds[nodeC], nil)
	cfgC.Replication.ListenAddr = ""
	dbC, err := Open(ctx, cfgC)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbC.Close() })
	fetchC := fetchAddr(t, dbC)

	// D serves our database but holds no objects: requests miss.
	cfgD := fetchReplConfig(t.TempDir(), nodeD, dbid, creds[nodeD], nil)
	cfgD.Replication.ListenAddr = ""
	dbD, err := Open(ctx, cfgD)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbD.Close() })
	fetchD := fetchAddr(t, dbD)

	cfgB := fetchReplConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{replA}}})
	cfgB.Files.FetchPeers = []Peer{
		{NodeID: nodeC, Addrs: []string{fetchC}},
		{NodeID: nodeD, Addrs: []string{fetchD}},
		{NodeID: nodeA, Addrs: []string{fetchA}},
	}
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbB.Close() })

	data := []byte("fallback payload")
	uploadBytes(t, dbA, "shared/fallback", data)
	waitForFileMeta(t, dbB, "shared/fallback", 20*time.Second)
	// Rotation starts at index 0 on a fresh node: C refuses, D misses,
	// A serves.
	if err := dbB.FetchFile(ctx, "shared/fallback"); err != nil {
		t.Fatalf("fetch with fallback: %v", err)
	}
	r, err := dbB.OpenFile(ctx, "shared/fallback")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("fetched bytes differ from upload")
	}
	stats := dbB.FileFetchStats()
	if stats.Completed != 1 || stats.Failed != 0 {
		t.Fatalf("fetch stats %+v", stats)
	}
	if stats.LastError == "" {
		t.Fatal("fallback attempts left no diagnostics")
	}
}

func TestFileFetchResume(t *testing.T) {
	ctx := context.Background()
	dbA, dbB, nodeA, _, _, _ := openFetchPair(t, ctx)

	data := make([]byte, 150_000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	info := uploadBytes(t, dbA, "shared/resume", data)
	st := waitForFileMeta(t, dbB, "shared/resume", 20*time.Second)

	// Plant a staged prefix: the first half of A's container, as if a
	// previous attempt was interrupted mid-transfer.
	container, err := os.ReadFile(filepath.Join(dbA.cfg.Path, "files", "objects", info.Digest.String()+".spfo"))
	if err != nil {
		t.Fatal(err)
	}
	half := len(container) / 2
	stage := filepath.Join(dbB.files.fetch.staging, st.Digest.String()+"-"+nodeA.String()+".part")
	if err := os.WriteFile(stage, container[:half], 0600); err != nil {
		t.Fatal(err)
	}
	servedBefore := dbA.files.fetch.server.Stats().BytesOut
	if err := dbB.FetchFile(ctx, "shared/resume"); err != nil {
		t.Fatalf("resumed fetch: %v", err)
	}
	served := dbA.files.fetch.server.Stats().BytesOut - servedBefore
	if served >= uint64(len(container)) {
		t.Fatalf("server sent %d bytes for a %d-byte container with a %d-byte staged prefix; resume did not apply", served, len(container), half)
	}
	r, err := dbB.OpenFile(ctx, "shared/resume")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("resumed bytes differ from upload")
	}
}

func TestFileFetchWorker(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dbA, err := Open(ctx, fetchReplConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbA.Close() })
	replA := waitForAddr(t, dbA, 5*time.Second)
	fetchA := fetchAddr(t, dbA)

	cfgB := fetchReplConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{replA}}})
	cfgB.Files.FetchPeers = []Peer{{NodeID: nodeA, Addrs: []string{fetchA}}}
	cfgB.Files.FetchInterval = 200 * time.Millisecond
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbB.Close() })

	data := []byte("worker pulls this")
	uploadBytes(t, dbA, "shared/worker", data)
	// No manual FetchFile: replication wakes the scan, the worker pulls.
	waitForFileAvailable(t, dbB, "shared/worker", 30*time.Second)
	r, err := dbB.OpenFile(ctx, "shared/worker")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("worker-fetched bytes differ from upload")
	}
	if n := dbB.FileFetchStats().Completed; n < 1 {
		t.Fatalf("worker completed %d fetches", n)
	}
}

func TestFileFetchNoSources(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	info := uploadBytes(t, db, "lonely/file", []byte("no peers"))
	objPath := filepath.Join(db.cfg.Path, "files", "objects", info.Digest.String()+".spfo")
	if err := os.Remove(objPath); err != nil {
		t.Fatal(err)
	}
	err = db.FetchFile(ctx, "lonely/file")
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("no fetch sources")) {
		t.Fatalf("fetch without sources: got %v", err)
	}
}

func TestFetchStagingSweep(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	staging := db.files.fetch.staging
	hex64 := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	orphan := filepath.Join(staging, hex64+"-node.part")
	if err := os.WriteFile(orphan, []byte("orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatal(err)
	}
	fresh := filepath.Join(staging, hex64+"-other.part")
	if err := os.WriteFile(fresh, []byte("fresh"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FilesGC(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("aged orphan staging survives GC past its grace")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatal("fresh staging swept before its grace")
	}
	if _, err := db.FilesGC(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fresh); !os.IsNotExist(err) {
		t.Fatal("staging survives zero-grace GC")
	}
}
