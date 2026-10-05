// File-crash acceptance: SIGKILL the uploader mid-upload and the
// fetcher mid-fetch, then prove crash consistency: every listed object
// downloads byte-complete and digest-verified on both nodes, metadata
// converges exactly, and on-disk inventory holds no unattributable
// payloads.
//
// Crash-debris rules (from product code, not invented): object bytes
// publish before their metadata commits (files.go), so a killed upload
// may leave an orphan .spfo that FilesGC would reclaim past grace;
// Put stages to .stage-* (removed on clean exit, orphaned by SIGKILL);
// fetches stage to <digest>-<suffix>.part. The suite therefore requires
// every .spfo/.stage-*/.part to be attributable to a known upload or
// fetch attempt and bounded in count — never silently extra.
package filescrash_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

const objectKey = "4242424242424242424242424242424242424242424242424242424242424242"

// attempt records one upload try: digest/size are always known (the
// payload is deterministic), ok reports the uploader's acknowledgement.
type attempt struct {
	digestHex string
	size      int64
	ok        bool
}

func TestFileCrashLeavesNoPartialOrPhantom(t *testing.T) {
	bigMB := envInt("MURMUR_FILES_CRASH_BIG_MB", 3)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "files-crash",
		NumNodes: 2,
		Files: &harness.FilesOptions{
			ObjectKeyHex: objectKey, MaxFileBytes: 4 << 20, FetchIntervalMs: -1,
		},
	})

	var mu sync.Mutex
	attempted := map[string]*attempt{}
	record := func(name string, data []byte, ok bool) {
		sum := sha256.Sum256(data)
		mu.Lock()
		attempted[name] = &attempt{digestHex: hex.EncodeToString(sum[:]), size: int64(len(data)), ok: ok}
		mu.Unlock()
	}
	var fetchAttempts atomic.Int64

	// Positive control: honest upload replicates, fetches, verifies.
	ctrl := payload("ctrl/a.bin", 64<<10)
	st, err := cluster.UploadFile(0, "ctrl/a.bin", ctrl)
	if err != nil {
		t.Fatalf("control upload: %v", err)
	}
	record("ctrl/a.bin", ctrl, true)
	checkUploadDigest(t, st, "ctrl/a.bin", ctrl)
	if err := cluster.WaitFileAvailable(1, "ctrl/a.bin", false, 30*time.Second); err != nil {
		t.Fatalf("control metadata: %v", err)
	}
	fetchAttempts.Add(1)
	if _, err := cluster.FetchFile(1, "ctrl/a.bin"); err != nil {
		t.Fatalf("control fetch: %v", err)
	}
	checkDownload(t, cluster, 1, "ctrl/a.bin", ctrl)
	t.Log("positive control: upload/replicate/fetch/verify works")
	// Baseline inventory before any crash: the clean path must leave
	// zero unattributable payloads (the before half of before/after).
	verifyInventory(t, cluster, attempted, int(fetchAttempts.Load()))

	// Phase 1: SIGKILL the uploader mid-upload (in-flight coordinated).
	uploaderKilled := false
	for round := 1; round <= 3 && !uploaderKilled; round++ {
		var inFlight atomic.Int64
		var errored atomic.Int64
		done := make(chan struct{})
		go func(round int) {
			defer close(done)
			for i := 0; i < 8; i++ {
				name := fmt.Sprintf("crash/u-%d-%d.bin", round, i)
				data := payload(name, 1<<20)
				inFlight.Add(1)
				up, err := cluster.UploadFile(0, name, data)
				inFlight.Add(-1)
				if err != nil {
					errored.Add(1)
					record(name, data, false)
					continue
				}
				record(name, data, true)
				if up.Digest != hexOf(data) {
					t.Errorf("round %d upload %s digest %s, want %s", round, name, up.Digest, hexOf(data))
					return
				}
			}
		}(round)
		if !waitInflight(t, &inFlight, 10*time.Second) {
			<-done
			t.Fatalf("round %d: no upload ever in flight", round)
		}
		t.Logf("round %d: SIGKILL uploader with %d upload(s) in flight", round, inFlight.Load())
		cluster.KillNode(0)
		<-done
		restartNode(t, cluster, 0)
		if errored.Load() == 0 {
			t.Logf("round %d: kill landed between uploads; retrying", round)
			continue
		}
		uploaderKilled = true
		t.Logf("round %d: uploader killed mid-upload (%d errored uploads)", round, errored.Load())
	}
	if !uploaderKilled {
		t.Fatal("no valid mid-upload kill in 3 rounds")
	}

	// Phase 2: SIGKILL the fetcher mid-fetch (InFlight coordinated).
	fetcherKilled := false
	for round := 1; round <= 3 && !fetcherKilled; round++ {
		name := fmt.Sprintf("crash/f-%d.bin", round)
		data := payload(name, bigMB<<20)
		if _, err := cluster.UploadFile(0, name, data); err != nil {
			t.Fatalf("round %d big upload: %v", round, err)
		}
		record(name, data, true)
		if err := cluster.WaitFileAvailable(1, name, false, 30*time.Second); err != nil {
			t.Fatalf("round %d big metadata: %v", round, err)
		}
		fetchDone := make(chan struct{})
		go func() { defer close(fetchDone); fetchAttempts.Add(1); _, _ = cluster.FetchFile(1, name) }()
		killed, tooSlow := killWhileFetching(t, cluster, 1, name, 15*time.Second)
		<-fetchDone
		if tooSlow {
			t.Logf("round %d: fetch completed before the kill; retrying", round)
			continue
		}
		if !killed {
			t.Fatalf("round %d: fetch never went in flight", round)
		}
		restartNode(t, cluster, 1)
		fetcherKilled = true
		t.Logf("round %d: fetcher killed mid-fetch of %s", round, name)
		// The interrupted fetch must complete cleanly after restart:
		// staged partials never poison the verified install.
		big := lastBigName(attempted)
		fetchAttempts.Add(1)
		if _, err := cluster.FetchFile(1, big); err != nil {
			t.Fatalf("post-crash fetch: %v", err)
		}
		checkDownloadByName(t, cluster, 1, big, attempted)
	}
	if !fetcherKilled {
		t.Fatal("no valid mid-fetch kill in 3 rounds")
	}

	// Phase 3: full recovery proof.
	waitMetadataSettled(t, cluster, attempted, 60*time.Second)
	verifyAllObjects(t, cluster, attempted, &fetchAttempts)
	verifyInventory(t, cluster, attempted, int(fetchAttempts.Load()))
	t.Log("files-crash proven: no partials, no phantoms, inventory attributable")
}

// killWhileFetching SIGKILLs idx once its fetch stats show an in-flight
// fetch of name. It reports (killed, tooSlow): tooSlow means the fetch
// completed first and the round proves nothing.
func killWhileFetching(t *testing.T, c *harness.Cluster, idx int, name string, timeout time.Duration) (bool, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st, err := c.StatFile(idx, name); err == nil && st.Available {
			return false, true
		}
		if fs, err := c.FetchStats(idx); err == nil && fs.InFlight > 0 {
			t.Logf("killing fetcher with InFlight=%d", fs.InFlight)
			c.KillNode(idx)
			return true, false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false, false
}

func restartNode(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	c.StartNode(idx)
	c.UnlockNode(idx, c.Nodes[idx].KeyHex)
	c.WaitNodeReady(idx)
}

func waitInflight(t *testing.T, inFlight *atomic.Int64, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if inFlight.Load() > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// waitMetadataSettled polls until both nodes list identical metadata and
// every acknowledged upload is present everywhere.
func waitMetadataSettled(t *testing.T, c *harness.Cluster, attempted map[string]*attempt, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lists := make([][]harness.FileStatus, len(c.Nodes))
		ok := true
		for i := range c.Nodes {
			l, err := c.SearchFiles(i, "", "")
			if err != nil {
				ok = false
				break
			}
			lists[i] = l
		}
		if ok && metadataDigest(lists[0]) == metadataDigest(lists[1]) && allAckedListed(lists[0], attempted) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	for i := range c.Nodes {
		l, _ := c.SearchFiles(i, "", "")
		t.Logf("node %d at timeout: %d files digest=%x", i, len(l), sha256.Sum256([]byte(metadataDigest(l))))
	}
	t.Fatal("file metadata did not settle identically on both nodes")
}

func allAckedListed(list []harness.FileStatus, attempted map[string]*attempt) bool {
	present := map[string]bool{}
	for _, f := range list {
		present[f.Name] = true
	}
	for name, a := range attempted {
		if a.ok && !present[name] {
			return false
		}
	}
	return true
}

func metadataDigest(list []harness.FileStatus) string {
	names := make([]string, 0, len(list))
	byName := map[string]harness.FileStatus{}
	for _, f := range list {
		names = append(names, f.Name)
		byName[f.Name] = f
	}
	sort.Strings(names)
	var sb strings.Builder
	for _, n := range names {
		f := byName[n]
		fmt.Fprintf(&sb, "%s|%s|%d|%v;", f.Name, f.Digest, f.Size, f.Deleted)
	}
	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:])
}

// verifyAllObjects downloads every listed object on both nodes and
// requires byte-complete, digest-verified content; anything listed must
// have been attempted (no phantoms) and every acked upload must be
// listed (no lost writes).
func verifyAllObjects(t *testing.T, c *harness.Cluster, attempted map[string]*attempt, fetchAttempts *atomic.Int64) {
	t.Helper()
	for i := range c.Nodes {
		list, err := c.SearchFiles(i, "", "")
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range list {
			a, tried := attempted[f.Name]
			if !tried {
				t.Fatalf("node %d lists phantom object %q (never uploaded)", i, f.Name)
			}
			if f.Deleted {
				t.Fatalf("node %d object %q unexpectedly deleted", i, f.Name)
			}
			if f.Digest != a.digestHex || f.Size != a.size {
				t.Fatalf("node %d %q metadata digest=%s size=%d, want %s/%d",
					i, f.Name, f.Digest, f.Size, a.digestHex, a.size)
			}
			st, err := c.StatFile(i, f.Name)
			if err != nil {
				t.Fatal(err)
			}
			if !st.Available {
				fetchAttempts.Add(1)
				if _, err := c.FetchFile(i, f.Name); err != nil {
					t.Fatalf("node %d fetch %q: %v", i, f.Name, err)
				}
			}
			got, err := c.DownloadFile(i, f.Name)
			if err != nil {
				t.Fatalf("node %d %q listed but not downloadable (partial?): %v", i, f.Name, err)
			}
			if sum := sha256.Sum256(got); hex.EncodeToString(sum[:]) != a.digestHex {
				t.Fatalf("node %d %q digest mismatch (partial/corrupt)", i, f.Name)
			}
			if int64(len(got)) != a.size {
				t.Fatalf("node %d %q length %d, want %d (partial)", i, f.Name, len(got), a.size)
			}
		}
		for name, a := range attempted {
			if !a.ok {
				continue
			}
			st, err := c.StatFile(i, name)
			if err != nil || !st.Exists || st.Deleted {
				t.Fatalf("node %d lost acked upload %q (err=%v st=%+v)", i, name, err, st)
			}
		}
	}
	l0, _ := c.SearchFiles(0, "", "")
	l1, _ := c.SearchFiles(1, "", "")
	t.Logf("all %d listed objects byte-complete on both nodes; metadata digests equal", len(l0))
	if len(l0) != len(l1) {
		t.Fatalf("object counts differ: node0=%d node1=%d", len(l0), len(l1))
	}
}

// verifyInventory walks each node's directory tree and requires every
// object payload (.spfo), upload stage file (.stage-*), and fetch stage
// file (*.part) to be attributable to a known upload/fetch attempt and
// bounded in count. Anything else is an unattributable leak.
func verifyInventory(t *testing.T, c *harness.Cluster, attempted map[string]*attempt, fetchAttempts int) {
	t.Helper()
	knownDigests := map[string]bool{}
	for _, a := range attempted {
		knownDigests[a.digestHex] = true
	}
	erroredUploads := 0
	for _, a := range attempted {
		if !a.ok {
			erroredUploads++
		}
	}
	for i := range c.Nodes {
		var spfo, stage, part []string
		_ = filepath.WalkDir(c.Nodes[i].Dir, func(path string, e fs.DirEntry, err error) error {
			if err != nil || e.IsDir() {
				return nil
			}
			switch base := filepath.Base(path); {
			case strings.HasSuffix(base, ".spfo"):
				spfo = append(spfo, base)
			case strings.HasPrefix(base, ".stage-"):
				stage = append(stage, base)
			case strings.HasSuffix(base, ".part"):
				part = append(part, base)
			}
			return nil
		})
		// Published objects: referenced by metadata or attributable to
		// a killed upload (bytes publish before metadata commits).
		meta, err := c.SearchFiles(i, "", "")
		if err != nil {
			t.Fatal(err)
		}
		referenced := map[string]bool{}
		for _, f := range meta {
			referenced[f.Digest] = true
		}
		orphans := 0
		for _, base := range spfo {
			digest := strings.TrimSuffix(base, ".spfo")
			if idx := strings.LastIndex(digest, ".g"); idx >= 0 {
				digest = digest[:idx]
			}
			if referenced[digest] || knownDigests[digest] {
				if !referenced[digest] {
					orphans++
				}
				continue
			}
			t.Fatalf("node %d object file %s unattributable to any upload", i, base)
		}
		if orphans > erroredUploads {
			t.Fatalf("node %d has %d unreferenced objects from %d errored uploads", i, orphans, erroredUploads)
		}
		// Upload stage debris exists only where uploads were killed,
		// bounded by the errored attempts (clean exits remove them).
		if i == 1 && len(stage) != 0 {
			t.Fatalf("node 1 never uploaded yet holds %d stage files", len(stage))
		}
		if len(stage) > erroredUploads {
			t.Fatalf("node %d holds %d stage files from %d errored uploads", i, len(stage), erroredUploads)
		}
		// Fetch stage files are digest-prefixed: attributable to known
		// payloads and bounded by fetch attempts (nothing sweeps them
		// automatically, so completed fetches legitimately leave them).
		if i == 0 && len(part) != 0 {
			t.Fatalf("node 0 never fetched yet holds %d part files", len(part))
		}
		for _, base := range part {
			prefix := strings.TrimSuffix(base, ".part")
			if j := strings.IndexByte(prefix, '-'); j >= 0 {
				prefix = prefix[:j]
			}
			if !knownDigests[prefix] {
				t.Fatalf("node %d stage file %s unattributable to any upload", i, base)
			}
		}
		if len(part) > fetchAttempts+2 {
			t.Fatalf("node %d holds %d part files from %d fetch attempts", i, len(part), fetchAttempts)
		}
		t.Logf("node %d inventory: %d objects (%d crash orphans), %d stage, %d part — all attributable",
			i, len(spfo), orphans, len(stage), len(part))
	}
}

func lastBigName(attempted map[string]*attempt) string {
	best := ""
	for name, a := range attempted {
		if a.ok && strings.HasPrefix(name, "crash/f-") && name > best {
			best = name
		}
	}
	return best
}

func checkUploadDigest(t *testing.T, st harness.FileStatus, name string, data []byte) {
	t.Helper()
	if st.Digest != hexOf(data) {
		t.Fatalf("upload %s digest %s, want %s", name, st.Digest, hexOf(data))
	}
}

func checkDownload(t *testing.T, c *harness.Cluster, idx int, name string, want []byte) {
	t.Helper()
	got, err := c.DownloadFile(idx, name)
	if err != nil {
		t.Fatalf("node %d download %s: %v", idx, name, err)
	}
	if sum := sha256.Sum256(got); hex.EncodeToString(sum[:]) != hexOf(want) {
		t.Fatalf("node %d %s digest mismatch", idx, name)
	}
}

func checkDownloadByName(t *testing.T, c *harness.Cluster, idx int, name string, attempted map[string]*attempt) {
	t.Helper()
	a, ok := attempted[name]
	if !ok {
		t.Fatalf("no attempt recorded for %s", name)
	}
	st, err := c.StatFile(idx, name)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Available {
		t.Fatalf("node %d %s not available after post-crash fetch", idx, name)
	}
	got, err := c.DownloadFile(idx, name)
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(got); hex.EncodeToString(sum[:]) != a.digestHex {
		t.Fatalf("node %d %s digest mismatch after post-crash fetch", idx, name)
	}
}

// payload renders deterministic bytes unique to name.
func payload(name string, n int) []byte {
	seed := sha256.Sum256([]byte(name))
	data := make([]byte, n)
	for i := range data {
		data[i] = byte((int(seed[i%32]) + i*17) % 251)
	}
	return data
}

func hexOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
