// File-corrupt-source acceptance: a published object's bytes are corrupted
// on the source node (simulated disk corruption while the node is stopped)
// and a peer fetch must then FAIL digest verification with no partial
// install. Honest objects keep fetching afterwards.
//
// Product facts this suite relies on (read from source, not invented):
//   - filefetch serves stored container bytes verbatim; the RECEIVER
//     verifies against replicated metadata (objectstore/store.go Serve).
//   - verification failure surfaces as ErrInvalidObject
//     ("objectstore: invalid encrypted object" + ": chunk authentication
//     failed" / ": length or digest mismatch" / ...) and the failed fetch
//     discards its staging file (files_fetch.go fetchAddr).
package filescorruptsource_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

const objectKey = "4242424242424242424242424242424242424242424242424242424242424242"

// errInvalidObject is the exact product sentinel for verification failure
// (objectstore/store.go: ErrInvalidObject). The suite asserts the fetch
// error contains this string; the ": ..." suffix varies with which
// container record the corruption lands in.
const errInvalidObject = "objectstore: invalid encrypted object"

func TestCorruptSourceFetchFailsVerification(t *testing.T) {
	victimSize := envInt("MURMUR_FILES_CORRUPT_VICTIM_KB", 256) << 10
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "files-corrupt-source",
		NumNodes: 2,
		Files: &harness.FilesOptions{
			ObjectKeyHex: objectKey, MaxFileBytes: 4 << 20, FetchIntervalMs: -1,
		},
	})

	// Positive control: honest upload replicates, fetches, verifies.
	ctrl := payload("ctrl/a.bin", 64<<10)
	st, err := cluster.UploadFile(0, "ctrl/a.bin", ctrl)
	if err != nil {
		t.Fatalf("control upload: %v", err)
	}
	if st.Digest != hexOf(ctrl) {
		t.Fatalf("control upload digest %s, want %s", st.Digest, hexOf(ctrl))
	}
	if err := cluster.WaitFileAvailable(1, "ctrl/a.bin", false, 30*time.Second); err != nil {
		t.Fatalf("control metadata: %v", err)
	}
	if _, err := cluster.FetchFile(1, "ctrl/a.bin"); err != nil {
		t.Fatalf("control fetch: %v", err)
	}
	checkDownload(t, cluster, 1, "ctrl/a.bin", ctrl)
	t.Log("positive control: upload/replicate/fetch/verify works")

	// Publish the victim object honestly; replicate metadata only.
	victim := payload("victim/blob.bin", victimSize)
	vst, err := cluster.UploadFile(0, "victim/blob.bin", victim)
	if err != nil {
		t.Fatalf("victim upload: %v", err)
	}
	if vst.Digest != hexOf(victim) {
		t.Fatalf("victim upload digest %s, want %s", vst.Digest, hexOf(victim))
	}
	if err := cluster.WaitFileAvailable(1, "victim/blob.bin", false, 30*time.Second); err != nil {
		t.Fatalf("victim metadata: %v", err)
	}
	victimDigest := vst.Digest
	t.Logf("victim published: digest=%s size=%d", victimDigest, len(victim))

	// Locate the source-side object file before stopping the node.
	srcObj := findObjectFile(t, cluster.Nodes[0].Dir, victimDigest)
	before, err := os.ReadFile(srcObj)
	if err != nil {
		t.Fatalf("read source object: %v", err)
	}
	t.Logf("source object: %s (%d container bytes)", srcObj, len(before))
	if len(before) < 1024 {
		t.Fatalf("source object unexpectedly small (%d bytes); refusing to corrupt", len(before))
	}
	failedBefore, err := cluster.FetchStats(1)
	if err != nil {
		t.Fatalf("fetch-stats baseline: %v", err)
	}

	// Corrupt the published bytes while the source node is stopped
	// (honest disk-corruption simulation: same length, flipped bytes in
	// the middle of the container, i.e. inside chunk ciphertext).
	cluster.StopNode(0)
	flip := append([]byte(nil), before...)
	for i := 0; i < 16; i++ {
		off := len(flip)/2 + i
		flip[off] ^= 0xFF
	}
	if err := os.WriteFile(srcObj, flip, 0600); err != nil {
		t.Fatalf("write corrupted object: %v", err)
	}
	after, err := os.ReadFile(srcObj)
	if err != nil || len(after) != len(before) {
		t.Fatalf("corrupted object length changed (err=%v)", err)
	}
	t.Logf("flipped 16 bytes at container offset %d (length preserved)", len(flip)/2)
	cluster.StartNode(0)
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)

	// The peer fetch must FAIL digest verification.
	fetchErr := fetchMustFail(t, cluster, "victim/blob.bin")
	if !strings.Contains(fetchErr.Error(), errInvalidObject) {
		t.Fatalf("fetch error %q does not mention verification failure %q", fetchErr, errInvalidObject)
	}
	t.Logf("corrupt fetch failed closed as required: %v", fetchErr)
	failedAfter, err := cluster.FetchStats(1)
	if err != nil {
		t.Fatalf("fetch-stats after: %v", err)
	}
	if failedAfter.Failed <= failedBefore.Failed {
		t.Fatalf("fetch-failed counter did not advance (before=%d after=%d); failure not observed",
			failedBefore.Failed, failedAfter.Failed)
	}

	// No partial install on the fetching node.
	pst, err := cluster.StatFile(1, "victim/blob.bin")
	if err != nil {
		t.Fatalf("victim status: %v", err)
	}
	if !pst.Exists || pst.Deleted {
		t.Fatalf("victim metadata wrongly altered by failed fetch: %+v", pst)
	}
	if pst.Available {
		t.Fatal("victim reports Available after a failed-verification fetch (partial install)")
	}
	if _, err := cluster.DownloadFile(1, "victim/blob.bin"); err == nil {
		t.Fatal("victim downloads after a failed-verification fetch (partial install)")
	}
	if obj := findObjectFileQuiet(cluster.Nodes[1].Dir, victimDigest); obj != "" {
		t.Fatalf("fetching node holds object file %s for an unverified digest (partial install)", obj)
	}
	if part := findPartFile(cluster.Nodes[1].Dir, victimDigest); part != "" {
		t.Fatalf("fetching node holds staging file %s after failed verification (staging not discarded)", part)
	}

	// No phantom metadata: the listing still shows the victim with its
	// original honest digest/size (corruption is bytes-only, metadata
	// untouched) on both nodes.
	for i := range cluster.Nodes {
		list, err := cluster.SearchFiles(i, "", "")
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, f := range list {
			if f.Name != "victim/blob.bin" {
				continue
			}
			found = true
			if f.Digest != victimDigest || f.Size != int64(len(victim)) || f.Deleted {
				t.Fatalf("node %d victim metadata altered: %+v", i, f)
			}
		}
		if !found {
			t.Fatalf("node %d lost victim metadata after failed fetch", i)
		}
	}

	// The corruption is persistent, not transient: a second fetch fails
	// the same verification again.
	again := fetchMustFail(t, cluster, "victim/blob.bin")
	if !strings.Contains(again.Error(), errInvalidObject) {
		t.Fatalf("second fetch error %q does not mention %q", again, errInvalidObject)
	}
	t.Logf("second fetch failed closed again: %v", again)

	// Honest objects must still fetch afterwards.
	honest := payload("honest/b.bin", 128<<10)
	if _, err := cluster.UploadFile(0, "honest/b.bin", honest); err != nil {
		t.Fatalf("honest upload after corruption: %v", err)
	}
	if err := cluster.WaitFileAvailable(1, "honest/b.bin", false, 30*time.Second); err != nil {
		t.Fatalf("honest metadata after corruption: %v", err)
	}
	if _, err := cluster.FetchFile(1, "honest/b.bin"); err != nil {
		t.Fatalf("honest fetch after corruption: %v", err)
	}
	checkDownload(t, cluster, 1, "honest/b.bin", honest)
	checkDownload(t, cluster, 1, "ctrl/a.bin", ctrl)
	t.Log("corrupt-source proven: bad bytes fail verification, honest objects unaffected")
}

// fetchMustFail runs one fetch and requires an error return.
func fetchMustFail(t *testing.T, c *harness.Cluster, name string) error {
	t.Helper()
	_, err := c.FetchFile(1, name)
	if err == nil {
		t.Fatalf("fetch of %q from corrupt source succeeded; want verification failure", name)
	}
	return err
}

// findObjectFile locates the .spfo object file for digest under dir.
func findObjectFile(t *testing.T, dir, digest string) string {
	t.Helper()
	if p := findObjectFileQuiet(dir, digest); p != "" {
		return p
	}
	t.Fatalf("no .spfo object file for digest %s under %s", digest, dir)
	return ""
}

func findObjectFileQuiet(dir, digest string) string {
	var found string
	_ = filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() || found != "" {
			return nil
		}
		base := filepath.Base(path)
		if strings.HasSuffix(base, ".spfo") && strings.Contains(base, digest) {
			found = path
		}
		return nil
	})
	return found
}

// findPartFile locates a fetch-staging (*.part) file for digest under dir.
func findPartFile(dir, digest string) string {
	var found string
	_ = filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() || found != "" {
			return nil
		}
		base := filepath.Base(path)
		if strings.HasSuffix(base, ".part") && strings.HasPrefix(base, digest) {
			found = path
		}
		return nil
	})
	return found
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
