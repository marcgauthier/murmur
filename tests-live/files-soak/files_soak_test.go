// Multi-process encrypted file fetch soak: two spedsql daemon processes
// upload, replicate, fetch, and verify file objects over a configurable
// duration, with latency SLO gates.
package filessoak_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

const soakObjectKey = "5151515151515151515151515151515151515151515151515151515151515151"

func envSeconds(t *testing.T, key string, fallback int) time.Duration {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return time.Duration(fallback) * time.Second
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		t.Fatalf("%s must be a positive number of seconds", key)
	}
	return time.Duration(n) * time.Second
}

func TestEncryptedFileFetchSoak(t *testing.T) {
	duration := envSeconds(t, "SPEDSQL_FILES_SOAK_DURATION_SECONDS", 5)
	interval := envSeconds(t, "SPEDSQL_FILES_SOAK_INTERVAL_SECONDS", 1)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "files-soak",
		NumNodes: 2,
		Files:    &harness.FilesOptions{ObjectKeyHex: soakObjectKey, MaxFileBytes: 4 << 20, FetchIntervalMs: -1},
	})

	deadline := time.Now().Add(duration)
	var uploadLatencies, endToEndLatencies []time.Duration
	expected := make(map[string][32]byte)
	for round := 0; time.Now().Before(deadline); round++ {
		name := fmt.Sprintf("soak/file-%04d.bin", round)
		data := make([]byte, 64<<10)
		for i := range data {
			data[i] = byte((round*31 + i*17) % 251)
		}
		want := sha256.Sum256(data)
		start := time.Now()
		if _, err := cluster.UploadFile(0, name, data); err != nil {
			t.Fatalf("upload round %d: %v", round, err)
		}
		uploadLatencies = append(uploadLatencies, time.Since(start))
		expected[name] = want

		endToEndStart := time.Now()
		if err := cluster.WaitFileAvailable(1, name, false, 20*time.Second); err != nil {
			t.Fatalf("metadata round %d: %v", round, err)
		}
		if _, err := cluster.FetchFile(1, name); err != nil {
			t.Fatalf("fetch round %d: %v", round, err)
		}
		got, err := cluster.DownloadFile(1, name)
		if err != nil {
			t.Fatalf("download round %d: %v", round, err)
		}
		if sum := sha256.Sum256(got); sum != want {
			t.Fatalf("round %d digest %x, want %x", round, sum, want)
		}
		endToEndLatencies = append(endToEndLatencies, time.Since(endToEndStart))
		search, err := cluster.SearchFiles(1, "", "file-")
		if err != nil {
			t.Fatalf("search round %d: %v", round, err)
		}
		if len(search) != round+1 {
			t.Fatalf("search returned %d files after round %d, want %d", len(search), round, round+1)
		}
		if time.Now().Before(deadline) {
			time.Sleep(interval)
		}
	}
	if len(uploadLatencies) == 0 {
		t.Fatal("soak completed no uploads")
	}
	stats, err := cluster.FetchStats(1)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Failed != 0 || stats.Completed != uint64(len(expected)) {
		t.Fatalf("fetch stats %+v for %d files", stats, len(expected))
	}
	if st, err := cluster.StatFile(1, "soak/file-0000.bin"); err != nil || !st.Exists || !st.Available {
		t.Fatalf("first file status %+v, err=%v", st, err)
	}
	sort.Slice(uploadLatencies, func(i, j int) bool { return uploadLatencies[i] < uploadLatencies[j] })
	sort.Slice(endToEndLatencies, func(i, j int) bool { return endToEndLatencies[i] < endToEndLatencies[j] })
	p95Index := func(n int) int { return min(n-1, (95*n+99)/100-1) }
	uploadP95, transferP95 := uploadLatencies[p95Index(len(uploadLatencies))], endToEndLatencies[p95Index(len(endToEndLatencies))]
	t.Logf("file soak duration=%s rounds=%d upload_p95=%s metadata_fetch_verify_p95=%s fetched_bytes=%d", duration, len(expected), uploadP95, transferP95, stats.BytesFetched)
	if uploadP95 > 5*time.Second {
		t.Fatalf("upload p95 %s exceeded 5s SLO", uploadP95)
	}
	if transferP95 > 20*time.Second {
		t.Fatalf("end-to-end p95 %s exceeded 20s SLO", transferP95)
	}
}
