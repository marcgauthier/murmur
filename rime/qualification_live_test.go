package rime_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/internal/rimequal"
	"github.com/marcgauthier/murmur/rime"
)

func TestQualificationConcurrentWorkload(t *testing.T) {
	for _, shards := range []int{1, 64} {
		t.Run(fmt.Sprint(shards), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQualificationLiveWorker$", "-test.timeout=20s")
			cmd.Env = append(os.Environ(), fmt.Sprintf("RIME_LIVE_SHARDS=%d", shards))
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("live worker: %v\n%s", err, out)
			}
		})
	}
}
func TestQualificationLiveWorker(t *testing.T) {
	value := os.Getenv("RIME_LIVE_SHARDS")
	if value == "" {
		t.Skip("subprocess only")
	}
	shards, err := strconv.Atoi(value)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var output bytes.Buffer
	sample, err := rimequal.Run(ctx, rimequal.Config{Rows: 128, Shards: shards, Readers: 4, Writers: 4, Seed: 127, SampleInterval: 100 * time.Millisecond, Output: &output})
	if err != nil {
		t.Fatalf("%v\n%s", err, output.String())
	}
	// Two tables x 128 rows x (ID unique + Bucket hash). Unique values are
	// counted once: no redundant hash bucket is maintained for them.
	if sample.Events == 0 || sample.IndexEntries != 512 || sample.P99NS == 0 {
		t.Fatalf("missing observations: %+v", sample)
	}
}

func TestQualificationRetainedMemory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQualificationMemoryWorker$", "-test.timeout=35s")
	cmd.Env = append(os.Environ(), "RIME_MEMORY_WORKER=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("memory worker: %v\n%s", err, out)
	}
}
func TestQualificationMemoryWorker(t *testing.T) {
	if os.Getenv("RIME_MEMORY_WORKER") != "1" {
		t.Skip("subprocess only")
	}
	initialGoroutines := runtime.NumGoroutine()
	db, tab := openDevices(t, rime.WithGCInterval(time.Millisecond))
	defer db.Close()
	const keys = 512
	var baseline uint64
	for cycle := 0; cycle < 16; cycle++ {
		for i := 0; i < keys; i++ {
			key := fmt.Sprint(i)
			mustSave(t, tab, Device{ID: key, Hostname: fmt.Sprintf("%d-%d-%s", cycle, i, string(make([]byte, 64))), Site: fmt.Sprint(cycle), Status: cycle, Latency: i})
		}
		// Exercise distinct cache fingerprints and trie paths, not just one plan.
		for i := 0; i < 300; i++ {
			if _, err := tab.Where(rime.SF[Device](tab, "Site").Eq(fmt.Sprint(i))).Count(); err != nil {
				t.Fatal(err)
			}
		}
		db.GC()
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if cycle == 3 {
			baseline = mem.HeapAlloc
		}
		// A broad retention envelope tolerates runtime/cache noise while detecting
		// retained payloads or plans growing proportionally with churn cycles.
		if cycle > 3 && mem.HeapAlloc > baseline+4*1024*1024+baseline/4 {
			t.Fatalf("heap grows with fixed cardinality: baseline=%d cycle=%d heap=%d", baseline, cycle, mem.HeapAlloc)
		}
		st := tab.TableStats()
		if st.Records != keys || st.Versions != keys {
			t.Fatalf("retained versions: %+v", st)
		}
		// 8 per key: ID/Hostname unique (counted once each, no redundant
		// hash buckets), Site/Status hash, Status/Latency ordered, Hostname
		// prefix, site_status compound. Constancy across cycles is the
		// no-leak signal.
		if st.IndexEntries != int64(keys*8) {
			t.Fatalf("index retention: %d want %d", st.IndexEntries, keys*8)
		}
	}
	if _, err := tab.Where().Delete(); err != nil {
		t.Fatal(err)
	}
	db.GC()
	st := tab.TableStats()
	if st.Records != 0 || st.Versions != 0 || st.Tombstones != 0 || st.IndexEntries != 0 {
		t.Fatalf("delete residue: %+v", st)
	}
	db.Close()
	if runtime.NumGoroutine() > initialGoroutines+2 {
		t.Fatalf("background worker leak: before=%d after=%d", initialGoroutines, runtime.NumGoroutine())
	}
}
