package rime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/marcgauthier/murmur/rime"
)

// TestRimeWriteProfile is opt-in diagnostic instrumentation, not a latency
// gate. Profiles exclude setup and validation. Separate processes isolate the
// runtime's cumulative mutex/block profiles. No external-engine work enters the capture.
func TestRimeWriteProfile(t *testing.T) {
	dir := os.Getenv("RIME_PROFILE_DIR")
	if dir == "" {
		t.Skip("set RIME_PROFILE_DIR to capture write profiles")
	}
	op := os.Getenv("RIME_PROFILE_OP")
	if op == "" {
		op = "update"
	}
	if op != "insert" && op != "update" && op != "delete" && op != "hotspot" {
		t.Fatalf("invalid operation %q", op)
	}
	kind := os.Getenv("RIME_PROFILE_KIND")
	if kind == "" {
		kind = "cpu"
	}
	if kind != "cpu" && kind != "contention" {
		t.Fatalf("invalid profile kind %q", kind)
	}
	rows := getBenchEnvInt("RIME_BENCH_ROWS", 100000)
	writers := getBenchEnvInt("RIME_PROFILE_WRITERS", 4)
	batch := getBenchEnvInt("RIME_PROFILE_BATCH", 1)
	rounds := getBenchEnvInt("RIME_PROFILE_ROUNDS", 5)
	if op == "insert" || op == "delete" || op == "hotspot" {
		rounds = 1
	}
	if rows < writers || rows%writers != 0 {
		t.Fatal("rows must be divisible by writers")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(dir, fmt.Sprintf("%s-w%d-b%d-%s", op, writers, batch, kind))
	users := make([]*BenchUser, rows)
	for i := range users {
		id, name, email, age := benchUserRow(i + 1)
		users[i] = &BenchUser{ID: id, Name: name, Email: email, Age: age}
	}
	db, table, _ := openRimeBenchDB(t)
	defer db.Close()
	if op != "insert" {
		if err := table.UpsertMany(users); err != nil {
			t.Fatal(err)
		}
	}
	writeProfile := func(name string) {
		f, err := os.Create(prefix + "-" + name + ".pprof")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	if kind == "cpu" {
		f, err := os.Create(prefix + "-alloc-base.pprof")
		if err != nil {
			t.Fatal(err)
		}
		err = pprof.WriteHeapProfile(f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if kind == "cpu" {
		f, err := os.Create(prefix + "-cpu.pprof")
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			t.Fatal(err)
		}
		defer f.Close()
		defer pprof.StopCPUProfile()
	} else {
		previous := runtime.SetMutexProfileFraction(1)
		defer runtime.SetMutexProfileFraction(previous)
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
	}
	operations := rows * rounds
	duration, retries, commits := runConcurrentWrites(t, operations, writers, batch, func(ctx context.Context, start, end int) error {
		return db.WriteTxContext(ctx, func(tx *rime.Tx) error {
			for i := start; i < end; i++ {
				// Each writer cycles only its own key partition, even over rounds.
				partition := rows / writers
				id := (i/(partition*rounds))*partition + i%partition + 1
				if op == "hotspot" {
					id = i%min(rows, 64) + 1
				}
				var err error
				switch op {
				case "insert":
					err = table.In(tx).Insert(users[id-1])
				case "delete":
					err = table.In(tx).Delete(id)
				default:
					err = table.In(tx).Update(id, func(u *BenchUser) error { u.Age++; return nil })
				}
				if err != nil {
					return err
				}
			}
			return nil
		})
	})
	if kind == "cpu" {
		pprof.StopCPUProfile()
	} else {
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
		writeProfile("mutex")
		writeProfile("block")
	}
	runtime.ReadMemStats(&after)
	if kind == "cpu" {
		runtime.GC()
		writeProfile("heap")
	}
	report := map[string]any{
		"operation": op, "kind": kind, "rows": rows, "writers": writers, "batch": batch, "rounds": rounds,
		"operations": operations, "seconds": duration.Seconds(), "rows_per_second": float64(operations) / duration.Seconds(),
		"conflicts": retries, "commits": commits, "allocated_bytes": after.TotalAlloc - before.TotalAlloc,
		"allocations": after.Mallocs - before.Mallocs, "go": runtime.Version(), "cpus": runtime.NumCPU(),
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prefix+".json", data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", data)
	// Validate exact state after stopping profiles so assertions don't pollute
	// samples. Include all hotspot increments and complete deletion.
	result, err := table.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	wantCount := rows
	if op == "delete" {
		wantCount = 0
	}
	if len(result) != wantCount {
		t.Fatalf("rows=%d want=%d", len(result), wantCount)
	}
	for _, u := range result {
		want := *users[u.ID-1]
		if op == "update" {
			want.Age += rounds
		}
		if op == "hotspot" && u.ID <= min(rows, 64) {
			want.Age += (operations-u.ID)/min(rows, 64) + 1
		}
		if *u != want {
			t.Fatalf("key %d: got %+v want %+v", u.ID, u, want)
		}
	}
}
