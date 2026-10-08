// Live spool storage-engine checks: a real multi-process crash test
// (SIGKILL mid-write, then reopen and verify durability markers) and
// a concurrent soak with default options, key rotation, and
// reclamation. Run with: bash tests-live/run.sh spool
package spoollive_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/spool"
)

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("bad %s=%q", name, v)
		}
		return n
	}
	return def
}

// liveValue deterministically renders the value for (round, slot) so
// crash recovery can verify every byte without a reference copy.
func liveValue(round, slot int) []byte {
	out := make([]byte, 128)
	binary.BigEndian.PutUint64(out[0:8], uint64(round)<<32|uint64(uint32(slot)))
	x := uint64(round)*0x9E3779B97F4A7C15 + uint64(slot)*0xBF58476D1CE4E5B9 + 1
	for i := 8; i < len(out); i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x >> 56)
	}
	return out
}

func liveKey(round, slot int) string {
	return fmt.Sprintf("r%07d/s%05d", round, slot)
}

func parseLiveKey(t *testing.T, k string) (int, int) {
	t.Helper()
	var r, s int
	if _, err := fmt.Sscanf(k, "r%d/s%d", &r, &s); err != nil {
		t.Fatalf("unparseable key %q: %v", k, err)
	}
	return r, s
}

// TestSpoolChildWriter is the crash victim: re-executed as a child
// process, it appends checksummed rounds forever until killed. Each
// round is flushed with Sync durability and then acknowledged in a
// synced progress marker, so the parent can require every
// acknowledged round to survive the kill.
func TestSpoolChildWriter(t *testing.T) {
	if os.Getenv("MURMUR_SPOOL_CHILD") != "1" {
		t.Skip("child entrypoint only")
	}
	dir := os.Getenv("MURMUR_SPOOL_DIR")
	keyHex := os.Getenv("MURMUR_SPOOL_KEY")
	roundKeys, _ := strconv.Atoi(os.Getenv("MURMUR_SPOOL_ROUND_KEYS"))
	master, err := hex.DecodeString(keyHex)
	if err != nil || len(master) != 32 || dir == "" || roundKeys <= 0 {
		fmt.Fprintf(os.Stderr, "child: bad env\n")
		os.Exit(2)
	}
	o := spool.DefaultOptions(filepath.Join(dir, "store"))
	o.MasterKey = master
	o.Durability = spool.DurabilitySync
	st, err := spool.Open(o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: open: %v\n", err)
		os.Exit(2)
	}
	defer st.Close()
	marker := filepath.Join(dir, "progress")
	for round := 0; ; round++ {
		for slot := 0; slot < roundKeys; slot++ {
			if err := st.Put([]byte(liveKey(round, slot)), liveValue(round, slot)); err != nil {
				fmt.Fprintf(os.Stderr, "child: put: %v\n", err)
				os.Exit(2)
			}
		}
		if err := st.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "child: flush: %v\n", err)
			os.Exit(2)
		}
		f, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: marker: %v\n", err)
			os.Exit(2)
		}
		fmt.Fprintf(f, "%d\n", round)
		if err := f.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "child: marker sync: %v\n", err)
			os.Exit(2)
		}
		f.Close()
	}
}

// TestSpoolGroupChildWriter is the group-crash victim: re-executed
// as a child process, it commits Sync groups forever until killed,
// acknowledging each group in a synced marker file.
func TestSpoolGroupChildWriter(t *testing.T) {
	if os.Getenv("MURMUR_SPOOL_CHILD") != "group" {
		t.Skip("child entrypoint only")
	}
	dir := os.Getenv("MURMUR_SPOOL_DIR")
	keyHex := os.Getenv("MURMUR_SPOOL_KEY")
	groupKeys, _ := strconv.Atoi(os.Getenv("MURMUR_SPOOL_GROUP_KEYS"))
	master, err := hex.DecodeString(keyHex)
	if err != nil || len(master) != 32 || dir == "" || groupKeys <= 0 {
		fmt.Fprintf(os.Stderr, "child: bad env\n")
		os.Exit(2)
	}
	o := spool.DefaultOptions(filepath.Join(dir, "store"))
	o.MasterKey = master
	st, err := spool.Open(o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: open: %v\n", err)
		os.Exit(2)
	}
	defer st.Close()
	marker := filepath.Join(dir, "progress")
	for group := 0; ; group++ {
		muts := make([]spool.Mutation, groupKeys)
		for slot := range muts {
			muts[slot] = spool.Mutation{Key: []byte(liveKey(group, slot)), Value: liveValue(group, slot)}
		}
		if err := st.Commit(muts, spool.DurabilitySync); err != nil {
			fmt.Fprintf(os.Stderr, "child: commit: %v\n", err)
			os.Exit(2)
		}
		f, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: marker: %v\n", err)
			os.Exit(2)
		}
		fmt.Fprintf(f, "%d\n", group)
		if err := f.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "child: marker sync: %v\n", err)
			os.Exit(2)
		}
		f.Close()
	}
}

// TestSpoolGroupCrashAtomicity kills a live group committer and
// requires per-group atomicity: acknowledged groups fully present,
// the in-flight group fully present or fully absent, never partial.
func TestSpoolGroupCrashAtomicity(t *testing.T) {
	writeSecs := envInt(t, "MURMUR_SPOOL_WRITE_SECONDS", 3)
	groupKeys := envInt(t, "MURMUR_SPOOL_GROUP_KEYS", 20)
	if testing.Short() {
		writeSecs, groupKeys = 1, 10
	}
	dir := t.TempDir()
	master := make([]byte, 32)
	for i := range master {
		master[i] = byte(i*7 + 1)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestSpoolGroupChildWriter$", "-test.count=1")
	child.Env = append(os.Environ(),
		"MURMUR_SPOOL_CHILD=group",
		"MURMUR_SPOOL_DIR="+dir,
		"MURMUR_SPOOL_KEY="+hex.EncodeToString(master),
		"MURMUR_SPOOL_GROUP_KEYS="+strconv.Itoa(groupKeys),
	)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()
	marker := filepath.Join(dir, "progress")
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		select {
		case err := <-waited:
			t.Fatalf("child exited early: %v\n%s", err, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			child.Process.Kill()
			t.Fatalf("child never committed a group\n%s", stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(time.Duration(writeSecs) * time.Second)
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	<-waited
	raw, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	acked, err := strconv.Atoi(string(bytes.TrimSpace(raw)))
	if err != nil || acked < 0 {
		t.Fatalf("bad marker %q", raw)
	}
	t.Logf("child acknowledged groups 0..%d (%d keys/group)", acked, groupKeys)
	storePath := filepath.Join(dir, "store")
	ro := spool.DefaultOptions(storePath)
	ro.MasterKey = master
	st, err := spool.Open(ro)
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer st.Close()
	seen := make(map[string]bool)
	if err := spool.Load(storePath, master, func(recs []spool.Record) error {
		for _, r := range recs {
			if r.Deleted {
				t.Fatalf("unexpected tombstone for %q", r.Key)
			}
			round, slot := parseLiveKey(t, string(r.Key))
			if !bytes.Equal(r.Value, liveValue(round, slot)) {
				t.Fatalf("key %q value mismatch", r.Key)
			}
			seen[string(r.Key)] = true
		}
		return nil
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	for g := 0; g <= acked; g++ {
		for s := 0; s < groupKeys; s++ {
			if !seen[liveKey(g, s)] {
				t.Fatalf("acknowledged key %s missing after kill", liveKey(g, s))
			}
		}
	}
	// The in-flight group is all or nothing.
	var present, beyond int
	for g := acked + 1; ; g++ {
		n := 0
		for s := 0; s < groupKeys; s++ {
			if seen[liveKey(g, s)] {
				n++
			}
		}
		if n == 0 {
			break
		}
		if g == acked+1 {
			present = n
		} else {
			beyond += n
		}
		if g > acked+100 {
			t.Fatalf("runaway groups past %d", g)
		}
	}
	if present != 0 && present != groupKeys {
		t.Fatalf("in-flight group partial: %d/%d keys", present, groupKeys)
	}
	if beyond != 0 {
		t.Fatalf("%d keys beyond the in-flight group", beyond)
	}
	t.Logf("in-flight group: %d/%d keys (atomic)", present, groupKeys)
}

// TestSpoolCrashRecovery kills a live writer and requires every
// acknowledged round to reload byte-identical, then exercises
// rotation, deletion, and reclamation on the recovered store.
func TestSpoolCrashRecovery(t *testing.T) {
	writeSecs := envInt(t, "MURMUR_SPOOL_WRITE_SECONDS", 3)
	roundKeys := envInt(t, "MURMUR_SPOOL_ROUND_KEYS", 200)
	if testing.Short() {
		writeSecs, roundKeys = 1, 100
	}
	dir := t.TempDir()
	master := make([]byte, 32)
	for i := range master {
		master[i] = byte(i*7 + 1)
	}

	child := exec.Command(os.Args[0], "-test.run=^TestSpoolChildWriter$", "-test.count=1")
	child.Env = append(os.Environ(),
		"MURMUR_SPOOL_CHILD=1",
		"MURMUR_SPOOL_DIR="+dir,
		"MURMUR_SPOOL_KEY="+hex.EncodeToString(master),
		"MURMUR_SPOOL_ROUND_KEYS="+strconv.Itoa(roundKeys),
	)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()

	// Wait for the first acknowledged round (proves the child is
	// past open and writing), then let it run.
	marker := filepath.Join(dir, "progress")
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		select {
		case err := <-waited:
			t.Fatalf("child exited early: %v\n%s", err, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			child.Process.Kill()
			t.Fatalf("child never wrote a round\n%s", stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(time.Duration(writeSecs) * time.Second)
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	<-waited // SIGKILL exit status is expected

	raw, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	acked, err := strconv.Atoi(string(bytes.TrimSpace(raw)))
	if err != nil || acked < 0 {
		t.Fatalf("bad marker %q", raw)
	}
	t.Logf("child acknowledged rounds 0..%d (%d keys/round)", acked, roundKeys)

	o := spool.DefaultOptions(filepath.Join(dir, "store"))
	o.MasterKey = master
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	seen := make(map[string]bool)
	loaded := 0
	err = spool.Load(filepath.Join(dir, "store"), master, func(recs []spool.Record) error {
		for _, r := range recs {
			if r.Deleted {
				t.Fatalf("unexpected tombstone for %q", r.Key)
			}
			round, slot := parseLiveKey(t, string(r.Key))
			if !bytes.Equal(r.Value, liveValue(round, slot)) {
				t.Fatalf("key %q value mismatch", r.Key)
			}
			seen[string(r.Key)] = true
			loaded++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded != len(seen) {
		t.Fatalf("loaded %d records for %d keys (dupes?)", loaded, len(seen))
	}
	// Every acknowledged round must be complete; the in-flight
	// round (acked+1) may be partial or absent.
	for r := 0; r <= acked; r++ {
		for s := 0; s < roundKeys; s++ {
			if !seen[liveKey(r, s)] {
				t.Fatalf("acknowledged key %s missing after kill", liveKey(r, s))
			}
		}
	}
	t.Logf("recovered %d keys across %d acked rounds (+in-flight)", len(seen), acked+1)

	// Rotation + deletion + reclamation on the recovered store, then
	// a clean reopen must show no resurrection.
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	for s := 0; s < roundKeys; s++ {
		if err := st.Delete([]byte(liveKey(0, s))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := st.Reclaim(); err != nil {
			t.Fatalf("Reclaim: %v", err)
		}
	}
	stats := st.Stats()
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	t.Logf("post-reclaim stats: %+v", stats)
	final := make(map[string]bool)
	seqs := make(map[string]uint64)
	if err := spool.Load(filepath.Join(dir, "store"), master, func(recs []spool.Record) error {
		for _, r := range recs {
			k := string(r.Key)
			if r.Sequence < seqs[k] {
				continue
			}
			seqs[k] = r.Sequence
			if r.Deleted {
				delete(final, k)
			} else {
				final[k] = true
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("final load: %v", err)
	}
	for s := 0; s < roundKeys; s++ {
		if final[liveKey(0, s)] {
			t.Fatalf("round 0 key %s resurrected", liveKey(0, s))
		}
	}
	if len(final) != len(seen)-roundKeys {
		t.Fatalf("final keys = %d, want %d", len(final), len(seen)-roundKeys)
	}
}

// TestSpoolLiveSoak hammers a default-configured store from many
// goroutines with interleaved rotation and reclamation, then
// verifies the full dataset reloads intact.
func TestSpoolLiveSoak(t *testing.T) {
	writers, perWriter := 64, 2000
	if testing.Short() {
		writers, perWriter = 8, 200
	}
	dir := t.TempDir()
	master := bytes.Repeat([]byte{0x5A}, 32)
	o := spool.DefaultOptions(filepath.Join(dir, "store"))
	o.MasterKey = master
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var puts atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("w%03d/k%06d", w, i)
				v := liveValue(w, i)
				if i%11 == 10 {
					if err := st.Delete([]byte(k)); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
					continue
				}
				if err := st.Put([]byte(k), v); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				puts.Add(1)
			}
		}(w)
	}
	// Interleave two rotations and reclamation while writers run.
	time.Sleep(200 * time.Millisecond)
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := st.Reclaim(); err != nil {
		t.Fatalf("Reclaim: %v", err)
	}
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	wg.Wait()
	writeDur := time.Since(start)
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	n := puts.Load()
	t.Logf("wrote %d records in %v (%.0f rec/s)", n, writeDur, float64(n)/writeDur.Seconds())

	got := make(map[string][]byte)
	if err := spool.Load(filepath.Join(dir, "store"), master, func(recs []spool.Record) error {
		for _, r := range recs {
			if r.Deleted {
				continue
			}
			got[string(r.Key)] = append([]byte(nil), r.Value...)
		}
		return nil
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	want := 0
	for w := 0; w < writers; w++ {
		for i := 0; i < perWriter; i++ {
			if i%11 == 10 {
				continue
			}
			want++
			k := fmt.Sprintf("w%03d/k%06d", w, i)
			if !bytes.Equal(got[k], liveValue(w, i)) {
				t.Fatalf("key %s mismatch", k)
			}
		}
	}
	if len(got) != want {
		t.Fatalf("loaded %d keys, want %d", len(got), want)
	}
}

// liveBigValue renders a deterministic size-byte value for (round, slot)
// so large-store recovery tests verify every byte without a reference
// copy. The full xorshift stream keeps large values incompressible, so
// byte counts on disk stay honest.
func liveBigValue(round, slot, size int) []byte {
	out := make([]byte, size)
	binary.BigEndian.PutUint64(out[0:8], uint64(round)<<32|uint64(uint32(slot)))
	x := uint64(round)*0x9E3779B97F4A7C15 + uint64(slot)*0xBF58476D1CE4E5B9 + 1
	for i := 8; i < len(out); i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out[i] = byte(x >> 56)
	}
	return out
}

// TestSpoolRebindChildWriter is the rebind-crash victim: re-executed as a
// child process, it builds a large store under context A, signals
// readiness, then blocks in RebindContext(B) until killed. Markers:
// "ready" (store built), "rebind-started", "rebind-done".
func TestSpoolRebindChildWriter(t *testing.T) {
	if os.Getenv("MURMUR_SPOOL_CHILD") != "rebind" {
		t.Skip("child entrypoint only")
	}
	dir := os.Getenv("MURMUR_SPOOL_DIR")
	keyHex := os.Getenv("MURMUR_SPOOL_KEY")
	ctxAHex := os.Getenv("MURMUR_SPOOL_CTXA")
	ctxBHex := os.Getenv("MURMUR_SPOOL_CTXB")
	keys, _ := strconv.Atoi(os.Getenv("MURMUR_SPOOL_REBIND_KEYS"))
	valKB, _ := strconv.Atoi(os.Getenv("MURMUR_SPOOL_REBIND_VALKB"))
	master, err := hex.DecodeString(keyHex)
	ctxA, errA := hex.DecodeString(ctxAHex)
	ctxB, errB := hex.DecodeString(ctxBHex)
	if err != nil || len(master) != 32 || dir == "" || keys <= 0 || valKB <= 0 ||
		errA != nil || len(ctxA) != 16 || errB != nil || len(ctxB) != 16 {
		fmt.Fprintf(os.Stderr, "child: bad env\n")
		os.Exit(2)
	}
	o := spool.DefaultOptions(filepath.Join(dir, "store"))
	o.MasterKey = master
	o.ContextID = ctxA
	st, err := spool.Open(o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: open: %v\n", err)
		os.Exit(2)
	}
	defer st.Close()
	for slot := 0; slot < keys; slot++ {
		if err := st.Put([]byte(liveKey(0, slot)), liveBigValue(0, slot, valKB<<10)); err != nil {
			fmt.Fprintf(os.Stderr, "child: put: %v\n", err)
			os.Exit(2)
		}
	}
	if err := st.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "child: flush: %v\n", err)
		os.Exit(2)
	}
	sig := func(name string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("1\n"), 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "child: marker %s: %v\n", name, err)
			os.Exit(2)
		}
	}
	sig("ready")
	sig("rebind-started")
	var ctxBArr [16]byte
	copy(ctxBArr[:], ctxB)
	if err := st.RebindContext(ctxBArr); err != nil {
		fmt.Fprintf(os.Stderr, "child: rebind: %v\n", err)
		os.Exit(2)
	}
	sig("rebind-done")
}

// TestSpoolRebindKillRecovery SIGKILLs a child blocked in RebindContext on
// a large store, then requires the parent to resume to context B with
// every byte intact. The kill must land mid-rebind: if the child finishes
// first the test fails honestly (raise MURMUR_SPOOL_REBIND_KEYS).
func TestSpoolRebindKillRecovery(t *testing.T) {
	keys := envInt(t, "MURMUR_SPOOL_REBIND_KEYS", 1500)
	valKB := envInt(t, "MURMUR_SPOOL_REBIND_VALKB", 100)
	killDelayMs := envInt(t, "MURMUR_SPOOL_REBIND_KILL_MS", 1000)
	if testing.Short() {
		keys, valKB, killDelayMs = 200, 100, 300
	}
	dir := t.TempDir()
	master := make([]byte, 32)
	for i := range master {
		master[i] = byte(i*7 + 1)
	}
	ctxA := bytes.Repeat([]byte{0xA1}, 16)
	ctxB := bytes.Repeat([]byte{0xB2}, 16)
	child := exec.Command(os.Args[0], "-test.run=^TestSpoolRebindChildWriter$", "-test.count=1")
	child.Env = append(os.Environ(),
		"MURMUR_SPOOL_CHILD=rebind",
		"MURMUR_SPOOL_DIR="+dir,
		"MURMUR_SPOOL_KEY="+hex.EncodeToString(master),
		"MURMUR_SPOOL_CTXA="+hex.EncodeToString(ctxA),
		"MURMUR_SPOOL_CTXB="+hex.EncodeToString(ctxB),
		"MURMUR_SPOOL_REBIND_KEYS="+strconv.Itoa(keys),
		"MURMUR_SPOOL_REBIND_VALKB="+strconv.Itoa(valKB),
	)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()
	waitMarker := func(name string, what string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Minute)
		for {
			if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
				return
			}
			select {
			case err := <-waited:
				t.Fatalf("child exited before %s: %v\n%s", what, err, stderr.String())
			default:
			}
			if time.Now().After(deadline) {
				child.Process.Kill()
				t.Fatalf("child never %s\n%s", what, stderr.String())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitMarker("ready", "built the store")
	waitMarker("rebind-started", "started the rebind")
	// Wait for provable mid-staging state (a .rew-* temp past 1MB)
	// instead of a blind sleep, so the kill lands mid-rebind on any
	// machine speed. The delay remains as a backstop only.
	killDeadline := time.Now().Add(time.Duration(killDelayMs) * time.Millisecond)
	staged := false
	segDir := filepath.Join(dir, "store", "segments")
	for time.Now().Before(killDeadline.Add(4 * time.Minute)) {
		select {
		case err := <-waited:
			t.Fatalf("child finished before mid-staging kill (%v); raise MURMUR_SPOOL_REBIND_KEYS\n%s", err, stderr.String())
		default:
		}
		entries, err := os.ReadDir(segDir)
		if err == nil {
			for _, e := range entries {
				if len(e.Name()) > 5 && e.Name()[:5] == ".rew-" {
					if info, err := e.Info(); err == nil && info.Size() > 1<<20 {
						staged = true
					}
				}
			}
		}
		if staged {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !staged {
		child.Process.Kill()
		t.Fatalf("never observed mid-staging state\n%s", stderr.String())
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	<-waited
	storePath := filepath.Join(dir, "store")
	ro := spool.DefaultOptions(storePath)
	ro.MasterKey = master
	ro.ContextID = ctxB
	st, err := spool.Open(ro)
	if err != nil {
		t.Fatalf("reopen after kill (resume must complete the rebind): %v", err)
	}
	defer st.Close()
	hint, err := spool.ReadKeyHint(storePath)
	if err != nil {
		t.Fatalf("ReadKeyHint: %v", err)
	}
	if hint.ContextID != [16]byte(ctxB) {
		t.Fatalf("context hint = %x, want %x", hint.ContextID, ctxB)
	}
	if _, err := os.Stat(filepath.Join(storePath, "maintenance.intent")); !os.IsNotExist(err) {
		t.Fatalf("intent remains after resume (err=%v)", err)
	}
	seen := 0
	if err := spool.Load(storePath, master, func(recs []spool.Record) error {
		for _, r := range recs {
			if r.Deleted {
				t.Fatalf("unexpected tombstone for %q", r.Key)
			}
			round, slot := parseLiveKey(t, string(r.Key))
			if !bytes.Equal(r.Value, liveBigValue(round, slot, valKB<<10)) {
				t.Fatalf("key %q value mismatch", r.Key)
			}
			seen++
		}
		return nil
	}); err != nil {
		t.Fatalf("load: %v", err)
	}
	if seen != keys {
		t.Fatalf("loaded %d keys, want %d", seen, keys)
	}
	// The old context no longer opens the rebound store.
	bad := spool.DefaultOptions(storePath)
	bad.MasterKey = master
	bad.ContextID = ctxA
	if st2, err := spool.Open(bad); err == nil {
		st2.Close()
		t.Fatalf("context A still opens a store rebound to B")
	}
	// The resumed store accepts writes.
	if err := st.Put([]byte("post-resume"), []byte("ok")); err != nil {
		t.Fatalf("post-resume put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("post-resume flush: %v", err)
	}
	t.Logf("rebind kill recovered: %d keys x %dKB intact under context B", keys, valKB)
}

// TestSpoolBackupUnderWrite streams a checkpoint copy while writers hammer
// the live store, then requires the streamed backup to open with a
// consistent cut: the quiesced anchor byte-exact, every value parseable,
// and the full fixed key set present.
func TestSpoolBackupUnderWrite(t *testing.T) {
	secs := envInt(t, "MURMUR_SPOOL_BACKUP_SECONDS", 6)
	writers := envInt(t, "MURMUR_SPOOL_BACKUP_WRITERS", 8)
	slots := envInt(t, "MURMUR_SPOOL_BACKUP_SLOTS", 50)
	if testing.Short() {
		secs, writers, slots = 2, 4, 20
	}
	dir := t.TempDir()
	master := make([]byte, 32)
	for i := range master {
		master[i] = byte(i*3 + 5)
	}
	storePath := filepath.Join(dir, "store")
	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()
	const anchorKey, anchorVal = "backup-anchor", "round-0-final"
	if err := st.Put([]byte(anchorKey), []byte(anchorVal)); err != nil {
		t.Fatalf("anchor put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("anchor flush: %v", err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	finals := make([]atomic.Int64, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var round int64
			for !stop.Load() {
				round++
				for s := 0; s < slots; s++ {
					val := make([]byte, 64)
					binary.BigEndian.PutUint64(val[0:8], uint64(round))
					binary.BigEndian.PutUint64(val[8:16], uint64(w))
					if err := st.Put([]byte(fmt.Sprintf("w%02d/s%05d", w, s)), val); err != nil {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
				finals[w].Store(round)
			}
		}(w)
	}
	// Rotation and reclamation pressure alongside the writers:
	// the capture must exclude their publications, never fail on
	// them. Errors channel the first background failure.
	bgErr := make(chan error, 2)
	var bgWG sync.WaitGroup
	bgWG.Add(2)
	go func() {
		defer bgWG.Done()
		for !stop.Load() {
			if err := st.RotateKey(); err != nil {
				bgErr <- err
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	go func() {
		defer bgWG.Done()
		for !stop.Load() {
			if err := st.Reclaim(); err != nil {
				bgErr <- err
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	time.Sleep(time.Duration(secs) * time.Second / 3)
	cp, err := st.Checkpoint(context.Background(), filepath.Join(dir, "ckpt"))
	if err != nil {
		stop.Store(true)
		wg.Wait()
		bgWG.Wait()
		t.Fatalf("checkpoint: %v", err)
	}
	// Stream the checkpoint files to a backup directory while
	// writers, rotation, and reclamation keep hammering the live
	// store.
	backupDir := filepath.Join(dir, "backup")
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		t.Fatalf("mkdir backup: %v", err)
	}
	for _, src := range cp.Files() {
		rel, err := filepath.Rel(cp.Dir, src)
		if err != nil {
			t.Fatalf("checkpoint path %s: %v", src, err)
		}
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("stream %s: %v", src, err)
		}
		dst := filepath.Join(backupDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(dst), err)
		}
		if err := os.WriteFile(dst, raw, 0o600); err != nil {
			t.Fatalf("write backup %s: %v", dst, err)
		}
		t.Logf("streamed %s (%d bytes)", rel, len(raw))
	}
	if err := cp.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	time.Sleep(time.Duration(secs) * time.Second / 3)
	stop.Store(true)
	wg.Wait()
	bgWG.Wait()
	select {
	case err := <-bgErr:
		t.Fatalf("background op: %v", err)
	default:
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	// The streamed backup must open with a consistent cut.
	bo := spool.DefaultOptions(backupDir)
	bo.MasterKey = master
	bst, err := spool.Open(bo)
	if err != nil {
		t.Fatalf("open backup: %v", err)
	}
	defer bst.Close()
	got := make(map[string][]byte)
	if err := spool.Load(backupDir, master, func(recs []spool.Record) error {
		for _, r := range recs {
			got[string(r.Key)] = append([]byte(nil), r.Value...)
		}
		return nil
	}); err != nil {
		t.Fatalf("load backup: %v", err)
	}
	if string(got[anchorKey]) != anchorVal {
		t.Fatalf("anchor = %q, want %q", got[anchorKey], anchorVal)
	}
	if len(got) != writers*slots+1 {
		t.Fatalf("backup has %d keys, want %d", len(got), writers*slots+1)
	}
	for w := 0; w < writers; w++ {
		for s := 0; s < slots; s++ {
			k := fmt.Sprintf("w%02d/s%05d", w, s)
			v := got[k]
			if len(v) != 64 {
				t.Fatalf("key %s value length %d, want 64 (torn stream?)", k, len(v))
			}
			round := int64(binary.BigEndian.Uint64(v[0:8]))
			owner := int(binary.BigEndian.Uint64(v[8:16]))
			if owner != w || round <= 0 || round > finals[w].Load() {
				t.Fatalf("key %s has impossible value (owner=%d round=%d)", k, owner, round)
			}
		}
	}
	t.Logf("backup-under-write consistent: %d keys, anchor exact", len(got))
}
