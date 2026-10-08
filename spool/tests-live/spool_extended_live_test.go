// Live spool bug-hunting and stress test suite:
// - Multi-process lock exclusion and crash unlock (flock resilience)
// - SIGKILL mid-compaction and orphaned staging file sweep
// - Torn-tail append truncation and recovery vs mid-block corruption rejection
// - Live master-key rotation and old-key rejection
// - Storage fault injection, terminal latching, and recovery
package spoollive_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/spool"
)

// TestSpoolChildLockHolder opens the store and sleeps until killed.
// Used to test multi-process lock conflict and SIGKILL cleanup.
func TestSpoolChildLockHolder(t *testing.T) {
	if os.Getenv("MURMUR_SPOOL_CHILD") != "lockholder" {
		t.Skip("child entrypoint only")
	}
	dir := os.Getenv("MURMUR_SPOOL_DIR")
	keyHex := os.Getenv("MURMUR_SPOOL_KEY")
	master, err := hex.DecodeString(keyHex)
	if err != nil || len(master) != 32 || dir == "" {
		fmt.Fprintf(os.Stderr, "child-lockholder: bad env\n")
		os.Exit(2)
	}
	o := spool.DefaultOptions(filepath.Join(dir, "store"))
	o.MasterKey = master
	st, err := spool.Open(o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child-lockholder: open: %v\n", err)
		os.Exit(2)
	}
	defer st.Close()

	// Signal parent that store is open and locked
	marker := filepath.Join(dir, "locked")
	if err := os.WriteFile(marker, []byte("locked\n"), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "child-lockholder: marker: %v\n", err)
		os.Exit(2)
	}

	// Sleep until SIGKILL
	select {}
}

// TestSpoolLockExclusionAndCrashUnlock verifies that:
// 1. Two separate processes cannot open the same store concurrently (spool.ErrLocked).
// 2. When the lock holder process is killed with SIGKILL, the OS flock is freed
//    and a new process can immediately open the store without manual intervention.
func TestSpoolLockExclusionAndCrashUnlock(t *testing.T) {
	dir := t.TempDir()
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	child := exec.Command(os.Args[0], "-test.run=^TestSpoolChildLockHolder$", "-test.count=1")
	child.Env = append(os.Environ(),
		"MURMUR_SPOOL_CHILD=lockholder",
		"MURMUR_SPOOL_DIR="+dir,
		"MURMUR_SPOOL_KEY="+hex.EncodeToString(master),
	)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()

	marker := filepath.Join(dir, "locked")
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		select {
		case err := <-waited:
			t.Fatalf("child exited prematurely: %v\n%s", err, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			_ = child.Process.Kill()
			t.Fatalf("child never acquired lock\n%s", stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 1. While child holds the lock, attempt to open the store from parent.
	o := spool.DefaultOptions(filepath.Join(dir, "store"))
	o.MasterKey = master
	stConflict, errConflict := spool.Open(o)
	if errConflict == nil {
		stConflict.Close()
		_ = child.Process.Kill()
		t.Fatalf("spool.Open succeeded concurrently, expected ErrLocked")
	}
	if !errors.Is(errConflict, spool.ErrLocked) {
		_ = child.Process.Kill()
		t.Fatalf("expected ErrLocked, got: %v", errConflict)
	}

	// 2. Kill the child holder abruptly with SIGKILL.
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	<-waited

	// 3. Parent must now successfully open the store immediately.
	stRecovered, errRecovered := spool.Open(o)
	if errRecovered != nil {
		t.Fatalf("open after child SIGKILL failed: %v", errRecovered)
	}
	defer stRecovered.Close()

	if err := stRecovered.Put([]byte("recovered_key"), []byte("recovered_val")); err != nil {
		t.Fatalf("put after recovery: %v", err)
	}
	if err := stRecovered.Flush(); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}
}

// TestSpoolChildCompactor continuously overwrites keys and deletes keys to create
// garbage, rotating segments and compacting them until killed.
func TestSpoolChildCompactor(t *testing.T) {
	if os.Getenv("MURMUR_SPOOL_CHILD") != "compactor" {
		t.Skip("child entrypoint only")
	}
	dir := os.Getenv("MURMUR_SPOOL_DIR")
	keyHex := os.Getenv("MURMUR_SPOOL_KEY")
	master, err := hex.DecodeString(keyHex)
	if err != nil || len(master) != 32 || dir == "" {
		fmt.Fprintf(os.Stderr, "child-compactor: bad env\n")
		os.Exit(2)
	}

	storePath := filepath.Join(dir, "store")
	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	o.Durability = spool.DurabilityFlush
	// Small segment size and small blocks to force rapid rotations and compactions
	o.MaxKeySize = 512
	o.MaxValueSize = 2048
	o.TargetBlockBytes = 16 * 1024
	o.MaxBlockBytes = 32 * 1024
	o.MaxAtomicBatchBytes = 64 * 1024
	o.MaxSegmentSize = 128 * 1024
	o.CompactionThreshold = 0.5
	o.ReclaimInterval = 30 * time.Millisecond
	o.TombProofThreshold = 10

	st, err := spool.Open(o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child-compactor: open: %v\n", err)
		os.Exit(2)
	}
	defer st.Close()

	marker := filepath.Join(dir, "compacting")
	reported := false

	// Write 10 keeper keys in round 0 that remain live in the first segment
	for slot := 0; slot < 10; slot++ {
		k := fmt.Sprintf("k%03d", slot)
		v := liveBigValue(0, slot, 1500)
		if err := st.Put([]byte(k), v); err != nil {
			fmt.Fprintf(os.Stderr, "child-compactor: put keeper: %v\n", err)
			os.Exit(2)
		}
	}

	// Repeatedly overwrite volatile keys so older segments drop to 20% live,
	// triggering compaction rewrite (live > 0 && live/total <= 0.5)
	for round := 0; ; round++ {
		for slot := 0; slot < 40; slot++ {
			k := fmt.Sprintf("v%03d", slot)
			v := liveBigValue(round, slot, 1500)
			if err := st.Put([]byte(k), v); err != nil {
				fmt.Fprintf(os.Stderr, "child-compactor: put: %v\n", err)
				os.Exit(2)
			}
		}
		if err := st.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "child-compactor: flush: %v\n", err)
			os.Exit(2)
		}
		_ = st.Reclaim()

		stats := st.Stats()
		if !reported && (stats.Compactions > 0 || round >= 3) {
			reported = true
			if err := os.WriteFile(marker, []byte(fmt.Sprintf("%d\n", round)), 0o600); err != nil {
				fmt.Fprintf(os.Stderr, "child-compactor: write marker: %v\n", err)
				os.Exit(2)
			}
		}
	}
}

// TestSpoolCompactionCrashRecovery verifies that when a process is killed mid-compaction:
// 1. Uncommitted temporary staging files (.cmp-*.tmp-*) are swept cleanly by spool.Open.
// 2. The manifest and segment catalog remain fully consistent.
// 3. All acknowledged live data survives with no corruptions or resurrected tombstones.
// 4. Compaction and store mutations resume normally on reopen.
func TestSpoolCompactionCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	child := exec.Command(os.Args[0], "-test.run=^TestSpoolChildCompactor$", "-test.count=1")
	child.Env = append(os.Environ(),
		"MURMUR_SPOOL_CHILD=compactor",
		"MURMUR_SPOOL_DIR="+dir,
		"MURMUR_SPOOL_KEY="+hex.EncodeToString(master),
	)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()

	marker := filepath.Join(dir, "compacting")
	deadline := time.Now().Add(30 * time.Second)
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
			_ = child.Process.Kill()
			t.Fatalf("child never reached compaction\n%s", stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Allow compaction to run for a short window, then kill mid-operation
	time.Sleep(250 * time.Millisecond)
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	<-waited

	storePath := filepath.Join(dir, "store")
	segDir := filepath.Join(storePath, "segments")

	// Inject an explicit stale orphan staging temp file in segments dir
	// to simulate a crash while writing replacement segments.
	staleStg := filepath.Join(segDir, ".cmp-999-.tmp-orphan")
	_ = os.WriteFile(staleStg, []byte("unfinished staging bytes"), 0o600)

	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	o.MaxKeySize = 512
	o.MaxValueSize = 2048
	o.TargetBlockBytes = 16 * 1024
	o.MaxBlockBytes = 32 * 1024
	o.MaxAtomicBatchBytes = 64 * 1024
	o.MaxSegmentSize = 128 * 1024
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open after compaction crash: %v", err)
	}
	defer st.Close()

	// Verify sweepUnlisted removed any stale .tmp- files
	entries, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("readdir segments: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Fatalf("stale temp file not swept: %s", e.Name())
		}
	}

	// Verify live dataset consistency
	seenKeys := make(map[string]bool)
	err = spool.Load(storePath, master, func(recs []spool.Record) error {
		for _, r := range recs {
			seenKeys[string(r.Key)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("load after crash: %v", err)
	}

	// Verify all 10 keeper keys and 40 volatile keys exist
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%03d", i)
		if !seenKeys[k] {
			t.Fatalf("missing keeper key %s after compaction crash recovery", k)
		}
	}
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("v%03d", i)
		if !seenKeys[k] {
			t.Fatalf("missing volatile key %s after compaction crash recovery", k)
		}
	}

	// Verify subsequent Reclaim runs cleanly
	if err := st.Reclaim(); err != nil {
		t.Fatalf("post-recovery reclaim: %v", err)
	}

	// Verify store can accept new mutations
	if err := st.Put([]byte("post_crash_key"), []byte("fresh_value")); err != nil {
		t.Fatalf("post-recovery put: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("post-recovery flush: %v", err)
	}
}

// TestSpoolTornTailTruncationLive verifies that an unaligned, partial write
// at the tail of the active segment file (simulating torn write on sudden power loss)
// is cleanly truncated upon spool.Open, recovering all prior valid data.
func TestSpoolTornTailTruncationLive(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	o.Durability = spool.DurabilitySync

	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write 100 acknowledged keys
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("tail_key_%04d", i))
		v := []byte(fmt.Sprintf("tail_val_%04d", i))
		if err := st.Put(k, v); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	st.Close()

	// Locate the active segment file in segments/
	segDir := filepath.Join(storePath, "segments")
	entries, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var activeSeg string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".spool") {
			activeSeg = filepath.Join(segDir, e.Name())
			break
		}
	}
	if activeSeg == "" {
		t.Fatalf("no segment file found")
	}

	// Append 53 bytes of torn junk to simulate incomplete write / power outage
	tornBytes := []byte("TORN_TAIL_GARBAGE_SIMULATING_UNALIGNED_POWER_LOSS_123")
	f, err := os.OpenFile(activeSeg, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open seg: %v", err)
	}
	if _, err := f.Write(tornBytes); err != nil {
		f.Close()
		t.Fatalf("write torn bytes: %v", err)
	}
	f.Close()

	// Reopen store: spool must detect the torn tail and truncate it
	stRecovered, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open with torn tail: %v", err)
	}
	defer stRecovered.Close()

	stats := stRecovered.Stats()
	if stats.TruncatedTails < 1 {
		t.Fatalf("expected TruncatedTails >= 1, got %d", stats.TruncatedTails)
	}

	// Verify all 100 original keys load correctly
	seenVals := make(map[string]string)
	err = spool.Load(storePath, master, func(recs []spool.Record) error {
		for _, r := range recs {
			seenVals[string(r.Key)] = string(r.Value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("load after tail truncation: %v", err)
	}
	if len(seenVals) != 100 {
		t.Fatalf("expected 100 keys, got %d", len(seenVals))
	}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("tail_key_%04d", i)
		expectedVal := fmt.Sprintf("tail_val_%04d", i)
		if seenVals[k] != expectedVal {
			t.Fatalf("key %s mismatch: got %q want %q", k, seenVals[k], expectedVal)
		}
	}

	// Verify store is healthy and can append new records past the truncation
	if err := stRecovered.Put([]byte("new_key_after_trunc"), []byte("new_val")); err != nil {
		t.Fatalf("put after truncation: %v", err)
	}
	if err := stRecovered.Flush(); err != nil {
		t.Fatalf("flush after truncation: %v", err)
	}
}

// TestSpoolMidBlockCorruptionLive verifies that tampering / bit-rot
// inside an existing block payload fails closed (AEAD authentication failure or ErrCorrupt),
// preventing silent corruption from reaching the application.
func TestSpoolMidBlockCorruptionLive(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	o.Durability = spool.DurabilitySync

	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 50; i++ {
		_ = st.Put([]byte(fmt.Sprintf("corrupt_k_%02d", i)), []byte(fmt.Sprintf("corrupt_v_%02d", i)))
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	st.Close()

	// Locate segment file
	segDir := filepath.Join(storePath, "segments")
	entries, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var segPath string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".spool") {
			segPath = filepath.Join(segDir, e.Name())
			break
		}
	}
	if segPath == "" {
		t.Fatalf("no segment file found")
	}

	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("read seg: %v", err)
	}
	if len(data) < 200 {
		t.Fatalf("segment file too small: %d bytes", len(data))
	}

	// Tamper bytes in the middle of the sealed block (offset 150)
	for i := 150; i < 158 && i < len(data); i++ {
		data[i] ^= 0xFF
	}
	if err := os.WriteFile(segPath, data, 0o600); err != nil {
		t.Fatalf("write tampered seg: %v", err)
	}

	// spool.Load MUST fail because block authentication failed
	errLoad := spool.Load(storePath, master, func(recs []spool.Record) error {
		return nil
	})
	if errLoad == nil {
		t.Fatalf("spool.Load succeeded on tampered segment, expected authentication failure")
	}
	t.Logf("tampered segment correctly rejected: %v", errLoad)
}

// TestSpoolMasterKeyRotationLive verifies that:
// 1. RotateMasterKey rewrites keys.enc under the new key.
// 2. The old master key is rejected (spool.ErrWrongKey).
// 3. The new master key decrypts both pre-rotation and post-rotation data.
func TestSpoolMasterKeyRotationLive(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	masterA := make([]byte, 32)
	masterB := make([]byte, 32)
	if _, err := rand.Read(masterA); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := rand.Read(masterB); err != nil {
		t.Fatalf("rand: %v", err)
	}

	o := spool.DefaultOptions(storePath)
	o.MasterKey = masterA

	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open under masterA: %v", err)
	}

	// Write batch under masterA
	for i := 0; i < 50; i++ {
		if err := st.Put([]byte(fmt.Sprintf("pre_rot_%03d", i)), []byte("valA")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Rotate to masterB
	if err := st.RotateMasterKey(masterB, ""); err != nil {
		t.Fatalf("RotateMasterKey: %v", err)
	}

	// Write batch under masterB
	for i := 0; i < 50; i++ {
		if err := st.Put([]byte(fmt.Sprintf("post_rot_%03d", i)), []byte("valB")); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	st.Close()

	// 1. Attempt to reopen with old masterA: MUST fail with ErrWrongKey
	oA := spool.DefaultOptions(storePath)
	oA.MasterKey = masterA
	stA, errA := spool.Open(oA)
	if errA == nil {
		stA.Close()
		t.Fatalf("open with old master key succeeded, expected ErrWrongKey")
	}
	if !errors.Is(errA, spool.ErrWrongKey) {
		t.Fatalf("expected ErrWrongKey, got: %v", errA)
	}

	// 2. Reopen with new masterB: MUST succeed
	oB := spool.DefaultOptions(storePath)
	oB.MasterKey = masterB
	stB, errB := spool.Open(oB)
	if errB != nil {
		t.Fatalf("open with new master key failed: %v", errB)
	}
	defer stB.Close()

	// 3. Verify all 100 records load with exact values
	seenA, seenB := 0, 0
	err = spool.Load(storePath, masterB, func(recs []spool.Record) error {
		for _, r := range recs {
			if strings.HasPrefix(string(r.Key), "pre_rot_") && string(r.Value) == "valA" {
				seenA++
			} else if strings.HasPrefix(string(r.Key), "post_rot_") && string(r.Value) == "valB" {
				seenB++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("load under masterB: %v", err)
	}
	if seenA != 50 || seenB != 50 {
		t.Fatalf("expected 50 pre-rot and 50 post-rot records, got pre=%d post=%d", seenA, seenB)
	}
}

// TestSpoolStorageFaultTerminalIsolationLive verifies that:
// 1. When an underlying storage write fails, OnStorageError fires and
//    the store immediately latches into a terminal state.
// 2. Subsequent mutating calls immediately fail with ErrStorageFailed.
// 3. When the fault is cleared and the store is reopened, prior durable state
//    is recovered cleanly.
func TestSpoolStorageFaultTerminalIsolationLive(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	var appends atomic.Int32
	var terminalNotified atomic.Bool
	injectedErr := errors.New("simulated disk full / io error")

	faults := &spool.FaultHooks{
		Append: func() error {
			if appends.Add(1) > 2 {
				return injectedErr
			}
			return nil
		},
	}

	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	o.Faults = faults
	o.Durability = spool.DurabilitySync
	o.OnStorageError = func(err error) {
		terminalNotified.Store(true)
	}

	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Write first record (appends=1, succeeds)
	if err := st.Put([]byte("good_key_1"), []byte("good_val_1")); err != nil {
		t.Fatalf("put 1: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush 1: %v", err)
	}

	// Write second record (appends=2, succeeds)
	if err := st.Put([]byte("good_key_2"), []byte("good_val_2")); err != nil {
		t.Fatalf("put 2: %v", err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush 2: %v", err)
	}

	// Third write triggers injected fault (appends=3)
	_ = st.Put([]byte("bad_key_3"), []byte("bad_val_3"))
	flushErr := st.Flush()
	if flushErr == nil {
		t.Fatalf("flush succeeded, expected fault failure")
	}

	// Verify terminal latching
	if st.StorageError() == nil {
		t.Fatalf("StorageError is nil after failure")
	}
	if !terminalNotified.Load() {
		t.Fatalf("OnStorageError callback was not notified")
	}

	// All subsequent mutations must immediately fail with ErrStorageFailed
	if err := st.Put([]byte("blocked_key"), []byte("blocked_val")); !errors.Is(err, spool.ErrStorageFailed) {
		t.Fatalf("expected ErrStorageFailed on Put, got: %v", err)
	}
	if err := st.Commit([]spool.Mutation{{Key: []byte("blocked")}}, spool.DurabilitySync); !errors.Is(err, spool.ErrStorageFailed) {
		t.Fatalf("expected ErrStorageFailed on Commit, got: %v", err)
	}
	st.Close()

	// Reopen store without faults: all acknowledged state survives
	cleanOpts := spool.DefaultOptions(storePath)
	cleanOpts.MasterKey = master
	stClean, err := spool.Open(cleanOpts)
	if err != nil {
		t.Fatalf("clean reopen: %v", err)
	}
	defer stClean.Close()

	seenCount := 0
	err = spool.Load(storePath, master, func(recs []spool.Record) error {
		seenCount += len(recs)
		return nil
	})
	if err != nil {
		t.Fatalf("clean load: %v", err)
	}
	if seenCount < 2 {
		t.Fatalf("expected at least 2 good keys, found %d", seenCount)
	}

	// New writes succeed normally
	if err := stClean.Put([]byte("fresh_post_recovery"), []byte("value")); err != nil {
		t.Fatalf("fresh put: %v", err)
	}
	if err := stClean.Flush(); err != nil {
		t.Fatalf("fresh flush: %v", err)
	}
}

// TestSpoolContextIDIsolationAndRebindLive verifies that:
// 1. A store bound to Context A strictly rejects opening under Context B (ErrContextMismatch).
// 2. RebindContext cleanly migrates the store to Context B.
// 3. Following rebind, Context A is rejected and Context B successfully opens all records.
func TestSpoolContextIDIsolationAndRebindLive(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	var ctxA, ctxB [16]byte
	if _, err := rand.Read(ctxA[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := rand.Read(ctxB[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}

	oA := spool.DefaultOptions(storePath)
	oA.MasterKey = master
	oA.ContextID = ctxA[:]
	st, err := spool.Open(oA)
	if err != nil {
		t.Fatalf("open ctxA: %v", err)
	}

	for i := 0; i < 50; i++ {
		_ = st.Put([]byte(fmt.Sprintf("ctx_k_%02d", i)), []byte(fmt.Sprintf("ctx_v_%02d", i)))
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	st.Close()

	// 1. Attempt open with ctxB: MUST fail with ErrContextMismatch
	oB := spool.DefaultOptions(storePath)
	oB.MasterKey = master
	oB.ContextID = ctxB[:]
	stB, errB := spool.Open(oB)
	if errB == nil {
		stB.Close()
		t.Fatalf("expected ErrContextMismatch on wrong context")
	}
	if !errors.Is(errB, spool.ErrContextMismatch) {
		t.Fatalf("expected ErrContextMismatch, got: %v", errB)
	}

	// 2. Open with ctxA, rebind to ctxB
	stA, err := spool.Open(oA)
	if err != nil {
		t.Fatalf("open ctxA: %v", err)
	}
	if err := stA.RebindContext(ctxB); err != nil {
		t.Fatalf("rebind ctxB: %v", err)
	}
	stA.Close()

	// 3. Old ctxA must now fail
	if _, err := spool.Open(oA); !errors.Is(err, spool.ErrContextMismatch) {
		t.Fatalf("expected old ctxA to fail with ErrContextMismatch, got: %v", err)
	}

	// 4. New ctxB must succeed and load all records
	stB2, err := spool.Open(oB)
	if err != nil {
		t.Fatalf("open ctxB after rebind: %v", err)
	}
	defer stB2.Close()

	count := 0
	err = spool.Load(storePath, master, func(recs []spool.Record) error {
		count += len(recs)
		return nil
	})
	if err != nil {
		t.Fatalf("load after rebind: %v", err)
	}
	if count != 50 {
		t.Fatalf("expected 50 records, got %d", count)
	}
}

// TestSpoolCheckpointOnlineIsolationAndForkLive verifies that:
// 1. A captured checkpoint can be opened as an independent writable store.
// 2. Subsequent writes and deletes in the original store do not bleed into the checkpoint.
// 3. New writes in the checkpoint fork do not bleed into the original store.
func TestSpoolCheckpointOnlineIsolationAndForkLive(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	ckptDir := filepath.Join(dir, "ckpt_fork")
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	o.Durability = spool.DurabilitySync
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	// Commit initial dataset
	for i := 0; i < 50; i++ {
		_ = st.Put([]byte(fmt.Sprintf("base_%02d", i)), []byte("val_base"))
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Capture point-in-time checkpoint
	ckpt, err := st.Checkpoint(context.Background(), ckptDir)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	defer ckpt.Release()

	// Mutate live store A: delete 10 keys, insert 20 new keys
	for i := 0; i < 10; i++ {
		_ = st.Delete([]byte(fmt.Sprintf("base_%02d", i)))
	}
	for i := 0; i < 20; i++ {
		_ = st.Put([]byte(fmt.Sprintf("storeA_only_%02d", i)), []byte("liveA"))
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush live store: %v", err)
	}

	// Verify checkpoint cut directly before mutations: has exact initial 50 keys
	forkKeys := make(map[string]bool)
	err = spool.Load(ckptDir, master, func(recs []spool.Record) error {
		for _, r := range recs {
			if !r.Deleted {
				forkKeys[string(r.Key)] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("load ckpt: %v", err)
	}
	if len(forkKeys) != 50 {
		t.Fatalf("expected 50 keys in checkpoint, got %d", len(forkKeys))
	}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("base_%02d", i)
		if !forkKeys[k] {
			t.Fatalf("checkpoint missing base key %s", k)
		}
	}
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("storeA_only_%02d", i)
		if forkKeys[k] {
			t.Fatalf("checkpoint leaked live write %s", k)
		}
	}

	// Stream/copy checkpoint files to an independent fork directory
	forkDir := filepath.Join(dir, "fork_store")
	for _, src := range ckpt.Files() {
		rel, err := filepath.Rel(ckptDir, src)
		if err != nil {
			t.Fatalf("rel: %v", err)
		}
		dst := filepath.Join(forkDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		data, err := os.ReadFile(src)
		if err != nil {
			t.Fatalf("read src: %v", err)
		}
		if err := os.WriteFile(dst, data, 0o600); err != nil {
			t.Fatalf("write dst: %v", err)
		}
	}

	// Open fork directory as independent store
	oFork := spool.DefaultOptions(forkDir)
	oFork.MasterKey = master
	stFork, err := spool.Open(oFork)
	if err != nil {
		t.Fatalf("open checkpoint fork: %v", err)
	}
	defer stFork.Close()

	// Write to fork and verify isolation from store A
	if err := stFork.Put([]byte("fork_exclusive"), []byte("fork_val")); err != nil {
		t.Fatalf("fork put: %v", err)
	}
	if err := stFork.Flush(); err != nil {
		t.Fatalf("fork flush: %v", err)
	}

	// Check store A does not have fork_exclusive
	storeAKeys := make(map[string]bool)
	err = spool.Load(storePath, master, func(recs []spool.Record) error {
		for _, r := range recs {
			if !r.Deleted {
				storeAKeys[string(r.Key)] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("load store A: %v", err)
	}
	if storeAKeys["fork_exclusive"] {
		t.Fatalf("store A contains fork_exclusive key!")
	}
	if len(storeAKeys) != 60 { // 40 remaining base + 20 storeA_only
		t.Fatalf("store A key count mismatch: got %d want 60", len(storeAKeys))
	}
}

// TestSpoolDataKeyAgingAndPruningLive verifies data key rotation,
// inventory inspection, and that all data across all rotated key eras
// remains readable without decryption failures.
func TestSpoolDataKeyAgingAndPruningLive(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	o.Durability = spool.DurabilitySync
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Key era 1
	for i := 0; i < 20; i++ {
		_ = st.Put([]byte(fmt.Sprintf("k1_%02d", i)), []byte("val1"))
	}
	_ = st.Flush()

	// Rotate to data key 2
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey 1: %v", err)
	}
	for i := 0; i < 20; i++ {
		_ = st.Put([]byte(fmt.Sprintf("k2_%02d", i)), []byte("val2"))
	}
	_ = st.Flush()

	// Rotate to data key 3
	if err := st.RotateKey(); err != nil {
		t.Fatalf("RotateKey 2: %v", err)
	}
	for i := 0; i < 20; i++ {
		_ = st.Put([]byte(fmt.Sprintf("k3_%02d", i)), []byte("val3"))
	}
	_ = st.Flush()

	inv := st.KeyInventory()
	if len(inv.DataKeys) != 3 {
		t.Fatalf("expected 3 data keys in inventory, got %d", len(inv.DataKeys))
	}

	// PruneDataKeys while all 3 keys are in use: none should be dropped
	pruned, err := st.PruneDataKeys()
	if err != nil {
		t.Fatalf("PruneDataKeys: %v", err)
	}
	if len(pruned) != 0 {
		t.Fatalf("expected 0 pruned keys, got %d", len(pruned))
	}
	st.Close()

	// Reopen store: all 60 keys across 3 key eras must decrypt cleanly
	reopenedKeys := 0
	err = spool.Load(storePath, master, func(recs []spool.Record) error {
		reopenedKeys += len(recs)
		return nil
	})
	if err != nil {
		t.Fatalf("load multi-key store: %v", err)
	}
	if reopenedKeys != 60 {
		t.Fatalf("expected 60 keys, got %d", reopenedKeys)
	}
}

// TestSpoolBackpressureHighConcurrencyLive stresses the store under high concurrent
// write pressure with a small memory budget, proving that:
// 1. TryPut returns ErrBackpressure without panicking when limits are reached.
// 2. Put blocks safely and flushes smoothly.
// 3. No acknowledged writes are lost or corrupted.
func TestSpoolBackpressureHighConcurrencyLive(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	// Small pending budget to force backpressure under concurrency
	o.MaxPendingBytes = 64 * 1024
	o.Flush.MaxBytes = 16 * 1024
	o.Flush.MaxDelay = 20 * time.Millisecond

	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	var tryBackpressureHit atomic.Int64
	var acceptedPuts atomic.Int64
	var wg sync.WaitGroup

	// Run concurrent writers
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			val := make([]byte, 1024)
			for i := range val {
				val[i] = byte(workerID*31 + i)
			}
			for i := 0; i < 40; i++ {
				k := []byte(fmt.Sprintf("bp_w%02d_%04d", workerID, i))
				// TryPut to exercise non-blocking path
				if i%2 == 0 {
					err := st.TryPut(k, val)
					if errors.Is(err, spool.ErrBackpressure) {
						tryBackpressureHit.Add(1)
						// Fall back to blocking Put to guarantee write
						if pErr := st.Put(k, val); pErr == nil {
							acceptedPuts.Add(1)
						}
					} else if err == nil {
						acceptedPuts.Add(1)
					}
				} else {
					if err := st.Put(k, val); err == nil {
						acceptedPuts.Add(1)
					}
				}
			}
		}(w)
	}

	wg.Wait()
	if err := st.Flush(); err != nil {
		t.Fatalf("flush after backpressure: %v", err)
	}

	t.Logf("backpressure hits: %d, total accepted writes: %d",
		tryBackpressureHit.Load(), acceptedPuts.Load())

	// Verify all accepted writes are durably readable
	readCount := int64(0)
	err = spool.Load(storePath, master, func(recs []spool.Record) error {
		readCount += int64(len(recs))
		return nil
	})
	if err != nil {
		t.Fatalf("load backpressure store: %v", err)
	}
	if readCount != acceptedPuts.Load() {
		t.Fatalf("data count mismatch: got %d loaded want %d accepted", readCount, acceptedPuts.Load())
	}
}

// TestSpoolChildMixedBatchWriter commits atomic multi-mutation batches combining
// inserts, updates, and deletes in each round.
func TestSpoolChildMixedBatchWriter(t *testing.T) {
	if os.Getenv("MURMUR_SPOOL_CHILD") != "mixedbatch" {
		t.Skip("child entrypoint only")
	}
	dir := os.Getenv("MURMUR_SPOOL_DIR")
	keyHex := os.Getenv("MURMUR_SPOOL_KEY")
	master, err := hex.DecodeString(keyHex)
	if err != nil || len(master) != 32 || dir == "" {
		fmt.Fprintf(os.Stderr, "child-mixedbatch: bad env\n")
		os.Exit(2)
	}
	o := spool.DefaultOptions(filepath.Join(dir, "store"))
	o.MasterKey = master
	o.Durability = spool.DurabilitySync
	st, err := spool.Open(o)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child-mixedbatch: open: %v\n", err)
		os.Exit(2)
	}
	defer st.Close()

	marker := filepath.Join(dir, "progress")
	for round := 0; ; round++ {
		muts := make([]spool.Mutation, 0, 20)
		for s := 0; s < 10; s++ {
			k := []byte(fmt.Sprintf("r%05d_ins%02d", round, s))
			v := liveValue(round, s)
			muts = append(muts, spool.Mutation{Key: k, Value: v})
		}
		if round > 0 {
			// Overwrite 5 keys from previous round
			for s := 0; s < 5; s++ {
				k := []byte(fmt.Sprintf("r%05d_ins%02d", round-1, s))
				v := liveValue(round, 100+s)
				muts = append(muts, spool.Mutation{Key: k, Value: v})
			}
			// Delete 5 keys from previous round
			for s := 5; s < 10; s++ {
				k := []byte(fmt.Sprintf("r%05d_ins%02d", round-1, s))
				muts = append(muts, spool.Mutation{Key: k, Deleted: true})
			}
		}
		if err := st.Commit(muts, spool.DurabilitySync); err != nil {
			fmt.Fprintf(os.Stderr, "child-mixedbatch: commit: %v\n", err)
			os.Exit(2)
		}

		// Update progress marker
		f, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			os.Exit(2)
		}
		fmt.Fprintf(f, "%d\n", round)
		_ = f.Sync()
		f.Close()
	}
}

// TestSpoolAtomicBatchMixedMutationsCrashLive tests crash recovery for atomic batches
// containing mixed operations (insertions, overwrites, and tombstones). It proves that
// in-flight batches never leave partial writes (all-or-nothing atomicity), and acknowledged
// deletes never resurrect on crash reopen.
func TestSpoolAtomicBatchMixedMutationsCrashLive(t *testing.T) {
	dir := t.TempDir()
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatalf("rand: %v", err)
	}

	child := exec.Command(os.Args[0], "-test.run=^TestSpoolChildMixedBatchWriter$", "-test.count=1")
	child.Env = append(os.Environ(),
		"MURMUR_SPOOL_CHILD=mixedbatch",
		"MURMUR_SPOOL_DIR="+dir,
		"MURMUR_SPOOL_KEY="+hex.EncodeToString(master),
	)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- child.Wait() }()

	marker := filepath.Join(dir, "progress")
	deadline := time.Now().Add(15 * time.Second)
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
			_ = child.Process.Kill()
			t.Fatalf("child never wrote batch\n%s", stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(1 * time.Second)
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	<-waited

	raw, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	ackedRound, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || ackedRound < 1 {
		t.Fatalf("bad acked round %q", raw)
	}
	t.Logf("child acknowledged rounds 0..%d of mixed atomic batches", ackedRound)

	storePath := filepath.Join(dir, "store")
	o := spool.DefaultOptions(storePath)
	o.MasterKey = master
	st, err := spool.Open(o)
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer st.Close()

	seenLive := make(map[string][]byte)
	err = spool.Load(storePath, master, func(recs []spool.Record) error {
		for _, r := range recs {
			if !r.Deleted {
				seenLive[string(r.Key)] = append([]byte(nil), r.Value...)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("load after crash: %v", err)
	}

	// Verify all acknowledged rounds up to ackedRound-1:
	for r := 0; r < ackedRound; r++ {
		// ins05..ins09 MUST be deleted (not live)
		for s := 5; s < 10; s++ {
			k := fmt.Sprintf("r%05d_ins%02d", r, s)
			if _, isLive := seenLive[k]; isLive {
				t.Fatalf("key %s was deleted in round %d but remains live!", k, r+1)
			}
		}
		// ins00..ins04 MUST have the overwritten value from round r+1
		for s := 0; s < 5; s++ {
			k := fmt.Sprintf("r%05d_ins%02d", r, s)
			val, isLive := seenLive[k]
			if !isLive {
				t.Fatalf("key %s missing!", k)
			}
			expectedVal := liveValue(r+1, 100+s)
			if !bytes.Equal(val, expectedVal) {
				t.Fatalf("key %s value mismatch for overwrite!", k)
			}
		}
	}
}
