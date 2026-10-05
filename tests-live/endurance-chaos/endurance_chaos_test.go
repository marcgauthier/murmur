// Package endurancechaos_test answers "can I make Murmur fail under
// realistic abuse?" at the longest timescale: a 24-72 hour live mesh with
// continuous writes while restarts, packet loss, latency, disk pressure,
// clock skew, key rotation, snapshot resyncs, and log GC all overlap.
//
// Every fault injector runs concurrently against one bounded working set;
// the verdict is exact cross-node convergence (equal row counts plus
// identical PK-ordered digests) with counter-proven GC progress, at least
// one snapshot resync, successful key rotations, and a post-verdict write.
//
// Scale and length are environment-driven; see README.md for the knobs.
// Defaults run a ~10 minute smoke of the full fault mix.
package endurancechaos_test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

const tableName = "endurance_rows"

type enduranceConfig struct {
	nodes         int
	duration      time.Duration
	settle        time.Duration
	writeInterval time.Duration
	maxRows       int
	seed          int64
	statusEvery   time.Duration
	restartEvery  time.Duration
	restartDown   time.Duration
	rotateEvery   time.Duration
	snapshotEvery time.Duration
	snapshotOff   time.Duration
	impairOn      time.Duration
	impairOff     time.Duration
	impairLatency int
	impairLoss    int
	diskEvery     time.Duration
	diskMB        int
	diskHold      time.Duration
	skewDur       time.Duration
}

func loadConfig(t *testing.T) enduranceConfig {
	t.Helper()
	cfg := enduranceConfig{
		nodes:         min(max(harness.EnvInt("MURMUR_ENDURANCE_NODES", 5), 3), 16),
		duration:      harness.EnvSeconds("MURMUR_ENDURANCE_DURATION_SECONDS", 600),
		settle:        harness.EnvSeconds("MURMUR_ENDURANCE_SETTLE_SECONDS", 300),
		writeInterval: harness.EnvMillis("MURMUR_ENDURANCE_WRITE_INTERVAL_MS", 25),
		maxRows:       harness.EnvInt("MURMUR_ENDURANCE_MAX_ROWS", 10000),
		seed:          int64(harness.EnvInt("MURMUR_ENDURANCE_SEED", 1)),
		statusEvery:   harness.EnvSeconds("MURMUR_ENDURANCE_STATUS_SECONDS", 30),
		restartEvery:  harness.EnvSeconds("MURMUR_ENDURANCE_RESTART_SECONDS", 90),
		restartDown:   harness.EnvSeconds("MURMUR_ENDURANCE_RESTART_DOWN_SECONDS", 8),
		rotateEvery:   harness.EnvSeconds("MURMUR_ENDURANCE_ROTATE_SECONDS", 180),
		snapshotEvery: harness.EnvSeconds("MURMUR_ENDURANCE_SNAPSHOT_SECONDS", 300),
		snapshotOff:   harness.EnvSeconds("MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS", 75),
		impairOn:      harness.EnvSeconds("MURMUR_ENDURANCE_IMPAIR_ON_SECONDS", 60),
		impairOff:     harness.EnvSeconds("MURMUR_ENDURANCE_IMPAIR_OFF_SECONDS", 20),
		impairLatency: harness.EnvInt("MURMUR_ENDURANCE_IMPAIR_LATENCY_MS", 150),
		impairLoss:    harness.EnvInt("MURMUR_ENDURANCE_IMPAIR_LOSS_PCT", 3),
		diskEvery:     harness.EnvSeconds("MURMUR_ENDURANCE_DISK_SECONDS", 180),
		diskMB:        harness.EnvInt("MURMUR_ENDURANCE_DISK_MB", 128),
		diskHold:      harness.EnvSeconds("MURMUR_ENDURANCE_DISK_HOLD_SECONDS", 60),
		skewDur:       harness.EnvSeconds("MURMUR_ENDURANCE_SKEW_SECONDS", 120),
	}
	if cfg.maxRows < 100 {
		t.Fatalf("MURMUR_ENDURANCE_MAX_ROWS must be >= 100")
	}
	if cfg.snapshotOff < 60 {
		t.Fatalf("MURMUR_ENDURANCE_SNAPSHOT_OFFLINE_SECONDS must be >= 60 (retention expiry + GC tick)")
	}
	return cfg
}

func TestEnduranceChaos(t *testing.T) {
	cfg := loadConfig(t)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "endurance-chaos",
		NumNodes:    cfg.nodes,
		AwaitUnlock: true,
		// Aggressive retention keeps log GC running under the sustained
		// workload and lets bounded offline windows force real snapshot
		// resyncs instead of log catch-up.
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 20000,
			MinRetainedBatches:       10,
		},
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: tableName,
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
				{Name: "val", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	rep := newReporter(t)
	rep.logf("config: nodes=%d duration=%s settle=%s writeInterval=%s maxRows=%d seed=%d",
		cfg.nodes, cfg.duration, cfg.settle, cfg.writeInterval, cfg.maxRows, cfg.seed)
	rep.logf("fault cadence: restart=%s rotate=%s snapshot=%s/%s impair=%s/%s disk=%s/%s skew=%s",
		cfg.restartEvery, cfg.rotateEvery, cfg.snapshotEvery, cfg.snapshotOff,
		cfg.impairOn, cfg.impairOff, cfg.diskEvery, cfg.diskHold, cfg.skewDur)

	// Baseline: one row everywhere before chaos starts.
	if err := cluster.ExecSQL(0, "INSERT INTO "+tableName+" (id, name, val) VALUES (?, ?, ?)",
		fmt.Sprintf("%032x", 0), "baseline", "baseline"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	if err := waitAgreement(cluster, cfg.nodes, 1, 2*time.Minute); err != nil {
		t.Fatalf("baseline replication: %v", err)
	}
	rep.logf("baseline row converged on all %d nodes", cfg.nodes)

	// --- preflights: detect degraded modes before the long run ---
	replPorts, err := clusterReplPorts(cluster)
	if err != nil {
		t.Fatalf("repl ports: %v", err)
	}
	netMode := "flap"
	if ok, reason := tcCapable(); ok {
		netMode = "tc"
		rep.logf("network impairment: tc/netem on repl ports (latency=%dms loss=%d%%)",
			cfg.impairLatency, cfg.impairLoss)
	} else {
		rep.logf("network impairment: DEGRADED to logical peer flaps (tc unusable: %s)", reason)
	}
	t.Cleanup(tcClear)

	skewSpec := ""
	skewNode := cfg.nodes - 1
	if lib := findFaketimeLib(); lib == "" {
		rep.logf("clock skew: DEGRADED away (no libfaketime found)")
	} else if daemonIsStatic(effectiveTestBinary()) {
		rep.logf("clock skew: DEGRADED away (daemon statically linked; LD_PRELOAD cannot intercept)")
	} else if spec, err := calibrateSkew(t, cluster, rep, skewNode, lib); err != nil {
		t.Fatalf("clock-skew preflight: %v", err)
	} else {
		skewSpec = spec
		rep.logf("clock skew: armed on node%d via FAKETIME=%q", skewNode+1, skewSpec)
	}

	fillerMB := cfg.diskMB
	if free, err := dirFreeBytes(cluster.RuntimeDir); err == nil {
		// Never risk the host filesystem: filler must leave 1 GiB free.
		if headroom := int64(free) - (1 << 30); headroom < int64(fillerMB)<<20 {
			fillerMB = int(headroom / (1 << 20))
			if fillerMB < 16 {
				fillerMB = 0
				rep.logf("disk pressure: DEGRADED away (only %d MiB free)", free>>20)
			} else {
				rep.logf("disk pressure: filler reduced to %d MiB (%d MiB free)", fillerMB, free>>20)
			}
		} else {
			rep.logf("disk pressure: %d MiB filler (%d MiB free)", fillerMB, free>>20)
		}
	}

	snapBase := scrapeAll(t, cluster, "spedsql_repl_snapshots_received_total")

	// --- shared chaos state ---
	ctl := &chaosControl{
		alive:      make([]bool, cfg.nodes),
		curKey:     make([]string, cfg.nodes),
		curKeyID:   make([]string, cfg.nodes),
		gcEver:     make([]bool, cfg.nodes),
		gcCollEver: make([]bool, cfg.nodes),
		gcFailMax:  make([]int64, cfg.nodes),
	}
	for i := range ctl.alive {
		ctl.alive[i] = true
		ctl.curKey[i] = cluster.Nodes[i].KeyHex
		// No explicit ID until the first rotation: the initial
		// unlock registers the registry under the daemon's
		// default ID, and restarts must present the same
		// (empty) identity. rotateNode records real IDs.
		ctl.curKeyID[i] = ""
	}
	registry := newIDRegistry()
	stopCh := make(chan struct{})
	var stopOnce sync.Once
	stopAll := func() { stopOnce.Do(func() { close(stopCh) }) }

	// Writers: bounded working set. Inserts until maxRows live ids, then
	// updates/deletes recycle the set so a 72h run keeps churning
	// replication and GC without unbounded growth or an O(N) verdict.
	var writersWG sync.WaitGroup
	numWriters := min(max(cfg.nodes, 2), 8)
	for w := 0; w < numWriters; w++ {
		writersWG.Add(1)
		go writerLoop(cluster, ctl, registry, rep, cfg, w, stopCh, &writersWG)
	}

	// Background fault cyclers (no daemon lifecycle; safe off-goroutine).
	var bgWG sync.WaitGroup
	bgWG.Add(1)
	go impairLoop(cluster, replPorts, rep, cfg, netMode, stopCh, &bgWG)
	if fillerMB > 0 {
		bgWG.Add(1)
		go diskLoop(cluster, rep, cfg, fillerMB, stopCh, &bgWG)
	}
	bgWG.Add(1)
	go statusLoop(cluster, ctl, rep, cfg, stopCh, &bgWG)

	// --- chaos director (test goroutine: owns all daemon lifecycle) ---
	rep.phase("chaos", fmt.Sprintf("all fault injectors live for %s", cfg.duration))
	directive := runDirector(t, cluster, ctl, rep, cfg, skewNode, skewSpec, stopCh)

	// Faults stop; every node must be up before the verdict window.
	stopAll()
	bgWG.Wait()
	ensureAllUp(t, cluster, ctl, rep)
	rep.logf("writers stopping: attempted=%d acked=%d transient-errors=%d live-ids=%d",
		rep.attempted.Load(), rep.succeeded.Load(), rep.failed.Load(), registry.len())
	writersWG.Wait()

	if rep.succeeded.Load() == 0 {
		rep.verdict(false, cluster, "no writes acked during the entire run")
		t.Fatalf("endurance verdict: 0 of %d writes acked; see report for distinct errors",
			rep.attempted.Load())
	}

	rep.phase("verdict", fmt.Sprintf("waiting up to %s for exact convergence", cfg.settle))
	counts, digests, err := waitConverged(cluster, cfg.nodes, cfg.settle)
	if err != nil {
		cluster.DumpForensics("endurance-verdict")
		rep.verdict(false, cluster, err.Error())
		t.Fatalf("endurance verdict: %v\ncounts=%v\ndigests=%v", err, counts, digests)
	}
	rep.logf("converged: all %d nodes hold %d rows with digest %s", cfg.nodes, counts[0], digests[0])

	// Counter-proven post-checks: every claimed fault must have fired.
	// GC history (not base/after snapshots): per-node counters reset on
	// every restart, so a node revived seconds before the verdict would
	// falsely fail a snapshot comparison — and a restart would wipe
	// failure evidence. The sampler observed throughout the run.
	snapAfter := scrapeAll(t, cluster, "spedsql_repl_snapshots_received_total")
	for i := 0; i < cfg.nodes; i++ {
		ever, collEver, failMax := ctl.gcHistory(i)
		if !ever {
			rep.verdict(false, cluster, fmt.Sprintf("node%d GC never ran", i+1))
			t.Fatalf("node%d GC never observed running during chaos", i+1)
		}
		if !collEver {
			rep.verdict(false, cluster, fmt.Sprintf("node%d GC collected nothing", i+1))
			t.Fatalf("node%d GC never observed collecting during chaos", i+1)
		}
		if failMax != 0 {
			rep.verdict(false, cluster, fmt.Sprintf("node%d GC failures observed", i+1))
			t.Fatalf("node%d gc failures reached %d during chaos", i+1, failMax)
		}
	}
	rep.logf("GC proven on all %d nodes (runs/collect observed, failures flat)", cfg.nodes)

	faults := rep.faultSnapshot()
	if faults.restarts == 0 {
		rep.verdict(false, cluster, "zero restarts executed")
		t.Fatalf("chaos ran zero restarts; restart injector never fired")
	}
	if faults.rotations == 0 {
		rep.verdict(false, cluster, "zero key rotations executed")
		t.Fatalf("chaos ran zero key rotations; rotation injector never fired")
	}
	if faults.impairCycles == 0 {
		rep.verdict(false, cluster, "zero network-impairment cycles executed")
		t.Fatalf("chaos ran zero network-impairment cycles")
	}
	if fillerMB > 0 && faults.diskCycles == 0 {
		rep.verdict(false, cluster, "zero disk-pressure cycles executed")
		t.Fatalf("chaos ran zero disk-pressure cycles")
	}
	if directive.snapshotsPossible {
		if faults.snapshots == 0 {
			rep.verdict(false, cluster, "zero snapshot offline windows executed")
			t.Fatalf("chaos ran zero snapshot offline windows")
		}
		var snapDelta int64
		for i := 0; i < cfg.nodes; i++ {
			snapDelta += snapAfter[i] - snapBase[i]
		}
		if snapDelta < 1 {
			rep.verdict(false, cluster, "no snapshot resync observed")
			t.Fatalf("offline windows ran but no node received a snapshot (log catch-up hid the path?)")
		}
		rep.logf("snapshots proven: %d received across the mesh over %d offline windows",
			snapDelta, faults.snapshots)
	} else {
		rep.logf("snapshots: leg skipped (run too short for an offline window)")
	}
	if directive.skewScheduled && faults.skews == 0 {
		rep.verdict(false, cluster, "skew scheduled but never executed")
		t.Fatalf("clock skew was scheduled but the skew window never ran")
	}

	// Full mesh must be back: every node sees every peer.
	for i := 0; i < cfg.nodes; i++ {
		peers, err := nodeConnectedPeers(cluster.Nodes[i].APIAddr)
		if err != nil || peers != cfg.nodes-1 {
			rep.verdict(false, cluster, "mesh not fully reconnected")
			t.Fatalf("node%d connected peers=%d err=%v, want %d", i+1, peers, err, cfg.nodes-1)
		}
	}

	// A post-chaos write must traverse the healed mesh.
	if err := cluster.ExecSQL(0, "INSERT INTO "+tableName+" (id, name, val) VALUES (?, ?, ?)",
		fmt.Sprintf("%032x", 999_999_999), "post-chaos", "post-chaos"); err != nil {
		rep.verdict(false, cluster, "post-chaos write rejected")
		t.Fatalf("post-chaos write: %v", err)
	}
	if err := waitAgreement(cluster, cfg.nodes, counts[0]+1, cfg.settle); err != nil {
		cluster.DumpForensics("endurance-postwrite")
		rep.verdict(false, cluster, "post-chaos write did not converge")
		t.Fatalf("post-chaos convergence: %v", err)
	}

	var snapDelta int64
	for i := 0; i < cfg.nodes; i++ {
		snapDelta += snapAfter[i] - snapBase[i]
	}
	rep.verdict(true, cluster, fmt.Sprintf(
		"all %d nodes hold %d rows digest %s after %s of combined chaos "+
			"(restarts=%d rotations=%d impair=%s/%d disk=%d snapshots=%d/%d skews=%d)",
		cfg.nodes, counts[0]+1, digests[0], cfg.duration,
		faults.restarts, faults.rotations, netMode, faults.impairCycles,
		faults.diskCycles, faults.snapshots, snapDelta, faults.skews))
}

// --- chaos director: all daemon lifecycle runs here, on the test goroutine ---

type chaosControl struct {
	mu       sync.Mutex
	alive    []bool
	curKey   []string
	curKeyID []string
	// Restart-proof GC history: per-node counters reset on every
	// restart, so the sampler records the high-water observations and
	// the verdict asserts on those instead of base/after snapshots.
	gcEver     []bool
	gcCollEver []bool
	gcFailMax  []int64
}

func (c *chaosControl) observeGC(idx int, runs, collected, failures int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if runs > 0 {
		c.gcEver[idx] = true
	}
	if collected > 0 {
		c.gcCollEver[idx] = true
	}
	if failures > c.gcFailMax[idx] {
		c.gcFailMax[idx] = failures
	}
}

func (c *chaosControl) gcHistory(idx int) (ever, collEver bool, failMax int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gcEver[idx], c.gcCollEver[idx], c.gcFailMax[idx]
}

func (c *chaosControl) setAlive(idx int, up bool) {
	c.mu.Lock()
	c.alive[idx] = up
	c.mu.Unlock()
}

func (c *chaosControl) pickAlive(rng *mrand.Rand) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var up []int
	for i, ok := range c.alive {
		if ok {
			up = append(up, i)
		}
	}
	if len(up) == 0 {
		return -1
	}
	return up[rng.Intn(len(up))]
}

func (c *chaosControl) currentKey(idx int) (id, hex string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curKeyID[idx], c.curKey[idx]
}

func (c *chaosControl) setKey(idx int, id, hex string) {
	c.mu.Lock()
	c.curKeyID[idx], c.curKey[idx] = id, hex
	c.mu.Unlock()
}

// directorReport tells the verdict which scheduled legs actually fit in
// the run, so short dev passes skip legs loudly instead of failing on
// coverage they could never have produced.
type directorReport struct {
	snapshotsPossible bool
	skewScheduled     bool
}

func runDirector(t *testing.T, cluster *harness.Cluster, ctl *chaosControl, rep *reporter, cfg enduranceConfig, skewNode int, skewSpec string, stopCh <-chan struct{}) directorReport {
	t.Helper()
	rng := mrand.New(mrand.NewSource(cfg.seed*7919 + 17))
	start := time.Now()
	deadline := start.Add(cfg.duration)

	nextRestart := start.Add(cfg.restartEvery / 2)
	nextRotate := start.Add(cfg.rotateEvery / 2)
	rotateIdx := 0
	killNext := false

	// Snapshot windows need a full offline window plus reconvergence
	// room before the verdict; short runs skip the leg loudly.
	snapOK := true
	nextSnapStop := start.Add(cfg.duration / 4)
	if nextSnapStop.Add(cfg.snapshotOff + 60*time.Second).After(deadline) {
		nextSnapStop = start.Add(30 * time.Second)
		if nextSnapStop.Add(cfg.snapshotOff + 60*time.Second).After(deadline) {
			snapOK = false
			rep.logf("snapshot: run too short for an offline window, leg skipped")
		}
	}
	snapVictim := -1
	var snapRestartAt time.Time

	// One clock-skew window mid-run, fully inside the chaos period.
	skewOK := skewSpec != ""
	skewStart := start.Add(cfg.duration / 3)
	skewEnd := skewStart.Add(cfg.skewDur)
	if skewEnd.After(deadline.Add(-90 * time.Second)) {
		skewEnd = deadline.Add(-90 * time.Second)
	}
	skewActive := false
	if skewOK && skewEnd.Sub(skewStart) < 30*time.Second {
		skewOK = false
		rep.logf("clock skew: window too short for this duration, skipped")
	}

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		now := time.Now()
		if !now.Before(deadline) {
			break
		}
		select {
		case <-stopCh:
			return directorReport{snapshotsPossible: snapOK, skewScheduled: skewOK}
		case <-tick.C:
			now = time.Now()
		}

		if !now.Before(nextRestart) {
			// Never restart the skewed node out from under the skew
			// window or the offline snapshot victim.
			idx := pickRestartVictim(rng, cfg.nodes, skewNode, skewActive, snapVictim)
			down := cfg.restartDown
			rep.logf("restart: node%d going down (%s, %s downtime)",
				idx+1, map[bool]string{true: "SIGKILL", false: "graceful"}[killNext], down)
			ctl.setAlive(idx, false)
			if killNext {
				cluster.KillNode(idx)
			} else {
				cluster.StopNode(idx)
			}
			killNext = !killNext
			time.Sleep(down)
			keyID, keyHex := ctl.currentKey(idx)
			cluster.StartNode(idx)
			cluster.UnlockNodeWithKeyID(idx, keyID, keyHex)
			cluster.WaitNodeReady(idx)
			ctl.setAlive(idx, true)
			rep.countFault("restart")
			rep.logf("restart: node%d back", idx+1)
			nextRestart = time.Now().Add(cfg.restartEvery + time.Duration(rng.Int63n(int64(cfg.restartEvery)/2)))
		}

		if !now.Before(nextRotate) {
			idx := -1
			for k := 0; k < cfg.nodes; k++ {
				cand := (rotateIdx + k) % cfg.nodes
				ctl.mu.Lock()
				up := ctl.alive[cand]
				ctl.mu.Unlock()
				if up && cand != snapVictim {
					idx = cand
					break
				}
			}
			rotateIdx = (rotateIdx + 1) % cfg.nodes
			if idx < 0 {
				rep.logf("rotate: skipped, no node available")
			} else if err := rotateNode(cluster, ctl, idx, rep); err != nil {
				rep.logf("rotate: node%d FAILED: %v (counted, chaos continues)", idx+1, err)
			}
			nextRotate = time.Now().Add(cfg.rotateEvery)
		}

		if snapOK && snapVictim < 0 && !now.Before(nextSnapStop) {
			// Victim must not be the skewed node: skew needs a live
			// writer and the snapshot victim needs to be down.
			v := rng.Intn(cfg.nodes)
			if v == skewNode {
				v = (v + 1) % cfg.nodes
			}
			snapVictim = v
			rep.logf("snapshot: node%d going offline for %s (ranges must expire past retention)",
				v+1, cfg.snapshotOff)
			ctl.setAlive(v, false)
			cluster.StopNode(v)
			snapRestartAt = time.Now().Add(cfg.snapshotOff)
		}
		if snapVictim >= 0 && !now.Before(snapRestartAt) {
			v := snapVictim
			keyID, keyHex := ctl.currentKey(v)
			cluster.StartNode(v)
			cluster.UnlockNodeWithKeyID(v, keyID, keyHex)
			cluster.WaitNodeReady(v)
			ctl.setAlive(v, true)
			snapVictim = -1
			rep.countFault("snapshot")
			rep.logf("snapshot: node%d rejoined, must resync via snapshot", v+1)
			nextSnapStop = time.Now().Add(cfg.snapshotEvery)
		}

		if skewOK && !skewActive && !now.Before(skewStart) {
			rep.logf("skew: node%d clock jumping +5min for %s", skewNode+1, skewEnd.Sub(now).Round(time.Second))
			ctl.setAlive(skewNode, false)
			cluster.StopNode(skewNode)
			if err := startNodeWithEnv(cluster, skewNode, []string{
				"LD_PRELOAD=" + findFaketimeLib(),
				"FAKETIME=" + skewSpec,
				"FAKETIME_NO_CACHE=1",
			}); err != nil {
				t.Fatalf("skew start: %v", err)
			}
			keyID, keyHex := ctl.currentKey(skewNode)
			cluster.UnlockNodeWithKeyID(skewNode, keyID, keyHex)
			cluster.WaitNodeReady(skewNode)
			ctl.setAlive(skewNode, true)
			skewActive = true
			if hlc, err := nodeHLC(cluster.Nodes[skewNode].APIAddr); err == nil {
				rep.logf("skew: node%d live under faketime (HLC wall %d)", skewNode+1, int64(hlc>>16))
			}
		}
		if skewOK && skewActive && !now.Before(skewEnd) {
			rep.logf("skew: node%d clock restored", skewNode+1)
			ctl.setAlive(skewNode, false)
			cluster.StopNode(skewNode)
			keyID, keyHex := ctl.currentKey(skewNode)
			cluster.StartNode(skewNode)
			cluster.UnlockNodeWithKeyID(skewNode, keyID, keyHex)
			cluster.WaitNodeReady(skewNode)
			ctl.setAlive(skewNode, true)
			skewActive = false
			skewOK = false // one window per run
			rep.countFault("skew")
		}
	}

	// Rejoin anyone still down when the chaos window closes.
	if snapVictim >= 0 {
		keyID, keyHex := ctl.currentKey(snapVictim)
		cluster.StartNode(snapVictim)
		cluster.UnlockNodeWithKeyID(snapVictim, keyID, keyHex)
		cluster.WaitNodeReady(snapVictim)
		ctl.setAlive(snapVictim, true)
		rep.countFault("snapshot")
		rep.logf("snapshot: node%d rejoined at chaos end", snapVictim+1)
	}
	if skewActive {
		ctl.setAlive(skewNode, false)
		cluster.StopNode(skewNode)
		keyID, keyHex := ctl.currentKey(skewNode)
		cluster.StartNode(skewNode)
		cluster.UnlockNodeWithKeyID(skewNode, keyID, keyHex)
		cluster.WaitNodeReady(skewNode)
		ctl.setAlive(skewNode, true)
		rep.countFault("skew")
		rep.logf("skew: node%d clock restored at chaos end", skewNode+1)
	}
	return directorReport{snapshotsPossible: snapOK, skewScheduled: skewOK}
}

func pickRestartVictim(rng *mrand.Rand, nodes, skewNode int, skewActive bool, snapVictim int) int {
	for tries := 0; tries < 16; tries++ {
		v := rng.Intn(nodes)
		if v == snapVictim {
			continue
		}
		if skewActive && v == skewNode {
			continue
		}
		return v
	}
	return rng.Intn(nodes)
}

func rotateNode(cluster *harness.Cluster, ctl *chaosControl, idx int, rep *reporter) error {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	n := rep.nextRotation()
	keyID := fmt.Sprintf("endurance-k%d", n)
	keyHex := hex.EncodeToString(raw)
	if err := cluster.RotateKey(idx, keyID, keyHex, string(db.AES256GCM)); err != nil {
		return err
	}
	st, err := cluster.EncryptionStatus(idx)
	if err != nil {
		return err
	}
	if got, _ := st["ApplicationKeyID"].(string); got != keyID {
		return fmt.Errorf("active key id = %q, want %q", got, keyID)
	}
	if err := rewriteKeyConfig(cluster.Nodes[idx].ConfigFile, keyID, keyHex); err != nil {
		return err
	}
	ctl.setKey(idx, keyID, keyHex)
	rep.countFault("rotate")
	rep.logf("rotate: node%d now on %s", idx+1, keyID)
	return nil
}

// ensureAllUp restarts any node the director left down (a restart racing
// chaos end) so the verdict window starts with a full mesh.
func ensureAllUp(t *testing.T, cluster *harness.Cluster, ctl *chaosControl, rep *reporter) {
	t.Helper()
	for i := range cluster.Nodes {
		ctl.mu.Lock()
		up := ctl.alive[i]
		ctl.mu.Unlock()
		if up {
			continue
		}
		rep.logf("cleanup: node%d still down at chaos end, restarting", i+1)
		keyID, keyHex := ctl.currentKey(i)
		cluster.StartNode(i)
		cluster.UnlockNodeWithKeyID(i, keyID, keyHex)
		cluster.WaitNodeReady(i)
		ctl.setAlive(i, true)
	}
}

// calibrateSkew finds a working FAKETIME spec for a +5 minute offset on idx
// and leaves the node running normally. It fails fast: a present-but-broken
// libfaketime must never silently void skew coverage for a 72h run.
func calibrateSkew(t *testing.T, cluster *harness.Cluster, rep *reporter, idx int, lib string) (string, error) {
	t.Helper()
	hlcPre, err := nodeHLC(cluster.Nodes[idx].APIAddr)
	if err != nil {
		return "", fmt.Errorf("pre-skew HLC read: %v", err)
	}
	preWall := int64(hlcPre >> 16)
	for i, spec := range []string{"+0,0,0,0,5,0", "+5m", "+300s", "+300"} {
		cluster.StopNode(idx)
		if err := startNodeWithEnv(cluster, idx, []string{
			"LD_PRELOAD=" + lib,
			"FAKETIME=" + spec,
			"FAKETIME_NO_CACHE=1",
		}); err != nil {
			return "", err
		}
		cluster.UnlockNode(idx, cluster.Nodes[idx].KeyHex)
		cluster.WaitNodeReady(idx)
		probeID := fmt.Sprintf("%032x", 7000+i)
		if err := cluster.ExecSQL(idx, "INSERT INTO "+tableName+" (id, name, val) VALUES (?, ?, ?)",
			probeID, "skew-probe", spec); err != nil {
			rep.logf("skew preflight: spec %q probe write failed: %v", spec, err)
			continue
		}
		hlc, err := nodeHLC(cluster.Nodes[idx].APIAddr)
		if err != nil {
			rep.logf("skew preflight: spec %q HLC read failed: %v", spec, err)
			continue
		}
		delta := int64(hlc>>16) - preWall
		rep.logf("skew preflight: spec %q HLC wall delta %+ds", spec, delta)
		if delta >= 240 && delta <= 360 {
			cluster.StopNode(idx)
			cluster.StartNode(idx)
			cluster.UnlockNode(idx, cluster.Nodes[idx].KeyHex)
			cluster.WaitNodeReady(idx)
			return spec, nil
		}
	}
	return "", fmt.Errorf("libfaketime at %s present but no FAKETIME syntax skewed node%d", lib, idx+1)
}

// --- writers: bounded insert/update/delete mix ---

type idRegistry struct {
	mu   sync.Mutex
	ids  []string
	live map[string]struct{}
}

func newIDRegistry() *idRegistry {
	return &idRegistry{live: make(map[string]struct{})}
}

func (r *idRegistry) add(id string) {
	r.mu.Lock()
	r.ids = append(r.ids, id)
	r.live[id] = struct{}{}
	r.mu.Unlock()
}

func (r *idRegistry) remove(id string) {
	r.mu.Lock()
	delete(r.live, id)
	r.mu.Unlock()
}

func (r *idRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.live)
}

// pick returns a random live id, tolerating entries deleted since append.
func (r *idRegistry) pick(rng *mrand.Rand) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.live) == 0 || len(r.ids) == 0 {
		return "", false
	}
	for k := 0; k < 8; k++ {
		id := r.ids[rng.Intn(len(r.ids))]
		if _, ok := r.live[id]; ok {
			return id, true
		}
	}
	return "", false
}

func writerLoop(cluster *harness.Cluster, ctl *chaosControl, reg *idRegistry, rep *reporter, cfg enduranceConfig, w int, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := mrand.New(mrand.NewSource(cfg.seed*1000 + int64(w)))
	var seq uint64
	for {
		select {
		case <-stop:
			return
		default:
		}
		target := ctl.pickAlive(rng)
		if target < 0 {
			if !sleepInterruptible(cfg.writeInterval, stop) {
				return
			}
			continue
		}
		// Bounded set: inserts until maxRows, then recycle via
		// updates and deletes so pressure never stops growing state.
		roll := rng.Float64()
		var err error
		switch {
		case reg.len() < cfg.maxRows && roll < 0.7:
			seq++
			id := fmt.Sprintf("%08x%024x", w, seq)
			err = cluster.ExecSQL(target, "INSERT INTO "+tableName+" (id, name, val) VALUES (?, ?, ?)",
				id, fmt.Sprintf("writer-%d", w), fmt.Sprintf("v-%d-%d", w, seq))
			if err == nil {
				reg.add(id)
			}
		case roll < 0.85:
			id, ok := reg.pick(rng)
			if !ok {
				continue
			}
			seq++
			err = cluster.ExecSQL(target, "UPDATE "+tableName+" SET name = ?, val = ? WHERE id = ?",
				fmt.Sprintf("writer-%d", w), fmt.Sprintf("u-%d-%d", w, seq), id)
		default:
			id, ok := reg.pick(rng)
			if !ok {
				continue
			}
			err = cluster.ExecSQL(target, "DELETE FROM "+tableName+" WHERE id = ?", id)
			if err == nil {
				reg.remove(id)
			}
		}
		rep.countAttempt(err == nil)
		if err != nil {
			rep.sampleErr(err)
		}
		if !sleepInterruptible(cfg.writeInterval, stop) {
			return
		}
	}
}

// --- background impairment cyclers (no daemon lifecycle) ---

func impairLoop(cluster *harness.Cluster, replPorts []int, rep *reporter, cfg enduranceConfig, mode string, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := mrand.New(mrand.NewSource(cfg.seed*31 + 7))
	phases := []struct {
		name  string
		child string
		args  []string
	}{
		{"latency", "netem", []string{"delay", fmt.Sprintf("%dms", cfg.impairLatency)}},
		{"loss", "netem", []string{"loss", fmt.Sprintf("%d%%", cfg.impairLoss)}},
		{"latency+loss", "netem", []string{"delay", fmt.Sprintf("%dms", cfg.impairLatency), "loss", fmt.Sprintf("%d%%", cfg.impairLoss)}},
	}
	phase := 0
	// Stagger start so the first impairment lands after writers warm up.
	if !sleepInterruptible(15*time.Second, stop) {
		return
	}
	for {
		p := phases[phase%len(phases)]
		phase++
		if mode == "tc" {
			if err := tcApply(replPorts, p.child, p.args...); err != nil {
				rep.logf("impair: tc %s FAILED: %v", p.name, err)
			} else {
				rep.logf("impair: tc %s on repl ports for %s", p.name, cfg.impairOn)
			}
			if !sleepInterruptible(cfg.impairOn, stop) {
				tcClear()
				return
			}
			tcClear()
		} else {
			// Privilege-free fallback: flap one random edge both ways.
			a := rng.Intn(len(cluster.Nodes))
			b := (a + 1 + rng.Intn(len(cluster.Nodes)-1)) % len(cluster.Nodes)
			_ = cluster.RemovePeer(a, b)
			_ = cluster.RemovePeer(b, a)
			rep.logf("impair: flap %s <-> %s for %s (no tc privilege)",
				cluster.Nodes[a].Label, cluster.Nodes[b].Label, cfg.impairOn)
			interrupted := !sleepInterruptible(cfg.impairOn, stop)
			_ = cluster.AddPeer(a, b)
			_ = cluster.AddPeer(b, a)
			if interrupted {
				return
			}
		}
		rep.countFault("impair")
		if !sleepInterruptible(cfg.impairOff, stop) {
			return
		}
	}
}

func diskLoop(cluster *harness.Cluster, rep *reporter, cfg enduranceConfig, fillerMB int, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	idx := 0
	if !sleepInterruptible(30*time.Second, stop) {
		return
	}
	for {
		node := cluster.Nodes[idx%len(cluster.Nodes)]
		idx++
		path := node.Dir + "/endurance-filler.bin"
		freeBefore, _ := dirFreeBytes(node.Dir)
		if err := writeFiller(path, fillerMB); err != nil {
			rep.logf("disk: node%d filler FAILED: %v", node.Index+1, err)
			removeFiller(path)
		} else {
			freeAfter, _ := dirFreeBytes(node.Dir)
			rep.logf("disk: node%d holding %d MiB (free %d -> %d MiB) for %s",
				node.Index+1, fillerMB, freeBefore>>20, freeAfter>>20, cfg.diskHold)
			interrupted := !sleepInterruptible(cfg.diskHold, stop)
			removeFiller(path)
			rep.countFault("disk")
			if interrupted {
				return
			}
		}
		// Wait out the remainder of the period after the hold.
		if wait := cfg.diskEvery - cfg.diskHold; wait > 0 {
			if !sleepInterruptible(wait, stop) {
				return
			}
		}
	}
}

func statusLoop(cluster *harness.Cluster, ctl *chaosControl, rep *reporter, cfg enduranceConfig, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(cfg.statusEvery)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			min, max, down := -1, -1, 0
			for i := range cluster.Nodes {
				c, err := cluster.QueryRowCount(i, tableName)
				if err != nil {
					down++
					continue
				}
				if min < 0 || c < min {
					min = c
				}
				if c > max {
					max = c
				}
			}
			var pebbleMax, gcRuns, snaps int64
			var diskMin uint64
			for i := range cluster.Nodes {
				if body, err := scrapeMetrics(cluster.Nodes[i].APIAddr); err == nil {
					if v, ok := metricInt(body, "spedsql_pebble_size_bytes"); ok && v > pebbleMax {
						pebbleMax = v
					}
					runs, _ := metricInt(body, "spedsql_gc_runs_total")
					coll, _ := metricInt(body, "spedsql_gc_log_collected_total")
					fails, _ := metricInt(body, "spedsql_gc_failures_total")
					gcRuns += runs
					ctl.observeGC(i, runs, coll, fails)
					if v, ok := metricInt(body, "spedsql_repl_snapshots_received_total"); ok {
						snaps += v
					}
				}
				if free, err := dirFreeBytes(cluster.Nodes[i].Dir); err == nil {
					if diskMin == 0 || free < diskMin {
						diskMin = free
					}
				}
			}
			f := rep.faultSnapshot()
			rep.logf("status: writes ok=%d err=%d | rows min=%d max=%d down=%d/%d | "+
				"restart=%d rotate=%d impair=%d disk=%d snap=%d skew=%d | gcRuns=%d snaps=%d pebbleMax=%dMiB diskFreeMin=%dMiB",
				rep.succeeded.Load(), rep.failed.Load(), min, max, down, cfg.nodes,
				f.restarts, f.rotations, f.impairCycles, f.diskCycles, f.snapshots, f.skews,
				gcRuns, snaps, pebbleMax>>20, diskMin>>20)
		}
	}
}

// --- convergence waits ---

func waitAgreement(cluster *harness.Cluster, nodes, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := 0; i < nodes; i++ {
			n, err := cluster.QueryRowCount(i, tableName)
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := cluster.ComputeTableDigest(i, tableName, "id")
			if err != nil {
				ok = false
				break
			}
			if i == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("row agreement on %d rows not reached within %s", want, timeout)
}

// waitConverged polls until every node is reachable with equal row counts
// and identical digests. Counts are compared to each other, not to a fixed
// target, because deletes move the total during chaos.
func waitConverged(cluster *harness.Cluster, nodes int, timeout time.Duration) (counts []int, digests []string, err error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		counts = make([]int, nodes)
		digests = make([]string, nodes)
		ok := true
		for i := 0; i < nodes; i++ {
			n, qerr := cluster.QueryRowCount(i, tableName)
			d, derr := cluster.ComputeTableDigest(i, tableName, "id")
			if qerr != nil || derr != nil {
				ok = false
				break
			}
			counts[i], digests[i] = n, d
			if n != counts[0] || d != digests[0] {
				ok = false
			}
		}
		if ok {
			return counts, digests, nil
		}
		time.Sleep(2 * time.Second)
	}
	counts = make([]int, nodes)
	digests = make([]string, nodes)
	for i := 0; i < nodes; i++ {
		n, qerr := cluster.QueryRowCount(i, tableName)
		d, derr := cluster.ComputeTableDigest(i, tableName, "id")
		if qerr != nil {
			n = -1
		}
		if derr != nil {
			d = "unreachable:" + derr.Error()
		}
		counts[i], digests[i] = n, d
	}
	return counts, digests, fmt.Errorf("nodes did not converge within %s", timeout)
}

func scrapeAll(t *testing.T, cluster *harness.Cluster, name string) []int64 {
	t.Helper()
	out := make([]int64, len(cluster.Nodes))
	for i := range cluster.Nodes {
		v, err := nodeMetric(cluster.Nodes[i].APIAddr, name)
		if err != nil {
			t.Fatalf("node%d %s: %v", i+1, name, err)
		}
		out[i] = v
	}
	return out
}

// --- reporter: live progress plus a final markdown report ---

type faultCounts struct {
	restarts     int64
	rotations    int64
	impairCycles int64
	diskCycles   int64
	snapshots    int64
	skews        int64
}

type reporter struct {
	t     *testing.T
	start time.Time

	mu     sync.Mutex
	events []string

	attempted atomic.Int64
	succeeded atomic.Int64
	failed    atomic.Int64

	faultMu sync.Mutex
	faults  faultCounts
	rotSeq  int64

	errMu     sync.Mutex
	errSeen   map[string]int
	errSample []string
}

func newReporter(t *testing.T) *reporter {
	t.Helper()
	return &reporter{t: t, start: time.Now(), errSeen: map[string]int{}}
}

func (r *reporter) elapsed() time.Duration {
	return time.Since(r.start).Round(time.Second)
}

func (r *reporter) phase(name, detail string) {
	r.t.Helper()
	msg := fmt.Sprintf("[T+%s] === PHASE: %s === %s", r.elapsed(), name, detail)
	r.t.Log(msg)
	r.mu.Lock()
	r.events = append(r.events, msg)
	r.mu.Unlock()
}

func (r *reporter) logf(format string, args ...any) {
	r.t.Helper()
	msg := fmt.Sprintf("[T+%s] %s", r.elapsed(), fmt.Sprintf(format, args...))
	r.t.Log(msg)
	r.mu.Lock()
	r.events = append(r.events, msg)
	r.mu.Unlock()
}

func (r *reporter) countAttempt(ok bool) {
	r.attempted.Add(1)
	if ok {
		r.succeeded.Add(1)
	} else {
		r.failed.Add(1)
	}
}

func (r *reporter) countFault(kind string) {
	r.faultMu.Lock()
	defer r.faultMu.Unlock()
	switch kind {
	case "restart":
		r.faults.restarts++
	case "rotate":
		r.faults.rotations++
	case "impair":
		r.faults.impairCycles++
	case "disk":
		r.faults.diskCycles++
	case "snapshot":
		r.faults.snapshots++
	case "skew":
		r.faults.skews++
	}
}

func (r *reporter) nextRotation() int64 {
	r.faultMu.Lock()
	defer r.faultMu.Unlock()
	r.rotSeq++
	return r.rotSeq
}

func (r *reporter) faultSnapshot() faultCounts {
	r.faultMu.Lock()
	defer r.faultMu.Unlock()
	return r.faults
}

func (r *reporter) sampleErr(err error) {
	msg := err.Error()
	if len(msg) > 200 {
		msg = msg[:200]
	}
	r.errMu.Lock()
	defer r.errMu.Unlock()
	r.errSeen[msg]++
	if len(r.errSample) < 8 {
		for _, s := range r.errSample {
			if s == msg {
				return
			}
		}
		r.errSample = append(r.errSample, msg)
	}
}

func (r *reporter) verdict(pass bool, cluster *harness.Cluster, summary string) string {
	r.t.Helper()
	status := "PASS"
	if !pass {
		status = "FAIL"
		cluster.MarkFailed(summary)
	}
	msg := fmt.Sprintf("[T+%s] === VERDICT: %s === %s", r.elapsed(), status, summary)
	r.t.Log(msg)
	r.mu.Lock()
	r.events = append(r.events, msg)
	events := append([]string(nil), r.events...)
	r.mu.Unlock()
	r.errMu.Lock()
	errSample := append([]string(nil), r.errSample...)
	errSeen := make(map[string]int, len(r.errSeen))
	for k, v := range r.errSeen {
		errSeen[k] = v
	}
	r.errMu.Unlock()
	f := r.faultSnapshot()

	path := fmt.Sprintf("endurance-report-%d.md", time.Now().Unix())
	var b []byte
	b = append(b, fmt.Sprintf("# Endurance-chaos run report — %s\n\n", status)...)
	b = append(b, fmt.Sprintf("- finished: %s\n- wall time: %s\n- writes attempted: %d\n- writes acked: %d\n- writes failed (transient): %d\n"+
		"- faults: restarts=%d rotations=%d impairCycles=%d diskCycles=%d snapshots=%d skews=%d\n- verdict: %s\n\n",
		time.Now().UTC().Format(time.RFC3339), r.elapsed(),
		r.attempted.Load(), r.succeeded.Load(), r.failed.Load(),
		f.restarts, f.rotations, f.impairCycles, f.diskCycles, f.snapshots, f.skews, summary)...)
	if len(errSample) > 0 {
		b = append(b, "## Distinct write errors\n\n"...)
		for _, s := range errSample {
			b = append(b, fmt.Sprintf("- (%dx) %s\n", errSeen[s], s)...)
		}
		b = append(b, "\n"...)
	}
	b = append(b, "## Timeline\n\n"...)
	for _, e := range events {
		b = append(b, ("- " + e + "\n")...)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		r.t.Logf("report write failed: %v", err)
		return ""
	}
	r.t.Logf("report written to %s", path)
	return path
}
