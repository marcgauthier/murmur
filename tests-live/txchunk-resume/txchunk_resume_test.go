// Chunked-transaction resume acceptance: a single transaction larger
// than the 8 MiB replication frame travels as ~64 KiB transaction
// chunks (replication.sendBatchOrChunks). The receiver is SIGKILLed
// mid-transfer (coordinated via its inbound frame-bytes metric) and
// restarted; the retry must complete with all-or-nothing visibility,
// the applied watermark must never advance before completion, and all
// digests must converge without any snapshot transfer.
package txchunkresume_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

const (
	sender  = 0
	victim  = 1
	witness = 2
)

func TestChunkedTransactionResumesAfterReceiverSIGKILL(t *testing.T) {
	rows := envInt("MURMUR_TXCHUNK_ROWS", 1200)
	valueBytes := envInt("MURMUR_TXCHUNK_VALUE_BYTES", 9000)
	maxAttempts := envInt("MURMUR_TXCHUNK_ATTEMPTS", 3)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "txchunk-resume",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "chunk_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "v", Type: schema.ColText, Nullable: true},
			},
		}}},
	})
	senderID := cluster.Nodes[sender].NodeID.String()
	victimID := cluster.Nodes[victim].NodeID.String()

	// Positive control (honest behavior works): a multi-chunk
	// transaction replicates to every node with no killing.
	baseStmt := buildBigInsert("base-", 0, rows, valueBytes)
	payloadMB := float64(len(baseStmt)) / (1 << 20)
	t.Logf("big-tx statement %.1f MiB (%d rows x %d B values)", payloadMB, rows, valueBytes)
	if len(baseStmt) <= 8<<20 {
		t.Fatalf("big-tx statement is %d bytes, want > 8 MiB so the batch must travel chunked", len(baseStmt))
	}
	if err := cluster.ExecSQL(sender, baseStmt); err != nil {
		t.Fatalf("base big-tx commit: %v", err)
	}
	waitConverged(t, cluster, "chunk_rows", rows, 120*time.Second)
	t.Logf("positive control: %.1f MiB transaction replicated to all nodes", payloadMB)

	// Resume trials. Each attempt commits a fresh big transaction while
	// the victim is partitioned, heals, SIGKILLs the victim once batch
	// bytes are flowing, restarts it, and watches the retry.
	expectedRows := rows
	resumed := false
	for attempt := 1; attempt <= maxAttempts && !resumed; attempt++ {
		prefix := fmt.Sprintf("a%d-", attempt)
		t.Logf("resume attempt %d/%d", attempt, maxAttempts)

		// Quiet baseline BEFORE partitioning (RemovePeer drops the
		// peer record, so the sender-side watermark is only
		// observable while peered): the victim must have acked
		// everything the sender has before the trial starts.
		seqPre := nodeStatus(t, cluster.Nodes[sender].APIAddr).LocalSeq
		waitHave(t, cluster, sender, victimID, senderID, seqPre, 15*time.Second)

		partition(t, cluster, victim)
		waitConnectedPeers(t, cluster, victim, 0, 15*time.Second)
		snapRecvPreKill := metricValue(t, cluster.Nodes[victim].APIAddr, "spedsql_repl_snapshots_received_total")
		if snapRecvPreKill != 0 {
			t.Fatalf("victim snapshots_received = %v before trial, want 0", snapRecvPreKill)
		}

		stmt := buildBigInsert(prefix, attempt*1000000, rows, valueBytes)
		if err := cluster.ExecSQL(sender, stmt); err != nil {
			t.Fatalf("attempt %d big-tx commit: %v", attempt, err)
		}
		seqTx := nodeStatus(t, cluster.Nodes[sender].APIAddr).LocalSeq
		if seqTx <= seqPre {
			t.Fatalf("attempt %d: sender local_seq did not advance (%d -> %d)", attempt, seqPre, seqTx)
		}
		expectedRows += rows

		// Survivors converge on the trial rows; the victim must miss them.
		waitPrefixCount(t, cluster, sender, prefix, rows, 60*time.Second)
		waitPrefixCount(t, cluster, witness, prefix, rows, 60*time.Second)
		if n := prefixCount(t, cluster, victim, prefix); n != 0 {
			t.Fatalf("attempt %d: victim sees %d trial rows while partitioned, want 0", attempt, n)
		}

		// Heal, then SIGKILL once batch bytes are flowing to the
		// victim (256 KiB proves chunk frames are in flight: session
		// handshake/ack traffic is orders of magnitude smaller).
		bytesBefore := metricValue(t, cluster.Nodes[victim].APIAddr, "spedsql_repl_frame_bytes_received_total")
		heal(t, cluster, victim)
		if tooSlow := killOnceBytesFlow(t, cluster, victim, prefix, rows, bytesBefore); tooSlow {
			t.Logf("attempt %d: victim converged before the kill landed; retrying with fresh rows", attempt)
			waitConverged(t, cluster, "chunk_rows", expectedRows, 60*time.Second)
			continue
		}
		restartNode(t, cluster, victim)

		batchesPost := watchRetry(t, cluster, senderID, victimID, prefix, rows, seqPre, seqTx)
		t.Logf("attempt %d: victim applied %d batches after restart", attempt, batchesPost)
		if batchesPost < 1 {
			// The victim applied nothing after restarting, so it
			// must have applied the trial before the kill: the
			// trial proves nothing about resume; retry fresh.
			t.Logf("attempt %d: no post-restart apply; kill landed too late, retrying", attempt)
			waitConverged(t, cluster, "chunk_rows", expectedRows, 60*time.Second)
			continue
		}
		resumed = true

		// The retry must not have used the snapshot path anywhere.
		if got := metricValue(t, cluster.Nodes[victim].APIAddr, "spedsql_repl_snapshots_received_total"); got != 0 {
			t.Fatalf("victim snapshots_received = %v, want 0 (resume must use chunks/log, not snapshot)", got)
		}
		for i := range cluster.Nodes {
			if got := metricValue(t, cluster.Nodes[i].APIAddr, "spedsql_repl_snapshots_sent_total"); got != 0 {
				t.Fatalf("node %d snapshots_sent = %v, want 0", i, got)
			}
		}
	}
	if !resumed {
		t.Fatalf("no valid resume trial in %d attempts (kill never landed before convergence)", maxAttempts)
	}

	// Final: exact row counts and identical ordered digests everywhere.
	waitConverged(t, cluster, "chunk_rows", expectedRows, 60*time.Second)
}

// partition isolates idx from both peers in both directions.
func partition(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	for peer := range c.Nodes {
		if peer == idx {
			continue
		}
		if err := c.RemovePeer(idx, peer); err != nil {
			t.Fatalf("remove peer %d->%d: %v", idx, peer, err)
		}
		if err := c.RemovePeer(peer, idx); err != nil {
			t.Fatalf("remove peer %d->%d: %v", peer, idx, err)
		}
	}
}

func heal(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	for peer := range c.Nodes {
		if peer == idx {
			continue
		}
		if err := c.AddPeer(idx, peer); err != nil {
			t.Fatalf("add peer %d->%d: %v", idx, peer, err)
		}
		if err := c.AddPeer(peer, idx); err != nil {
			t.Fatalf("add peer %d->%d: %v", peer, idx, err)
		}
	}
}

// killOnceBytesFlow SIGKILLs the victim once inbound session bytes prove
// the trial transfer is underway. It reports true when the victim
// converged first (the kill would prove nothing; the caller retries).
func killOnceBytesFlow(t *testing.T, c *harness.Cluster, idx int, prefix string, rows int, bytesBefore float64) (tooSlow bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if n := prefixCount(t, c, idx, prefix); n == rows {
			return true
		}
		if got := metricValue(t, c.Nodes[idx].APIAddr, "spedsql_repl_frame_bytes_received_total"); got-bytesBefore >= 262144 {
			t.Logf("killing victim mid-transfer after %.0f KiB in flight", (got-bytesBefore)/1024)
			c.KillNode(idx)
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	// No bytes flowed at all: fail loudly rather than killing an idle
	// node and claiming a mid-transfer resume.
	n := prefixCount(t, c, idx, prefix)
	t.Fatalf("victim inbound bytes never advanced within 15s (trial rows visible: %d/%d)", n, rows)
	return false
}

func restartNode(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	c.StartNode(idx)
	c.UnlockNode(idx, c.Nodes[idx].KeyHex)
	c.WaitNodeReady(idx)
}

// watchRetry polls the retry to completion and enforces the resume
// contract on every sample: the sender always shows the full trial
// (its commit was durable) and the victim shows none or all of it
// (never a partial transaction; remote rows materialize into SQLite
// in one transaction per flush, so visibility jumps 0 -> all).
//
// The victim's applied watermark for the trial origin (as acked to
// the sender) advances on the durable Pebble apply, which leads SQL
// visibility by up to one RemoteApplyInterval (1s default): the
// watermark join is therefore lag-bounded rather than per-sample.
// A watermark that advanced before the full batch applied would
// expose partial rows (caught above) or stall completion; here the
// first advanced-watermark sample must precede full visibility by at
// most the materialization bound. It returns the victim's
// post-restart applied-batch count.
func watchRetry(t *testing.T, c *harness.Cluster, senderID, victimID, prefix string, rows int, seqPre, seqTx uint64) int64 {
	t.Helper()
	const materializationBound = 10 * time.Second // 10x the 1s RemoteApplyInterval
	deadline := time.Now().Add(120 * time.Second)
	var batches int64
	var firstAdvanced, firstFull time.Time
	for time.Now().Before(deadline) {
		now := time.Now()
		have := peerHave(t, c.Nodes[sender].APIAddr, victimID, senderID)
		visVictim := prefixCount(t, c, victim, prefix)
		visSender := prefixCount(t, c, sender, prefix)
		batches = int64(metricValue(t, c.Nodes[victim].APIAddr, "spedsql_repl_batches_received_total"))
		if visSender != rows {
			t.Fatalf("sender shows %d/%d trial rows after committing them", visSender, rows)
		}
		if visVictim != 0 && visVictim != rows {
			t.Fatalf("victim shows %d/%d trial rows: partial transaction visible", visVictim, rows)
		}
		if have > seqPre && firstAdvanced.IsZero() {
			firstAdvanced = now
		}
		if visVictim == rows && firstFull.IsZero() {
			firstFull = now
		}
		if visVictim == rows && have >= seqTx {
			if firstAdvanced.IsZero() || firstFull.IsZero() {
				t.Fatalf("converged without observing watermark advance (advanced=%v full=%v)", firstAdvanced, firstFull)
			}
			t.Logf("watermark advanced %v before full trial visibility", firstFull.Sub(firstAdvanced))
			if lag := firstFull.Sub(firstAdvanced); lag > materializationBound {
				t.Fatalf("watermark advanced %v before completion, want <= %v (early advance)", lag, materializationBound)
			}
			return batches
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("retry did not complete within 120s (victim rows: %d/%d, watermark: %d, want >= %d)",
		prefixCount(t, c, victim, prefix), rows, peerHave(t, c.Nodes[sender].APIAddr, victimID, senderID), seqTx)
	return 0
}

// buildBigInsert renders one single-statement multi-row INSERT whose
// payload exceeds the 8 MiB replication frame, forcing the chunked
// transaction path. Blob ids use x-hex literals; values carry the trial
// prefix and pad with a quote-free alphabet.
func buildBigInsert(prefix string, idBase, rows, valueBytes int) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO chunk_rows (id, v) VALUES ")
	row := make([]byte, valueBytes)
	for i := 0; i < rows; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		copy(row, prefix)
		for j := len(prefix); j < valueBytes; j++ {
			row[j] = byte('a' + (idBase+i+j)%26)
		}
		fmt.Fprintf(&sb, "(x'%032x', '%s')", idBase+i, row)
	}
	return sb.String()
}

func prefixCount(t *testing.T, c *harness.Cluster, idx int, prefix string) int {
	t.Helper()
	res, err := c.QuerySQL(idx, "SELECT count(*) FROM chunk_rows WHERE v LIKE ?", prefix+"%")
	if err != nil {
		t.Fatalf("node %d prefix count: %v", idx, err)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return 0
	}
	var n int
	_, _ = fmt.Sscanf(fmt.Sprint(res.Rows[0][0]), "%d", &n)
	return n
}

func waitPrefixCount(t *testing.T, c *harness.Cluster, idx int, prefix string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n := prefixCount(t, c, idx, prefix); n == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %d %q rows = %d, want %d within %v", idx, prefix, prefixCount(t, c, idx, prefix), want, timeout)
}

func waitConverged(t *testing.T, c *harness.Cluster, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, table)
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, table, "id")
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
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	for i := range c.Nodes {
		n, _ := c.QueryRowCount(i, table)
		t.Logf("node %d at timeout: count=%d", i, n)
	}
	t.Fatalf("nodes did not converge on %d %s rows with equal digests within %v", want, table, timeout)
}

func waitConnectedPeers(t *testing.T, c *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if nodeStatus(t, c.Nodes[idx].APIAddr).ConnectedPeers == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %d connected peers = %d, want %d within %v",
		idx, nodeStatus(t, c.Nodes[idx].APIAddr).ConnectedPeers, want, timeout)
}

// waitHave polls the sender's record of the peer's applied watermark
// for one origin until it reaches want.
func waitHave(t *testing.T, c *harness.Cluster, senderIdx int, peerID, originID string, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := peerHave(t, c.Nodes[senderIdx].APIAddr, peerID, originID); got >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("sender view of %s watermark for %s = %d, want >= %d within %v",
		peerID, originID, peerHave(t, c.Nodes[senderIdx].APIAddr, peerID, originID), want, timeout)
}

type nodeStatusJSON struct {
	LocalSeq       uint64 `json:"local_seq"`
	HLC            uint64 `json:"hlc"`
	ConnectedPeers int    `json:"connected_peers"`
}

func nodeStatus(t *testing.T, apiAddr string) nodeStatusJSON {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		t.Fatalf("status %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	var st nodeStatusJSON
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode status %s: %v", apiAddr, err)
	}
	return st
}

type peerStatusJSON struct {
	NodeID string            `json:"NodeID"`
	Have   map[string]uint64 `json:"Have"`
}

func peerHave(t *testing.T, apiAddr, peerID, originID string) uint64 {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/debug/peers")
	if err != nil {
		t.Fatalf("debug/peers %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	var peers []peerStatusJSON
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		t.Fatalf("decode peers %s: %v", apiAddr, err)
	}
	for _, p := range peers {
		if p.NodeID == peerID {
			return p.Have[originID]
		}
	}
	t.Fatalf("peer %s not present in /v1/debug/peers", peerID)
	return 0
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatalf("metrics %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || line[0] == '#' || !strings.HasPrefix(line, name) {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(line, name))
		if i := strings.LastIndex(rest, " "); i >= 0 {
			rest = rest[i+1:]
		}
		v, err := strconv.ParseFloat(rest, 64)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		return v
	}
	t.Fatalf("counter %s not present in /metrics", name)
	return 0
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
