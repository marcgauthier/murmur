// Subscription-backlog acceptance: an SSE subscription on node1 stays
// open across a partition that accumulates a bounded heavy-write backlog
// on node2, then receives the full backlog after healing with no reset.
//
// Backlog bounds (from product code, not invented): each subscriber
// channel holds SubscriptionConfig.EventBufferSize events (default 64)
// and a slow consumer whose channel fills is reset (see
// subscriptionManager.dispatch); cursor resumption retains
// MaxRetainedEvents (default 256). The write window (default 40 commits)
// sits inside both caps, and the suite asserts the observable
// consequences: no reset at any point, strictly increasing cursors,
// every update event delivered, and a final event carrying every row.
package subscribebacklog_test

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

// Product caps (SubscriptionConfig defaults in config.go). The suite
// never reconfigures the daemon, so these are the live bounds.
const (
	eventBufferCap = 64
	retainedCap    = 256
)

func TestSubscriptionBacklogDeliversFullyAfterHeal(t *testing.T) {
	backlogRows := envInt("SPEDSQL_SUBSCRIBE_BACKLOG_ROWS", 40)
	if backlogRows >= eventBufferCap {
		t.Fatalf("SPEDSQL_SUBSCRIBE_BACKLOG_ROWS=%d must stay below the %d-event subscriber buffer",
			backlogRows, eventBufferCap)
	}
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "subscribe-backlog",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "sb_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "body", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	sub, err := cluster.Subscribe(0, `SELECT body FROM sb_rows ORDER BY body`)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	first := harness.NextStreamEvent(t, sub, 15*time.Second)
	if first.Type != string(db.EventInitial) {
		t.Fatalf("first event = %q, want %q", first.Type, db.EventInitial)
	}
	lastCursor := first.Cursor
	updates := 0

	// Positive control: a far-side write arrives while meshed.
	mustExec(t, cluster, 1, fmt.Sprintf("%032x", 1), "base-0")
	waitBodies(t, sub, map[string]bool{"base-0": true}, 20*time.Second, &lastCursor, &updates)
	t.Logf("positive control: meshed far-side write arrived (cursor=%d)", lastCursor)

	// Partition both directions; the subscription must observe silence.
	if err := cluster.RemovePeer(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RemovePeer(1, 0); err != nil {
		t.Fatal(err)
	}
	mustExec(t, cluster, 1, fmt.Sprintf("%032x", 2), "probe-0")
	waitRowCount(t, cluster, 1, 2, 15*time.Second)
	assertSilent(t, sub, "probe-0", 3*time.Second)

	// Bounded heavy-write window on the far side while partitioned.
	for i := 0; i < backlogRows; i++ {
		mustExec(t, cluster, 1, fmt.Sprintf("%032x", 1000+i), fmt.Sprintf("burst-%d", i))
	}
	waitRowCount(t, cluster, 1, 2+backlogRows, 30*time.Second)
	// Anti-vacuity: node1 must actually miss the whole burst range.
	if n, err := cluster.QueryRowCount(0, "sb_rows"); err != nil || n != 1 {
		t.Fatalf("node1 count = %d (err=%v), want 1 (it must miss the burst)", n, err)
	}
	assertSilent(t, sub, "burst-", 3*time.Second)
	t.Logf("partition held: %d far-side rows invisible to node1 and its subscription", backlogRows)

	// Heal: the full backlog must arrive with no reset and strictly
	// increasing cursors.
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"base-0": true, "probe-0": true}
	for i := 0; i < backlogRows; i++ {
		want[fmt.Sprintf("burst-%d", i)] = true
	}
	waitBodies(t, sub, want, 60*time.Second, &lastCursor, &updates)
	t.Logf("backlog delivered: %d update events, last cursor=%d", updates, lastCursor)

	// The delivered backlog stayed within the retained-history cap.
	if updates >= retainedCap {
		t.Fatalf("observed %d update events, want < %d retained-history cap", updates, retainedCap)
	}

	// Exact row counts and identical ordered digests on both nodes.
	waitConverged(t, cluster, "sb_rows", len(want), 60*time.Second)

	// The stream is still live after the backlog: a post-heal write
	// on either node emits promptly with no reset.
	mustExec(t, cluster, 0, fmt.Sprintf("%032x", 9000), "post-heal")
	want["post-heal"] = true
	waitBodies(t, sub, want, 30*time.Second, &lastCursor, &updates)
	waitConverged(t, cluster, "sb_rows", len(want), 30*time.Second)
	t.Logf("post-heal write emitted; stream live at cursor=%d", lastCursor)
}

// mustExec inserts one row, failing loudly on error.
func mustExec(t *testing.T, c *harness.Cluster, idx int, id, body string) {
	t.Helper()
	if err := c.ExecSQL(idx, "INSERT INTO sb_rows (id, body) VALUES (?, ?)", id, body); err != nil {
		t.Fatalf("node %d insert %s: %v", idx, body, err)
	}
}

// eventBodies extracts the text bodies of one stream event, sorted.
func eventBodies(ev harness.StreamEvent) []string {
	var out []string
	for _, row := range ev.Rows {
		for _, v := range row {
			out = append(out, v.S)
		}
	}
	sort.Strings(out)
	return out
}

// waitBodies drains the subscription until one event carries exactly the
// wanted body set. Any reset fails immediately; cursors must strictly
// increase across every observed event.
func waitBodies(t *testing.T, sub *harness.Subscription, want map[string]bool, timeout time.Duration, lastCursor *uint64, updates *int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ev := harness.NextStreamEvent(t, sub, time.Until(deadline))
		if ev.Type == string(db.EventReset) {
			t.Fatalf("subscription reset (cursor=%d, err=%s); backlog exceeded product caps", ev.Cursor, ev.Error)
		}
		if ev.Cursor <= *lastCursor {
			t.Fatalf("cursor went %d -> %d, want strictly increasing", *lastCursor, ev.Cursor)
		}
		*lastCursor = ev.Cursor
		if ev.Type == string(db.EventUpdate) {
			*updates++
		}
		got := eventBodies(ev)
		if len(got) != len(want) {
			continue
		}
		match := true
		for _, b := range got {
			if !want[b] {
				match = false
				break
			}
		}
		if match {
			return
		}
	}
	t.Fatalf("no event carried the %d wanted bodies within %v", len(want), timeout)
}

// assertSilent fails if the subscription emits a reset or any event
// containing a body with the given prefix within the window. Silence is
// the observable proof the partition holds for the stream.
func assertSilent(t *testing.T, sub *harness.Subscription, prefix string, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		select {
		case ev, ok := <-sub.Events():
			if !ok {
				t.Fatalf("subscription closed during partition silence window")
			}
			if ev.Type == string(db.EventReset) {
				t.Fatalf("subscription reset during partition (err=%s)", ev.Error)
			}
			for _, b := range eventBodies(ev) {
				if len(b) >= len(prefix) && b[:len(prefix)] == prefix {
					t.Fatalf("partitioned body %q leaked to the subscription", b)
				}
			}
		case <-time.After(time.Until(deadline)):
			return
		}
	}
}

func waitRowCount(t *testing.T, c *harness.Cluster, idx int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, err := c.QueryRowCount(idx, "sb_rows"); err == nil && n == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	n, _ := c.QueryRowCount(idx, "sb_rows")
	t.Fatalf("node %d count = %d, want %d within %v", idx, n, want, timeout)
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
		time.Sleep(100 * time.Millisecond)
	}
	for i := range c.Nodes {
		n, _ := c.QueryRowCount(i, table)
		d, _ := c.ComputeTableDigest(i, table, "id")
		t.Logf("node %d at timeout: count=%d digest=%s", i, n, d)
	}
	t.Fatalf("nodes did not converge on %d %s rows with equal digests within %v", want, table, timeout)
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
