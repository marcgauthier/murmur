package mergepolicies_test

import (
	"fmt"
	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDisconnectedPoliciesForwardAndRestart(t *testing.T) {
	cfg := &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "items", Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}, {Name: "count", Type: schema.ColText, MergePolicy: schema.PN_COUNTER}, {Name: "tags", Type: schema.ColText, MergePolicy: schema.OR_SET}, {Name: "maxv", Type: schema.ColInteger, MergePolicy: schema.MAX}, {Name: "minv", Type: schema.ColReal, MergePolicy: schema.MIN}}}}}
	c := harness.NewCluster(t, harness.ClusterOptions{Name: "merge-policies", NumNodes: 3, AwaitUnlock: true, ManualPeers: true, Schema: cfg})
	row := "00000000-0000-0000-0000-000000000001"
	huge := "10000000000000000000000000000000000000000"
	op := func(node int, kind, column, value string) {
		t.Helper()
		request := map[string]string{"Table": "items", "Column": column, "Row": row, "Operation": kind}
		if kind == "increment" {
			request["Delta"] = value
		} else {
			request["Element"] = value
			request["Kind"] = "string"
		}
		if err := c.MergeOperation(node, request); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if err := c.ExecSQL(i, "INSERT INTO items VALUES (?, '0','[]',0,100)", strings.ReplaceAll(row, "-", "")); err != nil {
			t.Fatal(err)
		}
	}
	op(0, "increment", "count", huge)
	op(1, "increment", "count", "7")
	op(2, "increment", "count", "-2")
	op(0, "add", "tags", "shared")
	op(0, "add", "tags", "red")
	op(1, "remove", "tags", "shared")
	op(1, "add", "tags", "blue")
	op(2, "add", "tags", "green")
	for i := 0; i < 3; i++ {
		if err := c.ExecSQL(i, "UPDATE items SET maxv=?,minv=?", []int{10, 20, 5}[i], []int{30, 15, 7}[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range [][2]int{{0, 1}, {1, 0}} {
		if err := c.AddPeer(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	wait := func(node int, want string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			r, err := c.QuerySQL(node, "SELECT count FROM items")
			if err == nil && len(r.Rows) == 1 && r.Rows[0][0] == want {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("node %d did not reach %s", node, want)
	}
	n, _ := new(big.Int).SetString(huge, 10)
	n.Add(n, big.NewInt(7))
	wait(1, n.String())
	c.StopNode(0)
	for _, edge := range [][2]int{{1, 2}, {2, 1}} {
		if err := c.AddPeer(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	n.Sub(n, big.NewInt(2))
	wait(1, n.String())
	wait(2, n.String())
	for _, node := range []int{1, 2} {
		r, err := c.QuerySQL(node, "SELECT tags,maxv,minv FROM items")
		if err != nil {
			t.Fatal(err)
		}
		tags := fmt.Sprint(r.Rows[0][0])
		for _, tag := range []string{"shared", "red", "blue", "green"} {
			if !strings.Contains(tags, tag) {
				t.Fatalf("lost %s in %s", tag, tags)
			}
		}
		if r.Rows[0][1] != float64(20) || r.Rows[0][2] != float64(7) {
			t.Fatalf("extrema: %v", r.Rows)
		}
	}
	a, err := c.MergeState(1, "items", row)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.MergeState(2, "items", row)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("causal state differs: %v %v", a, b)
	}
	c.StopNode(2)
	c.StartNode(2)
	c.WaitNodeReady(2)
	c.UnlockNode(2, c.Nodes[2].KeyHex)
	wait(2, n.String())
	op(1, "remove", "tags", "shared")
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		r, err := c.QuerySQL(2, "SELECT tags FROM items")
		if err == nil && len(r.Rows) == 1 && !strings.Contains(fmt.Sprint(r.Rows[0][0]), "shared") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("observed removal did not converge after restart")
}
