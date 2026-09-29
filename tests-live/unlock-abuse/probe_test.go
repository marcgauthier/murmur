package unlockabuse_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func TestProbeConvergenceTiming(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "probe-conv", NumNodes: 2, AwaitUnlock: true, SchemaSQL: schemaSQL,
	})
	if err := cluster.ExecSQL(0, "INSERT INTO abuse_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 1), "x"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	time.Sleep(8 * time.Second)
	for i := range cluster.Nodes {
		for _, p := range []string{"/v1/status", "/v1/debug/peers"} {
			resp, err := http.DefaultClient.Get("https://" + cluster.Nodes[i].APIAddr + p)
			if err != nil {
				t.Logf("node%d %s err=%v", i, p, err)
				continue
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Logf("node%d %s [%d]: %.1500s", i, p, resp.StatusCode, string(b))
		}
		c0, e0 := cluster.QueryRowCount(0, "abuse_rows")
		c1, e1 := cluster.QueryRowCount(1, "abuse_rows")
		t.Logf("counts=%d,%d errs=%v,%v", c0, c1, e0, e1)
	}
	cluster.DumpForensics("probe")
	t.Fatal("probe done")
}
