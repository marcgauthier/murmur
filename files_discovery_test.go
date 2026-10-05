package murmur

import (
	"context"
	"testing"

	"github.com/marcgauthier/murmur/replication"
)

func TestMergeFetchSources(t *testing.T) {
	self := NewNodeID()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	static := []Peer{
		{NodeID: self, Addrs: []string{"127.0.0.1:1"}},
		{NodeID: nodeA, Addrs: []string{"10.0.0.1:7844"}},
	}
	discovered := []Peer{
		{NodeID: self, Addrs: []string{"127.0.0.1:2"}},
		{NodeID: nodeA, Addrs: []string{"10.9.9.9:7844"}},
		{NodeID: nodeB, Addrs: []string{"10.0.0.2:7844"}},
	}
	got := mergeFetchSources(self, static, discovered)
	if len(got) != 2 {
		t.Fatalf("merged = %v, want 2 sources", got)
	}
	if got[0].NodeID != nodeA || len(got[0].Addrs) != 1 || got[0].Addrs[0] != "10.0.0.1:7844" {
		t.Fatalf("static entry lost precedence: %+v", got[0])
	}
	if got[1].NodeID != nodeB {
		t.Fatalf("discovered entry missing: %+v", got[1])
	}
	if got := mergeFetchSources(self, nil, nil); len(got) != 0 {
		t.Fatalf("empty merge = %v", got)
	}
	if got := mergeFetchSources(self, nil, discovered[:1]); len(got) != 0 {
		t.Fatalf("self-only discovered merge = %v", got)
	}
}

// TestFetchAdvertisesBoundEndpoint proves a serving node publishes its
// actual bound fetch address (ephemeral ports included) in SWIM metadata
// once the fetch server starts.
func TestFetchAdvertisesBoundEndpoint(t *testing.T) {
	ctx := context.Background()
	node := NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, node)
	cfg := replConfig(t.TempDir(), node, dbid, creds[node], nil)
	cfg.Replication.Membership.Enabled = true
	cfg.Files.Enabled = true
	cfg.Files.ObjectKey = append([]byte(nil), testObjectKey...)
	cfg.Files.FetchAddr = "127.0.0.1:0"
	cfg.Files.FetchInterval = -1
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ms := db.replManager().Membership()
	if ms == nil {
		t.Fatal("membership service not started")
	}
	meta, err := replication.DecodeNodeMetadata(ms.NodeMeta(512), dbid)
	if err != nil {
		t.Fatal(err)
	}
	bound := db.files.fetch.server.Addr()
	if meta.FetchEndpoint == "" {
		t.Fatal("no fetch endpoint advertised")
	}
	if meta.FetchEndpoint != bound {
		t.Fatalf("advertised %q, bound %q", meta.FetchEndpoint, bound)
	}
	if !db.fetchDiscoveryActive() {
		t.Fatal("fetch discovery not active with a membership service")
	}
}

// TestFetchDiscoveryInactiveWithoutMembership proves static-only behavior
// is unchanged when no membership service exists: no discovered sources.
func TestFetchDiscoveryInactiveWithoutMembership(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.fetchDiscoveryActive() {
		t.Fatal("fetch discovery active without a membership service")
	}
	if got := db.files.discoveredSources(); len(got) != 0 {
		t.Fatalf("discovered sources = %v without membership", got)
	}
	if got := db.files.sources(); len(got) != 0 {
		t.Fatalf("sources = %v without static peers or membership", got)
	}
}
