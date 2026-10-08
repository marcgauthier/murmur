package state

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
)

func openUnsyncedTestStore(t *testing.T, node ids.NodeID, async bool) *Store {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := origin.NewKeyRegistry(map[ids.NodeID]ed25519.PublicKey{node: pub})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.TempDir(), node, ids.DBID{}, withTestKey(Options{
		Limits:          codec.DefaultLimits(),
		OriginSigning:   origin.Config{PrivateKey: priv, TrustedKeys: reg},
		AsyncDurability: async,
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestUnsyncedBytesTracksNoSyncCommits(t *testing.T) {
	ctx := context.Background()
	s := openUnsyncedTestStore(t, ids.NewNodeID(), true)
	if got := s.UnsyncedBytes(); got != 0 {
		t.Fatalf("fresh store unsynced = %d, want 0", got)
	}
	batch := groupLocalBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text("unsynced")})
	if _, err := s.CommitLocal(ctx, batch); err != nil {
		t.Fatal(err)
	}
	first := s.UnsyncedBytes()
	if first == 0 {
		t.Fatal("unsynced stayed 0 after a NoSync commit")
	}
	if _, err := s.CommitLocal(ctx, groupLocalBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text("more")},
	)); err != nil {
		t.Fatal(err)
	}
	if got := s.UnsyncedBytes(); got <= first {
		t.Fatalf("unsynced = %d after two commits, want growth past %d", got, first)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := s.UnsyncedBytes(); got != 0 {
		t.Fatalf("unsynced after Sync = %d, want 0", got)
	}
}

func TestUnsyncedBytesStaysZeroWhenSync(t *testing.T) {
	ctx := context.Background()
	s := openUnsyncedTestStore(t, ids.NewNodeID(), false)
	if _, err := s.CommitLocal(ctx, groupLocalBatch(s, s.ClockNow(),
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text("synced")},
	)); err != nil {
		t.Fatal(err)
	}
	if got := s.UnsyncedBytes(); got != 0 {
		t.Fatalf("sync-mode unsynced = %d, want 0", got)
	}
}
