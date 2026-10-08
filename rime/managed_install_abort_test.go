package rime

import (
	"context"
	"testing"
)

type managedInstallAbortRecord struct {
	ID    string `rime:"primary"`
	Name  string `rime:"index,prefix"`
	Group string `rime:"index"`
	Rank  int    `rime:"index"`
}

func TestAbortPrunesPreparedPrefixAndCompoundPaths(t *testing.T) {
	db := New()
	table, err := Register[managedInstallAbortRecord](db,
		WithTableShards[managedInstallAbortRecord](1),
		WithCompound[managedInstallAbortRecord]("group_rank", "Group", "Rank"))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := table.In(tx).Insert(&managedInstallAbortRecord{
		ID: "abort-row", Name: "abort-prefix", Group: "abort-group", Rank: 1,
	}); err != nil {
		t.Fatal(err)
	}
	p, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	if got := table.idx.prefixLookup("Name", "abort-"); len(got) != 0 {
		t.Fatalf("unpublished prefix path contains records: %v", got)
	}
	if got := table.idx.comp["group_rank"].lookup([]any{"abort-group", 1}); len(got) != 0 {
		t.Fatalf("unpublished compound path contains records: %v", got)
	}
	if err := p.Abort(); err != nil {
		t.Fatal(err)
	}
	if len(table.idx.prefix["Name"].root.children) != 0 {
		t.Fatal("aborted preparation retained empty prefix nodes")
	}
	if len(table.idx.comp["group_rank"].root.kids) != 0 {
		t.Fatal("aborted preparation retained empty compound nodes")
	}
	db.Close()
}
