package rime

import "testing"

func TestShardInitialAndPreparedGrowthMapsInstallWithoutAllocating(t *testing.T) {
	type install struct {
		shard *tShard[int]
		key   any
		chain *chain[int]
	}
	initial := [2]install{
		{shard: newTShard[int](), key: "initial-a", chain: &chain[int]{vers: []version[int]{{commit: 1, val: new(int)}}, total: 1}},
		{shard: newTShard[int](), key: "initial-b", chain: &chain[int]{vers: []version[int]{{commit: 1, val: new(int)}}, total: 1}},
	}
	next := 0
	allocs := testing.AllocsPerRun(1, func() {
		item := initial[next]
		item.shard.rows[item.key] = item.chain
		next++
	})
	if allocs != 0 {
		t.Fatalf("initial shard insertion allocated %.2f times", allocs)
	}

	makeGrowth := func() install {
		shard := newTShard[int]()
		for i := 0; i < 6; i++ {
			shard.rows[i] = &chain[int]{vers: []version[int]{{commit: 1, val: new(int)}}, total: 1}
		}
		table := &Table[int]{shards: []*tShard[int]{shard}}
		key := any(6)
		prepared := table.prepareShardRows(0, []any{key})
		if prepared == nil {
			t.Fatal("seventh row did not prepare shard-map growth")
		}
		table.installPreparedShardRows(0, prepared)
		return install{shard: shard, key: key, chain: &chain[int]{vers: []version[int]{{commit: 2, val: new(int)}}, total: 1}}
	}
	growth := [2]install{makeGrowth(), makeGrowth()}
	next = 0
	allocs = testing.AllocsPerRun(1, func() {
		item := growth[next]
		item.shard.rows[item.key] = item.chain
		next++
	})
	if allocs != 0 {
		t.Fatalf("prepared shard growth insertion allocated %.2f times", allocs)
	}
}
