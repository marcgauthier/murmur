package rime

import "testing"

func TestPreparedHashInsertionDoesNotAllocate(t *testing.T) {
	type state struct {
		ix     *indexSet
		m      map[string]hashBucket
		values []string
		keys   []any
	}
	makeState := func() state {
		m := make(map[string]hashBucket, uniqueMapInitialHint)
		m["collision"] = hashBucket{single: "old"}
		m["set"] = hashBucket{set: map[any]struct{}{"set-a": {}, "set-b": {}}, setHint: 2}
		ix := &indexSet{hashHints: map[string]int{"field": uniqueMapInitialHint}}
		ix.hashStr = map[string]map[string]hashBucket{"field": m}
		values := []string{"collision", "set", "repeated"}
		for i := 0; i < 9; i++ {
			values = append(values, "distinct-"+string(rune('a'+i)))
		}
		keys := []any{"new", "set-c", "set-d", "repeat-a", "repeat-b"}
		for i := 0; i < 9; i++ {
			keys = append(keys, "key-"+string(rune('a'+i)))
		}
		adds := make(map[string]int, len(values))
		for _, value := range values {
			adds[value] = 1
		}
		adds["repeated"], adds["set"] = 2, 2
		seeds := make([]func(), 0, 1)
		reserveHashValues(ix, ix.hashStr, "field", adds, &seeds)
		if _, exists := ix.hashStr["field"]["repeated"]; exists {
			t.Fatal("preparing absent-value buckets changed logical membership")
		}
		for _, seed := range seeds {
			seed()
		}
		return state{ix: ix, m: ix.hashStr["field"], values: values, keys: keys}
	}
	measure := func(apply func(*state)) float64 {
		states := [2]state{makeState(), makeState()}
		next := 0
		allocs := testing.AllocsPerRun(1, func() {
			apply(&states[next])
			next++
		})
		return allocs
	}
	checks := []struct {
		name  string
		apply func(*state)
	}{
		{"single promotion", func(s *state) { hashAdd(s.m, "collision", s.keys[0]) }},
		{"existing set growth", func(s *state) {
			hashAdd(s.m, "set", s.keys[1])
			hashAdd(s.m, "set", s.keys[2])
		}},
		{"repeated new value", func(s *state) {
			hashAdd(s.m, "repeated", s.keys[3])
			hashAdd(s.m, "repeated", s.keys[4])
		}},
		{"outer map growth", func(s *state) {
			for i := 3; i < len(s.values); i++ {
				hashAdd(s.m, s.values[i], s.keys[i+2])
			}
		}},
	}
	for _, check := range checks {
		if got := measure(check.apply); got != 0 {
			t.Errorf("prepared hash %s allocated %.2f times during install", check.name, got)
		}
	}
}
