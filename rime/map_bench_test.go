package rime_test

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// MapInterface abstracts concurrent map operations for uniform benchmarking.
type MapInterface interface {
	Load(key string) (string, bool)
	Store(key string, val string)
}

// syncMapWrapper wraps standard library sync.Map.
type syncMapWrapper struct {
	m sync.Map
}

func (s *syncMapWrapper) Load(key string) (string, bool) {
	v, ok := s.m.Load(key)
	if !ok {
		return "", false
	}
	return v.(string), true
}

func (s *syncMapWrapper) Store(key string, val string) {
	s.m.Store(key, val)
}

// rimeSingleShardSim simulates a single RIME shard (sync.RWMutex + standard Go map).
type rimeSingleShardSim struct {
	mu sync.RWMutex
	m  map[string]string
}

func newRimeSingleShardSim() *rimeSingleShardSim {
	return &rimeSingleShardSim{m: make(map[string]string)}
}

func (r *rimeSingleShardSim) Load(key string) (string, bool) {
	r.mu.RLock()
	v, ok := r.m[key]
	r.mu.RUnlock()
	return v, ok
}

func (r *rimeSingleShardSim) Store(key string, val string) {
	r.mu.Lock()
	r.m[key] = val
	r.mu.Unlock()
}

// rime64ShardsSim simulates RIME's default 64-shard partitioned architecture (rime/shard.go).
type rime64ShardsSim struct {
	shards [64]struct {
		mu sync.RWMutex
		m  map[string]string
	}
}

func newRime64ShardsSim() *rime64ShardsSim {
	r := &rime64ShardsSim{}
	for i := range r.shards {
		r.shards[i].m = make(map[string]string)
	}
	return r
}

func (r *rime64ShardsSim) shardIndex(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % 64)
}

func (r *rime64ShardsSim) Load(key string) (string, bool) {
	idx := r.shardIndex(key)
	s := &r.shards[idx]
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	return v, ok
}

func (r *rime64ShardsSim) Store(key string, val string) {
	idx := r.shardIndex(key)
	s := &r.shards[idx]
	s.mu.Lock()
	s.m[key] = val
	s.mu.Unlock()
}

// atomicCellShardSim simulates RIME's 64 shards using atomic pointer cells for 100% lock-free reads.
type atomicCellShardSim struct {
	shards [64]struct {
		active   atomic.Pointer[map[string]*atomic.Pointer[string]]
		insertMu sync.Mutex
	}
}

func newAtomicCellShardSim() *atomicCellShardSim {
	r := &atomicCellShardSim{}
	for i := range r.shards {
		initial := make(map[string]*atomic.Pointer[string])
		r.shards[i].active.Store(&initial)
	}
	return r
}

func (r *atomicCellShardSim) shardIndex(key string) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % 64)
}

func (r *atomicCellShardSim) Load(key string) (string, bool) {
	idx := r.shardIndex(key)
	s := &r.shards[idx]
	m := *s.active.Load()
	cell, ok := m[key]
	if !ok {
		return "", false
	}
	valPtr := cell.Load()
	if valPtr == nil {
		return "", false
	}
	return *valPtr, true
}

func (r *atomicCellShardSim) Store(key string, val string) {
	idx := r.shardIndex(key)
	s := &r.shards[idx]

	// Fast path: update existing key in-place (lock-free, no map copying; &val escapes and allocates)
	m := *s.active.Load()
	if cell, ok := m[key]; ok {
		cell.Store(&val)
		return
	}

	// Slow path: new key insert under mutex
	s.insertMu.Lock()
	defer s.insertMu.Unlock()

	// Double-check under lock
	m = *s.active.Load()
	if cell, ok := m[key]; ok {
		cell.Store(&val)
		return
	}

	cloned := make(map[string]*atomic.Pointer[string], len(m)+1)
	for k, v := range m {
		cloned[k] = v
	}
	cell := &atomic.Pointer[string]{}
	cell.Store(&val)
	cloned[key] = cell
	s.active.Store(&cloned)
}

const (
	benchKeyCount = 50_000
	benchVal      = "rime-benchmark-payload-value-data"
)

func generateKeys(n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%08d", i)
	}
	return keys
}

func setupBenchmarkMap(factory func() MapInterface, keys []string) MapInterface {
	m := factory()
	for _, k := range keys {
		m.Store(k, benchVal)
	}
	return m
}

// MapFactoryMap defines the implementations to evaluate.
var mapFactories = []struct {
	name    string
	factory func() MapInterface
}{
	{name: "sync.Map", factory: func() MapInterface { return &syncMapWrapper{} }},
	{name: "RimeSingleShard", factory: func() MapInterface { return newRimeSingleShardSim() }},
	{name: "Rime64Shards", factory: func() MapInterface { return newRime64ShardsSim() }},
	{name: "AtomicCell64Shards", factory: func() MapInterface { return newAtomicCellShardSim() }},
}

// BenchmarkMap_Read_Uniform tests 100% reads across uniformly distributed keys.
func BenchmarkMap_Read_Uniform(b *testing.B) {
	keys := generateKeys(benchKeyCount)
	for _, tc := range mapFactories {
		b.Run(tc.name, func(b *testing.B) {
			m := setupBenchmarkMap(tc.factory, keys)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				idx := 0
				for pb.Next() {
					key := keys[idx%len(keys)]
					v, ok := m.Load(key)
					if !ok || v == "" {
						b.Fatalf("missing key %s", key)
					}
					idx++
				}
			})
		})
	}
}

// BenchmarkMap_Read_HotKey tests 100% reads where 99% of requests hit the same 5 hot keys.
func BenchmarkMap_Read_HotKey(b *testing.B) {
	keys := generateKeys(benchKeyCount)
	schedule := mapSchedule()
	hotKeys := keys[:5]
	for _, tc := range mapFactories {
		b.Run(tc.name, func(b *testing.B) {
			m := setupBenchmarkMap(tc.factory, keys)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				idx := 0
				for pb.Next() {
					var key string
					step := schedule[idx%len(schedule)]
					idx++
					if idx%100 != 0 {
						key = hotKeys[step%len(hotKeys)]
					} else {
						key = keys[step%len(keys)]
					}
					v, ok := m.Load(key)
					if !ok || v == "" {
						b.Fatalf("missing key %s", key)
					}
				}
			})
		})
	}
}

// BenchmarkMap_Write_Disjoint inserts a fixed 4096-key batch into a fresh map.
// One benchmark iteration is a batch; ns/insert and inserts/s normalize its cost.
// Setup/key formatting is excluded, and every implementation reaches the same size.
func BenchmarkMap_Write_Disjoint(b *testing.B) {
	keys := generateKeys(4096)
	for _, tc := range mapFactories {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			b.StopTimer()
			for round := 0; round < b.N; round++ {
				m := tc.factory()
				workers := runtime.GOMAXPROCS(0)
				var wg sync.WaitGroup
				wg.Add(workers)
				b.StartTimer()
				for worker := 0; worker < workers; worker++ {
					go func(worker int) {
						defer wg.Done()
						for i := worker; i < len(keys); i += workers {
							m.Store(keys[i], benchVal)
						}
					}(worker)
				}
				wg.Wait()
				b.StopTimer()
				for _, key := range keys {
					if value, ok := m.Load(key); !ok || value != benchVal {
						b.Fatalf("incorrect insert %s", key)
					}
				}
			}
			total := float64(b.N) * float64(len(keys))
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/total, "ns/insert")
			b.ReportMetric(total/b.Elapsed().Seconds(), "inserts/s")
		})
	}
}

// BenchmarkMap_KeyGeneration isolates formatting excluded from map timings.
func BenchmarkMap_KeyGeneration(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkKeySink = fmt.Sprintf("key-%08d", i)
	}
}

var benchmarkKeySink string

// BenchmarkMap_Write_HotKey tests 100% writes heavily contending on 5 hot keys.
func BenchmarkMap_Write_HotKey(b *testing.B) {
	hotKeys := []string{"hot-0", "hot-1", "hot-2", "hot-3", "hot-4"}
	for _, tc := range mapFactories {
		b.Run(tc.name, func(b *testing.B) {
			m := setupBenchmarkMap(tc.factory, hotKeys)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				idx := 0
				for pb.Next() {
					k := hotKeys[idx%len(hotKeys)]
					m.Store(k, benchVal)
					idx++
				}
			})
		})
	}
}

// BenchmarkMap_Mixed_90Read10Write tests 90% read / 10% write OLTP workload.
func BenchmarkMap_Mixed_90Read10Write(b *testing.B) {
	keys := generateKeys(benchKeyCount)
	schedule := mapSchedule()
	for _, tc := range mapFactories {
		b.Run(tc.name, func(b *testing.B) {
			m := setupBenchmarkMap(tc.factory, keys)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				idx := 0
				for pb.Next() {
					step := schedule[idx%len(schedule)]
					idx++
					k := keys[step%len(keys)]
					if idx%10 == 0 {
						m.Store(k, benchVal)
					} else {
						if value, ok := m.Load(k); !ok || value != benchVal {
							panic("incorrect mixed read")
						}
					}
				}
			})
		})
	}
}

// BenchmarkMap_Mixed_50Read50Write tests existing-key updates, without churn.
func BenchmarkMap_Mixed_50Read50Write(b *testing.B) {
	keys := generateKeys(benchKeyCount)
	schedule := mapSchedule()
	for _, tc := range mapFactories {
		b.Run(tc.name, func(b *testing.B) {
			m := setupBenchmarkMap(tc.factory, keys)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				idx := 0
				for pb.Next() {
					step := schedule[idx%len(schedule)]
					idx++
					k := keys[step%len(keys)]
					if idx%2 == 0 {
						m.Store(k, benchVal)
					} else {
						if value, ok := m.Load(k); !ok || value != benchVal {
							panic("incorrect mixed read")
						}
					}
				}
			})
		})
	}
}

// Fixed pseudo-random trace: generation never enters the timed operation loop.
func mapSchedule() []int {
	r := rand.New(rand.NewPCG(127, 1))
	trace := make([]int, 4096)
	for i := range trace {
		trace[i] = r.IntN(benchKeyCount)
	}
	return trace
}
