package rime

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/rime/trigram"
)

type trigramBenchDoc struct {
	ID   string `rime:"primary"`
	Body string `rime:"trigram"`
}

type trigramBenchPlain struct {
	ID   string `rime:"primary"`
	Body string
}

var trigramBenchWords = []string{
	"database", "replication", "network", "connection", "server", "client",
	"storage", "index", "query", "search", "node", "cluster", "shard",
	"replica", "leader", "follower", "snapshot", "backup", "restore",
	"transaction", "commit", "quorum", "gossip", "membership", "leader",
	"payload", "segment", "compaction", "encryption", "witness",
}

func trigramBenchBody(i, size int) string {
	var b strings.Builder
	b.Grow(size + 64)
	fmt.Fprintf(&b, "document %06d ", i)
	if i == 0 {
		// Unique word for selective-query benchmarks.
		b.WriteString("xyzzyspoon ")
	}
	for b.Len() < size {
		for j, w := range trigramBenchWords {
			if b.Len() >= size {
				break
			}
			if (i+j)%7 == 0 {
				fmt.Fprintf(&b, "%s-%d ", w, (i*31+j)%500)
			} else {
				b.WriteString(w)
				b.WriteByte(' ')
			}
		}
	}
	return b.String()
}

func setupTrigramBenchTable(b testing.TB, n, bodySize int) (*DB, *Table[trigramBenchDoc]) {
	b.Helper()
	db := New()
	b.Cleanup(func() { db.Close() })
	table, err := Register[trigramBenchDoc](db)
	if err != nil {
		b.Fatal(err)
	}
	const batch = 500
	rows := make([]*trigramBenchDoc, 0, batch)
	flush := func() {
		if len(rows) == 0 {
			return
		}
		if err := table.InsertMany(rows); err != nil {
			b.Fatal(err)
		}
		rows = rows[:0]
	}
	for i := 0; i < n; i++ {
		rows = append(rows, &trigramBenchDoc{ID: fmt.Sprintf("d%06d", i), Body: trigramBenchBody(i, bodySize)})
		if len(rows) == batch {
			flush()
		}
	}
	flush()
	return db, table
}

func trigramBenchHeapMB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / 1024 / 1024
}

// BenchmarkTrigramIndexFootprint reports heap per document with and without
// the trigram index over two identical 5000-document corpora. The index-only
// overhead subtracts the plain corpus delta from the indexed corpus delta;
// both corpora hold identical bodies, so the remainder is approximately the
// postings map.
func BenchmarkTrigramIndexFootprint(b *testing.B) {
	const n, bodySize = 5000, 2048
	base := trigramBenchHeapMB()

	plainDB := New()
	plain, err := Register[trigramBenchPlain](plainDB)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i += 500 {
		rows := make([]*trigramBenchPlain, 0, 500)
		for j := i; j < i+500 && j < n; j++ {
			rows = append(rows, &trigramBenchPlain{ID: fmt.Sprintf("p%06d", j), Body: trigramBenchBody(j, bodySize)})
		}
		if err := plain.InsertMany(rows); err != nil {
			b.Fatal(err)
		}
	}
	afterPlain := trigramBenchHeapMB()

	// Inline setup: the shared helper registers Cleanup, which would pin
	// the corpus across repeated harness executions and poison deltas.
	indexedDB := New()
	indexed, err := Register[trigramBenchDoc](indexedDB)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i += 500 {
		rows := make([]*trigramBenchDoc, 0, 500)
		for j := i; j < i+500 && j < n; j++ {
			rows = append(rows, &trigramBenchDoc{ID: fmt.Sprintf("d%06d", j), Body: trigramBenchBody(j, bodySize)})
		}
		if err := indexed.InsertMany(rows); err != nil {
			b.Fatal(err)
		}
	}
	afterIndexed := trigramBenchHeapMB()

	// Tear down so repeated harness executions measure fresh deltas.
	plainDB.Close()
	indexedDB.Close()
	runtime.GC()

	plainDelta := afterPlain - base
	indexedDelta := afterIndexed - afterPlain
	b.ReportMetric(plainDelta*1024*1024/float64(n), "B/doc-plain")
	b.ReportMetric(indexedDelta*1024*1024/float64(n), "B/doc-indexed")
	b.ReportMetric(indexedDelta-plainDelta, "index-MB")
	b.Logf("heap base=%.1fMB plain=%.1fMB indexed=%.1fMB", base, afterPlain, afterIndexed)
	// The corpora above measure once; keep the harness loop trivial.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
	}
}

func BenchmarkTrigramSearchContains(b *testing.B) {
	_, table := setupTrigramBenchTable(b, 5000, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := table.SearchTrigram("Body", "replica set", trigram.Contains, trigram.Options{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrigramSearchSimilar(b *testing.B) {
	_, table := setupTrigramBenchTable(b, 5000, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := table.SearchTrigram("Body", "databse", trigram.Similar, trigram.Options{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrigramSearchFuzzyIndexed(b *testing.B) {
	_, table := setupTrigramBenchTable(b, 5000, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := table.SearchTrigram("Body", "replaca", trigram.Fuzzy, trigram.Options{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrigramSearchSimilarSelective(b *testing.B) {
	_, table := setupTrigramBenchTable(b, 5000, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := table.SearchTrigram("Body", "xyzzyspoon", trigram.Similar, trigram.Options{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrigramSearchFuzzySelective(b *testing.B) {
	_, table := setupTrigramBenchTable(b, 5000, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := table.SearchTrigram("Body", "xyzzyspooon", trigram.Fuzzy, trigram.Options{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrigramSearchFuzzyScanFallback(b *testing.B) {
	_, table := setupTrigramBenchTable(b, 5000, 2048)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := table.SearchTrigram("Body", "cat", trigram.Fuzzy, trigram.Options{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStringContainsScanBaseline(b *testing.B) {
	_, table := setupTrigramBenchTable(b, 5000, 2048)
	body := SF[trigramBenchDoc](table, "Body")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := table.Where(body.Contains("replica set")).Count(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLikeScanBaseline(b *testing.B) {
	_, table := setupTrigramBenchTable(b, 5000, 2048)
	body := SF[trigramBenchDoc](table, "Body")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := table.Where(body.Like("%replica set%")).Count(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTrigramDeleteHotPosting times deletes whose trigrams sit in
// ~5000-key postings; every op deletes a distinct pre-inserted row.
func BenchmarkTrigramDeleteHotPosting(b *testing.B) {
	db := New()
	defer db.Close()
	table, err := Register[trigramBenchDoc](db)
	if err != nil {
		b.Fatal(err)
	}
	rows := make([]*trigramBenchDoc, 0, b.N)
	for i := 0; i < b.N; i++ {
		rows = append(rows, &trigramBenchDoc{ID: fmt.Sprintf("hot%06d", i), Body: "the shared database replica set " + trigramBenchBody(i, 512)})
	}
	for i := 0; i < len(rows); i += 500 {
		end := i + 500
		if end > len(rows) {
			end = len(rows)
		}
		if err := table.InsertMany(rows[i:end]); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := table.Delete(fmt.Sprintf("hot%06d", i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPlainDeleteBaseline times deletes without any trigram index,
// isolating the index cost in BenchmarkTrigramDeleteHotPosting.
func BenchmarkPlainDeleteBaseline(b *testing.B) {
	db := New()
	defer db.Close()
	table, err := Register[trigramBenchPlain](db)
	if err != nil {
		b.Fatal(err)
	}
	rows := make([]*trigramBenchPlain, 0, b.N)
	for i := 0; i < b.N; i++ {
		rows = append(rows, &trigramBenchPlain{ID: fmt.Sprintf("hot%06d", i), Body: "the shared database replica set " + trigramBenchBody(i, 512)})
	}
	for i := 0; i < len(rows); i += 500 {
		end := i + 500
		if end > len(rows) {
			end = len(rows)
		}
		if err := table.InsertMany(rows[i:end]); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := table.Delete(fmt.Sprintf("hot%06d", i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTrigramBulkLoad compares bulk insert with and without the
// trigram index maintained per row.
func BenchmarkTrigramBulkLoad(b *testing.B) {
	b.Run("indexed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db := New()
			table, err := Register[trigramBenchDoc](db)
			if err != nil {
				b.Fatal(err)
			}
			rows := make([]*trigramBenchDoc, 0, 200)
			for j := 0; j < 200; j++ {
				rows = append(rows, &trigramBenchDoc{ID: fmt.Sprintf("b%d-%d", i, j), Body: trigramBenchBody(j, 1024)})
			}
			b.StartTimer()
			if err := table.InsertMany(rows); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			db.Close()
		}
	})
	b.Run("plain", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db := New()
			table, err := Register[trigramBenchPlain](db)
			if err != nil {
				b.Fatal(err)
			}
			rows := make([]*trigramBenchPlain, 0, 200)
			for j := 0; j < 200; j++ {
				rows = append(rows, &trigramBenchPlain{ID: fmt.Sprintf("b%d-%d", i, j), Body: trigramBenchBody(j, 1024)})
			}
			b.StartTimer()
			if err := table.InsertMany(rows); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			db.Close()
		}
	})
}
