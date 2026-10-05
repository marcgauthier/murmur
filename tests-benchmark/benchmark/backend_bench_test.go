package benchmark

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
	_ "modernc.org/sqlite"
)

// BenchmarkQueryMaterialization measures point and range lookups against the
// in-memory SQL view rebuilt by the replicated database.
func BenchmarkQueryMaterialization(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			tmpl := templateFor(b, n)
			dest := b.TempDir()
			if err := copyDir(tmpl.dir, dest); err != nil {
				b.Fatal(err)
			}
			cfg := benchConfig(dest, tmpl.node, tmpl.dbid)
			db, err := murmur.Open(context.Background(), cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = db.Close() })
			ctx := context.Background()
			rng := rand.New(rand.NewSource(21))
			var lat latency
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				var rows *murmur.Rows
				var err error
				if i%2 == 0 {
					rows, err = db.QueryContext(ctx,
						`SELECT name, phone, score FROM contacts WHERE id = ?`, tmpl.ids[rng.Intn(len(tmpl.ids))][:])
				} else {
					lo := (i * 131) % 900
					rows, err = db.QueryContext(ctx,
						`SELECT id FROM contacts WHERE score BETWEEN ? AND ? LIMIT 100`, lo, lo+100)
				}
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

// BenchmarkSQLiteBaseline runs the same point/range lookups against stock
// SQLite (modernc, in-memory) over an identically seeded dataset: the
// no-replication-stack reference point.
func BenchmarkSQLiteBaseline(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, err := sql.Open("sqlite", ":memory:")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = db.Close() })
			ctx := context.Background()
			for _, ddl := range []string{
				`CREATE TABLE contacts (id BLOB PRIMARY KEY, name TEXT, phone TEXT, score INTEGER)`,
				`CREATE INDEX idx_contacts_name ON contacts(name)`,
				`CREATE INDEX idx_contacts_score ON contacts(score)`,
			} {
				if _, err := db.ExecContext(ctx, ddl); err != nil {
					b.Fatal(err)
				}
			}
			rng := rand.New(rand.NewSource(42))
			ids := make([][]byte, 0, n)
			for i := 0; i < n; i++ {
				id := make([]byte, 16)
				if _, err := rng.Read(id); err != nil {
					b.Fatal(err)
				}
				ids = append(ids, id)
				name := fmt.Sprintf("%s %s %d", firstNames[i%len(firstNames)], lastNames[(i/len(firstNames))%len(lastNames)], i)
				if _, err := db.ExecContext(ctx,
					`INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)`,
					id, name, fmt.Sprintf("555-%04d", i%10000), rng.Intn(1000)); err != nil {
					b.Fatal(err)
				}
			}
			qrng := rand.New(rand.NewSource(23))
			var lat latency
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				var rows *sql.Rows
				var err error
				if i%2 == 0 {
					rows, err = db.QueryContext(ctx,
						`SELECT name, phone, score FROM contacts WHERE id = ?`, ids[qrng.Intn(len(ids))])
				} else {
					lo := (i * 131) % 900
					rows, err = db.QueryContext(ctx,
						`SELECT id FROM contacts WHERE score BETWEEN ? AND ? LIMIT 100`, lo, lo+100)
				}
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var a, c any
					var sc any
					if i%2 == 0 {
						_ = rows.Scan(&a, &c, &sc)
					} else {
						_ = rows.Scan(&a)
					}
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					b.Fatal(err)
				}
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}
