// Package benchmark holds the repeatable performance suite (PLAN section 59-61).
//
// Dataset sizes: 10K rows always; 100K rows unless -short; 1M rows when
// REPLICATEDDB_BENCH_ROWS=1000000. Run with:
//
//	go test ./benchmark/ -bench . -benchtime 2s
//	go test ./benchmark/ -bench . -short          # 10K datasets only
package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	replicateddb "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/crypto"
	"github.com/nomadsql/replicateddb/schema"
)

// randNew is math/rand.New exposed for the sync benchmarks (which seed
// identically so datasets match across benchmark kinds).
func randNew(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

var firstNames = []string{"ann", "bob", "cid", "dan", "eve", "fin", "gus", "hal", "ivy", "jay", "kay", "leo", "max", "ned", "oda", "pam", "quin", "ray", "sue", "tim"}
var lastNames = []string{"smith", "jones", "taylor", "brown", "davies", "evans", "wilson", "thomas", "taylor2", "moore"}

// benchSchema is the benchmark schema: a contacts table plus an orders table
// for join/group workloads.
func benchSchema() []schema.TableSchema {
	return []schema.TableSchema{
		{
			Name: "contacts",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
				{Name: "phone", Type: schema.ColText, Nullable: true},
				{Name: "score", Type: schema.ColInteger, Nullable: true},
			},
		},
		{
			Name: "orders",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "contact_id", Type: schema.ColBlob, Nullable: true},
				{Name: "amount", Type: schema.ColInteger, Nullable: true},
			},
		},
	}
}

// benchLocalDDL holds local-only indexes and a derived FTS index with
// maintenance triggers (rebuilt, never replicated).
func benchLocalDDL() []string {
	return []string{
		`CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name)`,
		`CREATE INDEX IF NOT EXISTS idx_contacts_score ON contacts(score)`,
		`CREATE INDEX IF NOT EXISTS idx_orders_contact ON orders(contact_id)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS contacts_fts USING fts5(name, phone)`,
		`CREATE TRIGGER IF NOT EXISTS contacts_ai AFTER INSERT ON contacts BEGIN
			INSERT INTO contacts_fts(rowid, name, phone) VALUES (new.rowid, new.name, new.phone); END`,
		`CREATE TRIGGER IF NOT EXISTS contacts_ad AFTER DELETE ON contacts BEGIN
			DELETE FROM contacts_fts WHERE rowid = old.rowid; END`,
		`CREATE TRIGGER IF NOT EXISTS contacts_au AFTER UPDATE OF name, phone ON contacts BEGIN
			UPDATE contacts_fts SET name = new.name, phone = new.phone WHERE rowid = new.rowid; END`,
	}
}

// datasetSizes returns the row counts to benchmark.
func datasetSizes(b *testing.B) []int {
	if v := os.Getenv("REPLICATEDDB_BENCH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return []int{n}
		}
	}
	if testing.Short() {
		return []int{10_000}
	}
	return []int{10_000, 100_000}
}

func sizeName(n int) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%dm", n/1_000_000)
	}
	return fmt.Sprintf("%dk", n/1_000)
}

// benchKey is the fixed benchmark storage key (explicit encryption is
// mandatory and part of the measured production configuration).
var benchKey = bytes.Repeat([]byte{0x62}, 32)

// benchProvider returns the storage-key provider matching benchConfig:
// key "bench" under the default AES-256-GCM write algorithm. Direct
// registry opens must use it (an empty algorithm derives a different
// KEK and fails authentication).
func benchProvider() *crypto.MapProvider {
	return &crypto.MapProvider{
		Keys:      map[string][]byte{"bench": benchKey},
		CurrentID: "bench",
		Algorithm: crypto.DefaultAlgorithm,
	}
}

// benchConfig returns the standard single-node benchmark configuration:
// benchmark schema with local indexes/FTS plus explicit encryption and
// database identity.
func benchConfig(path string, node replicateddb.NodeID, dbid replicateddb.DBID) replicateddb.Config {
	return replicateddb.Config{
		Path:   path,
		NodeID: node,
		DBID:   dbid,
		Schema: replicateddb.SchemaConfig{
			Version: 1, Tables: benchSchema(), LocalDDL: benchLocalDDL(),
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key: bytes.Clone(benchKey), KeyID: "bench",
		},
	}
}

// openBenchDB opens a fresh single-node database.
func openBenchDB(b *testing.B, path string) *replicateddb.DB {
	b.Helper()
	db, err := replicateddb.Open(context.Background(), benchConfig(path, replicateddb.NewNodeID(), replicateddb.NewDBID()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// populate inserts n contacts (+2 orders each) in 5000-row transactions
// (sized under MaxBatchMutations/MaxTransactionBytes for fast setup).
// It returns the contact row IDs for point lookups.
func populate(b *testing.B, db *replicateddb.DB, n int) []replicateddb.RowID {
	b.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	ids := make([]replicateddb.RowID, 0, n)
	const perTx = 5000
	for base := 0; base < n; base += perTx {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		end := base + perTx
		if end > n {
			end = n
		}
		for i := base; i < end; i++ {
			id := replicateddb.NewRowID()
			ids = append(ids, id)
			name := fmt.Sprintf("%s %s %d", firstNames[i%len(firstNames)], lastNames[(i/len(firstNames))%len(lastNames)], i)
			phone := fmt.Sprintf("555-%04d", i%10000)
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)`,
				id[:], name, phone, rng.Intn(1000)); err != nil {
				b.Fatal(err)
			}
			for o := 0; o < 2; o++ {
				oid := replicateddb.NewRowID()
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO orders (id, contact_id, amount) VALUES (?, ?, ?)`,
					oid[:], id[:], rng.Intn(500)); err != nil {
					b.Fatal(err)
				}
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	return ids
}

// drainRows fully consumes and closes rows (read benchmarks must do this;
// an open Rows stalls writers).
func drainRows(b *testing.B, rows *replicateddb.Rows) int {
	b.Helper()
	defer rows.Close()
	n := 0
	cols := rows.Columns()
	dest := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range dest {
		ptrs[i] = &dest[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			b.Fatal(err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
	return n
}
