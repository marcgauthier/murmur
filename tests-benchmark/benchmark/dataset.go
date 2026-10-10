// Package benchmark holds the repeatable performance suite (PLAN section 59-61).
//
// Dataset sizes: 10K rows always; 100K rows unless -short; 1M rows when
// MURMUR_BENCH_ROWS=1000000. Run with:
//
//	go -C tests-benchmark/benchmark test -bench . -benchtime 2s
//	go -C tests-benchmark/benchmark test -bench . -short          # 10K datasets only
package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"github.com/marcgauthier/murmur/internal/testdb"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/ids"
)

// randNew is math/rand.New exposed for the sync benchmarks (which seed
// identically so datasets match across benchmark kinds).
func randNew(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

var firstNames = []string{"ann", "bob", "cid", "dan", "eve", "fin", "gus", "hal", "ivy", "jay", "kay", "leo", "max", "ned", "oda", "pam", "quin", "ray", "sue", "tim"}
var lastNames = []string{"smith", "jones", "taylor", "brown", "davies", "evans", "wilson", "thomas", "taylor2", "moore"}

// benchContact is the benchmark contacts record plus an orders record for
// join/group workloads.
type benchContact struct {
	ID    ids.RowID `rime:"primary"`
	Name  string
	Phone string
	Score int64
}

type benchOrder struct {
	ID        ids.RowID `rime:"primary"`
	ContactID ids.RowID
	Amount    int64
}

// mustBenchTables returns the compiled benchmark table definitions.
// Definition failures are programmer errors, so it panics.
func mustBenchTables() []murmur.TableDefinition {
	contacts, err := murmur.Model[benchContact](murmur.ModelOptions{
		Name: "contacts", TableID: 91,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Name": 2, "Phone": 3, "Score": 4},
		},
	})
	if err != nil {
		panic(err)
	}
	orders, err := murmur.Model[benchOrder](murmur.ModelOptions{
		Name: "orders", TableID: 92,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "ContactID": 2, "Amount": 3},
		},
	})
	if err != nil {
		panic(err)
	}
	return []murmur.TableDefinition{contacts, orders}
}

// datasetSizes returns the row counts to benchmark.
func datasetSizes(b *testing.B) []int {
	if v := os.Getenv("MURMUR_BENCH_ROWS"); v != "" {
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
// typed benchmark tables plus explicit encryption and database identity.
func benchConfig(path string, node murmur.NodeID, dbid murmur.DBID) murmur.Config {
	return testdb.Configure(murmur.Config{
		Path:   path,
		NodeID: node,
		DBID:   dbid,
		Tables: mustBenchTables(),
		Spool:  murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{
			Key: bytes.Clone(benchKey), KeyID: "bench",
		},
	})
}

// openBenchDB opens a fresh single-node database.
func openBenchDB(b *testing.B, path string) *murmur.DB {
	b.Helper()
	db, err := murmur.Open(context.Background(), benchConfig(path, murmur.NewNodeID(), murmur.NewDBID()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// populate inserts n contacts (+2 orders each) in 5000-row transactions
// (sized under MaxBatchMutations/MaxTransactionBytes for fast setup).
// It returns the contact row IDs for point lookups.
func populate(b testing.TB, db *murmur.DB, n int) []murmur.RowID {
	b.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	ids := make([]murmur.RowID, 0, n)
	const perTx = 5000
	for base := 0; base < n; base += perTx {
		tx, err := db.BeginTx(ctx)
		if err != nil {
			b.Fatal(err)
		}
		end := base + perTx
		if end > n {
			end = n
		}
		for i := base; i < end; i++ {
			id := murmur.NewRowID()
			ids = append(ids, id)
			name := fmt.Sprintf("%s %s %d", firstNames[i%len(firstNames)], lastNames[(i/len(firstNames))%len(lastNames)], i)
			phone := fmt.Sprintf("555-%04d", i%10000)
			if err := tx.InsertItem(&benchContact{
				ID: id, Name: name, Phone: phone, Score: int64(rng.Intn(1000)),
			}); err != nil {
				b.Fatal(err)
			}
			for o := 0; o < 2; o++ {
				if err := tx.InsertItem(&benchOrder{
					ID: murmur.NewRowID(), ContactID: id, Amount: int64(rng.Intn(500)),
				}); err != nil {
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
