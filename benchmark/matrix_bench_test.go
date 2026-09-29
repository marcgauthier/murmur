package benchmark

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	replicateddb "github.com/marcgauthier/spedsql"
)

// BenchmarkCipherMatrix measures single-cell update throughput across
// cipher x block-compression x value-compressibility combinations on a
// fixed 2K-row dataset, reporting on-disk bytes alongside latency so
// encrypted-container overhead and compression effects compare directly.
// All combinations run with encryption enabled (production configuration).
func BenchmarkCipherMatrix(b *testing.B) {
	ciphers := []struct {
		name string
		alg  replicateddb.EncryptionAlgorithm
	}{
		{"aes256gcm", replicateddb.AES256GCM},
		{"chacha20", replicateddb.ChaCha20Poly1305},
		{"aegis256", replicateddb.AEGIS256},
	}
	compression := []struct {
		name string
		cfg  replicateddb.CompressionConfig
	}{
		{"zstd3", replicateddb.CompressionConfig{Algorithm: replicateddb.CompressionZstd, ZstdLevel: 3}},
		{"none", replicateddb.CompressionConfig{Algorithm: replicateddb.CompressionNone}},
	}
	values := []struct {
		name string
		blob bool // random bytes vs compressible text
	}{
		{"text", false},
		{"random", true},
	}
	const n = 2000
	for _, c := range ciphers {
		for _, comp := range compression {
			for _, v := range values {
				name := fmt.Sprintf("%s/%s/%s", c.name, comp.name, v.name)
				b.Run(name, func(b *testing.B) {
					ctx := context.Background()
					cfg := benchConfig(b.TempDir(), replicateddb.NewNodeID(), replicateddb.NewDBID())
					cfg.Encryption.Algorithm = c.alg
					cfg.Pebble.Compression = comp.cfg
					db, err := replicateddb.Open(ctx, cfg)
					if err != nil {
						b.Fatal(err)
					}
					defer db.Close()
					ids := populateValues(b, db, n, v.blob)
					populateBytes := dirBytes(b, cfg.Path)
					var lat latency
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						start := time.Now()
						if _, err := db.ExecContext(ctx,
							`UPDATE contacts SET phone = ? WHERE id = ?`,
							matrixValue(v.blob, i), ids[i%len(ids)][:]); err != nil {
							b.Fatal(err)
						}
						lat.record(time.Since(start))
					}
					lat.report(b, 1, "ops")
					b.ReportMetric(float64(populateBytes)/1e6, "populate-dir-MB")
					b.ReportMetric(float64(dirBytes(b, cfg.Path))/1e6, "final-dir-MB")
				})
			}
		}
	}
}

// populateValues inserts n contacts with compressible text or random
// 64-byte hex values, returning row IDs.
func populateValues(b *testing.B, db *replicateddb.DB, n int, blob bool) []replicateddb.RowID {
	b.Helper()
	ctx := context.Background()
	ids := make([]replicateddb.RowID, 0, n)
	const perTx = 1000
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
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)`,
				id[:], fmt.Sprintf("matrix %d", i), matrixValue(blob, i), i%1000); err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	return ids
}

func matrixValue(blob bool, i int) string {
	if !blob {
		return fmt.Sprintf("555-%04d", i%10000)
	}
	var raw [64]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

// dirBytes sums regular file sizes under root.
func dirBytes(b *testing.B, root string) int64 {
	b.Helper()
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return total
}
