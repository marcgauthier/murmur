package cmd

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur/tool/format"
	"golang.org/x/crypto/chacha20poly1305"
)

type BenchCommand struct{}

func (c *BenchCommand) Name() string        { return "bench" }
func (c *BenchCommand) Description() string { return "Benchmark storage I/O, encryption ciphers, and SQLite speed" }
func (c *BenchCommand) Usage() string {
	return "murmur bench [--duration=3s] [--json]"
}

func init() {
	Register(&BenchCommand{})
}

type benchResult struct {
	AES256GCM_MBs    float64 `json:"aes_256_gcm_mb_per_sec"`
	ChaCha20_MBs     float64 `json:"chacha20_poly1305_mb_per_sec"`
	SHA256_MBs       float64 `json:"sha256_mb_per_sec"`
	SQLInsertsPerSec float64 `json:"sql_inserts_per_sec"`
	SQLReadsPerSec   float64 `json:"sql_reads_per_sec"`
}

func (c *BenchCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	duration := 2 * time.Second

	for _, arg := range args {
		if strings.HasPrefix(arg, "--duration=") {
			dStr := strings.TrimPrefix(arg, "--duration=")
			if d, err := time.ParseDuration(dStr); err == nil {
				duration = d
			} else if sec, err := strconv.Atoi(dStr); err == nil {
				duration = time.Duration(sec) * time.Second
			}
		}
	}

	if !globalOpts.JSON {
		fmt.Fprintf(stdout, "Running Murmur Performance Benchmarks (duration per test: %s)...\n", duration)
	}

	// 1. Benchmark SHA-256
	buf := make([]byte, 64*1024)
	_, _ = rand.Read(buf)

	shaStart := time.Now()
	var shaBytes int64
	for time.Since(shaStart) < duration {
		h := sha256.Sum256(buf)
		_ = h
		shaBytes += int64(len(buf))
	}
	shaMBs := float64(shaBytes) / (1024 * 1024) / time.Since(shaStart).Seconds()

	// 2. Benchmark AES-256-GCM
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	nonce := make([]byte, gcm.NonceSize())
	dst := make([]byte, 0, len(buf)+gcm.Overhead())

	aesStart := time.Now()
	var aesBytes int64
	for time.Since(aesStart) < duration {
		dst = gcm.Seal(dst[:0], nonce, buf, nil)
		aesBytes += int64(len(buf))
	}
	aesMBs := float64(aesBytes) / (1024 * 1024) / time.Since(aesStart).Seconds()

	// 3. Benchmark ChaCha20-Poly1305
	chacha, _ := chacha20poly1305.New(key)
	cNonce := make([]byte, chacha.NonceSize())
	cDst := make([]byte, 0, len(buf)+chacha.Overhead())

	chachaStart := time.Now()
	var chachaBytes int64
	for time.Since(chachaStart) < duration {
		cDst = chacha.Seal(cDst[:0], cNonce, buf, nil)
		chachaBytes += int64(len(buf))
	}
	chachaMBs := float64(chachaBytes) / (1024 * 1024) / time.Since(chachaStart).Seconds()

	// 4. Benchmark Pure-Go Murmur / SQLite Inserts & Reads
	tempDir, err := os.MkdirTemp("", "murmur-bench-*")
	if err != nil {
		return fmt.Errorf("create temp bench dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	db, err := openLocalDB(ctx, filepath.Join(tempDir, "data"), globalOpts, false)
	if err != nil {
		return fmt.Errorf("open benchmark database: %w", err)
	}
	defer db.Close()

	_, _ = db.ExecContext(ctx, "CREATE TABLE bench (id TEXT PRIMARY KEY, val TEXT);")

	insertStart := time.Now()
	var insertCount int64
	tx, _ := db.BeginTx(ctx, nil)
	for time.Since(insertStart) < duration {
		insertCount++
		_, _ = tx.ExecContext(ctx, "INSERT INTO bench VALUES (?, ?)", fmt.Sprintf("id-%d", insertCount), "bench-payload-val-1234567890")
		if insertCount%200 == 0 {
			_ = tx.Commit()
			tx, _ = db.BeginTx(ctx, nil)
		}
	}
	_ = tx.Commit()
	insertsPerSec := float64(insertCount) / time.Since(insertStart).Seconds()

	readStart := time.Now()
	var readCount int64
	for time.Since(readStart) < duration {
		readCount++
		var v string
		_ = db.QueryRowContext(ctx, "SELECT val FROM bench WHERE id = ?", fmt.Sprintf("id-%d", (readCount%insertCount)+1)).Scan(&v)
	}
	readsPerSec := float64(readCount) / time.Since(readStart).Seconds()

	res := benchResult{
		AES256GCM_MBs:    aesMBs,
		ChaCha20_MBs:     chachaMBs,
		SHA256_MBs:       shaMBs,
		SQLInsertsPerSec: insertsPerSec,
		SQLReadsPerSec:   readsPerSec,
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, res, true)
	}

	fmt.Fprintln(stdout, "=== Murmur Benchmark Results ===")
	format.RenderKV(stdout, [][2]string{
		{"SHA-256 Throughput", fmt.Sprintf("%.2f MB/s", shaMBs)},
		{"AES-256-GCM Throughput", fmt.Sprintf("%.2f MB/s", aesMBs)},
		{"ChaCha20-Poly1305", fmt.Sprintf("%.2f MB/s", chachaMBs)},
		{"SQL Batched Inserts", fmt.Sprintf("%.2f tx/sec", insertsPerSec)},
		{"SQL Point Reads", fmt.Sprintf("%.2f queries/sec", readsPerSec)},
	})

	return nil
}
