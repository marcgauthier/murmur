package cmd

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/tool/format"
	"golang.org/x/crypto/chacha20poly1305"
)

type BenchCommand struct{}

func (c *BenchCommand) Name() string { return "bench" }
func (c *BenchCommand) Description() string {
	return "Benchmark encryption ciphers and typed RIME storage"
}
func (c *BenchCommand) Usage() string {
	return "murmur bench [--duration=3s] [--json]"
}

func init() {
	Register(&BenchCommand{})
}

type benchResult struct {
	AES256GCM_MBs      float64 `json:"aes_256_gcm_mb_per_sec"`
	ChaCha20_MBs       float64 `json:"chacha20_poly1305_mb_per_sec"`
	SHA256_MBs         float64 `json:"sha256_mb_per_sec"`
	RecordWritesPerSec float64 `json:"rime_record_writes_per_sec"`
	RecordReadsPerSec  float64 `json:"rime_record_reads_per_sec"`
}

type benchRecord struct {
	ID  ids.RowID `rime:"primary"`
	Val string
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

	// 4. Benchmark durable typed record writes and point reads.
	tempDir, err := os.MkdirTemp("", "murmur-bench-*")
	if err != nil {
		return fmt.Errorf("create temp bench dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	definition, err := murmur.Define[benchRecord]("bench", 1, murmur.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Val": 2},
	})
	if err != nil {
		return fmt.Errorf("define benchmark record: %w", err)
	}
	nodeID := murmur.NewNodeID()
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate benchmark signing key: %w", err)
	}
	trustedKeys, err := origin.NewKeyRegistry(nil)
	if err != nil {
		return fmt.Errorf("create benchmark origin registry: %w", err)
	}
	if err := trustedKeys.Add(nodeID, signingKey.Public().(ed25519.PublicKey)); err != nil {
		return fmt.Errorf("register benchmark origin key: %w", err)
	}
	db, err := murmur.Open(ctx, murmur.Config{
		Path:   filepath.Join(tempDir, "data"),
		NodeID: nodeID,
		Schema: murmur.SchemaConfig{Version: 1},
		Tables: []murmur.TableDefinition{definition},
		Spool:  murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{
			Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "benchmark-key",
		},
		OriginSigning: murmur.OriginSigningConfig{PrivateKey: signingKey, TrustedKeys: trustedKeys},
	})
	if err != nil {
		return fmt.Errorf("open benchmark database: %w", err)
	}
	defer db.Close()
	bench, err := murmur.TableOf[benchRecord](db, "bench")
	if err != nil {
		return fmt.Errorf("open benchmark table: %w", err)
	}

	insertStart := time.Now()
	var insertCount int64
	var lastID ids.RowID
	for time.Since(insertStart) < duration {
		batch := make([]*benchRecord, 200)
		for i := range batch {
			batch[i] = &benchRecord{ID: murmur.NewRowID(), Val: "bench-payload-val-1234567890"}
		}
		if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
			return bench.InsertMany(tx, batch)
		}); err != nil {
			return fmt.Errorf("write benchmark batch: %w", err)
		}
		insertCount += int64(len(batch))
		lastID = batch[len(batch)-1].ID
	}
	insertsPerSec := float64(insertCount) / time.Since(insertStart).Seconds()

	readStart := time.Now()
	var readCount int64
	for time.Since(readStart) < duration {
		readCount++
		if _, err := bench.Get(lastID); err != nil {
			return fmt.Errorf("read benchmark record: %w", err)
		}
	}
	readsPerSec := float64(readCount) / time.Since(readStart).Seconds()

	res := benchResult{
		AES256GCM_MBs:      aesMBs,
		ChaCha20_MBs:       chachaMBs,
		SHA256_MBs:         shaMBs,
		RecordWritesPerSec: insertsPerSec,
		RecordReadsPerSec:  readsPerSec,
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, res, true)
	}

	fmt.Fprintln(stdout, "=== Murmur Benchmark Results ===")
	format.RenderKV(stdout, [][2]string{
		{"SHA-256 Throughput", fmt.Sprintf("%.2f MB/s", shaMBs)},
		{"AES-256-GCM Throughput", fmt.Sprintf("%.2f MB/s", aesMBs)},
		{"ChaCha20-Poly1305", fmt.Sprintf("%.2f MB/s", chachaMBs)},
		{"RIME Durable Record Writes", fmt.Sprintf("%.2f records/sec", insertsPerSec)},
		{"RIME Point Reads", fmt.Sprintf("%.2f reads/sec", readsPerSec)},
	})

	return nil
}
