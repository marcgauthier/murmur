// backupagent is a suite-local third mesh member for the backup-under-fire
// live suite. It joins the daemon mesh over QUIC replication, converges on
// the live data, and takes online (zero-downtime) backups on stdin command
// while the mesh stays under write load. One throttled backup mode slows
// the archive stream so the parent can SIGKILL the agent mid-backup and
// prove the partial artifact is rejected.
//
// Protocol (stdin commands, stdout responses, one line each):
//
//	WAIT <rows> <timeout_s>  -> WAIT_OK <rows> | WAIT_TIMEOUT <rows>
//	COUNT                   -> COUNT <rows>
//	BACKUP <subdir> <chunk_bytes> <sleep_ms>
//	                        -> BACKUP_OK <name> | BACKUP_ERR <msg>
//	EXIT                    -> BYE (exit 0)
package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/origin"
)

var out = bufio.NewWriter(os.Stdout)

func emit(format string, args ...any) {
	fmt.Fprintf(out, format+"\n", args...)
	_ = out.Flush()
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "backupagent: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	pebbleDir := flag.String("pebble-dir", "", "agent data directory")
	nodeIDS := flag.String("node-id", "", "agent node ID (UUID text)")
	dbIDS := flag.String("db-id", "", "cluster DBID (UUID text)")
	keyHex := flag.String("key-hex", "", "hex-encoded 32-byte storage key")
	keyID := flag.String("key-id", "", "storage key ID")
	schemaFile := flag.String("schema", "", "db.SchemaConfig JSON file")
	replAddr := flag.String("repl-addr", "", "QUIC replication listen addr")
	peersFlag := flag.String("peers", "", "comma-separated nodeID=addr peers")
	caFile := flag.String("ca", "", "cluster CA PEM file")
	certFile := flag.String("cert", "", "agent node cert PEM file")
	keyFile := flag.String("key", "", "agent node key PEM file")
	originKeyFile := flag.String("origin-key", "", "Ed25519 private key file")
	originKeysFile := flag.String("origin-public-keys", "", "explicit NodeID/public key JSON registry")
	table := flag.String("table", "", "table for WAIT/COUNT readiness")
	flag.Parse()

	nodeID, err := db.ParseNodeID(*nodeIDS)
	if err != nil {
		fail("parse node id: %v", err)
	}
	dbID, err := db.ParseDBID(*dbIDS)
	if err != nil {
		fail("parse db id: %v", err)
	}
	key, err := hex.DecodeString(*keyHex)
	if err != nil {
		fail("decode key: %v", err)
	}
	schemaRaw, err := os.ReadFile(*schemaFile)
	if err != nil {
		fail("read schema: %v", err)
	}
	var schemaCfg db.SchemaConfig
	if err := json.Unmarshal(schemaRaw, &schemaCfg); err != nil {
		fail("parse schema: %v", err)
	}
	caPEM, err := os.ReadFile(*caFile)
	if err != nil {
		fail("read ca: %v", err)
	}
	certPEM, err := os.ReadFile(*certFile)
	if err != nil {
		fail("read cert: %v", err)
	}
	keyPEM, err := os.ReadFile(*keyFile)
	if err != nil {
		fail("read key: %v", err)
	}
	var peers []db.Peer
	for _, p := range strings.Split(*peersFlag, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		idS, addr, ok := strings.Cut(p, "=")
		if !ok {
			fail("bad peer %q, want nodeID=addr", p)
		}
		id, err := db.ParseNodeID(idS)
		if err != nil {
			fail("parse peer id: %v", err)
		}
		peers = append(peers, db.Peer{NodeID: id, Addrs: []string{addr}})
	}
	originKey, err := os.ReadFile(*originKeyFile)
	if err != nil {
		fail("read signing key: %v", err)
	}
	originKeysRaw, err := os.ReadFile(*originKeysFile)
	if err != nil {
		fail("read origin keys: %v", err)
	}
	var encodedKeys map[string]string
	if err := json.Unmarshal(originKeysRaw, &encodedKeys); err != nil {
		fail("parse origin keys: %v", err)
	}
	keys := make(map[db.NodeID]ed25519.PublicKey)
	for textID, textKey := range encodedKeys {
		id, err := db.ParseNodeID(textID)
		if err != nil {
			fail("parse origin: %v", err)
		}
		key, err := hex.DecodeString(textKey)
		if err != nil {
			fail("parse origin key: %v", err)
		}
		keys[id] = key
	}
	registry, err := origin.NewKeyRegistry(keys)
	if err != nil {
		fail("origin registry: %v", err)
	}
	var snapshotSources []db.NodeID
	for _, peer := range peers {
		snapshotSources = append(snapshotSources, peer.NodeID)
	}

	ctx := context.Background()
	handle, err := db.Open(ctx, db.Config{
		OriginSigning: db.OriginSigningConfig{PrivateKey: originKey, TrustedKeys: registry},
		Path:          *pebbleDir,
		NodeID:        nodeID,
		DBID:          dbID,
		Schema:        schemaCfg,
		Pebble:        db.DefaultPebbleConfig(),
		Encryption: db.EncryptionConfig{
			Key:   key,
			KeyID: *keyID,
		},
		Replication: db.ReplicationConfig{
			TrustedSnapshotSources: snapshotSources,
			ListenAddr:             *replAddr,
			TLS: &db.TLSCredential{
				CertPEM: certPEM,
				KeyPEM:  keyPEM,
				CAPEM:   caPEM,
			},
			Peers: peers,
			// Fast test intervals, mirroring the testnode daemon.
			SendInterval: 15 * time.Millisecond,
			DialInterval: 50 * time.Millisecond,
			AckInterval:  50 * time.Millisecond,
		},
	})
	if err != nil {
		emit("BOOT_ERR %v", err)
		_ = out.Flush()
		fail("open: %v", err)
	}
	defer handle.Close()
	emit("READY")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 64*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		cmd, arg, _ := strings.Cut(line, " ")
		switch cmd {
		case "WAIT":
			var want int
			var timeoutS int
			if _, err := fmt.Sscanf(arg, "%d %d", &want, &timeoutS); err != nil {
				emit("WAIT_ERR bad args %q", arg)
				continue
			}
			got := waitRows(ctx, handle, *table, want, time.Duration(timeoutS)*time.Second)
			if got >= want {
				emit("WAIT_OK %d", got)
			} else {
				emit("WAIT_TIMEOUT %d", got)
			}
		case "COUNT":
			emit("COUNT %d", countRows(ctx, handle, *table))
		case "BACKUP":
			var subdir string
			var chunk, sleepMs int
			if _, err := fmt.Sscanf(arg, "%s %d %d", &subdir, &chunk, &sleepMs); err != nil {
				emit("BACKUP_ERR bad args %q", arg)
				continue
			}
			name, err := takeBackup(ctx, handle, subdir, chunk, sleepMs)
			if err != nil {
				emit("BACKUP_ERR %v", oneLine(err.Error()))
				continue
			}
			emit("BACKUP_OK %s", name)
		case "EXIT":
			emit("BYE")
			return
		default:
			emit("ERR unknown command %q", cmd)
		}
	}
	if err := scanner.Err(); err != nil {
		fail("stdin: %v", err)
	}
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 400 {
		s = s[:400]
	}
	return s
}

func countRows(ctx context.Context, handle *db.DB, table string) int {
	rows, err := handle.QueryContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table))
	if err != nil {
		return -1
	}
	defer rows.Close()
	if !rows.Next() {
		return -1
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		return -1
	}
	return n
}

func waitRows(ctx context.Context, handle *db.DB, table string, want int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	got := 0
	for time.Now().Before(deadline) {
		if n := countRows(ctx, handle, table); n > got {
			got = n
		}
		if got >= want {
			return got
		}
		select {
		case <-ctx.Done():
			return got
		case <-time.After(100 * time.Millisecond):
		}
	}
	return got
}

func takeBackup(ctx context.Context, handle *db.DB, subdir string, chunk, sleepMs int) (string, error) {
	dest, err := backup.NewLocalDestination(subdir)
	if err != nil {
		return "", err
	}
	var out backup.Destination = dest
	if chunk > 0 && sleepMs > 0 {
		out = &throttleDest{inner: dest, chunk: chunk, sleep: time.Duration(sleepMs) * time.Millisecond}
	}
	if _, err := handle.Backup(ctx, backup.Config{Destination: out}); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(subdir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tar.gz") {
			return e.Name(), nil
		}
	}
	return "", fmt.Errorf("no archive appeared in %s", subdir)
}

// throttleDest slows the archive stream so the parent can deterministically
// SIGKILL mid-backup: every chunk bytes streamed sleeps once.
type throttleDest struct {
	inner backup.Destination
	chunk int
	sleep time.Duration
}

func (d *throttleDest) Type() string { return d.inner.Type() }

func (d *throttleDest) WriteBackup(ctx context.Context, name string, r io.Reader, sizeHint int64) error {
	return d.inner.WriteBackup(ctx, name, &throttleReader{r: r, chunk: d.chunk, sleep: d.sleep}, sizeHint)
}

func (d *throttleDest) ReadBackup(ctx context.Context, name string) (io.ReadCloser, error) {
	return d.inner.ReadBackup(ctx, name)
}

func (d *throttleDest) ListBackups(ctx context.Context, dbID string) ([]backup.BackupInfo, error) {
	return d.inner.ListBackups(ctx, dbID)
}

func (d *throttleDest) DeleteBackup(ctx context.Context, name string) error {
	return d.inner.DeleteBackup(ctx, name)
}

type throttleReader struct {
	r     io.Reader
	chunk int
	sleep time.Duration
}

func (t *throttleReader) Read(p []byte) (int, error) {
	if len(p) > t.chunk {
		p = p[:t.chunk]
	}
	n, err := t.r.Read(p)
	if n > 0 {
		time.Sleep(t.sleep)
	}
	return n, err
}
