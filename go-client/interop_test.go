package murmur

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestDirectRustPeer(t *testing.T) {
	fixture := os.Getenv("MURMUR_RUST_FIXTURE")
	if fixture == "" {
		suffix := ""
		if runtime.GOOS == "windows" {
			suffix = ".exe"
		}
		fixture = filepath.Join("native", "target", "debug", "examples", "peer_fixture"+suffix)
	}
	if _, err := os.Stat(fixture); err != nil {
		t.Skip("build the Rust peer fixture: cargo build --manifest-path native/Cargo.toml --example peer_fixture")
	}
	ids := []NodeID{{1}, {2}}
	configs := clusterConfigs(t, ids...)
	database := NodeID{44}
	dir := t.TempDir()
	configuration := filepath.Join(dir, "rust.json")
	raw, err := json.Marshal(map[string]any{"path": filepath.Join(dir, "rust-db"), "key": Bytes(testKey()), "database": database, "node": ids[0], "config": configs[0]})
	must(t, err)
	must(t, os.WriteFile(configuration, raw, 0600))
	command := exec.Command(fixture, configuration)
	input, err := command.StdinPipe()
	must(t, err)
	output, err := command.StdoutPipe()
	must(t, err)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	must(t, command.Start())
	t.Cleanup(func() {
		_ = input.Close()
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	scanner := bufio.NewScanner(output)
	address := make(chan string, 1)
	go func() {
		if scanner.Scan() {
			address <- scanner.Text()
		} else {
			address <- ""
		}
	}()
	var peerAddress string
	select {
	case peerAddress = <-address:
		if peerAddress == "" {
			t.Fatalf("fixture startup: %s", stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fixture startup timeout")
	}
	goDB := openNetworkDB(t, filepath.Join(dir, "go-db"), configs[1], database, ids[1])
	must(t, goDB.Migrate(NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")))
	must(t, goDB.SetSearchIndexes([]SearchIndex{FTS5Index("docs_fts", "docs", "title")}))
	replication, err := goDB.Replication()
	must(t, err)
	defer replication.Release()
	must(t, replication.AddPeer(peerAddress))
	waitFor(t, func() bool {
		r, err := goDB.Query("SELECT title FROM docs WHERE id=900")
		return err == nil && len(r.Rows) == 1 && r.Rows[0][0].Text == "Written by Rust"
	})
	_, err = goDB.Exec("INSERT INTO docs VALUES(901,'Written by Go')")
	must(t, err)
	must(t, goDB.Flush())
	waitFor(t, func() bool {
		_, err := input.Write([]byte("count\n"))
		if err != nil {
			return false
		}
		if !scanner.Scan() {
			return false
		}
		return strings.TrimSpace(scanner.Text()) == "2"
	})
	must(t, goDB.RefreshIndexes())
	results, err := goDB.Query("SELECT COUNT(*) FROM docs_fts WHERE docs_fts MATCH 'Written'")
	must(t, err)
	if results.Rows[0][0].Integer != 2 {
		t.Fatal(results)
	}
	_, err = input.Write([]byte("stop\n"))
	must(t, err)
	must(t, input.Close())
	must(t, command.Wait())
}
