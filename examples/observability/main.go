// Command observability shows the diagnostics surface: a slog-backed
// Logger receiving package logs, plus Status and Metrics snapshots
// after a small workload.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/observability
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	replicateddb "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
)

// slogLogger adapts the standard slog logger to replicateddb.Logger.
type slogLogger struct{ log *slog.Logger }

func (l slogLogger) Debug(msg string, args ...any) { l.log.Debug(msg, args...) }
func (l slogLogger) Info(msg string, args ...any)  { l.log.Info(msg, args...) }
func (l slogLogger) Warn(msg string, args ...any)  { l.log.Warn(msg, args...) }
func (l slogLogger) Error(msg string, args ...any) { l.log.Error(msg, args...) }

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-observability-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := replicateddb.Open(ctx, replicateddb.Config{
		Path:   dir,
		NodeID: replicateddb.NewNodeID(),
		Schema: replicateddb.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{{
				Name: "events",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "body", Type: schema.ColText, Nullable: true},
				},
			}},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "observability-key",
		},
		Logger: slogLogger{slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5; i++ {
		id := replicateddb.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO events (id, body) VALUES (?, ?)`,
			id[:], fmt.Sprintf("event-%d", i)); err != nil {
			log.Fatal(err)
		}
	}

	st := db.Status()
	fmt.Printf("status: state=%s localSeq=%d hlc=%d peers=%d\n",
		st.State, st.LocalSeq, st.HLC, st.ConnectedPeers)
	m := db.Metrics()
	fmt.Printf("metrics: localCommits=%d rebuilds=%d gcRuns=%d\n",
		m.LocalCommits, m.Rebuilds, m.GCRuns)
}
