// Command observability demonstrates slog-backed Murmur logs, status, and
// metrics with record writes.
//
// Run it with:
//
//	go run ./examples/observability
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
)

type event struct {
	ID   ids.RowID `rime:"primary"`
	Body string
}

type slogLogger struct{ log *slog.Logger }

func (l slogLogger) Debug(msg string, args ...any) { l.log.Debug(msg, args...) }
func (l slogLogger) Info(msg string, args ...any)  { l.log.Info(msg, args...) }
func (l slogLogger) Warn(msg string, args ...any)  { l.log.Warn(msg, args...) }
func (l slogLogger) Error(msg string, args ...any) { l.log.Error(msg, args...) }

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-observability-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	definition, err := murmur.Model[event](murmur.ModelOptions{
		Name: "events", TableID: 6,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path: dir, NodeID: murmur.NewNodeID(), Schema: murmur.SchemaConfig{Version: 1},
		Tables: []murmur.TableDefinition{definition}, Spool: murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "observability-key"},
		Logger:     slogLogger{slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		batch := make([]*event, 5)
		for i := range batch {
			batch[i] = &event{ID: murmur.NewRowID(), Body: fmt.Sprintf("event-%d", i)}
		}
		return tx.InsertMany(batch)
	}); err != nil {
		log.Fatal(err)
	}
	st := db.Status()
	fmt.Printf("status: state=%s localSeq=%d hlc=%d peers=%d\n", st.State, st.LocalSeq, st.HLC, st.ConnectedPeers)
	m := db.Metrics()
	fmt.Printf("metrics: localCommits=%d rebuilds=%d gcRuns=%d\n", m.LocalCommits, m.Rebuilds, m.GCRuns)
}
