// Command backup-restore demonstrates online backup and clone restore
// under a fresh writer identity.
//
// Run it with:
//
//	go run ./examples/backup-restore
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
)

type record struct {
	ID   ids.RowID `rime:"primary"`
	Body string
}

func definition() murmur.TableDefinition {
	d, err := murmur.Model[record](murmur.ModelOptions{
		Name: "records", TableID: 4,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	return d
}

func open(ctx context.Context, path string, nodeID murmur.NodeID) *murmur.DB {
	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path: path, NodeID: nodeID, Schema: murmur.SchemaConfig{Version: 1},
		Tables: []murmur.TableDefinition{definition()}, Spool: murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "backup-key"},
	}))
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "murmur-backup-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)

	source := open(ctx, filepath.Join(base, "node"), murmur.NewNodeID())
	if err := source.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		batch := make([]*record, 5)
		for i := range batch {
			batch[i] = &record{ID: murmur.NewRowID(), Body: fmt.Sprintf("record-%d", i)}
		}
		return tx.InsertMany(batch)
	}); err != nil {
		log.Fatal(err)
	}

	dest, err := backup.NewLocalDestination(filepath.Join(base, "backups"))
	if err != nil {
		log.Fatal(err)
	}
	meta, err := backup.CreateBackup(ctx, source, backup.Config{Destination: dest})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("backup %s at %s\n", meta.BackupID, meta.CreatedAt.Format("15:04:05"))
	if err := source.Close(); err != nil {
		log.Fatal(err)
	}

	fresh := murmur.NewNodeID()
	restored := filepath.Join(base, "restored")
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source: dest, TargetPath: restored, KeysPath: filepath.Join(restored, "keys"), FreshNodeID: fresh.String(),
	}); err != nil {
		log.Fatal(err)
	}
	clone := open(ctx, restored, fresh)
	defer clone.Close()
	n, err := clone.Count(ctx, record{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows in clone: %d\n", n)
	if n != 5 {
		log.Fatalf("want 5 restored rows, got %d", n)
	}
	if err := clone.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return tx.InsertItem(&record{ID: murmur.NewRowID(), Body: "post-restore"})
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("clone accepts fresh writes")
}
