// Command backup-restore shows online backup and clone restore: back up
// a live node to a local directory, restore under a fresh writer
// identity, and prove the clone carries every row and accepts writes.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/backup-restore
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	replicateddb "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/backup"
	"github.com/marcgauthier/spedsql/schema"
)

func schemaTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "records",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "body", Type: schema.ColText, Nullable: true},
		},
	}}
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "spedsql-backup-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(base)

	db, err := replicateddb.Open(ctx, replicateddb.Config{
		Path:   filepath.Join(base, "node"),
		NodeID: replicateddb.NewNodeID(),
		Schema: replicateddb.SchemaConfig{Version: 1, Tables: schemaTables()},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "backup-key",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		id := replicateddb.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO records (id, body) VALUES (?, ?)`,
			id[:], fmt.Sprintf("record-%d", i)); err != nil {
			log.Fatal(err)
		}
	}

	// Online backup while the node keeps running.
	dest, err := backup.NewLocalDestination(filepath.Join(base, "backups"))
	if err != nil {
		log.Fatal(err)
	}
	meta, err := backup.CreateBackup(ctx, db, backup.Config{Destination: dest})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("backup %s at %s\n", meta.BackupID, meta.CreatedAt.Format("15:04:05"))
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Clone restore under a fresh writer identity: reusing the source
	// NodeID is rejected so origin sequences can never repeat.
	fresh := replicateddb.NewNodeID()
	restored := filepath.Join(base, "restored")
	if _, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      dest,
		TargetPath:  restored,
		KeysPath:    filepath.Join(restored, "keys"),
		FreshNodeID: fresh.String(),
	}); err != nil {
		log.Fatal(err)
	}
	clone, err := replicateddb.Open(ctx, replicateddb.Config{
		Path:   restored,
		NodeID: fresh,
		Schema: replicateddb.SchemaConfig{Version: 1, Tables: schemaTables()},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "backup-key",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer clone.Close()

	var n int
	if err := clone.QueryRowContext(ctx, `SELECT count(*) FROM records`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows in clone: %d\n", n)
	if n != 5 {
		log.Fatalf("want 5 restored rows, got %d", n)
	}
	id := replicateddb.NewRowID()
	if _, err := clone.ExecContext(ctx,
		`INSERT INTO records (id, body) VALUES (?, ?)`, id[:], "post-restore"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("clone accepts fresh writes")
}
