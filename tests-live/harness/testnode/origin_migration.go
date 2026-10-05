package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	db "github.com/marcgauthier/murmur"
)

// runOriginMigration is a deliberately offline fixture command: it never
// starts an HTTP listener or replication, and uses the normal credential loader.
func runOriginMigration(args []string) {
	fs := flag.NewFlagSet("migrate-origin-baseline", flag.ExitOnError)
	config := fs.String("config", "", "node config file")
	keyID := fs.String("key-id", "remote-unlock-key", "storage wrapping key ID")
	_ = fs.Parse(args)
	raw, err := os.ReadFile(*config)
	if err != nil {
		migrationFail(err)
	}
	var cfg NodeConfigFile
	if err := json.Unmarshal(raw, &cfg); err != nil {
		migrationFail(err)
	}
	node, err := db.ParseNodeID(cfg.NodeID)
	if err != nil {
		migrationFail(err)
	}
	dbid, err := db.ParseDBID(cfg.DBID)
	if err != nil {
		migrationFail(err)
	}
	key, err := hex.DecodeString(cfg.KeyHex)
	if err != nil {
		migrationFail(err)
	}
	d := &NodeDaemon{cfg: cfg, nodeID: node, dbID: dbid, originMigration: true}
	if _, err := d.openDatabase(context.Background(), key, *keyID); err != nil {
		migrationFail(err)
	}
}
func migrationFail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
