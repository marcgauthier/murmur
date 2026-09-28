package replicateddb

import (
	"context"
	"testing"
)

func TestOpenRejectsInvalidAllowedNetworks(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Replication.AllowedNetworks = []string{"127.0.0.0/8", "bogus"}
	if _, err := Open(ctx, cfg); err == nil {
		t.Fatal("Open with invalid AllowedNetworks succeeded, want error")
	}
}

func TestOpenAcceptsAllowedNetworks(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Replication.AllowedNetworks = []string{"127.0.0.0/8", "::1/128"}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
