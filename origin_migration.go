package murmur

import "context"

// MigrateOriginBaseline converts an explicitly trusted legacy database to the
// signed format offline. Stop every writer, synchronize the cluster, and make
// a backup before calling. The supplied schema/encryption/signing credentials
// must match the database. Unsigned history and incomplete transfers are
// discarded; current state, receipts and origin watermarks are preserved.
func MigrateOriginBaseline(ctx context.Context, cfg Config) error {
	cfg.originBaselineMigration = true
	cfg.Replication = ReplicationConfig{}
	db, err := Open(ctx, cfg)
	if err != nil {
		return err
	}
	return db.Close()
}
