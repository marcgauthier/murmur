package murmur

import "context"

// MigrateMergePolicies upgrades a signed format-4 database offline, preserving
// its historical signed transactions and all-LWW schema identities.
// Stop all writers and take a recoverable backup before calling this function.
func MigrateMergePolicies(ctx context.Context, cfg Config) error {
	cfg.mergePolicyMigration = true
	cfg.Replication = ReplicationConfig{}
	db, err := Open(ctx, cfg)
	if err != nil {
		return err
	}
	return db.Close()
}
