// Package testdb provisions signing identities for external test fixtures only.
package testdb

import (
	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/internal/testidentity"
)

func Configure(cfg murmur.Config) murmur.Config {
	cfg.OriginSigning = testidentity.Config(cfg.NodeID)
	for _, p := range cfg.Replication.Peers {
		cfg.Replication.TrustedSnapshotSources = append(cfg.Replication.TrustedSnapshotSources, p.NodeID)
	}
	return cfg
}
