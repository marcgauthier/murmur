package murmur

import (
	"context"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
)

func openSignedFixture(ctx context.Context, cfg Config) (*DB, error) {
	cfg.OriginSigning = testidentity.Config(cfg.NodeID)
	for _, peer := range cfg.Replication.Peers {
		cfg.Replication.TrustedSnapshotSources = append(cfg.Replication.TrustedSnapshotSources, peer.NodeID)
	}
	return Open(ctx, cfg)
}
func applyRemoteFixture(db *DB, ctx context.Context, b *codec.MutationBatch) error {
	return db.ApplyRemote(ctx, testidentity.Sign(b, db.store.DBID()))
}
func applyRemoteGroupFixture(db *DB, ctx context.Context, bs []*codec.MutationBatch) error {
	for _, b := range bs {
		testidentity.Sign(b, db.store.DBID())
	}
	return db.ApplyRemoteGroup(ctx, bs)
}
func commitRemoteFixture(s *state.Store, ctx context.Context, b *codec.MutationBatch) (state.MergeResult, error) {
	return s.CommitRemote(ctx, testidentity.Sign(b, s.DBID()))
}
func commitRemoteGroupFixture(s *state.Store, ctx context.Context, bs []*codec.MutationBatch) (state.MergeResult, error) {
	for _, b := range bs {
		testidentity.Sign(b, s.DBID())
	}
	return s.CommitRemoteGroup(ctx, bs)
}

func openStateSignedFixture(path string, node ids.NodeID, dbid ids.DBID, opt state.Options) (*state.Store, error) {
	opt.OriginSigning = testidentity.Config(node)
	if opt.Spool.Encryption == 0 && len(opt.Spool.MasterKey) == 0 && opt.Spool.Passphrase == "" {
		opt.Spool.Encryption = spool.EncryptionAES256GCM
		opt.Spool.MasterKey = append([]byte(nil), testKey...)
		opt.Spool.WrappingKeyID = testKeyID
	}
	return state.Open(path, node, dbid, opt)
}
