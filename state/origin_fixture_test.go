package state

import (
	"context"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
)

var fixtureDBID = testidentity.DBID

func openSignedFixture(path string, node ids.NodeID, dbid ids.DBID, opt Options) (*Store, error) {
	opt.OriginSigning = testidentity.Config(node)
	if dbid.IsZero() {
		dbid = testidentity.DBID
	}
	return Open(path, node, dbid, opt)
}
func commitRemoteFixture(s *Store, ctx context.Context, b *codec.MutationBatch) (MergeResult, error) {
	return s.CommitRemote(ctx, testidentity.Sign(b, s.DBID()))
}
func commitRemoteGroupFixture(s *Store, ctx context.Context, bs []*codec.MutationBatch) (MergeResult, error) {
	for _, b := range bs {
		testidentity.Sign(b, s.DBID())
	}
	return s.CommitRemoteGroup(ctx, bs)
}
func encodeChunksFixture(b *codec.MutationBatch, max int64) ([][]byte, error) {
	return codec.EncodeTransactionChunks(testidentity.Sign(b, b.DBID), max)
}
func visitChunksFixture(b *codec.MutationBatch, max int64, fn func(uint32, []byte) error) error {
	return codec.VisitTransactionChunks(testidentity.Sign(b, b.DBID), max, fn)
}
