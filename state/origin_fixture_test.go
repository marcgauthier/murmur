package state

import (
	"context"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/spool"
)

var fixtureDBID = testidentity.DBID

// testMasterKey gives every fixture store AES-256-GCM persistence so the
// suite exercises the production encryption path.
var testMasterKey = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
}

func openSignedFixture(path string, node ids.NodeID, dbid ids.DBID, opt Options) (*Store, error) {
	opt.OriginSigning = testidentity.Config(node)
	if dbid.IsZero() {
		dbid = testidentity.DBID
	}
	// Default fixtures to encrypted persistence; callers that set their
	// own key material, passphrase, or explicit EncryptionNone keep it.
	if len(opt.Spool.MasterKey) == 0 && opt.Spool.Passphrase == "" &&
		opt.Spool.Encryption != spool.EncryptionNone {
		opt.Spool.MasterKey = append([]byte(nil), testMasterKey...)
	}
	return Open(path, node, dbid, opt)
}

// withTestKey fills AES test key material into direct-Open options,
// mirroring openSignedFixture for tests that bypass it.
func withTestKey(opt Options) Options {
	if len(opt.Spool.MasterKey) == 0 && opt.Spool.Passphrase == "" &&
		opt.Spool.Encryption != spool.EncryptionNone {
		opt.Spool.MasterKey = append([]byte(nil), testMasterKey...)
	}
	return opt
}

// deleteSync removes one key through a synchronous commit.
func deleteSync(s *Store, key []byte) error {
	b := s.mem.newBatch()
	defer b.Close()
	if err := b.Delete(key); err != nil {
		return err
	}
	return s.commitBatch(b, true)
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
