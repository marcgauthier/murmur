package replication

import (
	"context"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
)

var fixtureDBID = testidentity.DBID

var testMasterKey = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
}

func openSignedFixture(path string, node ids.NodeID, dbid ids.DBID, opt state.Options) (*state.Store, error) {
	opt.OriginSigning = testidentity.Config(node)
	if len(opt.Spool.MasterKey) == 0 && opt.Spool.Passphrase == "" &&
		opt.Spool.Encryption != spool.EncryptionNone {
		opt.Spool.MasterKey = append([]byte(nil), testMasterKey...)
	}
	return state.Open(path, node, dbid, opt)
}
func commitRemoteFixture(s *state.Store, ctx context.Context, b *codec.MutationBatch) (state.MergeResult, error) {
	return s.CommitRemote(ctx, testidentity.Sign(b, s.DBID()))
}
func encodeBatchesFixture(dst []byte, bs []*codec.MutationBatch) []byte {
	for _, b := range bs {
		if b.SignatureVersion == 0 {
			testidentity.Sign(b, b.DBID)
		}
	}
	return EncodeBatches(dst, bs)
}
func encodeChunksFixture(b *codec.MutationBatch, max int64) ([][]byte, error) {
	return codec.EncodeTransactionChunks(testidentity.Sign(b, b.DBID), max)
}

func trustSnapshotFixture(m *Manager, p *peerState) {
	if !m.snapshotSourceTrusted(p.id) {
		m.cfg.TrustedSnapshotSources = append(m.cfg.TrustedSnapshotSources, p.id)
	}
}
func onSnapshotChunkFixture(m *Manager, p *peerState, ps *peerSession, f *Frame) error {
	trustSnapshotFixture(m, p)
	return m.onSnapshotChunk(p, ps, f)
}
func onSnapshotDoneFixture(m *Manager, p *peerState, ps *peerSession) error {
	trustSnapshotFixture(m, p)
	return m.onSnapshotDone(p, ps)
}
func onSnapshotManifestFixture(m *Manager, p *peerState, ps *peerSession, raw []byte) error {
	trustSnapshotFixture(m, p)
	return m.onSnapshotManifest(p, ps, raw)
}
func markSnapshotRequestedFixture(m *Manager, p *peerState) bool {
	trustSnapshotFixture(m, p)
	return m.markSnapshotRequested(p)
}
