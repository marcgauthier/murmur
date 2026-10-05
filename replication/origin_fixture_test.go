package replication

import (
	"context"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/state"
)

var fixtureDBID = testidentity.DBID

func openSignedFixture(path string, node ids.NodeID, dbid ids.DBID, opt state.Options) (*state.Store, error) {
	opt.OriginSigning = testidentity.Config(node)
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
