package replication

import (
	"errors"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func (m *Manager) snapshotSourceTrusted(id ids.NodeID) bool {
	for _, source := range m.cfg.TrustedSnapshotSources {
		if source == id {
			return true
		}
	}
	return false
}

func (m *Manager) verifyOrigin(b *codec.MutationBatch) error {
	if m.cfg.Store == nil {
		m.recordOriginFailure(codec.ErrOriginUnknown)
		return codec.ErrOriginUnknown
	}
	err := m.cfg.Store.VerifyOrigin(b)
	if err != nil {
		m.recordOriginFailure(err)
	}
	return err
}

func (m *Manager) recordOriginFailure(err error) {
	switch {
	case errors.Is(err, codec.ErrOriginUnsigned):
		m.st.originUnsigned.Add(1)
	case errors.Is(err, codec.ErrOriginUnknown):
		m.st.originUnknown.Add(1)
	case errors.Is(err, codec.ErrOriginDigest):
		m.st.originDigestMismatch.Add(1)
	default:
		m.st.originSignatureInvalid.Add(1)
	}
}
