package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/marcgauthier/murmur/codec"
)

// VerifyOrigin is also used at network ingress before scheduling work.
func (s *Store) VerifyOrigin(batch *codec.MutationBatch) error {
	return s.openOpt.OriginSigning.Verify(batch, s.dbID)
}
func (s *Store) VerifyOriginChunk(chunk *codec.TransactionChunk) error {
	return s.openOpt.OriginSigning.VerifyChunk(chunk, s.dbID)
}

func (s *Store) pinOriginKey(b *pebble.Batch, adopting bool) error {
	pub, _ := s.openOpt.OriginSigning.TrustedKeys.Lookup(s.nodeID)
	fingerprint := sha256.Sum256(pub)
	old, err := s.getDirect(SysKey("origin_signing_key"))
	if err == nil && !adopting && !bytes.Equal(old, fingerprint[:]) {
		return fmt.Errorf("state: signing key changed under the same NodeID")
	}
	if err != nil && !isNotFound(err) {
		return err
	}
	return b.Set(SysKey("origin_signing_key"), fingerprint[:], nil)
}

// finishOriginBaseline establishes explicitly trusted pre-signature state.
// It never manufactures signatures for historical transactions.
func (s *Store) finishOriginBaseline() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	for _, prefix := range []byte{prefixLog, prefixTxnStage, prefixSnapshot, prefixPeerAck} {
		if err := b.DeleteRange([]byte{prefix}, []byte{prefix + 1}, nil); err != nil {
			return err
		}
	}
	for _, name := range []string{sysFormat, sysMinReader, sysMinWriter} {
		if err := b.Set(SysKey(name), encodeU64(FormatVersion), nil); err != nil {
			return err
		}
	}
	if err := b.Set(SysKey("origin_trusted_baseline"), encodeU64(s.clock.Max()), nil); err != nil {
		return err
	}
	if err := s.pinOriginKey(b, false); err != nil {
		return err
	}
	if err := s.commitBatch(b, pebble.Sync); err != nil {
		return err
	}
	s.migratingOrigin = false
	return nil
}

// checkRemoteIdentity refuses conflicting retained receipts/log records. The
// caller holds writeMu; compacted history cannot prove origin non-equivocation.
func (s *Store) checkRemoteIdentity(batch *codec.MutationBatch) error {
	receipt, err := s.getDirect(ReceiptKey(batch.TxID))
	if err == nil && (len(receipt) != 24 || !bytes.Equal(receipt[:16], batch.OriginNode[:]) || !bytes.Equal(receipt[16:], encodeU64(batch.Sequence))) {
		return codec.ErrOriginConflict
	}
	if err != nil && !isNotFound(err) {
		return err
	}
	old, err := s.getDirect(LogKey(batch.OriginNode, batch.Sequence))
	if err == nil && !bytes.Equal(old, codec.EncodeBatch(nil, batch)) {
		return codec.ErrOriginConflict
	}
	if err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

// migratePreparedLegacy recovers a legacy intent only during an explicitly
// authorized, offline baseline migration, before signed format publication.
func (s *Store) migratePreparedLegacy(raw []byte) error {
	batch, rest, err := codec.DecodeLegacyBatch(raw, s.limits)
	if err != nil || len(rest) != 0 || len(batch.Mutations) == 0 {
		return fmt.Errorf("state: invalid legacy prepare record: %v", err)
	}
	_, err = s.CommitRemote(context.Background(), batch)
	return err
}
