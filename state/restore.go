package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/marcgauthier/murmur/ids"
)

// Restore errors returned during fresh-identity adoption.
var (
	// ErrRestoreIdentityMismatch indicates the Open NodeID is neither the
	// stored identity nor the restore intent's fresh identity. In
	// particular, reopening restored data under its original NodeID
	// (same-identity rollback) is rejected: peers may already hold
	// unrelated sequences under that origin.
	ErrRestoreIdentityMismatch = errors.New("state: restore requires opening with the fresh writer NodeID")
	// ErrRestoreIdentityReuse indicates a fresh NodeID that collides with
	// the backup source or a historical writer identity.
	ErrRestoreIdentityReuse = errors.New("state: fresh NodeID collides with a historical writer identity")
	// ErrRestoreIntentMismatch indicates a restore intent whose source
	// identity does not match the stored data.
	ErrRestoreIntentMismatch = errors.New("state: restore intent does not match stored identity")
)

// RestoreAdoption carries a validated restore intent into Open. The store's
// current node identity must equal Source; it is atomically replaced by
// Fresh with a reset origin sequence and a durable restore marker. For a
// coordinated reseed, the cluster identity moves from SourceDBID to NewDBID
// in the same batch; NewDBID zero preserves the stored DBID (clone).
type RestoreAdoption struct {
	// Source is the backup's writer identity. It must match the stored
	// node identity, proving the intent belongs to this data.
	Source ids.NodeID
	// Fresh is the new writer identity. It must equal the Open nodeID and
	// must not collide with any historical writer origin.
	Fresh ids.NodeID
	// SourceDBID is the backup's cluster identity. When NewDBID is set it
	// must match the stored DBID.
	SourceDBID ids.DBID
	// NewDBID is a reseed's replacement cluster identity. Zero preserves
	// the stored DBID.
	NewDBID ids.DBID
	// BackupID identifies the restored backup for the durable marker.
	BackupID string
}

// reseedSwap reports the replacement DBID when adoption carries a reseed
// whose source matches the stored cluster identity.
func (s *Store) reseedSwap(stored []byte) (ids.DBID, bool) {
	adoption := s.openOpt.Restore
	if adoption == nil || adoption.NewDBID.IsZero() {
		return ids.DBID{}, false
	}
	if len(stored) != 16 || string(stored) != string(adoption.SourceDBID[:]) {
		return ids.DBID{}, false
	}
	return adoption.NewDBID, true
}

// RestoreMarker is the durable record of a restore adoption.
type RestoreMarker struct {
	BackupID      string `json:"backup_id"`
	SourceNodeID  string `json:"source_node_id"`
	FreshNodeID   string `json:"fresh_node_id"`
	AdoptedMillis int64  `json:"adopted_millis"`
	Mode          string `json:"mode"`
}

// RestoreMarker returns the last restore adoption record, or ok=false when
// this store never adopted a restored identity.
func (s *Store) RestoreMarker() (marker RestoreMarker, ok bool, err error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	raw, err := s.getDirect(SysKey(sysRestoreMarker))
	if err != nil {
		if isNotFound(err) {
			return RestoreMarker{}, false, nil
		}
		return RestoreMarker{}, false, err
	}
	if err := json.Unmarshal(raw, &marker); err != nil {
		return RestoreMarker{}, false, fmt.Errorf("state: decode restore marker: %w", err)
	}
	return marker, true, nil
}

// adoptRestoreIdentity stages a fresh-identity adoption into b. It runs
// inside initMeta (writeMu held, no concurrent state users yet):
//
//   - the stored node identity becomes Fresh and the local origin sequence
//     resets to zero, so the new writer's A/1.. cannot collide with the
//     retired origin's unrelated A/1.. already held by peers;
//   - a durable restore marker records the backup and retired identity;
//   - node-specific state the fresh node must not inherit is cleared:
//     snapshot receive staging, peer acknowledgement records, inherited member
//     admission records / retention obligations, and peer exclusion/retirement policy.
//     Historical cells, tombstones, origin IDs, TxIDs, versions, logs,
//     receive watermarks, receipts, schema, HLC floor, generation, and DBID
//     are preserved untouched.
//
// The whole adoption commits atomically with the surrounding initMeta batch.
func (s *Store) adoptRestoreIdentity(b *batch, fresh ids.NodeID, stored []byte) error {
	adoption := s.openOpt.Restore
	if adoption == nil {
		return fmt.Errorf("state: data directory belongs to another node")
	}
	if fresh.IsZero() {
		return fmt.Errorf("%w: fresh identity is zero", ErrRestoreIdentityMismatch)
	}
	if adoption.Fresh != fresh {
		return fmt.Errorf("%w: have %s, intent requires %s",
			ErrRestoreIdentityMismatch, fresh, adoption.Fresh)
	}
	var storedID ids.NodeID
	if len(stored) != 16 {
		return fmt.Errorf("state: corrupt node identity")
	}
	copy(storedID[:], stored)
	if adoption.Source != storedID {
		return fmt.Errorf("%w: intent source %s, stored %s",
			ErrRestoreIntentMismatch, adoption.Source, storedID)
	}
	if adoption.Fresh == storedID {
		return fmt.Errorf("%w: %s is the backup source identity", ErrRestoreIdentityReuse, storedID)
	}
	// The fresh identity must not collide with any historical writer
	// origin retained in this store (including the retired source). This
	// closes rollback to any previously used identity, not just the
	// immediate source.
	origins, err := s.KnownOrigins()
	if err != nil {
		return err
	}
	for _, o := range origins {
		if o == fresh {
			return fmt.Errorf("%w: %s already wrote to this store", ErrRestoreIdentityReuse, fresh)
		}
	}

	mode := "clone"
	var dbSwap ids.DBID
	if !adoption.NewDBID.IsZero() {
		mode = "reseed"
		storedDB, err := s.getDirect(SysKey(sysDBID))
		if err != nil {
			if isNotFound(err) {
				return fmt.Errorf("%w: no stored cluster identity", ErrRestoreIntentMismatch)
			}
			return err
		}
		swap, ok := s.reseedSwap(storedDB)
		if !ok {
			return fmt.Errorf("%w: intent source DBID does not match stored data", ErrRestoreIntentMismatch)
		}
		if swap == adoption.SourceDBID {
			return fmt.Errorf("%w: reseed DBID equals the source cluster", ErrRestoreIdentityReuse)
		}
		dbSwap = swap
		// Validated: the old cluster log and staging go now, in bounded
		// commits ahead of the atomic identity swap below.
		for _, prefix := range []byte{prefixLog, prefixTxnStage} {
			if err := s.deletePrefixRange([]byte{prefix}); err != nil {
				return err
			}
		}
		if err := b.Set(SysKey("origin_trusted_baseline"), encodeU64(s.clock.Max())); err != nil {
			return err
		}
	}
	marker, err := json.Marshal(RestoreMarker{
		BackupID:      adoption.BackupID,
		SourceNodeID:  storedID.String(),
		FreshNodeID:   fresh.String(),
		AdoptedMillis: time.Now().UnixMilli(),
		Mode:          mode,
	})
	if err != nil {
		return fmt.Errorf("state: encode restore marker: %w", err)
	}
	if err := b.Set(SysKey(sysLocalNode), fresh[:]); err != nil {
		return err
	}
	if err := b.Set(SysKey(sysLocalSeq), encodeU64(0)); err != nil {
		return err
	}
	if !dbSwap.IsZero() {
		if err := b.Set(SysKey(sysDBID), dbSwap[:]); err != nil {
			return err
		}
	}
	if err := b.Set(SysKey(sysRestoreMarker), marker); err != nil {
		return err
	}
	// Clear snapshot receive staging (incomplete transfers only; committed
	// snapshot state lives under cell/tombstone/log/receive keys), peer
	// acknowledgement records, inherited member admission/retention records,
	// and inherited peer exclusion/retirement policy. The fresh node re-establishes
	// its own observations, membership decisions, and GC gating obligations.
	for _, prefix := range [][]byte{SnapshotKey("recv/"), {prefixPeerAck}, {prefixMember}, {prefixPeerExcluded}} {
		if err := s.snapshot(func(snap *snapshot) error {
			it, err := snap.NewIter(&iterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
			if err != nil {
				return err
			}
			defer it.Close()
			for it.SeekGE(prefix); it.Valid(); it.Next() {
				if err := b.Delete(append([]byte(nil), it.Key()...)); err != nil {
					return err
				}
			}
			return it.Error()
		}); err != nil {
			return err
		}
	}
	return nil
}
