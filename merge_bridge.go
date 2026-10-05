package murmur

import (
	"fmt"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

func (tx *Tx) ownershipEpoch(table uint32, row ids.RowID, col uint32) (crdt.Version, bool, error) {
	cells, err := tx.db.store.GetRow(table, row)
	if err != nil {
		return crdt.Version{}, false, err
	}
	shadow, present := cells[col^bridgeShadowXOR]
	var epoch crdt.Version
	active := false
	if present {
		epoch, _, active, err = codec.ShadowValue(shadow.Value, codec.Limits{MaxValueBytes: tx.db.cfg.MaxReplicatedValueBytes})
		if err != nil {
			return epoch, false, err
		}
		if !active {
			epoch = shadow.Version
		}
	}
	// SQL commits queued ahead of this writer are visible already. Their
	// immutable versions let a release establish the next branch before fsync.
	latest := shadow.Version
	tx.db.mergePendingMu.Lock()
	for _, batch := range tx.db.mergePending {
		if crdt.CompareVersion(batch.version, latest) <= 0 {
			continue
		}
		for _, m := range batch.mutations {
			if m.TableID != table || m.RowID != row || m.ColumnID != col^bridgeShadowXOR {
				continue
			}
			if m.Policy == schema.LWW && m.Value.Type == codec.TypeBlob && len(m.Value.B) == 1 && m.Value.B[0] == 0 {
				epoch, active, latest = batch.version, false, batch.version
			} else if m.Policy != schema.LWW && len(m.Records) > 0 {
				e, _, valid := codec.SplitEpochRecord(m.Records[0].Key)
				if valid {
					epoch, active, latest = e, true, batch.version
				}
			}
		}
	}
	tx.db.mergePendingMu.Unlock()
	return epoch, active, nil
}

// High operations join a branch whose stable epoch is the last release marker.
// Source components copied at takeover retain identity, so two receivers cannot
// count the baseline twice. New Low payloads keep joining the underlying field.
func (tx *Tx) routeMergeOwnership(mutations []codec.Mutation) ([]codec.Mutation, error) {
	if tx.bridgeImport != nil {
		return mutations, nil
	}
	var out []codec.Mutation
	seeded := make(map[string]bool)
	for _, m := range mutations {
		if m.Policy == schema.LWW {
			out = append(out, m)
			continue
		}
		policy, ok, err := tx.db.bridgePolicy(m.TableID, m.RowID, m.ColumnID)
		if err != nil {
			return nil, err
		}
		if !ok || policy.SourceDomain.IsZero() {
			out = append(out, m)
			continue
		}
		epoch, active, err := tx.ownershipEpoch(m.TableID, m.RowID, m.ColumnID)
		if err != nil {
			return nil, err
		}
		col := m.ColumnID
		target := fmt.Sprintf("%d/%s/%d", m.TableID, m.RowID, col)
		if !active && !seeded[target] {
			seeded[target] = true
			if m.Policy == schema.PN_COUNTER || m.Policy == schema.OR_SET {
				records, err := tx.db.store.CRDTRecords(m.TableID, m.RowID, col)
				if err != nil {
					return nil, err
				}
				tx.db.mergePendingMu.Lock()
				for _, batch := range tx.db.mergePending {
					for _, pending := range batch.mutations {
						if pending.TableID == m.TableID && pending.RowID == m.RowID && pending.ColumnID == col && pending.Policy == m.Policy && pending.Flags&codec.FlagCounterDelta == 0 {
							records = append(records, pending.Records...)
						}
					}
				}
				tx.db.mergePendingMu.Unlock()
				seed := codec.Mutation{TableID: m.TableID, RowID: m.RowID, ColumnID: col ^ bridgeShadowXOR, Policy: m.Policy, Flags: codec.FlagCRDTImport, Value: codec.Null()}
				for _, r := range records {
					seed.Records = append(seed.Records, codec.CRDTRecord{Key: codec.EpochRecord(epoch, r.Key), Data: r.Data})
				}
				if len(seed.Records) > 0 {
					out = append(out, seed)
				}
			}
			if m.Policy == schema.MAX || m.Policy == schema.MIN {
				cells, err := tx.db.store.GetRow(m.TableID, m.RowID)
				if err != nil {
					return nil, err
				}
				if base, ok := cells[col]; ok {
					out = append(out, codec.Mutation{TableID: m.TableID, RowID: m.RowID, ColumnID: col ^ bridgeShadowXOR, Policy: m.Policy, Value: base.Value, Records: []codec.CRDTRecord{{Key: codec.EpochRecord(epoch, []byte{'x'})}}})
				}
				tx.db.mergePendingMu.Lock()
				for _, batch := range tx.db.mergePending {
					for _, pending := range batch.mutations {
						if pending.TableID == m.TableID && pending.RowID == m.RowID && pending.ColumnID == col && pending.Policy == m.Policy {
							out = append(out, codec.Mutation{TableID: m.TableID, RowID: m.RowID, ColumnID: col ^ bridgeShadowXOR, Policy: m.Policy, Value: pending.Value, Records: []codec.CRDTRecord{{Key: codec.EpochRecord(epoch, []byte{'x'})}}})
						}
					}
				}
				tx.db.mergePendingMu.Unlock()
			}

		}
		policy.Owner = BridgeOwnerHigh
		policy.OverrideTxID = tx.txID
		out = append(out, bridgePolicyMutation(m.TableID, m.RowID, col, policy))
		m.ColumnID = col ^ bridgeShadowXOR
		if m.Flags&codec.FlagCounterDelta != 0 {
			m.Records = []codec.CRDTRecord{{Key: codec.EpochRecord(epoch, []byte{'p'})}}
		} else {
			for i := range m.Records {
				m.Records[i].Key = codec.EpochRecord(epoch, m.Records[i].Key)
			}
		}
		// Extrema carry the epoch in a marker record; their value remains numeric.
		if m.Policy == schema.MAX || m.Policy == schema.MIN {
			m.Records = []codec.CRDTRecord{{Key: codec.EpochRecord(epoch, []byte{'x'})}}
		}
		out = append(out, m)
	}
	return out, nil
}
func (tx *Tx) effectiveMergeRecords(table uint32, row ids.RowID, col uint32) (uint32, crdt.Version, bool, error) {
	epoch, active, err := tx.ownershipEpoch(table, row, col)
	if err != nil {
		return 0, epoch, false, err
	}
	if active {
		return col ^ bridgeShadowXOR, epoch, true, nil
	}
	return col, epoch, false, nil
}
func unwrapEpochRecords(records []codec.CRDTRecord, epoch crdt.Version) []codec.CRDTRecord {
	var out []codec.CRDTRecord
	for _, r := range records {
		e, key, ok := codec.SplitEpochRecord(r.Key)
		if ok && e == epoch {
			out = append(out, codec.CRDTRecord{Key: append([]byte(nil), key...), Data: append([]byte(nil), r.Data...)})
		}
	}
	return out
}
