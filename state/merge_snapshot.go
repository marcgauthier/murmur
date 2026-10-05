package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
	"math"
)

func (s *Store) iterateCRDTSnapshot(snap *pebble.Snapshot, fn func(codec.SnapshotCell) error) error {
	p := []byte{prefixCRDT}
	it, err := prefixIter(snap, p)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) <= 25 {
			return fmt.Errorf("state: malformed causal key")
		}
		table := binary.BigEndian.Uint32(key[1:5])
		var row ids.RowID
		copy(row[:], key[5:21])
		col := binary.BigEndian.Uint32(key[21:25])
		st, err := codec.DecodeCellState(it.Value(), s.limits)
		if err != nil {
			return err
		}
		if err := fn(codec.SnapshotCell{TableID: table, RowID: row, ColumnID: col, Version: st.Version, Value: st.Value, RecordKey: append([]byte(nil), key[25:]...)}); err != nil {
			return err
		}
	}
	return it.Error()
}

// Recompute projections after all causal records are joined. Atomic imports
// read their indexed batch; resumable imports read already-ingested records.
func (s *Store) reprojectSnapshot(b *pebble.Batch, indexed bool) error {
	opt := &pebble.IterOptions{LowerBound: []byte{prefixCRDT}, UpperBound: []byte{prefixCRDT + 1}}
	var it *pebble.Iterator
	var err error
	if indexed {
		it, err = b.NewIter(opt)
	} else {
		it, err = s.db.NewIter(opt)
	}
	if err != nil {
		return err
	}
	defer it.Close()
	var prefix []byte
	records := make(map[string][]byte)
	recordVersions := make(map[string]crdt.Version)
	var version crdt.Version
	flush := func() error {
		if prefix == nil {
			return nil
		}
		table := binary.BigEndian.Uint32(prefix[1:5])
		var row ids.RowID
		copy(row[:], prefix[5:21])
		col := binary.BigEndian.Uint32(prefix[21:25])
		var projection codec.Value

		key := CellKey(table, row, col)
		var raw []byte
		var closer interface{ Close() error }
		if indexed {
			raw, closer, err = b.Get(key)
		} else {
			raw, err = s.getDirect(key)
		}
		if err == nil {
			raw = append([]byte(nil), raw...)
			stored, e := codec.DecodeCellState(raw, s.limits)
			if closer != nil {
				closer.Close()
			}
			if e != nil {
				return e
			}
			if crdt.CompareVersion(stored.Version, version) > 0 {
				version = stored.Version
			}
		} else if !isNotFound(err) {
			return err
		}
		if s.isMergeShadow(table, col) {
			stored, decodeErr := codec.DecodeCellState(raw, s.limits)
			if decodeErr != nil {
				return decodeErr
			}
			epoch, _, active, e := codec.ShadowValue(stored.Value, s.limits)
			if e != nil {
				return e
			}
			if !active {
				return nil
			}
			version = stored.Version
			for key, ver := range recordVersions {
				e, _, ok := codec.SplitEpochRecord([]byte(key))
				if ok && e == epoch && crdt.CompareVersion(ver, version) > 0 {
					version = ver
				}
			}
			records = recordsAtEpoch(records, epoch)
			projection, err = ProjectCRDT(s.columnPolicy(table, col), records, s.limits.MaxValueBytes)
			projection = codec.ShadowProjection(epoch, projection)
		} else {
			projection, err = ProjectCRDT(s.columnPolicy(table, col), records, s.limits.MaxValueBytes)
		}
		if err != nil {
			return err
		}
		return b.Set(key, codec.EncodeCellState(nil, codec.CellState{Version: version, Value: projection}), nil)
	}
	for it.First(); it.Valid(); it.Next() {
		key := append([]byte(nil), it.Key()...)
		if len(key) <= 25 {
			return fmt.Errorf("state: malformed causal record")
		}
		if !bytes.Equal(prefix, key[:25]) {
			if err := flush(); err != nil {
				return err
			}
			prefix = key[:25]
			records = make(map[string][]byte)
			recordVersions = make(map[string]crdt.Version)
			version = crdt.Version{}
		}
		st, err := codec.DecodeCellState(it.Value(), s.limits)
		if err != nil {
			return err
		}
		records[string(key[25:])] = append([]byte(nil), st.Value.B...)
		recordVersions[string(key[25:])] = st.Version
		if crdt.CompareVersion(st.Version, version) > 0 {
			version = st.Version
		}
	}
	if err := it.Error(); err != nil {
		return err
	}
	return flush()
}

// Join scalar caches independently of causal-record delivery order. Shadow
// epochs choose the ownership branch; values within that branch use its policy.
func (s *Store) joinSnapshotPolicyCell(c *codec.SnapshotCell, stored codec.CellState, present bool) (codec.Value, bool, error) {
	policy := s.columnPolicy(c.TableID, c.ColumnID)
	incoming, old := c.Value, stored.Value
	shadow := s.isMergeShadow(c.TableID, c.ColumnID)
	projection := incoming
	if shadow {
		_, v, active, err := codec.ShadowValue(incoming, s.limits)
		if err != nil {
			return codec.Value{}, false, err
		}
		if active {
			projection = v
		} else {
			projection = codec.Null()
		}
	}
	if err := validateSnapshotProjection(policy, projection); err != nil {
		return codec.Value{}, false, err
	}
	var epoch crdt.Version
	if shadow {
		ie, iv, ia, err := codec.ShadowValue(incoming, s.limits)
		if err != nil {
			return codec.Value{}, false, err
		}
		if !present {
			return incoming, true, nil
		}
		oe, ov, oa, err := codec.ShadowValue(old, s.limits)
		if err != nil {
			return codec.Value{}, false, err
		}
		if !ia || !oa {
			take := crdt.CompareVersion(c.Version, stored.Version) > 0
			if ia && !oa {
				take = crdt.CompareVersion(ie, stored.Version) >= 0
			}
			if !ia && oa {
				take = crdt.CompareVersion(c.Version, oe) > 0
			}
			if take {
				return incoming, true, nil
			}
			return old, false, nil
		}
		cmp := crdt.CompareVersion(ie, oe)
		if cmp > 0 {
			return incoming, true, nil
		}
		if cmp < 0 {
			return old, false, nil
		}
		epoch, incoming, old = ie, iv, ov
	}
	if policy == schema.MAX || policy == schema.MIN {
		if incoming.Type != codec.TypeNull && incoming.Type != codec.TypeInteger && incoming.Type != codec.TypeReal || incoming.Type == codec.TypeReal && (math.IsNaN(incoming.F) || math.IsInf(incoming.F, 0)) {
			return codec.Value{}, false, fmt.Errorf("state: invalid snapshot extrema")
		}
		if !present || old.Type == codec.TypeNull {
			old = incoming
		} else if incoming.Type != codec.TypeNull {
			cmp := compareNumber(incoming, old)
			if cmp == 0 {
				cmp = bytes.Compare(codec.AppendValue(nil, incoming), codec.AppendValue(nil, old))
			}
			if policy == schema.MAX && cmp > 0 || policy == schema.MIN && cmp < 0 {
				old = incoming
			}
		}
	} else if !present {
		old = incoming
	}
	if shadow {
		old = codec.ShadowProjection(epoch, old)
	}
	take := !present || !old.Equal(stored.Value) || crdt.CompareVersion(c.Version, stored.Version) > 0
	if crdt.CompareVersion(stored.Version, c.Version) > 0 {
		c.Version = stored.Version
	}
	return old, take, nil
}

func validateSnapshotProjection(policy schema.MergePolicy, value codec.Value) error {
	if value.Type == codec.TypeNull {
		return nil
	}
	switch policy {
	case schema.MAX, schema.MIN:
		if value.Type == codec.TypeInteger || value.Type == codec.TypeReal && !math.IsNaN(value.F) && !math.IsInf(value.F, 0) {
			return nil
		}
	case schema.PN_COUNTER:
		if value.Type == codec.TypeText && value.S == "0" {
			return nil
		}
	case schema.OR_SET:
		if value.Type == codec.TypeText && value.S == "[]" {
			return nil
		}
	}
	return fmt.Errorf("state: invalid snapshot policy projection")
}
