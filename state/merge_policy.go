package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

const prefixCRDT byte = 0x0e

func crdtPrefix(table uint32, row ids.RowID, col uint32) []byte {
	k := CellKey(table, row, col)
	k[0] = prefixCRDT
	return k
}
func crdtKey(table uint32, row ids.RowID, col uint32, key []byte) []byte {
	return append(crdtPrefix(table, row, col), key...)
}

func (s *Store) columnPolicy(table, col uint32) schema.MergePolicy {
	r := s.mergeRegistry.Load()
	if r == nil {
		return schema.LWW
	}
	t := r.TableByID(table)
	if t == nil {
		return schema.LWW
	}
	c := t.ColumnByID(col)
	if c == nil {
		c = t.ColumnByID(col ^ 0x80000000)
	}
	if c == nil {
		return schema.LWW
	}
	return c.MergePolicy
}
func (s *Store) loadMergeRegistry() error {
	raw, err := s.getDirect(SchemaKey(schemaCurrentKey))
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	m, err := schema.DecodeManifest(raw)
	if err != nil {
		return err
	}
	r, err := m.Registry()
	if err == nil {
		s.mergeRegistry.Store(r)
	}
	return err
}

// CRDTRecords returns a read-cut of causal records; callers never receive aliases.
func (s *Store) CRDTRecords(table uint32, row ids.RowID, col uint32) ([]codec.CRDTRecord, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	var out []codec.CRDTRecord
	err := s.snapshot(func(snap *snapshot) error {
		p := crdtPrefix(table, row, col)
		it, err := prefixIter(snap, p)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.SeekGE(p); it.Valid(); it.Next() {
			st, err := codec.DecodeCellState(it.Value(), s.limits)
			if err != nil {
				return err
			}
			out = append(out, codec.CRDTRecord{Key: append([]byte(nil), it.Key()[len(p):]...), Data: append([]byte(nil), st.Value.B...)})
		}
		return it.Error()
	})
	return out, err
}

func canonicalNat(raw []byte) bool { return len(raw) == 0 || raw[0] != 0 }
func validateRecord(policy schema.MergePolicy, r codec.CRDTRecord, max int) error {
	_, r.Key, _ = codec.SplitEpochRecord(r.Key)
	if len(r.Key) == 0 {
		return fmt.Errorf("crdt: empty record key")
	}
	switch policy {
	case schema.PN_COUNTER:
		if len(r.Key) != 33 || (r.Key[0] != 'p' && r.Key[0] != 'n') || !canonicalNat(r.Data) {
			return fmt.Errorf("crdt: invalid counter component")
		}
	case schema.OR_SET:
		if len(r.Key) != 53 || (r.Key[0] != 'a' && r.Key[0] != 'r') {
			return fmt.Errorf("crdt: invalid set tag")
		}
		if bytes.Equal(r.Key[33:49], make([]byte, 16)) {
			return fmt.Errorf("crdt: zero add transaction")
		}
		if _, err := codec.DecodeSetElement(r.Data, max); err != nil {
			return err
		}
	default:
		return fmt.Errorf("crdt: causal record on scalar policy")
	}
	if bytes.Equal(r.Key[1:17], make([]byte, 16)) || bytes.Equal(r.Key[17:33], make([]byte, 16)) {
		return fmt.Errorf("crdt: zero actor identity")
	}
	return nil
}
func (s *Store) validatePolicyBatch(batch *codec.MutationBatch) (err error) {
	defer func() {
		if err != nil {
			s.policyRejected.Add(1)
		}
	}()

	for _, m := range batch.Mutations {
		if m.Flags & ^(codec.FlagTombstone|codec.FlagCRDTImport|codec.FlagBridgeReceipt) != 0 {
			return fmt.Errorf("crdt: invalid operation flags")
		}
		if batch.ProtocolVersion < 5 && (m.Policy != schema.LWW || len(m.Records) > 0 || m.Flags&codec.FlagBridgeReceipt != 0) {
			return fmt.Errorf("crdt: causal operations require protocol 5")
		}
		if m.Flags == codec.FlagBridgeReceipt {
			if m.TableID != 0xFFFFFFFE || m.ColumnID != 2 || m.Value.Type != codec.TypeBlob || len(m.Value.B) != 24 || m.Policy != schema.LWW || len(m.Records) != 0 {
				return fmt.Errorf("crdt: invalid bridge receipt")
			}
			continue
		}
		if m.IsTombstone() {
			if m.Policy != schema.LWW || len(m.Records) != 0 {
				return fmt.Errorf("crdt: invalid tombstone payload")
			}
			continue
		}
		p := s.columnPolicy(m.TableID, m.ColumnID)
		if batch.ProtocolVersion < 5 && (m.Policy != schema.LWW || len(m.Records) > 0) {
			return fmt.Errorf("crdt: causal operations require protocol 5")
		}
		shadow := s.isMergeShadow(m.TableID, m.ColumnID)
		if shadow && p != schema.LWW && m.Policy == schema.LWW && len(m.Records) == 0 && m.Flags == 0 {
			_, _, active, err := codec.ShadowValue(m.Value, s.limits)
			if err != nil || active {
				return fmt.Errorf("crdt: invalid ownership release")
			}
			continue
		}
		if m.Policy != p {
			return fmt.Errorf("crdt: schema/mutation policy mismatch: expected %s got %s", p, m.Policy)
		}
		if m.Flags & ^(codec.FlagTombstone|codec.FlagCRDTImport) != 0 {
			return fmt.Errorf("crdt: invalid operation flags")
		}
		if p == schema.LWW && len(m.Records) != 0 {
			return fmt.Errorf("crdt: records on LWW column")
		}
		if p == schema.MAX || p == schema.MIN {
			if shadow {
				if len(m.Records) != 1 {
					return fmt.Errorf("crdt: missing extrema ownership epoch")
				}
				_, base, ok := codec.SplitEpochRecord(m.Records[0].Key)
				if !ok || !bytes.Equal(base, []byte{'x'}) || len(m.Records[0].Data) != 0 {
					return fmt.Errorf("crdt: invalid extrema ownership epoch")
				}
			}
			if len(m.Records) != 0 && !shadow || m.Value.Type != codec.TypeNull && m.Value.Type != codec.TypeInteger && m.Value.Type != codec.TypeReal {
				return fmt.Errorf("crdt: invalid extrema value")
			}
			if m.Value.Type == codec.TypeReal && (math.IsNaN(m.Value.F) || math.IsInf(m.Value.F, 0)) {
				return fmt.Errorf("crdt: extrema must be finite")
			}
		}
		if p == schema.PN_COUNTER || p == schema.OR_SET {
			if len(m.Records) == 0 && m.Value.Type != codec.TypeNull && !(m.Value.Type == codec.TypeText && (p == schema.PN_COUNTER && m.Value.S == "0" || p == schema.OR_SET && m.Value.S == "[]")) {
				return fmt.Errorf("crdt: only neutral scalar initialization is allowed")
			}
			for _, r := range m.Records {
				_, base, epoch := codec.SplitEpochRecord(r.Key)
				if epoch != shadow {
					return fmt.Errorf("crdt: invalid ownership namespace")
				}
				if err := validateRecord(p, r, s.limits.MaxValueBytes); err != nil {
					return err
				}
				// Local database actor identities can never be rewritten by import.
				r.Key = base
				localDB := bytes.Equal(r.Key[1:17], s.dbID[:])
				imported := m.Flags&codec.FlagCRDTImport != 0
				if p == schema.PN_COUNTER || r.Key[0] == 'a' {
					if localDB && !bytes.Equal(r.Key[17:33], batch.OriginNode[:]) || !localDB && !imported {
						return fmt.Errorf("crdt: unauthorized actor")
					}
					if p == schema.OR_SET && !imported && !bytes.Equal(r.Key[33:49], batch.TxID[:]) {
						return fmt.Errorf("crdt: foreign add transaction")
					}
				}
			}
		}
	}
	return nil
}

func stageCell(s *Store, staged map[string]*remoteGroupCell, order *[]string, key []byte, table uint32, row ids.RowID, col uint32) (*remoteGroupCell, error) {
	if c := staged[string(key)]; c != nil {
		return c, nil
	}
	c := &remoteGroupCell{key: key, table: table, row: row, column: col}
	raw, err := s.getDirect(key)
	if err == nil {
		st, err := codec.DecodeCellState(raw, s.limits)
		if err != nil {
			return nil, err
		}
		c.value = st.Value
		c.version = st.Version
		c.present = true
	} else if !isNotFound(err) {
		return nil, err
	}
	staged[string(key)] = c
	*order = append(*order, string(key))
	return c, nil
}

// finalizeLocalPolicies replaces local deltas with absolute actor components,
// reading staged preceding group members before signing the transaction.
func (s *Store) finalizeLocalPolicies(batch *codec.MutationBatch, staged map[string]*remoteGroupCell, order *[]string) error {
	for i := range batch.Mutations {
		m := &batch.Mutations[i]
		if m.Flags&codec.FlagCounterDelta == 0 {
			continue
		}
		if m.Policy != schema.PN_COUNTER || m.Value.Type != codec.TypeText {
			return fmt.Errorf("crdt: invalid local delta")
		}
		delta, ok := new(big.Int).SetString(m.Value.S, 10)
		if !ok {
			return fmt.Errorf("crdt: invalid counter delta")
		}
		tag := byte('p')
		if delta.Sign() < 0 {
			tag = 'n'
			delta.Neg(delta)
		}
		key := append([]byte{tag}, s.dbID[:]...)
		key = append(key, s.nodeID[:]...)
		if s.isMergeShadow(m.TableID, m.ColumnID) {
			if len(m.Records) == 0 {
				return fmt.Errorf("crdt: missing ownership epoch")
			}
			epoch, _, ok := codec.SplitEpochRecord(m.Records[0].Key)
			if !ok {
				return fmt.Errorf("crdt: invalid epoch")
			}
			key = codec.EpochRecord(epoch, key)
		}
		c, err := stageCell(s, staged, order, crdtKey(m.TableID, m.RowID, m.ColumnID, key), m.TableID, m.RowID, m.ColumnID)
		if err != nil {
			return err
		}
		value := new(big.Int).SetBytes(c.value.B)
		value.Add(value, delta)
		c.value = codec.Blob(value.Bytes())
		c.present = true
		c.changed = true
		m.Records = []codec.CRDTRecord{{Key: key, Data: value.Bytes()}}
		m.Value = codec.Null()
		m.Flags &^= codec.FlagCounterDelta
	}
	return s.validatePolicyBatch(batch)
}

func (s *Store) mergePolicyMutation(m *codec.Mutation, ver crdt.Version, staged map[string]*remoteGroupCell, order *[]string) error {
	start := time.Now()
	s.policyMergeAttempts.Add(1)
	defer func() { s.policyMergeNanos.Add(uint64(time.Since(start))) }()

	cell, err := stageCell(s, staged, order, CellKey(m.TableID, m.RowID, m.ColumnID), m.TableID, m.RowID, m.ColumnID)
	if err != nil {
		return err
	}
	shadow := s.isMergeShadow(m.TableID, m.ColumnID)
	var epoch crdt.Version
	masked := false
	if shadow {
		if len(m.Records) > 0 {
			epoch, _, _ = codec.SplitEpochRecord(m.Records[0].Key)
		}
		oldEpoch, oldValue, active, e := codec.ShadowValue(cell.value, s.limits)
		if !cell.present {
			e = nil
		}
		if e != nil {
			return e
		}
		masked = cell.present && (active && crdt.CompareVersion(oldEpoch, epoch) > 0 || !active && crdt.CompareVersion(cell.version, epoch) > 0)
		if masked && (m.Policy == schema.MAX || m.Policy == schema.MIN) {
			return nil
		}
		if masked {
			// Join retired causal history without changing ownership visibility.
		} else if cell.present && active && oldEpoch == epoch {
			cell.value = oldValue
		} else {
			cell.value = codec.Null()
		}
	}
	if m.Policy == schema.MAX || m.Policy == schema.MIN {
		take := !cell.present || cell.value.Type == codec.TypeNull
		if m.Value.Type != codec.TypeNull && cell.present && cell.value.Type != codec.TypeNull {
			cmp := compareNumber(m.Value, cell.value)
			if cmp == 0 {
				cmp = bytes.Compare(codec.AppendValue(nil, m.Value), codec.AppendValue(nil, cell.value))
			}
			take = m.Policy == schema.MAX && cmp > 0 || m.Policy == schema.MIN && cmp < 0
		}
		if take {
			cell.value = m.Value
			cell.changed = true
		}
		if crdt.CompareVersion(ver, cell.version) > 0 {
			cell.version = ver
			cell.changed = true
		}
		cell.present = true
		if shadow {
			cell.value = codec.ShadowProjection(epoch, cell.value)
		}
		return nil
	}
	for _, r := range m.Records {
		c, err := stageCell(s, staged, order, crdtKey(m.TableID, m.RowID, m.ColumnID, r.Key), m.TableID, m.RowID, m.ColumnID)
		if err != nil {
			return err
		}
		take := !c.present
		if m.Policy == schema.PN_COUNTER {
			take = take || new(big.Int).SetBytes(r.Data).Cmp(new(big.Int).SetBytes(c.value.B)) > 0
		} else if c.present && !bytes.Equal(c.value.B, r.Data) {
			return fmt.Errorf("crdt: conflicting set tag")
		}
		if take {
			c.value = codec.Blob(append([]byte(nil), r.Data...))
			c.changed = true
			c.present = true
		}
		if crdt.CompareVersion(ver, c.version) > 0 {
			c.version = ver
			c.changed = true
		}
	}
	if masked {
		return nil
	}
	projection, err := s.projectCRDT(m.TableID, m.RowID, m.ColumnID, m.Policy, staged)
	if err != nil {
		return err
	}
	if len(m.Records) == 0 && !cell.present && m.Value.Type == codec.TypeNull {
		projection = codec.Null()
	}
	if shadow {
		projection = codec.ShadowProjection(epoch, projection)
	}
	if !cell.present || !cell.value.Equal(projection) {
		cell.value = projection
		cell.changed = true
	}
	if crdt.CompareVersion(ver, cell.version) > 0 {
		cell.version = ver
		cell.changed = true
	}
	cell.present = true
	return nil
}

func compareNumber(a, b codec.Value) int {
	if a.Type == codec.TypeInteger && b.Type == codec.TypeInteger {
		if a.I < b.I {
			return -1
		}
		if a.I > b.I {
			return 1
		}
		return 0
	}
	ar := new(big.Rat)
	br := new(big.Rat)
	if a.Type == codec.TypeInteger {
		ar.SetInt64(a.I)
	} else {
		ar.SetFloat64(a.F)
	}
	if b.Type == codec.TypeInteger {
		br.SetInt64(b.I)
	} else {
		br.SetFloat64(b.F)
	}
	return ar.Cmp(br)
}

func (s *Store) projectCRDT(table uint32, row ids.RowID, col uint32, policy schema.MergePolicy, staged map[string]*remoteGroupCell) (codec.Value, error) {
	p := crdtPrefix(table, row, col)
	records := make(map[string][]byte)
	it, err := s.mem.newIter(&iterOptions{LowerBound: p, UpperBound: prefixEnd(p)})
	if err != nil {
		return codec.Value{}, err
	}
	for it.First(); it.Valid(); it.Next() {
		st, err := codec.DecodeCellState(it.Value(), s.limits)
		if err != nil {
			it.Close()
			return codec.Value{}, err
		}
		records[string(it.Key()[len(p):])] = append([]byte(nil), st.Value.B...)
	}
	err = it.Error()
	it.Close()
	if err != nil {
		return codec.Value{}, err
	}
	for key, c := range staged {
		if bytes.HasPrefix([]byte(key), p) && c.present {
			records[key[len(p):]] = c.value.B
		}
	}
	if s.isMergeShadow(table, col) {
		var epoch crdt.Version
		have := false
		for key := range records {
			e, _, ok := codec.SplitEpochRecord([]byte(key))
			if ok && (!have || crdt.CompareVersion(e, epoch) > 0) {
				epoch = e
				have = true
			}
		}
		records = recordsAtEpoch(records, epoch)
	}
	return ProjectCRDT(policy, records, s.limits.MaxValueBytes)
}
func ProjectCRDT(policy schema.MergePolicy, records map[string][]byte, max int) (codec.Value, error) {
	if policy == schema.PN_COUNTER {
		total := new(big.Int)
		for key, value := range records {
			v := new(big.Int).SetBytes(value)
			if key[0] == 'n' {
				total.Sub(total, v)
			} else {
				total.Add(total, v)
			}
		}
		return codec.Text(total.String()), nil
	}
	unique := make(map[string]codec.SetElement)
	for key, value := range records {
		if key[0] != 'a' {
			continue
		}
		if _, removed := records["r"+key[1:]]; removed {
			continue
		}
		e, err := codec.DecodeSetElement(value, max)
		if err != nil {
			return codec.Value{}, err
		}
		unique[string(value)] = e
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	elements := make([]codec.SetElement, 0, len(keys))
	for _, key := range keys {
		elements = append(elements, unique[key])
	}
	value, err := codec.SetProjection(elements)
	return codec.Text(value), err
}

func writeStagedCells(b *batch, staged map[string]*remoteGroupCell, order []string) ([]WinningChange, error) {
	var winners []WinningChange
	for _, key := range order {
		c := staged[key]
		if !c.changed {
			continue
		}
		var raw []byte
		if c.key[0] == prefixReceipt {
			raw = c.value.B
		} else if c.tomb {
			raw = codec.EncodeTombstone(nil, c.version)
		} else {
			raw = codec.EncodeCellState(nil, codec.CellState{Version: c.version, Value: c.value})
		}
		if err := b.Set(c.key, raw); err != nil {
			return nil, err
		}
		if c.key[0] != prefixCRDT && c.key[0] != prefixReceipt {
			winners = append(winners, WinningChange{TableID: c.table, RowID: c.row, ColumnID: c.column, Value: c.value, Version: c.version, Tombstone: c.tomb})
		}
	}
	return winners, nil
}

// Record identifiers use fixed-width actor identity and transaction operation index.
func SetTag(db ids.DBID, node ids.NodeID, tx ids.TxID, index uint32) []byte {
	key := append([]byte{'a'}, db[:]...)
	key = append(key, node[:]...)
	key = append(key, tx[:]...)
	return binary.BigEndian.AppendUint32(key, index)
}

func (s *Store) isMergeShadow(table, col uint32) bool {
	r := s.mergeRegistry.Load()
	if r == nil {
		return false
	}
	t := r.TableByID(table)
	return t != nil && t.ColumnByID(col) == nil && t.ColumnByID(col^0x80000000) != nil
}
func recordsAtEpoch(records map[string][]byte, epoch crdt.Version) map[string][]byte {
	out := make(map[string][]byte)
	for key, data := range records {
		e, k, ok := codec.SplitEpochRecord([]byte(key))
		if ok && e == epoch {
			out[string(k)] = data
		}
	}
	return out
}
