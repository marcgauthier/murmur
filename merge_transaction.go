package murmur

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/sqlengine"
	"github.com/marcgauthier/murmur/state"
	"math"
	"math/big"
	"sort"
	"strings"
)

var ErrMergePolicyWrite = errors.New("murmur: counter/set columns require explicit CRDT operations")

type SetElement = codec.SetElement

var SetNull = codec.SetNull
var SetBool = codec.SetBool
var SetInt = codec.SetInt
var SetReal = codec.SetReal
var SetString = codec.SetString

func quoteMergeName(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func (tx *Tx) mergeColumn(table, column string, p schema.MergePolicy) (*schema.TableSchema, *schema.ColumnSchema, error) {
	if tx.done {
		return nil, nil, ErrTxDone
	}
	t := tx.db.schemaRegistry().Table(table)
	if t == nil {
		return nil, nil, fmt.Errorf("murmur: unknown table %q", table)
	}
	for i := range t.Columns {
		c := &t.Columns[i]
		if strings.EqualFold(c.Name, column) {
			if c.MergePolicy != p {
				return nil, nil, fmt.Errorf("murmur: column %q requires %s", column, p)
			}
			return t, c, nil
		}
	}
	return nil, nil, fmt.Errorf("murmur: unknown column %q", column)
}
func (tx *Tx) mergeValue(ctx context.Context, t *schema.TableSchema, c *schema.ColumnSchema, row ids.RowID) (codec.Value, error) {
	result, err := tx.stx.QueryRowContext(ctx, "SELECT "+quoteMergeName(c.Name)+" FROM "+quoteMergeName(t.Name)+" WHERE "+quoteMergeName(t.PKColumn().Name)+"=?", row[:])
	if err != nil {
		return codec.Value{}, err
	}
	var value any
	if err := result.Scan(&value); err != nil {
		return codec.Value{}, err
	}
	return codec.FromAny(value)
}
func (tx *Tx) writeMergeProjection(ctx context.Context, t *schema.TableSchema, c *schema.ColumnSchema, row ids.RowID, v codec.Value) error {
	before := len(tx.stx.Pending())
	var value any
	if v.Type == codec.TypeText {
		value = v.S
	}
	if _, err := tx.stx.ExecContext(ctx, "UPDATE "+quoteMergeName(t.Name)+" SET "+quoteMergeName(c.Name)+"=? WHERE "+quoteMergeName(t.PKColumn().Name)+"=?", value, row[:]); err != nil {
		return err
	}
	if tx.mergeEvents == nil {
		tx.mergeEvents = make(map[int]uint32)
	}
	for i := before; i < len(tx.stx.Pending()); i++ {
		tx.mergeEvents[i] = c.ID
	}
	return nil
}
func (tx *Tx) CounterValue(ctx context.Context, table, column string, row ids.RowID) (*big.Int, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	t, c, err := tx.mergeColumn(table, column, schema.PN_COUNTER)
	if err != nil {
		return nil, err
	}
	v, err := tx.mergeValue(ctx, t, c, row)
	if err != nil {
		return nil, err
	}
	if v.Type == codec.TypeNull {
		return new(big.Int), nil
	}
	value, ok := new(big.Int).SetString(v.S, 10)
	if !ok {
		return nil, fmt.Errorf("murmur: invalid counter projection")
	}
	return value, nil
}
func (tx *Tx) CounterAdd(ctx context.Context, table, column string, row ids.RowID, delta *big.Int) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	t, c, err := tx.mergeColumn(table, column, schema.PN_COUNTER)
	if err != nil {
		return err
	}
	if delta == nil {
		return fmt.Errorf("murmur: nil counter delta")
	}
	v, err := tx.mergeValue(ctx, t, c, row)
	if err != nil {
		return err
	}
	if delta.Sign() == 0 {
		return nil
	}
	total := new(big.Int)
	if v.Type != codec.TypeNull {
		if _, ok := total.SetString(v.S, 10); !ok {
			return fmt.Errorf("murmur: invalid counter projection")
		}
	}
	total.Add(total, delta)
	if len(delta.String()) > tx.db.cfg.MaxReplicatedValueBytes {
		return ErrValueTooLarge
	}
	if err := tx.writeMergeProjection(ctx, t, c, row, codec.Text(total.String())); err != nil {
		return err
	}
	tx.mergeMutations = append(tx.mergeMutations, codec.Mutation{TableID: t.ID, RowID: row, ColumnID: c.ID, Policy: c.MergePolicy, Flags: codec.FlagCounterDelta, Value: codec.Text(delta.String())})
	return nil
}
func (tx *Tx) setRecords(t *schema.TableSchema, c *schema.ColumnSchema, row ids.RowID) (map[string][]byte, error) {
	column, epoch, shadow, err := tx.effectiveMergeRecords(t.ID, row, c.ID)
	if err != nil {
		return nil, err
	}
	records, err := tx.db.store.CRDTRecords(t.ID, row, column)
	if shadow {
		records = unwrapEpochRecords(records, epoch)
	}
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte)
	for _, r := range records {
		out[string(r.Key)] = r.Data
	}
	add := func(m codec.Mutation, queued bool) {
		if m.TableID != t.ID || m.RowID != row {
			return
		}
		if queued && shadow && m.ColumnID == c.ID {
			return
		}
		records := m.Records
		if m.ColumnID == column && shadow {
			records = unwrapEpochRecords(records, epoch)
		} else if m.ColumnID != c.ID {
			return
		}
		for _, r := range records {
			out[string(r.Key)] = r.Data
		}
	}
	tx.db.mergePendingMu.Lock()
	for _, batch := range tx.db.mergePending {
		for _, m := range batch.mutations {
			if m.Policy == schema.OR_SET {
				add(m, true)
			}
		}
	}
	tx.db.mergePendingMu.Unlock()
	for _, m := range tx.mergeMutations {
		add(m, false)
	}
	return out, nil
}
func elementsFromRecords(records map[string][]byte, max int) ([]SetElement, error) {
	unique := make(map[string]SetElement)
	for key, data := range records {
		if key[0] != 'a' {
			continue
		}
		if _, ok := records["r"+key[1:]]; ok {
			continue
		}
		e, err := codec.DecodeSetElement(data, max)
		if err != nil {
			return nil, err
		}
		unique[string(data)] = e
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]SetElement, 0, len(keys))
	for _, key := range keys {
		out = append(out, unique[key])
	}
	return out, nil
}
func (tx *Tx) SetValues(ctx context.Context, table, column string, row ids.RowID) ([]SetElement, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	t, c, err := tx.mergeColumn(table, column, schema.OR_SET)
	if err != nil {
		return nil, err
	}
	if _, err := tx.mergeValue(ctx, t, c, row); err != nil {
		return nil, err
	}
	records, err := tx.setRecords(t, c, row)
	if err != nil {
		return nil, err
	}
	return elementsFromRecords(records, tx.db.cfg.MaxReplicatedValueBytes)
}
func (tx *Tx) SetAdd(ctx context.Context, table, column string, row ids.RowID, e SetElement) error {
	return tx.changeSet(ctx, table, column, row, e, true)
}
func (tx *Tx) SetRemove(ctx context.Context, table, column string, row ids.RowID, e SetElement) error {
	return tx.changeSet(ctx, table, column, row, e, false)
}
func (tx *Tx) changeSet(ctx context.Context, table, column string, row ids.RowID, e SetElement, add bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	t, c, err := tx.mergeColumn(table, column, schema.OR_SET)
	if err != nil {
		return err
	}
	if _, err := tx.mergeValue(ctx, t, c, row); err != nil {
		return err
	}
	data, err := e.Encode()
	if err != nil {
		return err
	}
	if len(data) > tx.db.cfg.MaxReplicatedValueBytes {
		return ErrValueTooLarge
	}
	records, err := tx.setRecords(t, c, row)
	if err != nil {
		return err
	}
	m := codec.Mutation{TableID: t.ID, RowID: row, ColumnID: c.ID, Policy: c.MergePolicy, Value: codec.Null()}
	if add {
		key := state.SetTag(tx.db.cfg.DBID, tx.db.cfg.NodeID, tx.txID, uint32(len(tx.mergeMutations)))
		m.Records = append(m.Records, codec.CRDTRecord{Key: key, Data: data})
		records[string(key)] = data
	} else {
		for key, value := range records {
			if key[0] == 'a' && bytes.Equal(value, data) {
				removed := "r" + key[1:]
				if _, ok := records[removed]; !ok {
					m.Records = append(m.Records, codec.CRDTRecord{Key: []byte(removed), Data: data})
					records[removed] = data
				}
			}
		}
		if len(m.Records) == 0 {
			return nil
		}
		sort.Slice(m.Records, func(i, j int) bool { return bytes.Compare(m.Records[i].Key, m.Records[j].Key) < 0 })
	}
	projection, err := state.ProjectCRDT(schema.OR_SET, records, tx.db.cfg.MaxReplicatedValueBytes)
	if err != nil {
		return err
	}
	if err := tx.writeMergeProjection(ctx, t, c, row, projection); err != nil {
		return err
	}
	tx.mergeMutations = append(tx.mergeMutations, m)
	return nil
}
func (tx *Tx) validateMergeCapture(raw []sqlengine.RawChange) error {
	for event, ch := range raw {
		for ordinal, c := range ch.Table.Columns {
			if c.MergePolicy == schema.LWW || ch.Op == sqlengine.OpDelete {
				continue
			}
			value := ch.New[ordinal]
			if c.MergePolicy == schema.MAX || c.MergePolicy == schema.MIN {
				if value.Type != codec.TypeNull && value.Type != codec.TypeInteger && value.Type != codec.TypeReal {
					return fmt.Errorf("murmur: extrema must be numeric")
				}
				if value.Type == codec.TypeReal && (math.IsNaN(value.F) || math.IsInf(value.F, 0)) {
					return fmt.Errorf("murmur: extrema must be finite")
				}
				continue
			}
			if ch.Op == sqlengine.OpUpdate && ch.Old[ordinal].Equal(value) {
				continue
			}
			if tx.bridgeImport != nil && value.Type == codec.TypeText && (c.MergePolicy == schema.PN_COUNTER && value.S == "0" || c.MergePolicy == schema.OR_SET && value.S == "[]") {
				continue
			}
			if tx.mergeEvents[event] == c.ID {
				continue
			}
			if ch.Op == sqlengine.OpInsert && (value.Type == codec.TypeNull && c.Nullable || value.Type == codec.TypeText && (c.MergePolicy == schema.PN_COUNTER && value.S == "0" || c.MergePolicy == schema.OR_SET && value.S == "[]")) {
				continue
			}
			return fmt.Errorf("%w: %s.%s", ErrMergePolicyWrite, ch.Table.Name, c.Name)
		}
	}
	return nil
}

// ImportMergeColumn preserves source causal identities in an authorized bridge
// import transaction. Ordinary transactions cannot call this import path.
func (tx *Tx) ImportMergeColumn(ctx context.Context, table, column string, row ids.RowID, p schema.MergePolicy, value codec.Value, records []codec.CRDTRecord) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.bridgeImport == nil {
		return fmt.Errorf("murmur: merge import requires a bridge import transaction")
	}
	t, c, err := tx.mergeColumn(table, column, p)
	if err != nil {
		return err
	}
	if _, err := tx.mergeValue(ctx, t, c, row); err != nil {
		return err
	}
	copyRecords := make([]codec.CRDTRecord, len(records))
	for i, r := range records {
		_, base, _ := codec.SplitEpochRecord(r.Key)
		if len(base) < 17 {
			return fmt.Errorf("murmur: bridge CRDT source mismatch")
		}
		copyRecords[i] = codec.CRDTRecord{Key: append([]byte(nil), r.Key...), Data: append([]byte(nil), r.Data...)}
	}
	tx.mergeMutations = append(tx.mergeMutations, codec.Mutation{TableID: t.ID, RowID: row, ColumnID: c.ID, Policy: p, Value: value, Records: copyRecords, Flags: codec.FlagCRDTImport})
	return nil
}
func (tx *Tx) attachMergeMutations(mutations []codec.Mutation) []codec.Mutation {
	for i := range mutations {
		m := &mutations[i]
		if m.IsTombstone() {
			continue
		}
		t := tx.db.schemaRegistry().TableByID(m.TableID)
		if t != nil {
			if c := t.ColumnByID(m.ColumnID); c != nil {
				m.Policy = c.MergePolicy
			}
		}
	}
	// Drop captured counter/set scalar changes; causal operations are authoritative.
	out := make([]codec.Mutation, 0, len(mutations)+len(tx.mergeMutations))
	for _, m := range mutations {
		handled := false
		for _, op := range tx.mergeMutations {
			if op.TableID == m.TableID && op.RowID == m.RowID && op.ColumnID == m.ColumnID {
				handled = true
				break
			}
		}
		if !handled {
			out = append(out, m)
		}
	}
	for _, op := range tx.mergeMutations {
		deleted := false
		for _, m := range mutations {
			if m.TableID == op.TableID && m.RowID == op.RowID && m.IsTombstone() {
				deleted = true
			}
		}
		if !deleted {
			out = append(out, op)
		}
	}
	return out
}
func mutationsHaveMergePolicies(m []codec.Mutation) bool {
	for _, v := range m {
		if v.Policy != schema.LWW {
			return true
		}
	}
	return false
}

// RecordBridgeSourceReceipt binds a source receipt to this import's atomic commit.
func (tx *Tx) RecordBridgeSourceReceipt(txID ids.TxID, origin ids.NodeID, sequence uint64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return ErrTxDone
	}
	if tx.bridgeImport == nil {
		return fmt.Errorf("murmur: bridge receipt outside import")
	}
	raw := append([]byte(nil), origin[:]...)
	raw = binary.BigEndian.AppendUint64(raw, sequence)
	tx.mergeMutations = append(tx.mergeMutations, codec.Mutation{TableID: BridgePolicyTableID, RowID: ids.RowID(txID), ColumnID: 2, Flags: codec.FlagBridgeReceipt, Value: codec.Blob(raw)})
	return nil
}

// Queued SQL projections can be observed while the durable writer finalizes
// and signs their batches. Keep an immutable payload copy for those readers.
func copyMergeMutations(src []codec.Mutation) []codec.Mutation {
	var out []codec.Mutation
	for _, m := range src {
		if m.Policy == schema.LWW && !(m.Value.Type == codec.TypeBlob && bytes.Equal(m.Value.B, []byte{0})) {
			continue
		}
		m.Value.B = append([]byte(nil), m.Value.B...)
		records := make([]codec.CRDTRecord, len(m.Records))
		for j, r := range m.Records {
			records[j] = codec.CRDTRecord{Key: append([]byte(nil), r.Key...), Data: append([]byte(nil), r.Data...)}
		}
		m.Records = records
		out = append(out, m)
	}
	return out
}

type pendingMergeBatch struct {
	version   crdt.Version
	mutations []codec.Mutation
}
