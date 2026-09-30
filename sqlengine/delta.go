package sqlengine

import (
	"fmt"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

// CellKey identifies one replicated cell.
type CellKey struct {
	TableID  uint32
	RowID    ids.RowID
	ColumnID uint32
}

// RowKey identifies one replicated row.
type RowKey struct {
	TableID uint32
	RowID   ids.RowID
}

// CellDelta is the coalesced before/after image of one cell.
type CellDelta struct {
	HaveOrig bool
	Orig     codec.Value
	HaveLast bool
	Last     codec.Value
}

// Delta coalesces a transaction's raw changes by (table, row, column).
type Delta struct {
	cells map[CellKey]*CellDelta
	rows  map[RowKey]*rowFlags
	// ordToCol caches ordinal->columnID per table.
	ordToCol map[uint32][]uint32
}

type rowFlags struct {
	inserted bool
	deleted  bool
}

// NewDelta returns an empty delta.
func NewDelta() *Delta {
	return &Delta{
		cells:    make(map[CellKey]*CellDelta),
		rows:     make(map[RowKey]*rowFlags),
		ordToCol: make(map[uint32][]uint32),
	}
}

func (d *Delta) ordMap(ch RawChange) []uint32 {
	m, ok := d.ordToCol[ch.Table.ID]
	if !ok {
		m = make([]uint32, len(ch.Table.Columns))
		for i := range ch.Table.Columns {
			m[i] = ch.Table.Columns[i].ID
		}
		d.ordToCol[ch.Table.ID] = m
	}
	return m
}

func (d *Delta) rowOf(table uint32, row ids.RowID) *rowFlags {
	k := RowKey{TableID: table, RowID: row}
	f, ok := d.rows[k]
	if !ok {
		f = &rowFlags{}
		d.rows[k] = f
	}
	return f
}

func (d *Delta) setCell(table uint32, row ids.RowID, col uint32, orig *codec.Value, last *codec.Value) {
	k := CellKey{TableID: table, RowID: row, ColumnID: col}
	c, ok := d.cells[k]
	if !ok {
		c = &CellDelta{}
		d.cells[k] = c
	}
	if orig != nil && !c.HaveOrig && !c.HaveLast {
		// First touch of this cell: record the before-image.
		c.HaveOrig = true
		c.Orig = *orig
	}
	if last != nil {
		c.HaveLast = true
		c.Last = *last
	} else {
		// Deletion removes the after-image.
		c.HaveLast = false
	}
}

// Add folds one captured change into the delta.
func (d *Delta) Add(ev RawChange) error {
	pkOrd := pkOrdinal(ev.Table)
	ordMap := d.ordMap(ev)
	switch ev.Op {
	case OpInsert:
		row, err := rowIDOf(ev.New, pkOrd)
		if err != nil {
			return err
		}
		f := d.rowOf(ev.Table.ID, row)
		f.inserted = true
		f.deleted = false // delete-then-insert resurrects
		for i, v := range ev.New {
			d.setCell(ev.Table.ID, row, ordMap[i], nil, &v)
		}
	case OpDelete:
		row, err := rowIDOf(ev.Old, pkOrd)
		if err != nil {
			return err
		}
		d.rowOf(ev.Table.ID, row).deleted = true
		for i, v := range ev.Old {
			d.setCell(ev.Table.ID, row, ordMap[i], &v, nil)
		}
	case OpUpdate:
		oldRow, err := rowIDOf(ev.Old, pkOrd)
		if err != nil {
			return err
		}
		newRow, err := rowIDOf(ev.New, pkOrd)
		if err != nil {
			return err
		}
		_ = d.rowOf(ev.Table.ID, oldRow)
		if oldRow != newRow {
			// PK rewrite: expand into delete(old) + insert(new).
			d.rowOf(ev.Table.ID, oldRow).deleted = true
			for i, v := range ev.Old {
				d.setCell(ev.Table.ID, oldRow, ordMap[i], &v, nil)
			}
			f := d.rowOf(ev.Table.ID, newRow)
			f.inserted = true
			f.deleted = false
			for i, v := range ev.New {
				d.setCell(ev.Table.ID, newRow, ordMap[i], nil, &v)
			}
			return nil
		}
		for i := range ev.New {
			if ev.Old[i].Equal(ev.New[i]) {
				continue // unchanged by this statement
			}
			o, n := ev.Old[i], ev.New[i]
			d.setCell(ev.Table.ID, oldRow, ordMap[i], &o, &n)
		}
	default:
		return fmt.Errorf("sqlengine: unknown op %d", ev.Op)
	}
	return nil
}

// Build emits the coalesced mutations:
//
//   - INSERT then DELETE in one txn -> nothing
//   - UPDATE then DELETE -> row tombstone only
//   - DELETE then INSERT -> final row state (resurrection)
//   - value restored to its before-image -> omitted
//
// maxValueBytes bounds TEXT/BLOB payloads; oversize values fail here so the
// caller can roll back SQL instead of dirtying the materializer.
func (d *Delta) Build(maxValueBytes int) ([]codec.Mutation, error) {
	var out []codec.Mutation
	for rk, f := range d.rows {
		switch {
		case f.inserted && f.deleted:
			continue // net nothing
		case f.deleted && !f.inserted:
			out = append(out, codec.Mutation{
				TableID: rk.TableID, RowID: rk.RowID,
				ColumnID: codec.ColumnTombstone,
				Flags:    codec.FlagTombstone,
			})
			continue
		}
	}
	// Tombstoned rows drop their cell deltas.
	for k, c := range d.cells {
		f := d.rows[RowKey{TableID: k.TableID, RowID: k.RowID}]
		if f == nil {
			continue
		}
		if f.inserted && f.deleted {
			continue
		}
		if f.deleted && !f.inserted {
			continue
		}
		if !c.HaveLast {
			continue
		}
		if !f.inserted && c.HaveOrig && c.Orig.Equal(c.Last) {
			continue // restored to before-image
		}
		if err := checkSize(c.Last, maxValueBytes); err != nil {
			return nil, err
		}
		out = append(out, codec.Mutation{
			TableID: k.TableID, RowID: k.RowID,
			ColumnID: k.ColumnID, Value: c.Last,
		})
	}
	return out, nil
}

// IsEmpty reports whether the delta carries no externally visible change.
func (d *Delta) IsEmpty() bool {
	for _, f := range d.rows {
		if f.inserted != f.deleted {
			// inserted-only or deleted-only rows are visible; both set is net nothing.
			if !(f.inserted && f.deleted) {
				return false
			}
		}
	}
	for k, c := range d.cells {
		f := d.rows[RowKey{TableID: k.TableID, RowID: k.RowID}]
		if f == nil || (f.inserted && f.deleted) || (f.deleted && !f.inserted) {
			continue
		}
		if !c.HaveLast {
			continue
		}
		if !f.inserted && c.HaveOrig && c.Orig.Equal(c.Last) {
			continue
		}
		return false
	}
	return true
}

func checkSize(v codec.Value, max int) error {
	var n int
	switch v.Type {
	case codec.TypeText:
		n = len(v.S)
	case codec.TypeBlob:
		n = len(v.B)
	default:
		return nil
	}
	if n > max {
		return fmt.Errorf("sqlengine: value of %d bytes exceeds limit %d", n, max)
	}
	return nil
}
