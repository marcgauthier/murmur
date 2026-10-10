// Package schema implements the stable table/column registry.
//
// Replication addresses cells by numeric (table, column) IDs so names are
// not repeated on every mutation. IDs are stable across column additions
// and reorderings: explicit IDs are honored, and zero IDs are derived
// deterministically from table/column names.
package schema

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/marcgauthier/murmur/internal/recordcodec"
)

// ErrUnsupportedSchema is returned for schemas that violate the v1
// replicated-schema rules.
var ErrUnsupportedSchema = errors.New("schema: unsupported schema")

// ColumnType is the replicated column type.
type ColumnType uint8

const (
	ColInteger ColumnType = iota + 1
	ColReal
	ColText
	ColBlob
)

func (t ColumnType) String() string {
	switch t {
	case ColInteger:
		return "INTEGER"
	case ColReal:
		return "REAL"
	case ColText:
		return "TEXT"
	case ColBlob:
		return "BLOB"
	default:
		return "UNKNOWN"
	}
}

// ParseColumnType parses a SQLite-ish type name.
func ParseColumnType(s string) (ColumnType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "INTEGER", "INT", "BIGINT", "BOOLEAN", "BOOL":
		return ColInteger, nil
	case "REAL", "DOUBLE", "FLOAT", "NUMERIC":
		return ColReal, nil
	case "TEXT", "VARCHAR", "CHAR", "CLOB":
		return ColText, nil
	case "BLOB":
		return ColBlob, nil
	default:
		return 0, fmt.Errorf("schema: unsupported column type %q", s)
	}
}

// ColumnSchema describes one replicated column.
type ColumnSchema struct {
	MergePolicy MergePolicy
	ID          uint32
	Name        string
	Type        ColumnType
	Nullable    bool
}

// TableSchema describes one replicated table.
type TableSchema struct {
	ID      uint32
	Name    string
	PK      uint32 // column ID of the primary key
	Columns []ColumnSchema
	// RecordDescriptor is a canonical rich Go record descriptor. Empty keeps
	// the historic scalar-table identity and wire format.
	RecordDescriptor []byte
}

// ColumnByID returns the column or nil.
func (t *TableSchema) ColumnByID(id uint32) *ColumnSchema {
	for i := range t.Columns {
		if t.Columns[i].ID == id {
			return &t.Columns[i]
		}
	}
	return nil
}

// PKColumn returns the primary key column or nil.
func (t *TableSchema) PKColumn() *ColumnSchema { return t.ColumnByID(t.PK) }

// Registry is the validated, ID-resolved schema for one epoch.
type Registry struct {
	Epoch  uint64
	Tables []*TableSchema
	byName map[string]*TableSchema
	byID   map[uint32]*TableSchema
	Hash   [32]byte
}

// BuildRegistry validates tables, resolves stable IDs, and hashes the
// canonical form. It enforces the v1 replicated-schema rules: explicit BLOB
// primary key, no rowid/autoincrement dependence, stable IDs.
func BuildRegistry(epoch uint64, tables []TableSchema) (*Registry, error) {
	r := &Registry{
		Epoch:  epoch,
		byName: make(map[string]*TableSchema, len(tables)),
		byID:   make(map[uint32]*TableSchema, len(tables)),
	}
	for i := range tables {
		t := tables[i] // copy so derived IDs don't alias caller memory
		if t.Name == "" {
			return nil, fmt.Errorf("schema: table %d has no name: %w", i, ErrUnsupportedSchema)
		}
		lower := strings.ToLower(t.Name)
		if _, dup := r.byName[lower]; dup {
			return nil, fmt.Errorf("schema: duplicate table %q: %w", t.Name, ErrUnsupportedSchema)
		}
		if len(t.Columns) == 0 {
			return nil, fmt.Errorf("schema: table %q has no columns: %w", t.Name, ErrUnsupportedSchema)
		}
		if len(t.RecordDescriptor) != 0 {
			if err := recordcodec.ValidateDescriptor(t.RecordDescriptor); err != nil {
				return nil, fmt.Errorf("schema: table %q has invalid rich record descriptor: %w", t.Name, err)
			}
		}
		if t.ID == 0 {
			t.ID = deriveID("table:" + lower)
		}
		if t.ID == 0 || t.ID == 0xFFFFFFFF {
			return nil, fmt.Errorf("schema: table %q has reserved id: %w", t.Name, ErrUnsupportedSchema)
		}
		if _, dup := r.byID[t.ID]; dup {
			return nil, fmt.Errorf("schema: duplicate table id %d: %w", t.ID, ErrUnsupportedSchema)
		}
		seenCols := make(map[uint32]bool, len(t.Columns))
		seenNames := make(map[string]bool, len(t.Columns))
		for j := range t.Columns {
			c := &t.Columns[j]
			if c.Name == "" {
				return nil, fmt.Errorf("schema: table %q column %d has no name: %w", t.Name, j, ErrUnsupportedSchema)
			}
			cn := strings.ToLower(c.Name)
			if seenNames[cn] {
				return nil, fmt.Errorf("schema: table %q duplicate column %q: %w", t.Name, c.Name, ErrUnsupportedSchema)
			}
			seenNames[cn] = true
			if c.ID == 0 {
				c.ID = deriveID("column:" + lower + ":" + cn)
			}
			if c.ID == 0 || c.ID == 0xFFFFFFFF {
				return nil, fmt.Errorf("schema: table %q column %q has reserved id: %w", t.Name, c.Name, ErrUnsupportedSchema)
			}
			if seenCols[c.ID] {
				return nil, fmt.Errorf("schema: table %q duplicate column id %d: %w", t.Name, c.ID, ErrUnsupportedSchema)
			}
			// Bridge shadow columns mirror app columns across the high bit
			// (c^0x80000000, same row). Ambiguous pairs and mappings onto
			// reserved IDs would make stored cells undecodable, so new
			// schemas reject them; legacy rows fail shadow writes closed
			// instead of corrupting.
			if c.ID == 0x80000000 || c.ID == 0x7FFFFFFF {
				return nil, fmt.Errorf("schema: table %q column %q maps to a reserved bridge shadow id: %w", t.Name, c.Name, ErrUnsupportedSchema)
			}
			if seenCols[c.ID^0x80000000] {
				return nil, fmt.Errorf("schema: table %q column %q collides with a bridge shadow id: %w", t.Name, c.Name, ErrUnsupportedSchema)
			}
			seenCols[c.ID] = true
			if c.Type == 0 {
				return nil, fmt.Errorf("schema: table %q column %q has no type: %w", t.Name, c.Name, ErrUnsupportedSchema)
			}
			if err := c.validateMergePolicy(); err != nil {
				return nil, err
			}
		}
		// Primary key rules: explicit, single, BLOB(16)-compatible, NOT NULL.
		pk := t.ColumnByID(t.PK)
		if pk == nil {
			// Allow naming the PK by column name zero value? No: require the
			// caller to set PK to the ID (possibly derived). Try to resolve
			// a column literally named "id" as a convenience, but only when
			// PK is unset.
			if t.PK == 0 {
				for j := range t.Columns {
					if strings.EqualFold(t.Columns[j].Name, "id") {
						t.PK = t.Columns[j].ID
						pk = &t.Columns[j]
						break
					}
				}
			}
			if pk == nil {
				return nil, fmt.Errorf("schema: table %q needs an explicit primary key column id: %w", t.Name, ErrUnsupportedSchema)
			}
		}
		if pk.Type != ColBlob {
			return nil, fmt.Errorf("schema: table %q primary key must be BLOB(16), got %s: %w", t.Name, pk.Type, ErrUnsupportedSchema)
		}
		if pk.Nullable {
			return nil, fmt.Errorf("schema: table %q primary key must be NOT NULL: %w", t.Name, ErrUnsupportedSchema)
		}
		if pk.MergePolicy != LWW {
			return nil, fmt.Errorf("schema: primary key cannot use %s: %w", pk.MergePolicy, ErrUnsupportedSchema)
		}
		tp := &TableSchema{
			ID:               t.ID,
			Name:             t.Name,
			PK:               t.PK,
			Columns:          append([]ColumnSchema(nil), t.Columns...),
			RecordDescriptor: append([]byte(nil), t.RecordDescriptor...),
		}
		r.Tables = append(r.Tables, tp)
		r.byName[lower] = tp
		r.byID[t.ID] = tp
	}
	r.Hash = r.canonicalHash()
	return r, nil
}

// Table returns the table by name (case-insensitive) or nil.
func (r *Registry) Table(name string) *TableSchema { return r.byName[strings.ToLower(name)] }

// TableByID returns the table by ID or nil.
func (r *Registry) TableByID(id uint32) *TableSchema { return r.byID[id] }

// canonicalHash hashes a canonical serialization of the registry.
func (r *Registry) canonicalHash() [32]byte {
	tables := append([]*TableSchema(nil), r.Tables...)
	sort.Slice(tables, func(i, j int) bool { return tables[i].ID < tables[j].ID })
	h := sha256.New()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], r.Epoch)
	h.Write(buf[:])
	for _, t := range tables {
		var id [4]byte
		binary.BigEndian.PutUint32(id[:], t.ID)
		h.Write(id[:])
		h.Write([]byte(strings.ToLower(t.Name)))
		h.Write([]byte{0})
		binary.BigEndian.PutUint32(id[:], t.PK)
		h.Write(id[:])
		cols := append([]ColumnSchema(nil), t.Columns...)
		sort.Slice(cols, func(i, j int) bool { return cols[i].ID < cols[j].ID })
		for _, c := range cols {
			binary.BigEndian.PutUint32(id[:], c.ID)
			h.Write(id[:])
			h.Write([]byte(strings.ToLower(c.Name)))
			h.Write([]byte{0, byte(c.Type)})
			if c.Nullable {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		}
		if len(t.RecordDescriptor) != 0 {
			h.Write([]byte("murmur/schema/rime-record/v1"))
			binary.BigEndian.PutUint32(id[:], uint32(len(t.RecordDescriptor)))
			h.Write(id[:])
			h.Write(t.RecordDescriptor)
		}
	}
	var out [32]byte
	// Preserve the identity of historical all-LWW schemas exactly.
	hasPolicies := false
	for _, table := range tables {
		for _, column := range table.Columns {
			if column.MergePolicy != LWW {
				hasPolicies = true
				break
			}
		}
		if hasPolicies {
			break
		}
	}
	if hasPolicies {
		h.Write([]byte("murmur/schema/merge-policies/v1"))
		for _, t := range tables {
			cols := append([]ColumnSchema(nil), t.Columns...)
			sort.Slice(cols, func(i, j int) bool { return cols[i].ID < cols[j].ID })
			for _, c := range cols {
				binary.BigEndian.PutUint32(buf[:4], t.ID)
				binary.BigEndian.PutUint32(buf[4:], c.ID)
				h.Write(buf[:])
				h.Write([]byte{byte(c.MergePolicy)})
			}
		}
	}
	copy(out[:], h.Sum(nil))
	return out
}

// deriveID deterministically maps a name to a nonzero, non-sentinel ID.
func deriveID(s string) uint32 {
	sum := sha256.Sum256([]byte(s))
	id := binary.BigEndian.Uint32(sum[:4])
	if id == 0 || id == 0xFFFFFFFF {
		id ^= 0x9e3779b9
	}
	return id
}

// StableID derives an identity using the registry's existing name algorithm.
func StableID(name string) uint32 { return deriveID(name) }
