// Schema manifests and canonical merge logic.
//
// The cluster uses state-based full schema synchronization
// (architecture/schema.md sections 6 and 50): every node persists complete,
// immutable schema revisions plus the ancestry needed to deduplicate
// compatible concurrent merges. Numeric versions never decide alone —
// ancestry and canonical declarations disambiguate equal-epoch branches.
package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
)

// Manifest is one immutable schema revision.
type Manifest struct {
	// Version is the authored epoch. Authored revisions use
	// 1 + max(parent epochs); compatible merges follow ancestry.
	Version uint64
	// CreatedOnNode authored this revision (the merging frontier's
	// greatest tuple for synthetic merges, never the receiver).
	CreatedOnNode ids.NodeID
	// TimeCreated is the authoring HLC timestamp.
	TimeCreated uint64
	// Hash is the canonical SHA-256 over version, tables, columns, and
	// types. It identifies content, not authorship: identical declarations
	// at one version hash identically on every node.
	Hash [32]byte
	// Tables carries full definitions with resolved stable IDs.
	Tables []TableSchema
	// Parents holds canonical parent revision identities, sorted.
	Parents [][32]byte
}

// RevisionID returns the canonical identity of a revision: the SHA-256 of
// its deterministic encoding, binding version, authorship, time, content
// hash, and parents.
func RevisionID(m *Manifest) [32]byte {
	return sha256.Sum256(EncodeManifest(m))
}

// EqualRevision reports whether two manifests are the same revision.
func EqualRevision(a, b *Manifest) bool {
	return RevisionID(a) == RevisionID(b)
}

// Limits bound decoded manifests (no unbounded network/storage allocs).
const (
	maxManifestTables    = 1024
	maxManifestColumns   = 4096
	maxManifestName      = 1024
	maxManifestParents   = 256
	maxManifestBytes     = 4 << 20
	maxRevisionAncestry  = 4096
	maxFrontierRevisions = 256
	maxAncestryWalkDepth = 4096
)

// manifestMagic versions the binary encoding.
var manifestMagic = []byte("SMF1")

// EncodeManifest appends the deterministic binary encoding of m. Tables,
// columns, and parents sort by ID so equivalent declarations encode
// identically regardless of declaration order.
func EncodeManifest(m *Manifest) []byte {
	tables := append([]TableSchema(nil), m.Tables...)
	sort.Slice(tables, func(i, j int) bool { return tables[i].ID < tables[j].ID })
	parents := append([][32]byte(nil), m.Parents...)
	sort.Slice(parents, func(i, j int) bool {
		return bytes.Compare(parents[i][:], parents[j][:]) < 0
	})
	var dst []byte
	richEncoding := hasRecordDescriptors(m.Tables)
	policyEncoding := richEncoding || hasMergePolicies(m.Tables)
	if richEncoding {
		dst = append(dst, []byte("SMF3")...)
	} else if policyEncoding {
		dst = append(dst, []byte("SMF2")...)
	} else {
		dst = append(dst, manifestMagic...)
	}
	dst = binary.BigEndian.AppendUint64(dst, m.Version)
	dst = append(dst, m.CreatedOnNode[:]...)
	dst = binary.BigEndian.AppendUint64(dst, m.TimeCreated)
	dst = append(dst, m.Hash[:]...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(parents)))
	for _, p := range parents {
		dst = append(dst, p[:]...)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(tables)))
	for i := range tables {
		t := &tables[i]
		dst = binary.BigEndian.AppendUint32(dst, t.ID)
		dst = binary.BigEndian.AppendUint32(dst, t.PK)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(t.Name)))
		dst = append(dst, t.Name...)
		cols := append([]ColumnSchema(nil), t.Columns...)
		sort.Slice(cols, func(i, j int) bool { return cols[i].ID < cols[j].ID })
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(cols)))
		for _, c := range cols {
			dst = binary.BigEndian.AppendUint32(dst, c.ID)
			dst = append(dst, byte(c.Type))
			if c.Nullable {
				dst = append(dst, 1)
			} else {
				dst = append(dst, 0)
			}
			if policyEncoding {
				dst = append(dst, byte(c.MergePolicy))
			}
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(c.Name)))
			dst = append(dst, c.Name...)
		}
		if richEncoding {
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(t.RecordDescriptor)))
			dst = append(dst, t.RecordDescriptor...)
		}
	}
	return dst
}

// DecodeManifest decodes and authenticates one manifest: the embedded hash
// must match the recomputed canonical hash or decoding fails closed.
func DecodeManifest(src []byte) (*Manifest, error) {
	if len(src) > maxManifestBytes {
		return nil, fmt.Errorf("schema: manifest of %d bytes exceeds limit", len(src))
	}
	rest := src
	need := func(n int) ([]byte, error) {
		if len(rest) < n {
			return nil, fmt.Errorf("schema: truncated manifest")
		}
		b := rest[:n]
		rest = rest[n:]
		return b, nil
	}
	magic, err := need(4)
	if err != nil {
		return nil, err
	}
	richEncoding := bytes.Equal(magic, []byte("SMF3"))
	policyEncoding := richEncoding || bytes.Equal(magic, []byte("SMF2"))
	if !policyEncoding && !bytes.Equal(magic, manifestMagic) {
		return nil, fmt.Errorf("schema: bad manifest magic")
	}
	m := &Manifest{}
	ver, err := need(8)
	if err != nil {
		return nil, err
	}
	m.Version = binary.BigEndian.Uint64(ver)
	author, err := need(16)
	if err != nil {
		return nil, err
	}
	copy(m.CreatedOnNode[:], author)
	tm, err := need(8)
	if err != nil {
		return nil, err
	}
	m.TimeCreated = binary.BigEndian.Uint64(tm)
	h, err := need(32)
	if err != nil {
		return nil, err
	}
	copy(m.Hash[:], h)
	np, err := need(4)
	if err != nil {
		return nil, err
	}
	nparents := binary.BigEndian.Uint32(np)
	if nparents > maxManifestParents {
		return nil, fmt.Errorf("schema: absurd parent count %d", nparents)
	}
	for i := uint32(0); i < nparents; i++ {
		p, err := need(32)
		if err != nil {
			return nil, err
		}
		var id [32]byte
		copy(id[:], p)
		m.Parents = append(m.Parents, id)
	}
	nt, err := need(4)
	if err != nil {
		return nil, err
	}
	ntables := binary.BigEndian.Uint32(nt)
	if ntables > maxManifestTables {
		return nil, fmt.Errorf("schema: absurd table count %d", ntables)
	}
	for i := uint32(0); i < ntables; i++ {
		var t TableSchema
		fixed, err := need(12)
		if err != nil {
			return nil, fmt.Errorf("schema: table %d: %w", i, err)
		}
		t.ID = binary.BigEndian.Uint32(fixed[0:4])
		t.PK = binary.BigEndian.Uint32(fixed[4:8])
		nameLen := binary.BigEndian.Uint32(fixed[8:12])
		if nameLen == 0 || nameLen > maxManifestName {
			return nil, fmt.Errorf("schema: table %d bad name length %d", i, nameLen)
		}
		name, err := need(int(nameLen))
		if err != nil {
			return nil, fmt.Errorf("schema: table %d: %w", i, err)
		}
		t.Name = string(append([]byte(nil), name...))
		nc, err := need(4)
		if err != nil {
			return nil, fmt.Errorf("schema: table %q: %w", t.Name, err)
		}
		ncols := binary.BigEndian.Uint32(nc)
		if ncols == 0 || ncols > maxManifestColumns {
			return nil, fmt.Errorf("schema: table %q bad column count %d", t.Name, ncols)
		}
		for j := uint32(0); j < ncols; j++ {
			var c ColumnSchema
			columnHeader := 10
			if policyEncoding {
				columnHeader++
			}
			cfixed, err := need(columnHeader)
			if err != nil {
				return nil, fmt.Errorf("schema: table %q column %d: %w", t.Name, j, err)
			}
			c.ID = binary.BigEndian.Uint32(cfixed[0:4])
			c.Type = ColumnType(cfixed[4])
			switch c.Type {
			case ColInteger, ColReal, ColText, ColBlob:
			default:
				return nil, fmt.Errorf("schema: table %q column %d bad type %d", t.Name, j, cfixed[4])
			}
			switch cfixed[5] {
			case 0:
				c.Nullable = false
			case 1:
				c.Nullable = true
			default:
				return nil, fmt.Errorf("schema: table %q column %d bad nullability", t.Name, j)
			}
			nameOffset := 6
			if policyEncoding {
				c.MergePolicy = MergePolicy(cfixed[6])
				nameOffset++
			}
			cnameLen := binary.BigEndian.Uint32(cfixed[nameOffset : nameOffset+4])
			if cnameLen == 0 || cnameLen > maxManifestName {
				return nil, fmt.Errorf("schema: table %q column %d bad name length", t.Name, j)
			}
			cname, err := need(int(cnameLen))
			if err != nil {
				return nil, fmt.Errorf("schema: table %q column %d: %w", t.Name, j, err)
			}
			c.Name = string(append([]byte(nil), cname...))
			t.Columns = append(t.Columns, c)
		}
		if richEncoding {
			rawLen, err := need(4)
			if err != nil {
				return nil, err
			}
			descriptorLen := binary.BigEndian.Uint32(rawLen)
			if descriptorLen > maxManifestBytes || descriptorLen > uint32(len(rest)) {
				return nil, fmt.Errorf("schema: invalid rich descriptor length %d", descriptorLen)
			}
			raw, err := need(int(descriptorLen))
			if err != nil {
				return nil, err
			}
			t.RecordDescriptor = append([]byte(nil), raw...)
		}
		m.Tables = append(m.Tables, t)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("schema: manifest has %d trailing bytes", len(rest))
	}
	if want := ContentHash(m.Version, m.Tables); want != m.Hash {
		return nil, fmt.Errorf("schema: manifest hash mismatch")
	}
	return m, nil
}

// ContentHash returns the canonical content hash for a version and table
// set: the same canonicalization the registry uses, so manifest hashes and
// registry hashes agree for identical declarations.
func ContentHash(version uint64, tables []TableSchema) [32]byte {
	ptrs := make([]*TableSchema, len(tables))
	for i := range tables {
		t := tables[i]
		ptrs[i] = &TableSchema{
			ID:               t.ID,
			Name:             t.Name,
			PK:               t.PK,
			Columns:          append([]ColumnSchema(nil), t.Columns...),
			RecordDescriptor: append([]byte(nil), t.RecordDescriptor...),
		}
	}
	r := &Registry{Epoch: version, Tables: ptrs}
	return r.canonicalHash()
}

func hasRecordDescriptors(tables []TableSchema) bool {
	for _, t := range tables {
		if len(t.RecordDescriptor) != 0 {
			return true
		}
	}
	return false
}

// Registry builds the validated ID-resolved registry for a manifest. IDs in
// a manifest are always resolved; BuildRegistry honors them and rejects any
// section 6 violation or ID collision.
func (m *Manifest) Registry() (*Registry, error) {
	return BuildRegistry(m.Version, m.Tables)
}

// IsSyntheticMerge reports whether the revision is a multi-parent merge.
func (m *Manifest) IsSyntheticMerge() bool { return len(m.Parents) > 1 }

// NewGenesis builds the first revision for a fresh database. Tables must
// already carry resolved IDs (BuildRegistry output).
func NewGenesis(tables []TableSchema, version uint64, author ids.NodeID, timeHLC uint64) (*Manifest, error) {
	cp := cloneTables(tables)
	if _, err := BuildRegistry(version, cp); err != nil {
		return nil, err
	}
	m := &Manifest{
		Version:       version,
		CreatedOnNode: author,
		TimeCreated:   timeHLC,
		Tables:        cp,
	}
	m.Hash = ContentHash(m.Version, m.Tables)
	return m, nil
}

// NewAuthoredRevision builds a single-parent authored revision. The full
// declaration must be an additive superset of the parent: drops, renames,
// type/nullability/PK changes, and ID collisions fail with an identifying
// error instead of forking incompatible content.
func NewAuthoredRevision(parent *Manifest, tables []TableSchema, author ids.NodeID, timeHLC uint64) (*Manifest, error) {
	cp := cloneTables(tables)
	if _, err := BuildRegistry(parent.Version+1, cp); err != nil {
		return nil, err
	}
	if err := checkSuperset(parent.Tables, cp); err != nil {
		return nil, err
	}
	parentID := RevisionID(parent)
	m := &Manifest{
		Version:       parent.Version + 1,
		CreatedOnNode: author,
		TimeCreated:   timeHLC,
		Tables:        cp,
		Parents:       [][32]byte{parentID},
	}
	m.Hash = ContentHash(m.Version, m.Tables)
	return m, nil
}

// AssignIDs preserves stable IDs by name from current into next: zero table
// or column IDs inherit the current ID when the (case-insensitive) name
// matches, explicit mismatches fail, and truly new names keep zero IDs for
// deterministic derivation by BuildRegistry.
func AssignIDs(current, next []TableSchema) ([]TableSchema, error) {
	byName := make(map[string]*TableSchema, len(current))
	byID := make(map[uint32]*TableSchema, len(current))
	for i := range current {
		t := &current[i]
		byName[lowerName(t.Name)] = t
		byID[t.ID] = t
	}
	out := cloneTables(next)
	for i := range out {
		t := &out[i]
		old, found := byName[lowerName(t.Name)]
		if !found && t.ID != 0 {
			old, found = byID[t.ID]
			if found && old.Name == t.Name {
				found = false
			}
		}
		if found {
			if t.ID == 0 {
				t.ID = old.ID
			} else if t.ID != old.ID {
				return nil, fmt.Errorf("schema: table %q changes stable id %d to %d: %w",
					t.Name, old.ID, t.ID, ErrUnsupportedSchema)
			}
			oldCols := make(map[string]*ColumnSchema, len(old.Columns))
			for j := range old.Columns {
				oldCols[lowerName(old.Columns[j].Name)] = &old.Columns[j]
			}
			for j := range t.Columns {
				c := &t.Columns[j]
				oldc, ok := oldCols[lowerName(c.Name)]
				if !ok {
					continue
				}
				if c.ID == 0 {
					c.ID = oldc.ID
				} else if c.ID != oldc.ID {
					return nil, fmt.Errorf("schema: table %q column %q changes stable id %d to %d: %w",
						t.Name, c.Name, oldc.ID, c.ID, ErrUnsupportedSchema)
				}
			}
			continue
		}
		if t.ID != 0 {
			if other, dup := byID[t.ID]; dup {
				return nil, fmt.Errorf("schema: new table %q reuses id %d of table %q: %w",
					t.Name, t.ID, other.Name, ErrUnsupportedSchema)
			}
		}
	}
	return out, nil
}

// UnionTables computes the additive union of two declarations. Shared
// tables and columns must be identical; distinct compatible additions merge.
// Anything else — same-name/different-definition, same-ID/different-name,
// PK changes — fails with ErrUnsupportedSchema identifying the object.
// The union never drops declarations, so receiving a numerically higher
// concurrent branch preserves local additions.
func UnionTables(a, b []TableSchema) ([]TableSchema, error) {
	merged := cloneTables(a)
	index := make(map[string]*TableSchema, len(merged))
	ids := make(map[uint32]*TableSchema, len(merged))
	for i := range merged {
		t := &merged[i]
		index[lowerName(t.Name)] = t
		ids[t.ID] = t
	}
	for _, bt := range b {
		if at, ok := index[lowerName(bt.Name)]; ok {
			if err := unionTable(at, &bt); err != nil {
				return nil, err
			}
			continue
		}
		if other, dup := ids[bt.ID]; dup {
			return nil, fmt.Errorf("schema: table id %d names both %q and %q: %w",
				bt.ID, other.Name, bt.Name, ErrUnsupportedSchema)
		}
		cp := bt
		cp.Columns = append([]ColumnSchema(nil), bt.Columns...)
		merged = append(merged, cp)
		t := &merged[len(merged)-1]
		index[lowerName(t.Name)] = t
		ids[t.ID] = t
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].ID < merged[j].ID })
	return merged, nil
}

// unionTable merges bt into at (same table name). Shared columns must match
// exactly; new columns append.
func unionTable(at *TableSchema, bt *TableSchema) error {
	if at.ID != bt.ID {
		return fmt.Errorf("schema: table %q has conflicting ids %d and %d: %w",
			at.Name, at.ID, bt.ID, ErrUnsupportedSchema)
	}
	if at.PK != bt.PK {
		return fmt.Errorf("schema: table %q changes primary key id %d to %d: %w",
			at.Name, at.PK, bt.PK, ErrUnsupportedSchema)
	}
	if !bytes.Equal(at.RecordDescriptor, bt.RecordDescriptor) {
		if len(at.RecordDescriptor) == 0 || len(bt.RecordDescriptor) == 0 {
			return fmt.Errorf("schema: table %q has conflicting rich record descriptors: %w", at.Name, ErrUnsupportedSchema)
		}
		merged, err := recordcodec.UnionDescriptors(at.RecordDescriptor, bt.RecordDescriptor)
		if err != nil {
			return fmt.Errorf("schema: table %q rich descriptor conflict: %v: %w", at.Name, err, ErrUnsupportedSchema)
		}
		at.RecordDescriptor = merged
	}
	byName := make(map[string]*ColumnSchema, len(at.Columns))
	byID := make(map[uint32]*ColumnSchema, len(at.Columns))
	for i := range at.Columns {
		c := &at.Columns[i]
		byName[lowerName(c.Name)] = c
		byID[c.ID] = c
	}
	for _, bc := range bt.Columns {
		if ac, ok := byName[lowerName(bc.Name)]; ok {
			if ac.ID != bc.ID {
				return fmt.Errorf("schema: table %q column %q has conflicting ids %d and %d: %w",
					at.Name, bc.Name, ac.ID, bc.ID, ErrUnsupportedSchema)
			}
			if ac.Type != bc.Type || ac.Nullable != bc.Nullable || ac.MergePolicy != bc.MergePolicy {
				return fmt.Errorf("schema: table %q column %q has conflicting definitions: %w",
					at.Name, bc.Name, ErrUnsupportedSchema)
			}
			continue
		}
		if other, dup := byID[bc.ID]; dup {
			return fmt.Errorf("schema: table %q column id %d names both %q and %q: %w",
				at.Name, bc.ID, other.Name, bc.Name, ErrUnsupportedSchema)
		}
		at.Columns = append(at.Columns, bc)
		byName[lowerName(bc.Name)] = &at.Columns[len(at.Columns)-1]
		byID[bc.ID] = &at.Columns[len(at.Columns)-1]
	}
	return nil
}

// checkSuperset verifies next contains every declaration of base identically:
// same tables/columns by name with equal IDs, types, nullability, and PKs.
// Missing or altered declarations fail, identifying the object.
func checkSuperset(base, next []TableSchema) error {
	byName := make(map[string]*TableSchema, len(next))
	byID := make(map[uint32]*TableSchema, len(next))
	for i := range next {
		t := &next[i]
		byName[lowerName(t.Name)] = t
		byID[t.ID] = t
	}
	for _, bt := range base {
		nt, ok := byName[lowerName(bt.Name)]
		if !ok {
			nt, ok = byID[bt.ID]
		}
		if !ok {
			return fmt.Errorf("schema: migration drops table %q (destructive changes need coordinated maintenance): %w",
				bt.Name, ErrUnsupportedSchema)
		}
		if nt.ID != bt.ID {
			return fmt.Errorf("schema: table %q changes stable id %d to %d: %w",
				bt.Name, bt.ID, nt.ID, ErrUnsupportedSchema)
		}
		if nt.PK != bt.PK {
			return fmt.Errorf("schema: table %q changes primary key: %w", bt.Name, ErrUnsupportedSchema)
		}
		if !recordcodec.DescriptorSuperset(bt.RecordDescriptor, nt.RecordDescriptor) {
			return fmt.Errorf("schema: migration changes rich record descriptor for table %q: %w", bt.Name, ErrUnsupportedSchema)
		}
		ncols := make(map[string]*ColumnSchema, len(nt.Columns))
		for i := range nt.Columns {
			c := &nt.Columns[i]
			ncols[lowerName(c.Name)] = c
		}
		for _, bc := range bt.Columns {
			nc, ok := ncols[lowerName(bc.Name)]
			if !ok {
				return fmt.Errorf("schema: migration drops table %q column %q (destructive changes need coordinated maintenance): %w",
					bt.Name, bc.Name, ErrUnsupportedSchema)
			}
			if nc.ID != bc.ID || nc.Type != bc.Type || nc.Nullable != bc.Nullable || nc.MergePolicy != bc.MergePolicy {
				return fmt.Errorf("schema: table %q column %q changes definition: %w",
					bt.Name, bc.Name, ErrUnsupportedSchema)
			}
		}
	}
	return nil
}

// IsSuperset reports whether next carries every declaration of base
// identically (additive-only evolution).
func IsSuperset(base, next []TableSchema) bool {
	return checkSuperset(base, next) == nil
}

// Frontier flattens tips into authored revisions: synthetic multi-parent
// merges expand recursively, then ancestors covered by another frontier
// member drop out. The result sorts by revision ID for determinism.
// Unknown revisions fail instead of guessing.
func Frontier(revs map[[32]byte]*Manifest, tips ...[32]byte) ([][32]byte, error) {
	if len(tips) > maxFrontierRevisions {
		return nil, fmt.Errorf("schema: frontier of %d tips exceeds limit", len(tips))
	}
	flat := make([][32]byte, 0, len(tips))
	seen := make(map[[32]byte]bool)
	var expand func(id [32]byte, depth int) error
	expand = func(id [32]byte, depth int) error {
		if depth > maxAncestryWalkDepth {
			return fmt.Errorf("schema: ancestry walk exceeds depth limit")
		}
		m, ok := revs[id]
		if !ok {
			return fmt.Errorf("schema: unknown revision %x", id[:8])
		}
		if !m.IsSyntheticMerge() {
			if !seen[id] {
				seen[id] = true
				flat = append(flat, id)
			}
			return nil
		}
		for _, p := range m.Parents {
			if err := expand(p, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	for _, tip := range tips {
		if err := expand(tip, 0); err != nil {
			return nil, err
		}
	}
	// Drop members covered by another member's ancestry.
	covered := make(map[[32]byte]bool, len(flat))
	for _, id := range flat {
		anc, err := ancestors(revs, id)
		if err != nil {
			return nil, err
		}
		for a := range anc {
			covered[a] = true
		}
	}
	out := flat[:0]
	for _, id := range flat {
		if !covered[id] {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out, nil
}

// ancestors returns the transitive parent closure of id (excluding id).
func ancestors(revs map[[32]byte]*Manifest, id [32]byte) (map[[32]byte]bool, error) {
	out := make(map[[32]byte]bool)
	stack := [][32]byte{id}
	depth := 0
	for len(stack) > 0 {
		depth++
		if depth > maxAncestryWalkDepth || len(out) > maxRevisionAncestry {
			return nil, fmt.Errorf("schema: ancestry walk exceeds limits")
		}
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		m, ok := revs[cur]
		if !ok {
			return nil, fmt.Errorf("schema: unknown revision %x", cur[:8])
		}
		for _, p := range m.Parents {
			if p == id || out[p] {
				continue
			}
			out[p] = true
			stack = append(stack, p)
		}
	}
	return out, nil
}

// AncestorOf reports whether anc is tip itself or a transitive parent,
// using only the revisions at hand. Unknown links fail instead of guessing.
func AncestorOf(revs map[[32]byte]*Manifest, anc, tip [32]byte) (bool, error) {
	if anc == tip {
		return true, nil
	}
	set, err := ancestors(revs, tip)
	if err != nil {
		return false, err
	}
	return set[anc], nil
}

// FrontierKey derives the stable identity of a frontier for merge-result
// reuse: the SHA-256 of the sorted member revision IDs.
func FrontierKey(frontier [][32]byte) [32]byte {
	cp := append([][32]byte(nil), frontier...)
	sort.Slice(cp, func(i, j int) bool {
		return bytes.Compare(cp[i][:], cp[j][:]) < 0
	})
	h := sha256.New()
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(cp)))
	h.Write(n[:])
	for _, id := range cp {
		h.Write(id[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// DeriveMerge computes the deterministic synthetic merge for a frontier of
// two or more authored revisions: canonical union content, epoch
// 1 + max(frontier epochs), authorship from the greatest frontier
// (epoch, TimeCreated, CreatedOnNode) tuple, and parents set to the sorted
// frontier. It never uses receive time or the merging node's identity.
func DeriveMerge(revs map[[32]byte]*Manifest, frontier [][32]byte) (*Manifest, error) {
	if len(frontier) < 2 {
		return nil, fmt.Errorf("schema: merge needs at least two frontier revisions")
	}
	if len(frontier) > maxFrontierRevisions {
		return nil, fmt.Errorf("schema: frontier of %d exceeds limit", len(frontier))
	}
	members := make([]*Manifest, 0, len(frontier))
	var maxEpoch uint64
	var best *Manifest
	for _, id := range frontier {
		m, ok := revs[id]
		if !ok {
			return nil, fmt.Errorf("schema: unknown frontier revision %x", id[:8])
		}
		members = append(members, m)
		if m.Version > maxEpoch {
			maxEpoch = m.Version
		}
		if best == nil || greaterTuple(m, best) {
			best = m
		}
	}
	union := cloneTables(members[0].Tables)
	for _, m := range members[1:] {
		var err error
		union, err = UnionTables(union, m.Tables)
		if err != nil {
			return nil, err
		}
	}
	// Validate the union before publishing it.
	if _, err := BuildRegistry(maxEpoch+1, union); err != nil {
		return nil, err
	}
	parents := append([][32]byte(nil), frontier...)
	sort.Slice(parents, func(i, j int) bool {
		return bytes.Compare(parents[i][:], parents[j][:]) < 0
	})
	m := &Manifest{
		Version:       maxEpoch + 1,
		CreatedOnNode: best.CreatedOnNode,
		TimeCreated:   best.TimeCreated,
		Tables:        union,
		Parents:       parents,
	}
	m.Hash = ContentHash(m.Version, m.Tables)
	return m, nil
}

// greaterTuple orders revisions by (Version, TimeCreated, CreatedOnNode).
func greaterTuple(a, b *Manifest) bool {
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	if a.TimeCreated != b.TimeCreated {
		return a.TimeCreated > b.TimeCreated
	}
	return bytes.Compare(a.CreatedOnNode[:], b.CreatedOnNode[:]) > 0
}

func lowerName(s string) string { return strings.ToLower(s) }

func cloneTables(in []TableSchema) []TableSchema {
	out := make([]TableSchema, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Columns = append([]ColumnSchema(nil), in[i].Columns...)
		out[i].RecordDescriptor = append([]byte(nil), in[i].RecordDescriptor...)
	}
	return out
}
