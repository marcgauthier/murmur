// Schema revision storage under key prefix 0x06.
//
// The store persists the current schema manifest plus immutable authored
// revisions and merge records (architecture/storage.md section 14 and
// architecture/schema.md section 6). Revisions never mutate: publishing a
// schema writes its revision record and repoints the current-manifest key
// in one atomic batch, keeping the cheap sys epoch/hash pair in sync.
package state

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/marcgauthier/spedsql/schema"
)

// Schema storage key names under prefix 0x06.
const (
	schemaCurrentKey  = "current"
	schemaRevPrefix   = "rev/"
	schemaMergePrefix = "merge/"
)

// MaxSchemaAncestryWalk bounds ancestry collection for sync decisions.
const MaxSchemaAncestryWalk = 4096

// ErrSchemaAncestryTooDeep reports an ancestry walk that exceeded its bound.
// Callers must defer the decision and request ancestry incrementally rather
// than guessing.
var ErrSchemaAncestryTooDeep = errors.New("state: schema ancestry exceeds walk bound")

func schemaRevKey(id [32]byte) []byte {
	return SchemaKey(schemaRevPrefix + string(id[:]))
}

func schemaMergeKey(frontier [32]byte) []byte {
	return SchemaKey(schemaMergePrefix + string(frontier[:]))
}

// LoadSchemaManifest returns the current manifest, or (nil, nil) when no
// schema was ever published (fresh database).
func (s *Store) LoadSchemaManifest() (*schema.Manifest, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	raw, err := s.getDirect(SchemaKey(schemaCurrentKey))
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	m, err := schema.DecodeManifest(raw)
	if err != nil {
		return nil, fmt.Errorf("state: decode current schema manifest: %w", err)
	}
	return m, nil
}

// StoreSchemaRevision validates and persists one revision, then publishes
// it as current — atomically with the sys epoch/hash pair. Re-storing an
// identical revision is a no-op publish of the same content.
func (s *Store) StoreSchemaRevision(m *schema.Manifest) error {
	if m == nil {
		return fmt.Errorf("state: nil schema manifest")
	}
	// Self-check through the canonical encoding before touching storage.
	raw := schema.EncodeManifest(m)
	if _, err := schema.DecodeManifest(raw); err != nil {
		return fmt.Errorf("state: refusing invalid schema manifest: %w", err)
	}
	if _, err := m.Registry(); err != nil {
		return fmt.Errorf("state: refusing unbuildable schema revision: %w", err)
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	id := schema.RevisionID(m)
	if err := b.Set(schemaRevKey(id), raw, nil); err != nil {
		return err
	}
	if err := b.Set(SchemaKey(schemaCurrentKey), raw, nil); err != nil {
		return err
	}
	if err := b.Set(SysKey(sysSchemaEpoch), encodeU64(m.Version), nil); err != nil {
		return err
	}
	if err := b.Set(SysKey(sysSchemaHash), m.Hash[:], nil); err != nil {
		return err
	}
	return s.commitBatch(b, pebble.Sync)
}

// StoreSchemaRevisions persists non-current ancestry (received from peers)
// without moving the current pointer. Every revision is validated.
func (s *Store) StoreSchemaRevisions(revs []*schema.Manifest) error {
	for _, m := range revs {
		if m == nil {
			return fmt.Errorf("state: nil schema revision")
		}
		if _, err := schema.DecodeManifest(schema.EncodeManifest(m)); err != nil {
			return fmt.Errorf("state: refusing invalid schema revision: %w", err)
		}
	}
	if len(revs) == 0 {
		return nil
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	for _, m := range revs {
		if err := b.Set(schemaRevKey(schema.RevisionID(m)), schema.EncodeManifest(m), nil); err != nil {
			return err
		}
	}
	return s.commitBatch(b, pebble.Sync)
}

// LoadSchemaRevision returns one stored revision, or (nil, nil) when absent.
func (s *Store) LoadSchemaRevision(id [32]byte) (*schema.Manifest, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	raw, err := s.getDirect(schemaRevKey(id))
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	m, err := schema.DecodeManifest(raw)
	if err != nil {
		return nil, fmt.Errorf("state: decode schema revision: %w", err)
	}
	return m, nil
}

// CollectSchemaAncestry walks parents from tips breadth-first, returning the
// revisions found, the parent IDs still missing, or an error when the walk
// exceeds maxRevs (pass MaxSchemaAncestryWalk for sync decisions).
func (s *Store) CollectSchemaAncestry(tips [][32]byte, maxRevs int) (map[[32]byte]*schema.Manifest, [][32]byte, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	found := make(map[[32]byte]*schema.Manifest)
	var missing [][32]byte
	queued := make(map[[32]byte]bool)
	queue := append([][32]byte(nil), tips...)
	for _, id := range queue {
		queued[id] = true
	}
	for len(queue) > 0 {
		if len(found) >= maxRevs {
			return nil, nil, ErrSchemaAncestryTooDeep
		}
		id := queue[0]
		queue = queue[1:]
		if found[id] != nil {
			continue
		}
		raw, err := s.getDirect(schemaRevKey(id))
		if err != nil {
			if isNotFound(err) {
				missing = append(missing, id)
				continue
			}
			return nil, nil, err
		}
		m, err := schema.DecodeManifest(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("state: decode schema revision: %w", err)
		}
		found[id] = m
		for _, p := range m.Parents {
			if !queued[p] {
				queued[p] = true
				queue = append(queue, p)
			}
		}
	}
	return found, missing, nil
}

// SchemaProvenanceKnown reports whether a revision with the given
// (version, hash) sits in the current manifest's ancestry (including the
// tip). It backs the retained-transaction rule: batches from a validated
// compatible ancestor stay applicable after an additive upgrade.
func (s *Store) SchemaProvenanceKnown(version uint64, hash [32]byte, maxDepth int) (bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	raw, err := s.getDirect(SchemaKey(schemaCurrentKey))
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	cur, err := schema.DecodeManifest(raw)
	if err != nil {
		return false, fmt.Errorf("state: decode current schema manifest: %w", err)
	}
	if cur.Version == version && cur.Hash == hash {
		return true, nil
	}
	seen := map[[32]byte]bool{schema.RevisionID(cur): true}
	queue := append([][32]byte(nil), cur.Parents...)
	depth := 0
	for len(queue) > 0 {
		depth++
		if depth > maxDepth {
			return false, ErrSchemaAncestryTooDeep
		}
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		raw, err := s.getDirect(schemaRevKey(id))
		if err != nil {
			if isNotFound(err) {
				continue // unknown links prove nothing; keep walking known ones
			}
			return false, err
		}
		m, err := schema.DecodeManifest(raw)
		if err != nil {
			return false, fmt.Errorf("state: decode schema revision: %w", err)
		}
		if m.Version == version && m.Hash == hash {
			return true, nil
		}
		queue = append(queue, m.Parents...)
	}
	return false, nil
}

// LoadMergeResult returns the persisted merge revision for a known frontier,
// or (zero, false, nil) when this frontier never merged here.
func (s *Store) LoadMergeResult(frontier [][32]byte) ([32]byte, bool, error) {
	var zero [32]byte
	s.gate.RLock()
	defer s.gate.RUnlock()
	raw, err := s.getDirect(schemaMergeKey(schema.FrontierKey(frontier)))
	if err != nil {
		if isNotFound(err) {
			return zero, false, nil
		}
		return zero, false, err
	}
	if len(raw) != 32 {
		return zero, false, fmt.Errorf("state: corrupt schema merge record")
	}
	var id [32]byte
	copy(id[:], raw)
	return id, true, nil
}

// StoreMergeResult records the merge revision derived for a frontier so
// repeat exchanges reuse it instead of deriving again.
func (s *Store) StoreMergeResult(frontier [][32]byte, revID [32]byte) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.dbSet(schemaMergeKey(schema.FrontierKey(frontier)), revID[:], pebble.Sync)
}
