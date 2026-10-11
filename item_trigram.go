package murmur

import (
	"context"
	"fmt"
	"strings"

	"github.com/marcgauthier/murmur/rime"
)

// IndexKind selects the index built by CreateIndex.
type IndexKind = rime.IndexKind

// IndexTrigram builds an in-memory trigram similarity index over a string
// field. The index is local derived state: every replica builds its own
// from replicated rows, and it is never persisted or replicated.
const IndexTrigram = rime.IndexTrigram

// TrigramHit is one ranked trigram search result. Key holds the row's RowID;
// hydrate it with GetItem.
type TrigramHit = rime.TrigramHit

// TrigramMode selects the SearchTrigram matching contract.
type TrigramMode = rime.TrigramMode

// Trigram search modes.
const (
	TrigramContains = rime.TrigramContains
	TrigramSimilar  = rime.TrigramSimilar
	TrigramFuzzy    = rime.TrigramFuzzy
)

// TrigramOptions tunes SearchTrigram; see rime.TrigramOptions.
type TrigramOptions = rime.TrigramOptions

// CreateIndex builds kind over table's field from the current rows and
// maintains it on later commits. Re-creating an existing index rebuilds it
// idempotently. The index is local to this node: it neither enters the
// replicated schema manifest nor affects peers, so adding or dropping one
// never requires migration. The build serializes with commits, so large
// tables stall writers while scanning; concurrent readers proceed. Field
// accepts Go struct field names, mapped through `name=` overrides.
func (db *DB) CreateIndex(ctx context.Context, table, field string, kind IndexKind) error {
	if db == nil {
		return ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := db.requireRead(); err != nil {
		return err
	}
	logical := field
	db.itemMu.RLock()
	for _, binding := range db.itemBindings {
		if strings.EqualFold(binding.table, table) {
			if lname, ok := binding.logical[field]; ok {
				logical = lname
			}
			break
		}
	}
	db.itemMu.RUnlock()
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	if db.recordDB == nil {
		return ErrClosed
	}
	rimeName := ""
	for _, definition := range db.recordDefinitions {
		if strings.EqualFold(definition.name, table) {
			rimeName = definition.name
			break
		}
	}
	if rimeName == "" {
		return fmt.Errorf("murmur: table %q is not registered: %w", table, ErrUnsupportedSchema)
	}
	return db.recordDB.CreateIndex(rimeName, logical, kind)
}

// SearchTrigram searches field's trigram index for query and returns ranked
// hits, best first. The model exemplar resolves the collection, as in
// Count and Exists; field accepts Go struct field names. The field must
// carry a trigram index (via the `rime:"trigram"` tag or CreateIndex).
// Hit keys hold RowIDs; hydrate them with GetItem. An empty query matches
// nothing.
func (db *DB) SearchTrigram(ctx context.Context, model any, field, query string, mode TrigramMode, opts TrigramOptions) ([]TrigramHit, error) {
	if db == nil {
		return nil, ErrClosed
	}
	binding, err := db.resolveItemBinding(model)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	logical, ok := binding.logical[field]
	if !ok {
		return nil, fmt.Errorf("murmur: unknown field %q for %s", field, binding.goType)
	}
	if binding.ops == nil || binding.ops.search == nil {
		return nil, fmt.Errorf("murmur: table %q has no search binding: %w", binding.table, ErrUnsupportedSchema)
	}
	return binding.ops.search(db, binding.table, logical, query, mode, opts)
}

// SearchTrigram searches field's trigram index on the latest published
// state, holding the materializer read lock for the search like other
// typed reads.
func (t *recordTable[T]) SearchTrigram(field, query string, mode rime.TrigramMode, opts rime.TrigramOptions) ([]rime.TrigramHit, error) {
	if t == nil || t.db == nil {
		return nil, ErrClosed
	}
	if err := t.db.requireRead(); err != nil {
		return nil, err
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return nil, err
	}
	defer unlock()
	return inner.SearchTrigram(field, query, mode, opts)
}
