// Schema storage, migration, and replication synchronization.
//
// The database persists complete immutable schema revisions plus ancestry
// (architecture/schema.md sections 6 and 50) and converges with peers
// through the replication schema-sync protocol: adopt compatible
// descendants, merge concurrent additive branches deterministically, and
// fail closed on anything else. Data replication stays gated until both
// sides verify identical schemas; retained batches from validated
// compatible ancestors remain applicable after additive upgrades.
package replicateddb

import (
	"context"
	"fmt"

	"github.com/nomadsql/replicateddb/replication"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/state"
)

// schemaRegistry returns the live registry. The pointer is swapped on
// migration/adoption, so all post-Open readers go through db.mu.
func (db *DB) schemaRegistry() *schema.Registry {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.reg
}

func (db *DB) setSchemaRegistry(reg *schema.Registry) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.reg = reg
}

// schemaIdentity returns the cached published schema identity.
func (db *DB) schemaIdentity() replication.SchemaIdentity {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.schemaId
}

func (db *DB) setSchemaIdentity(id replication.SchemaIdentity) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.schemaId = id
}

// openSchemaManifest reconciles the configured schema with Pebble storage.
// Fresh databases publish the configuration as the genesis revision;
// reopens require the configuration to match the persisted manifest
// exactly (migrations go through Migrate, never config drift). It returns
// the registry built from the authoritative persisted tables.
func openSchemaManifest(store *state.Store, cfg Config) (*schema.Registry, *schema.Manifest, error) {
	stored, err := store.LoadSchemaManifest()
	if err != nil {
		return nil, nil, err
	}
	if stored == nil {
		reg, err := schema.BuildRegistry(cfg.Schema.Version, cfg.Schema.Tables)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
		}
		m, err := schema.NewGenesis(registryTables(reg), cfg.Schema.Version, cfg.NodeID, store.ClockNow())
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
		}
		if err := store.StoreSchemaRevision(m); err != nil {
			return nil, nil, err
		}
		return reg, m, nil
	}
	reg, err := schema.BuildRegistry(cfg.Schema.Version, cfg.Schema.Tables)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	if reg.Epoch != stored.Version || reg.Hash != stored.Hash {
		return nil, nil, fmt.Errorf("%w: stored epoch %d != %d (migrate with Migrate, not config drift)",
			ErrSchemaMismatch, stored.Version, reg.Epoch)
	}
	// The persisted manifest is authoritative for content (the hash match
	// above proves identical IDs, names, and types), but the manifest
	// encoding sorts columns by ID while the engine's DDL follows
	// declaration order. Keep the config-built registry so restarts
	// preserve declaration order: rebuilding from the stored manifest
	// would permute physical column order on every reopen whenever
	// ID order differs from declaration order.
	return reg, stored, nil
}

func registryTables(reg *schema.Registry) []schema.TableSchema {
	out := make([]schema.TableSchema, len(reg.Tables))
	for i, p := range reg.Tables {
		out[i] = *p
		out[i].Columns = append([]schema.ColumnSchema(nil), p.Columns...)
	}
	return out
}

// Migrate publishes a new schema revision built from the full new table
// declaration: additive changes (new tables, new columns) are applied
// online, everything else fails with ErrUnsupportedSchema identifying the
// object. Writes and replication block during publication; reads are
// rejected while the materializer rebuilds.
//
// After a successful migration the application should persist the new
// declaration (version + tables) as its configuration: a reopen requires
// the configuration to match the published manifest exactly. The new
// version is always current+1; read it back with SchemaInfo.
func (db *DB) Migrate(ctx context.Context, newTables []schema.TableSchema) error {
	db.mu.Lock()
	st := db.dbState
	db.mu.Unlock()
	if st != StateReady {
		return fmt.Errorf("replicateddb: migrate in state %s", st)
	}
	// Serialize with writes, applies, rotation, and adoption. Lock order
	// (outer to inner): scheduler ticket, writeMu, applyMu, db.mu, store
	// gate. Migrations are maintenance-class writers.
	ticket, err := db.sched.Admit(ctx, WriterMaintenance)
	if err != nil {
		return fmt.Errorf("replicateddb: writer admission: %w", err)
	}
	defer ticket.Release()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	if st := db.getState(); st != StateReady {
		return fmt.Errorf("replicateddb: migrate in state %s", st)
	}
	cur, err := db.store.LoadSchemaManifest()
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("replicateddb: no published schema")
	}
	assigned, err := schema.AssignIDs(cur.Tables, newTables)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	reg, err := schema.BuildRegistry(cur.Version+1, assigned)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	if schema.ContentHash(cur.Version, registryTables(reg)) == cur.Hash {
		return nil // already there; no-op
	}
	next, err := schema.NewAuthoredRevision(cur, registryTables(reg), db.cfg.NodeID, db.store.ClockNow())
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	if err := db.checkMigrationBackfill(cur.Tables, next.Tables); err != nil {
		return err
	}
	_ = ctx
	if err := db.publishSchemaRevision(cur, next); err != nil {
		return err
	}
	db.metrics.schemaMigrations.Add(1)
	return nil
}

// checkMigrationBackfill rejects adding a non-nullable column (no DEFAULT
// is modeled) to a table with visible rows: SQLite cannot backfill it and
// the materializer could never rebuild. The check runs before anything is
// persisted.
func (db *DB) checkMigrationBackfill(oldTables, nextTables []schema.TableSchema) error {
	oldByName := make(map[string]*schema.TableSchema, len(oldTables))
	for i := range oldTables {
		t := &oldTables[i]
		oldByName[t.Name] = t
	}
	for i := range nextTables {
		nt := &nextTables[i]
		ot, existed := oldByName[nt.Name]
		if !existed {
			continue // new tables start empty
		}
		known := make(map[string]bool, len(ot.Columns))
		for _, c := range ot.Columns {
			known[c.Name] = true
		}
		for _, c := range nt.Columns {
			if known[c.Name] || c.Nullable {
				continue
			}
			n, err := db.countVisibleRows(ot.ID)
			if err != nil {
				return err
			}
			if n > 0 {
				return fmt.Errorf("%w: table %q gains non-nullable column %q with %d rows present (backfill first, then migrate)",
					ErrUnsupportedSchema, nt.Name, c.Name, n)
			}
		}
	}
	return nil
}

func (db *DB) countVisibleRows(tableID uint32) (int, error) {
	n := 0
	if err := db.store.IterateTable(tableID, func(r *state.Row) error {
		if r.Visible() {
			n++
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// publishSchemaRevision durably publishes next (already validated against
// cur), applies the additive DDL, rebuilds and revalidates the
// materializer, swaps the live registry, and recycles replication sessions
// so peers re-handshake against the new identity. Reads are rejected while
// the materializer is dirty; a post-Pebble DDL failure rebuilds before
// readiness, failing to StateFailed when unrecoverable.
func (db *DB) publishSchemaRevision(cur, next *schema.Manifest) error {
	ddl, err := schema.MigrationDDL(cur.Tables, next.Tables)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	if err := db.store.StoreSchemaRevision(next); err != nil {
		return err
	}
	newReg, err := next.Registry()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSchemaMismatch, err)
	}
	if err := db.engine.MigrateTo(newReg, ddl); err != nil {
		db.log.Warn("schema DDL failed; rebuilding materializer", "err", err.Error())
	}
	// Rebuild regardless: it validates the migrated state and recovers a
	// partially applied DDL from generated definitions. Callers hold
	// applyMu, which rebuildLocked requires.
	if err := db.rebuildLocked(); err != nil {
		return fmt.Errorf("replicateddb: rebuild after schema publish: %w", err)
	}
	db.setSchemaRegistry(newReg)
	db.setSchemaIdentity(replication.SchemaIdentity{
		Epoch:       next.Version,
		Hash:        next.Hash,
		Author:      next.CreatedOnNode,
		TimeCreated: next.TimeCreated,
	})
	if repl := db.replManager(); repl != nil {
		repl.RefreshSchema(db.schemaIdentity())
	}
	return nil
}

// --- replication.SchemaSyncer ---

// CurrentSchema implements replication.SchemaSyncer.
func (db *DB) CurrentSchema() replication.SchemaIdentity {
	return db.schemaIdentity()
}

// RevisionsForPeer implements replication.SchemaSyncer: our current tip
// first, then requested and ancestor revisions within budget.
func (db *DB) RevisionsForPeer(wantCurrent bool, wantIDs [][32]byte, maxBytes int) ([]*schema.Manifest, error) {
	cur, err := db.store.LoadSchemaManifest()
	if err != nil {
		return nil, err
	}
	if cur == nil {
		return nil, nil
	}
	var out []*schema.Manifest
	budget := maxBytes
	take := func(m *schema.Manifest) bool {
		n := len(schema.EncodeManifest(m)) + 4
		if n > budget {
			return false
		}
		budget -= n
		out = append(out, m)
		return true
	}
	if wantCurrent {
		if !take(cur) {
			return nil, fmt.Errorf("replicateddb: current schema exceeds %d bytes", maxBytes)
		}
	}
	seen := map[[32]byte]bool{schema.RevisionID(cur): true}
	for _, id := range wantIDs {
		if len(out) >= 64 {
			break
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		m, err := db.store.LoadSchemaRevision(id)
		if err != nil {
			return nil, err
		}
		if m == nil {
			continue // unknown IDs are skipped, never guessed
		}
		if !take(m) {
			break
		}
	}
	if wantCurrent {
		// Walk back from current so the peer can verify ancestry without
		// another round trip.
		queue := append([][32]byte(nil), cur.Parents...)
		for len(queue) > 0 && len(out) < 64 {
			id := queue[0]
			queue = queue[1:]
			if seen[id] {
				continue
			}
			seen[id] = true
			m, err := db.store.LoadSchemaRevision(id)
			if err != nil {
				return nil, err
			}
			if m == nil {
				continue
			}
			if !take(m) {
				break
			}
			queue = append(queue, m.Parents...)
		}
	}
	return out, nil
}

// SchemaProvenanceKnown implements replication.SchemaSyncer. Errors fail
// closed (unknown).
func (db *DB) SchemaProvenanceKnown(epoch uint64, hash [32]byte) bool {
	ok, err := db.store.SchemaProvenanceKnown(epoch, hash, state.MaxSchemaAncestryWalk)
	if err != nil {
		db.log.Warn("schema provenance lookup failed", "err", err.Error())
		return false
	}
	return ok
}

// SyncSchemas implements replication.SchemaSyncer: it validates received
// revisions and advances local schema toward the peer — no-op on match,
// adopt on compatible descendant, deterministic merge on concurrent
// branches, request-more-ancestry when unprovable, and fail-closed
// incompatibility otherwise. Nothing is applied or acknowledged on the
// error path.
// SyncSchemas implements replication.SchemaSyncer and records the decision
// outcome. Adoption, merge, and merge-reuse counts are recorded inside
// syncSchemas where the branches are distinguishable.
func (db *DB) SyncSchemas(ctx context.Context, revs []*schema.Manifest) replication.SyncDecision {
	dec := db.syncSchemas(ctx, revs)
	switch {
	case dec.Err != nil:
		db.metrics.schemaConflicts.Add(1)
	case dec.Agreed:
		db.metrics.schemaAgreements.Add(1)
	}
	if len(dec.NeedIDs) > 0 {
		db.metrics.schemaSyncNeeds.Add(1)
	}
	return dec
}

func (db *DB) syncSchemas(ctx context.Context, revs []*schema.Manifest) replication.SyncDecision {
	fail := func(err error) replication.SyncDecision {
		return replication.SyncDecision{Err: err}
	}
	// Mesh-driven schema adoption is a remote-class writer; admission
	// precedes all locks and honors the session context.
	ticket, err := db.sched.Admit(ctx, WriterRemote)
	if err != nil {
		// Shutting down or canceled: transient defer, the peer retries.
		return replication.SyncDecision{}
	}
	defer ticket.Release()
	if len(revs) == 0 {
		return fail(fmt.Errorf("%w: empty schema exchange", ErrSchemaMismatch))
	}
	for i, r := range revs {
		if r == nil {
			return fail(fmt.Errorf("%w: nil revision %d", ErrSchemaMismatch, i))
		}
		if _, err := schema.DecodeManifest(schema.EncodeManifest(r)); err != nil {
			return fail(fmt.Errorf("%w: revision %d invalid: %w", ErrSchemaMismatch, i, err))
		}
		if _, err := r.Registry(); err != nil {
			return fail(fmt.Errorf("%w: revision %d unbuildable: %w", ErrSchemaMismatch, i, err))
		}
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	if st := db.getState(); st != StateReady {
		// Transient (rotation/maintenance/closing): defer without error;
		// the peer's ack tick re-requests until we can decide.
		return replication.SyncDecision{}
	}
	cur, err := db.store.LoadSchemaManifest()
	if err != nil {
		return fail(err)
	}
	if cur == nil {
		return fail(fmt.Errorf("%w: no local schema", ErrSchemaMismatch))
	}
	// Persist validated received ancestry (idempotent) for future merges.
	if err := db.store.StoreSchemaRevisions(revs); err != nil {
		return fail(err)
	}
	remoteTip := revs[0]
	if schema.EqualRevision(cur, remoteTip) {
		return replication.SyncDecision{Agreed: true, SendAck: true}
	}
	if remoteTip.Version == cur.Version && remoteTip.Hash == cur.Hash {
		// Same content, different authorship (independent identical
		// declarations): already converged, no churn.
		return replication.SyncDecision{Agreed: true, SendAck: true}
	}
	local, revMap, err := db.schemaRevisionMap(cur, revs)
	if err != nil {
		return fail(err)
	}
	localID := schema.RevisionID(cur)
	remoteID := schema.RevisionID(remoteTip)
	// Compatible descendant covering us: adopt it without merging.
	// Coverage compares content (version+hash), not revision IDs: fresh
	// nodes author byte-distinct geneses for identical declarations, and
	// only content decides compatibility.
	if yes, missing, err := ancestryCoversContent(revMap, cur.Version, cur.Hash, remoteID); err != nil {
		return fail(err)
	} else if len(missing) > 0 {
		return replication.SyncDecision{NeedIDs: missing}
	} else if yes {
		if !schema.IsSuperset(cur.Tables, remoteTip.Tables) {
			return fail(fmt.Errorf("%w: remote revision is not an additive descendant", ErrSchemaMismatch))
		}
		if err := db.publishSchemaRevision(cur, remoteTip); err != nil {
			return fail(err)
		}
		db.metrics.schemaAdoptions.Add(1)
		return replication.SyncDecision{Agreed: true, SendAck: true}
	}
	// We cover the peer: send ours so they adopt; agreement follows ack.
	if yes, missing, err := ancestryCoversContent(local, remoteTip.Version, remoteTip.Hash, localID); err != nil {
		return fail(err)
	} else if len(missing) == 0 && yes {
		return replication.SyncDecision{SendRevisions: true}
	} else if len(missing) > 0 {
		// Our own ancestry must be complete; gaps mean corruption.
		return fail(fmt.Errorf("%w: local ancestry incomplete", ErrSchemaMismatch))
	}
	// Divergent branches: deterministic canonical merge.
	frontier, missing, err := schemaFrontier(revMap, localID, remoteID)
	if err != nil {
		return fail(err)
	}
	if len(missing) > 0 {
		return replication.SyncDecision{NeedIDs: missing}
	}
	merge, err := schema.DeriveMerge(revMap, frontier)
	if err != nil {
		return fail(fmt.Errorf("%w: incompatible concurrent schemas: %w", ErrSchemaMismatch, err))
	}
	// Reuse the persisted result for a known frontier (verified equal).
	if id, ok, err := db.store.LoadMergeResult(frontier); err != nil {
		return fail(err)
	} else if ok {
		stored, err := db.store.LoadSchemaRevision(id)
		if err != nil {
			return fail(err)
		}
		if stored == nil || !schema.EqualRevision(stored, merge) {
			return fail(fmt.Errorf("%w: stored merge disagrees with derived merge", ErrSchemaMismatch))
		}
		merge = stored
		db.metrics.schemaMergeReuse.Add(1)
	} else if err := db.store.StoreMergeResult(frontier, schema.RevisionID(merge)); err != nil {
		return fail(err)
	}
	if err := db.publishSchemaRevision(cur, merge); err != nil {
		return fail(err)
	}
	db.metrics.schemaMerges.Add(1)
	return replication.SyncDecision{Agreed: true, SendAck: true, SendRevisions: true}
}

// schemaRevisionMap indexes local ancestry plus received revisions by ID,
// returning the local-only map and the combined map.
func (db *DB) schemaRevisionMap(cur *schema.Manifest, revs []*schema.Manifest) (map[[32]byte]*schema.Manifest, map[[32]byte]*schema.Manifest, error) {
	local, _, err := db.store.CollectSchemaAncestry([][32]byte{schema.RevisionID(cur)}, state.MaxSchemaAncestryWalk)
	if err != nil {
		return nil, nil, err
	}
	combined := make(map[[32]byte]*schema.Manifest, len(local)+len(revs))
	for id, r := range local {
		combined[id] = r
	}
	for _, r := range revs {
		combined[schema.RevisionID(r)] = r
	}
	return local, combined, nil
}

// ancestryCoversContent reports whether tip's ancestry within revMap
// contains the given content (version+hash), listing parent IDs still
// missing instead of guessing. Content, not revision identity, decides:
// equal declarations hash equally across authors.
func ancestryCoversContent(revMap map[[32]byte]*schema.Manifest, version uint64, hash [32]byte, tip [32]byte) (bool, [][32]byte, error) {
	var missing [][32]byte
	seen := map[[32]byte]bool{}
	queue := [][32]byte{tip}
	depth := 0
	for len(queue) > 0 {
		depth++
		if depth > 4096 || len(seen) > 4096 {
			return false, nil, fmt.Errorf("%w: ancestry walk exceeds bounds", ErrSchemaMismatch)
		}
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		m, ok := revMap[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		if m.Version == version && m.Hash == hash {
			return true, nil, nil
		}
		queue = append(queue, m.Parents...)
	}
	return false, missing, nil
}

// schemaFrontier flattens tips like schema.Frontier but returns missing
// revision IDs instead of failing on unknown links.
func schemaFrontier(revMap map[[32]byte]*schema.Manifest, tips ...[32]byte) ([][32]byte, [][32]byte, error) {
	var missing [][32]byte
	seenMissing := map[[32]byte]bool{}
	flat := make([][32]byte, 0, len(tips))
	seen := map[[32]byte]bool{}
	var expand func(id [32]byte, depth int) error
	expand = func(id [32]byte, depth int) error {
		if depth > 4096 {
			return fmt.Errorf("%w: ancestry walk exceeds bounds", ErrSchemaMismatch)
		}
		m, ok := revMap[id]
		if !ok {
			if !seenMissing[id] {
				seenMissing[id] = true
				missing = append(missing, id)
			}
			return nil
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
			return nil, nil, err
		}
	}
	if len(missing) > 0 {
		return nil, missing, nil
	}
	frontier, err := schema.Frontier(revMap, flat...)
	if err != nil {
		return nil, nil, err
	}
	return frontier, nil, nil
}
