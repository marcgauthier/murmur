package bridge

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

// Importer applies opened bundles as ordinary database transactions, so
// accepted imports redistribute among High mesh peers as regular replicated
// writes. Typed row mutations, ownership policy, provenance and receipts commit
// atomically through Spool; file metadata commits separately. Completion
// receipts and progress commit together after all effects succeed, so
// interrupted imports remain retryable.
type Importer struct {
	db *db.DB

	mu  sync.Mutex
	pks map[string]string

	// Tests inject storage faults after effects commit, before completion.
	beforeCompletion      func()
	beforeCompletionError func() error
	beforeFileApply       func()
}

var ErrIdentityCollision = errors.New("bridge: imported row identity collides with High-owned data")
var ErrProtectedDelete = errors.New("bridge: Low delete is protected by High-owned fields")

type ProtectedDeleteError struct {
	Table  string
	Row    ids.RowID
	Fields []string
}

func (e *ProtectedDeleteError) Error() string {
	return fmt.Sprintf("%v: %s row %s has High-owned fields %s", ErrProtectedDelete, e.Table, e.Row, strings.Join(e.Fields, ", "))
}

func (e *ProtectedDeleteError) Unwrap() error { return ErrProtectedDelete }

// NewImporter binds an importer to an open High database.
func NewImporter(database *db.DB) (*Importer, error) {
	if database == nil {
		return nil, fmt.Errorf("bridge: importer requires a database")
	}
	return &Importer{db: database, pks: make(map[string]string)}, nil
}

// ApplyBundle imports row effects atomically and file metadata separately,
// recording durable completion only after both succeed. Row source receipts
// commit with row effects; completion receipts and stream progress share a
// second atomic batch. Replays repair interrupted completion without repeating
// completed row-only imports; mixed bundles retry both effects to convergence.
func (im *Importer) ApplyBundle(ctx context.Context, b *Bundle) error {
	return im.applyBundleResolved(ctx, b, "")
}

// ResolvePolicyHold records an explicit High decision and resumes ordered
// import. ResolutionKeepHigh consumes the Low delete without deleting the
// row; ResolutionAcceptLowDelete applies it and releases its field ownership.
func (im *Importer) ResolvePolicyHold(ctx context.Context, inbox *Inbox, stream string, first uint64, resolution string) error {
	if inbox == nil {
		return fmt.Errorf("bridge: policy resolution requires an inbox")
	}
	if err := inbox.ResolvePolicyHold(stream, first, resolution); err != nil {
		return err
	}
	_, err := im.Drain(ctx, inbox)
	return err
}

func (im *Importer) applyBundleResolved(ctx context.Context, b *Bundle, resolution string) error {
	// Resolve primary keys from the manifest before preparing rows.
	tables := make(map[string]bool)
	for _, batch := range b.Batches {
		for _, rec := range batch.Records {
			tables[rec.Table] = true
		}
	}
	for table := range tables {
		if table == db.BridgeFileTableName {
			continue // file metadata has no SQL primary key
		}
		if _, err := im.pkColumn(ctx, table); err != nil {
			return err
		}
	}

	// Row source receipts commit with row effects. For a bundle containing
	// files they cannot prove file completion; require its terminal receipt.
	hasFiles := bundleHasFileRecords(b)
	allRecorded := len(b.Batches) > 0
	if hasFiles {
		allRecorded = false
		if !b.Manifest.BundleID.IsZero() {
			var err error
			allRecorded, err = im.db.HasTransactionReceipt(b.Manifest.BundleID)
			if err != nil {
				return err
			}
		}
	}
	for _, batch := range b.Batches {
		if batch.TxID.IsZero() {
			allRecorded = false
			break
		}
		has, err := im.db.HasTransactionReceipt(sourceReceiptID(b, batch))
		if err != nil {
			return err
		}
		if !has {
			allRecorded = false
			break
		}
	}
	if allRecorded {
		return im.completeBundle(b)
	}
	prepared, err := im.prepareBundle(ctx, b, resolution)
	if err != nil {
		return err
	}

	// File records split from row records: typed rows and files use their
	// respective managed Spool commits. Quarantine retry replays both to
	// convergence.
	var rowBatches []Batch
	var filePuts []db.BridgeFilePut
	var fileDeletes []ids.RowID
	for _, batch := range prepared.Batches {
		var rows Batch
		rows.TxID, rows.Origin, rows.Sequence, rows.HLC = batch.TxID, batch.Origin, batch.Sequence, batch.HLC
		for _, rec := range batch.Records {
			if rec.Table != db.BridgeFileTableName {
				rows.Records = append(rows.Records, rec)
				continue
			}
			switch rec.Op {
			case RecordDelete:
				fileDeletes = append(fileDeletes, rec.Row)
			case RecordPut:
				put, err := filePutRecord(rec)
				if err != nil {
					return err
				}
				filePuts = append(filePuts, put)
			default:
				return fmt.Errorf("bridge: import: unknown record op %d", rec.Op)
			}
		}
		if len(rows.Records) > 0 {
			rowBatches = append(rowBatches, rows)
		}
	}

	if len(rowBatches) > 0 {
		// The local import transaction always mints a fresh TxID. Reusing
		// the source BundleID here would give every independent High
		// importer the same TxID for the same bundle; on the High mesh
		// those batches then hit the duplicate-TxID fast path, which
		// acknowledges without advancing the origin watermark, so the next
		// local batch gaps forever and no High peer ever converges.
		// Deduplication still keys on source identity: the skip check and
		// the receipts recorded below use the source batch/Bundle IDs.
		txID := ids.NewTxID()
		_, typed, err := im.db.BridgeTypedRowExists(rowBatches[0].Records[0].Table, rowBatches[0].Records[0].Row)
		if err != nil {
			return err
		}
		if !typed {
			return fmt.Errorf("bridge: import requires managed typed RIME tables")
		}
		mutations, err := im.typedImportMutations(ctx, b, rowBatches)
		if err != nil {
			return err
		}
		receipts := make([]db.BridgeSourceReceipt, 0, len(rowBatches))
		for _, batch := range rowBatches {
			receipts = append(receipts, db.BridgeSourceReceipt{
				TxID: sourceReceiptID(b, batch), Origin: batch.Origin, Sequence: batch.Sequence,
			})
		}
		if err := im.db.CommitTypedBridgeImport(ctx, txID, b.Manifest.SourceDomain, b.Manifest.Stream, b.Manifest.BundleID, b.Manifest.SeqFirst, b.Manifest.SeqLast, resolution == "accept-low-delete", receipts, mutations); err != nil {
			return err
		}
	}

	if len(filePuts) > 0 || len(fileDeletes) > 0 {
		if im.beforeFileApply != nil {
			im.beforeFileApply()
		}
		if err := im.db.BridgeApplyFileMetadata(ctx, b.Manifest.SourceDomain, b.Manifest.Stream, b.Manifest.BundleID, b.Manifest.SeqFirst, b.Manifest.SeqLast, resolution == "accept-low-delete", filePuts, fileDeletes); err != nil {
			return err
		}
	}

	return im.completeBundle(b)
}

func (im *Importer) typedImportMutations(ctx context.Context, b *Bundle, batches []Batch) ([]codec.Mutation, error) {
	tables, err := im.db.SchemaTables()
	if err != nil {
		return nil, err
	}
	tableByName := make(map[string]schema.TableSchema, len(tables))
	for _, table := range tables {
		tableByName[strings.ToLower(table.Name)] = table
	}
	type rowKey struct {
		table uint32
		row   ids.RowID
	}
	type rowState struct {
		table        schema.TableSchema
		deleted      bool
		needsPrimary bool
		columns      map[uint32]codec.Mutation
	}
	rows := make(map[rowKey]*rowState)
	var order []rowKey
	for _, batch := range batches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, rec := range batch.Records {
			table, ok := tableByName[strings.ToLower(rec.Table)]
			if !ok {
				return nil, fmt.Errorf("bridge: import: typed table %q is absent from the schema manifest", rec.Table)
			}
			key := rowKey{table: table.ID, row: rec.Row}
			row := rows[key]
			if row == nil {
				row = &rowState{table: table, columns: make(map[uint32]codec.Mutation)}
				row.needsPrimary, _, err = im.db.BridgeTypedRowExists(rec.Table, rec.Row)
				if err != nil {
					return nil, err
				}
				row.needsPrimary = !row.needsPrimary
				rows[key] = row
				order = append(order, key)
			}
			switch rec.Op {
			case RecordDelete:
				row.deleted = true
				row.needsPrimary = true
				clear(row.columns)
			case RecordPut:
				if row.deleted {
					clear(row.columns)
					row.needsPrimary = true
					row.deleted = false
				}
				for _, column := range rec.Columns {
					var declared *schema.ColumnSchema
					for i := range table.Columns {
						if strings.EqualFold(table.Columns[i].Name, column.Column) {
							declared = &table.Columns[i]
							break
						}
					}
					if declared == nil || declared.MergePolicy != column.Policy {
						return nil, fmt.Errorf("bridge: import: typed field %s.%s does not match the schema manifest", rec.Table, column.Column)
					}
					if declared.ID == table.PK {
						row.needsPrimary = false
					}
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if column.Policy == schema.LWW {
						typed, err := im.db.ValidateBridgeTypedField(table.Name, declared.ID, rec.Row, column.Value)
						if err != nil {
							return nil, err
						}
						if !typed {
							return nil, fmt.Errorf("bridge: typed field validation lost typed schema mode")
						}
					}
					mutation := codec.Mutation{
						TableID: table.ID, RowID: rec.Row, ColumnID: declared.ID,
						Policy: column.Policy, Value: column.Value, Records: column.Records,
					}
					if column.Policy == schema.PN_COUNTER || column.Policy == schema.OR_SET {
						mutation.Flags = codec.FlagCRDTImport
					}
					if column.Policy == schema.PN_COUNTER || column.Policy == schema.OR_SET {
						previous, exists := row.columns[declared.ID]
						if exists {
							mutation.Records = append(append([]codec.CRDTRecord(nil), previous.Records...), mutation.Records...)
						}
					} else if column.Policy == schema.MAX || column.Policy == schema.MIN {
						previous, exists := row.columns[declared.ID]
						if exists {
							cmp, err := compareBridgeExtrema(previous.Value, mutation.Value)
							if err != nil {
								return nil, err
							}
							if column.Policy == schema.MAX && cmp > 0 || column.Policy == schema.MIN && cmp < 0 {
								mutation.Value = previous.Value
							}
						}
					}
					row.columns[declared.ID] = mutation
				}
			default:
				return nil, fmt.Errorf("bridge: import: unknown record op %d", rec.Op)
			}
		}
	}
	var out []codec.Mutation
	for _, key := range order {
		row := rows[key]
		if row.deleted {
			out = append(out, codec.Mutation{TableID: key.table, RowID: key.row, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone})
			continue
		}
		if row.needsPrimary {
			return nil, fmt.Errorf("bridge: typed insert for %s row %s lacks its primary field", row.table.Name, key.row)
		}
		columnIDs := make([]uint32, 0, len(row.columns))
		for id := range row.columns {
			columnIDs = append(columnIDs, id)
		}
		sort.Slice(columnIDs, func(i, j int) bool { return columnIDs[i] < columnIDs[j] })
		for _, id := range columnIDs {
			out = append(out, row.columns[id])
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("bridge: typed row import has no effects")
	}
	return out, nil
}

func compareBridgeExtrema(a, b codec.Value) (int, error) {
	toRat := func(value codec.Value) (*big.Rat, error) {
		switch value.Type {
		case codec.TypeInteger:
			return new(big.Rat).SetInt64(value.I), nil
		case codec.TypeReal:
			rat := new(big.Rat).SetFloat64(value.F)
			if rat == nil {
				return nil, fmt.Errorf("bridge: non-finite extrema value")
			}
			return rat, nil
		default:
			return nil, fmt.Errorf("bridge: extrema value has type %s", valueTypeName(value.Type))
		}
	}
	ra, err := toRat(a)
	if err != nil {
		return 0, err
	}
	rb, err := toRat(b)
	if err != nil {
		return 0, err
	}
	return ra.Cmp(rb), nil
}

func bundleHasFileRecords(b *Bundle) bool {
	for _, batch := range b.Batches {
		for _, rec := range batch.Records {
			if rec.Table == db.BridgeFileTableName {
				return true
			}
		}
	}
	return false
}

func (im *Importer) completeBundle(b *Bundle) error {
	if im.beforeCompletion != nil {
		im.beforeCompletion()
	}
	if im.beforeCompletionError != nil {
		if err := im.beforeCompletionError(); err != nil {
			return err
		}
	}
	// Publish all completion receipts together with progress. Neither the
	// success path nor replay recovery may acknowledge a partial completion.
	receipts := make([]ids.TxID, 0, len(b.Batches)+1)
	if !b.Manifest.BundleID.IsZero() {
		receipts = append(receipts, b.Manifest.BundleID)
	}
	for _, batch := range b.Batches {
		if !batch.TxID.IsZero() {
			receipts = append(receipts, sourceReceiptID(b, batch))
		}
	}

	return im.db.CompleteBridgeImport(b.Manifest.Stream, b.Manifest.SeqLast, receipts)
}

// prepareBundle rejects unrelated High-row collisions, prevents Low values
// from replacing High-owned fields, and protects High-modified rows from Low
// deletes. Policy checks happen before opening the SQL transaction.
func (im *Importer) prepareBundle(ctx context.Context, b *Bundle, resolution string) (*Bundle, error) {
	prepared := *b
	prepared.Batches = make([]Batch, len(b.Batches))
	for bi, batch := range b.Batches {
		prepared.Batches[bi] = batch
		prepared.Batches[bi].Records = make([]Record, 0, len(batch.Records))
		for _, rec := range batch.Records {
			if rec.Table == db.BridgeFileTableName {
				keep, err := im.prepareFileRecord(ctx, rec, b, resolution)
				if err != nil {
					return nil, err
				}
				if keep != nil {
					prepared.Batches[bi].Records = append(prepared.Batches[bi].Records, *keep)
				}
				continue
			}
			rowPolicy, hasProvenance, err := im.db.BridgeRowProvenance(rec.Table, rec.Row)
			if err != nil {
				return nil, err
			}
			exists, err := im.rowExists(ctx, rec.Table, rec.Row)
			if err != nil {
				return nil, err
			}
			if hasProvenance {
				if rowPolicy.Owner == db.BridgeOwnerHigh || rowPolicy.SourceDomain != b.Manifest.SourceDomain || rowPolicy.Stream != b.Manifest.Stream && !bundleHasCRDT(b) {
					return nil, fmt.Errorf("%w: %s row %s", ErrIdentityCollision, rec.Table, rec.Row)
				}
			} else if exists {
				return nil, fmt.Errorf("%w: %s row %s has no matching Low provenance", ErrIdentityCollision, rec.Table, rec.Row)
			}
			switch rec.Op {
			case RecordDelete:
				if hasProvenance && !rowPolicy.Deleted {
					fields, err := im.db.BridgeHighOwnedColumns(rec.Table, rec.Row)
					if err != nil {
						return nil, err
					}
					if len(fields) > 0 {
						switch resolution {
						case "keep-high":
							continue
						case "accept-low-delete":
						default:
							return nil, &ProtectedDeleteError{Table: rec.Table, Row: rec.Row, Fields: fields}
						}
					}
				}
				prepared.Batches[bi].Records = append(prepared.Batches[bi].Records, rec)
			case RecordPut:
				pk, err := im.pkColumn(ctx, rec.Table)
				if err != nil {
					return nil, err
				}
				filtered := rec
				filtered.Columns = make([]ColumnValue, 0, len(rec.Columns))
				for _, column := range rec.Columns {
					if column.Column == pk {
						filtered.Columns = append(filtered.Columns, column)
						continue
					}
					policy, ok, err := im.db.BridgeFieldProvenance(rec.Table, rec.Row, column.Column)
					if err != nil {
						return nil, err
					}
					// Typed rows keep accepting the Low stream's underlying LWW
					// value while a High shadow controls what readers see. This
					// preserves the newest Low value for an ownership release.
					// Legacy SQL rows retain their historical filtering behavior.
					if ok && policy.Owner == db.BridgeOwnerHigh && column.Policy == schema.LWW {
						typed, _, err := im.db.BridgeTypedRowExists(rec.Table, rec.Row)
						if err != nil {
							return nil, err
						}
						if !typed {
							continue
						}
					}
					filtered.Columns = append(filtered.Columns, column)
				}
				if len(filtered.Columns) > 0 {
					prepared.Batches[bi].Records = append(prepared.Batches[bi].Records, filtered)
				}
			default:
				return nil, fmt.Errorf("bridge: import: unknown record op %d", rec.Op)
			}
		}
	}
	return &prepared, nil
}

// prepareFileRecord applies the row ownership policy to one file record:
// provenance collisions fail, Low deletes of High-owned files hold for an
// explicit decision, and High-owned fields filter out of puts. Unlike rows
// (partial puts fill NULLs), a filtered file put must keep name, digest,
// and size together or drop wholesale: partial file metadata is undecodable.
// Malformed records error (the bundle quarantines as poison).
func (im *Importer) prepareFileRecord(ctx context.Context, rec Record, b *Bundle, resolution string) (*Record, error) {
	_ = ctx
	rowPolicy, hasProvenance, err := im.db.BridgeFileProvenance(rec.Row)
	if err != nil {
		return nil, err
	}
	exists, err := im.db.BridgeFileRowExists(rec.Row)
	if err != nil {
		return nil, err
	}
	if hasProvenance {
		if rowPolicy.Owner == db.BridgeOwnerHigh || rowPolicy.SourceDomain != b.Manifest.SourceDomain || rowPolicy.Stream != b.Manifest.Stream {
			return nil, fmt.Errorf("%w: %s row %s", ErrIdentityCollision, rec.Table, rec.Row)
		}
	} else if exists {
		return nil, fmt.Errorf("%w: %s row %s has no matching Low provenance", ErrIdentityCollision, rec.Table, rec.Row)
	}
	switch rec.Op {
	case RecordDelete:
		if hasProvenance && !rowPolicy.Deleted {
			fields, err := im.db.BridgeFileHighOwnedColumns(rec.Row)
			if err != nil {
				return nil, err
			}
			if len(fields) > 0 {
				switch resolution {
				case "keep-high":
					return nil, nil
				case "accept-low-delete":
				default:
					return nil, &ProtectedDeleteError{Table: rec.Table, Row: rec.Row, Fields: fields}
				}
			}
		}
		out := rec
		return &out, nil
	case RecordPut:
		// Malformed originals are poison; filtering drops are silent.
		if err := validateFilePutColumns(rec); err != nil {
			return nil, err
		}
		if _, _, err := fileRecordObject(rec); err != nil {
			return nil, err
		}
		if _, err := fileRecordName(rec); err != nil {
			return nil, err
		}
		filtered := rec
		filtered.Columns = make([]ColumnValue, 0, len(rec.Columns))
		for _, column := range rec.Columns {
			if column.Column == db.BridgeFileColID {
				filtered.Columns = append(filtered.Columns, column)
				continue
			}
			policy, ok, err := im.db.BridgeFileFieldProvenance(rec.Row, column.Column)
			if err != nil {
				return nil, err
			}
			if ok && policy.Owner == db.BridgeOwnerHigh {
				continue
			}
			filtered.Columns = append(filtered.Columns, column)
		}
		if err := validateFilePutColumns(filtered); err != nil {
			return nil, nil // High-owned fields win wholesale
		}
		return &filtered, nil
	default:
		return nil, fmt.Errorf("bridge: import: unknown record op %d", rec.Op)
	}
}

// validateFilePutColumns requires exactly the file metadata shape: known
// columns only, with name, digest, and size all present.
func validateFilePutColumns(rec Record) error {
	seen := make(map[string]bool)
	for _, c := range rec.Columns {
		switch c.Column {
		case db.BridgeFileColID, db.BridgeFileColName, db.BridgeFileColDigest, db.BridgeFileColSize:
			seen[c.Column] = true
		default:
			return fmt.Errorf("bridge: file record has unknown column %q", c.Column)
		}
	}
	for _, need := range []string{db.BridgeFileColName, db.BridgeFileColDigest, db.BridgeFileColSize} {
		if !seen[need] {
			return fmt.Errorf("bridge: file record lacks %q (High-owned fields win wholesale)", need)
		}
	}
	return nil
}

func (im *Importer) rowExists(ctx context.Context, tableName string, row ids.RowID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return im.db.BridgeStoredRowExists(tableName, row)
}

// Drain imports contiguous staged bundles until a gap, hold, or failure.
// Every drain first syncs stream progress from authoritative storage and
// retries schema-held heads in order, so an administrator's local migration
// releases waiting bundles automatically.
// A schema-incompatible bundle is held durably (later sequences wait
// behind it) and the drain stops with a *HoldError; a bundle that fails
// against a compatible schema is quarantined (RetryQuarantined plus a
// later Drain replays it after operator resolution). No DDL is ever
// issued: each domain applies its own migrations.
func (im *Importer) Drain(ctx context.Context, in *Inbox) (int, error) {
	for _, stream := range in.Streams() {
		applied, ok, err := im.db.BridgeStreamProgress(stream)
		if err != nil {
			return 0, err
		}
		if ok {
			if err := in.SyncAuthoritativeProgress(stream, applied); err != nil {
				return 0, err
			}
		}
	}
	if _, err := in.RecheckHolds(func(bundle *Bundle) error {
		return im.checkSchema(ctx, bundle)
	}); err != nil {
		return 0, err
	}
	applied := 0
	for {
		if err := ctx.Err(); err != nil {
			return applied, err
		}
		bundle, _, err := in.NextImport()
		if err != nil {
			return applied, err
		}
		if bundle == nil {
			// No importable bundle: still install staged objects whose
			// metadata arrived earlier (objects may trail metadata by
			// an arbitrary number of drains).
			if _, ferr := im.importReadyFiles(ctx, in); ferr != nil {
				return applied, ferr
			}
			return applied, nil
		}
		m := bundle.Manifest
		if err := im.checkSchema(ctx, bundle); err != nil {
			var held *HoldError
			if errors.As(err, &held) {
				if herr := in.Hold(m.Stream, m.SeqFirst, m.BundleID.String(), m.SeqLast, m.SchemaEpoch, m.SchemaHash, held.Missing); herr != nil {
					return applied, herr
				}
				return applied, fmt.Errorf("bridge: import stream %q [%d..%d]: %w", m.Stream, m.SeqFirst, m.SeqLast, err)
			}
			// The schema could not be read (not merely unsatisfied):
			// leave the bundle staged for a later drain.
			return applied, err
		}
		resolution := in.PolicyResolution(m.Stream, m.SeqFirst)
		if err := im.applyBundleResolved(ctx, bundle, resolution); err != nil {
			if errors.Is(err, ErrProtectedDelete) {
				var protected *ProtectedDeleteError
				if errors.As(err, &protected) {
					if holdErr := in.HoldPolicy(m.Stream, m.SeqFirst, m.BundleID.String(), m.SeqLast, protected.Error()); holdErr != nil {
						return applied, holdErr
					}
				}
				return applied, fmt.Errorf("bridge: import stream %q [%d..%d]: %w", m.Stream, m.SeqFirst, m.SeqLast, err)
			}
			_ = in.Quarantine(m.Stream, m.SeqFirst, err.Error())
			return applied, fmt.Errorf("bridge: import stream %q [%d..%d]: %w", m.Stream, m.SeqFirst, m.SeqLast, err)
		}
		if err := in.MarkApplied(m.Stream, m.SeqFirst, m.SeqLast); err != nil {
			return applied, err
		}
		applied++
		// Newly imported metadata may complete staged objects.
		if _, ferr := im.importReadyFiles(ctx, in); ferr != nil {
			return applied, ferr
		}
	}
}

// checkSchema verifies the bundle's tables, columns, primary keys, and
// value-type compatibility against the authoritative local manifest without writing
// anything. A satisfied schema returns nil; missing or incompatible
// definitions return a *HoldError naming them; a schema that cannot be
// read returns the underlying error.
func (im *Importer) checkSchema(ctx context.Context, b *Bundle) error {
	need := make(map[string]map[string]map[codec.ValueType]bool)
	for _, batch := range b.Batches {
		for _, rec := range batch.Records {
			cols, ok := need[rec.Table]
			if !ok {
				cols = make(map[string]map[codec.ValueType]bool)
				need[rec.Table] = cols
			}
			for _, c := range rec.Columns {
				vts, ok := cols[c.Column]
				if !ok {
					vts = make(map[codec.ValueType]bool)
					cols[c.Column] = vts
				}
				vt := c.Value.Type
				if c.Policy == schema.PN_COUNTER || c.Policy == schema.OR_SET {
					vt = codec.TypeText
				}
				vts[vt] = true
			}
		}
	}
	for _, batch := range b.Batches {
		for _, record := range batch.Records {
			if record.Table == db.BridgeFileTableName {
				continue
			}
			for _, column := range record.Columns {
				p, err := im.db.ColumnMergePolicy(record.Table, column.Column)
				if err == nil && p != column.Policy {
					return &HoldError{Missing: []string{fmt.Sprintf("merge policy %s.%s requires %s", record.Table, column.Column, column.Policy)}}
				}
			}
		}
	}
	var held *HoldError
	tables := make([]string, 0, len(need))
	for table := range need {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		if table == db.BridgeFileTableName {
			// File metadata belongs to the dedicated file subsystem, not a
			// user-defined typed table. Shape validation happens at
			// prepare time (poison quarantines); a disabled subsystem
			// holds until the operator enables files and reopens.
			if !im.db.BridgeFilesEnabled() {
				held = holdMissing(held, "file storage")
			}
			continue
		}
		desc, err := im.describeTable(ctx, table)
		if err != nil {
			return err
		}
		if desc == nil {
			held = holdMissing(held, fmt.Sprintf("table %q", table))
			continue
		}
		if desc.pk == "" {
			held = holdMissing(held, fmt.Sprintf("table %q primary key", table))
			continue
		}
		cols := make([]string, 0, len(need[table]))
		for col := range need[table] {
			cols = append(cols, col)
		}
		sort.Strings(cols)
		for _, col := range cols {
			decl, ok := desc.cols[col]
			if !ok {
				held = holdMissing(held, fmt.Sprintf("table %q column %q", table, col))
				continue
			}
			vts := make([]codec.ValueType, 0, len(need[table][col]))
			for vt := range need[table][col] {
				vts = append(vts, vt)
			}
			sort.Slice(vts, func(i, j int) bool { return vts[i] < vts[j] })
			for _, vt := range vts {
				if typeCompatible(decl, vt) {
					continue
				}
				if vt == codec.TypeNull {
					held = holdMissing(held, fmt.Sprintf("table %q column %q must not be NULL", table, col))
				} else {
					held = holdMissing(held, fmt.Sprintf("table %q column %q expects %s, got %s", table, col, decl.typ, valueTypeName(vt)))
				}
			}
		}
	}
	if held != nil {
		return held
	}
	return nil
}

func holdMissing(held *HoldError, what string) *HoldError {
	if held == nil {
		held = &HoldError{}
	}
	held.Missing = append(held.Missing, what)
	return held
}

// pkColumn resolves (and caches) a table's single-column primary key from the
// authoritative High schema manifest.
func (im *Importer) pkColumn(ctx context.Context, table string) (string, error) {
	im.mu.Lock()
	if pk, ok := im.pks[table]; ok {
		im.mu.Unlock()
		return pk, nil
	}
	im.mu.Unlock()
	desc, err := im.describeTable(ctx, table)
	if err != nil {
		return "", err
	}
	if desc == nil || desc.pk == "" {
		return "", fmt.Errorf("bridge: import: table %s has no primary key", table)
	}
	pk := desc.pk
	im.mu.Lock()
	im.pks[table] = pk
	im.mu.Unlock()
	return pk, nil
}

// columnDesc is one live column's declared definition.
type columnDesc struct {
	typ     string // declared type name as reported by the engine
	notNull bool
}

// tableDesc is a table's live definition: columns plus single-column
// primary key.
type tableDesc struct {
	cols map[string]columnDesc
	pk   string
}

// describeTable reads the authoritative persisted table definition. A missing
// table returns a nil descriptor without an error; results are not cached so
// the schema gate sees the current manifest after a migration.
func (im *Importer) describeTable(ctx context.Context, table string) (*tableDesc, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tables, err := im.db.SchemaTables()
	if err != nil {
		return nil, fmt.Errorf("bridge: import: read schema for %s: %w", table, err)
	}
	for _, schemaTable := range tables {
		if !strings.EqualFold(schemaTable.Name, table) {
			continue
		}
		desc := &tableDesc{cols: make(map[string]columnDesc, len(schemaTable.Columns))}
		for _, column := range schemaTable.Columns {
			desc.cols[column.Name] = columnDesc{typ: column.Type.String(), notNull: !column.Nullable}
			if column.ID == schemaTable.PK {
				desc.pk = column.Name
			}
		}
		return desc, nil
	}
	return nil, nil
}

// typeCompatible reports whether a logical value may land in a column of
// the declared definition. Types must match exactly (through the schema
// package's SQLite-ish aliases) except for one lossless widening,
// INTEGER into REAL; NULL satisfies any nullable column. Anything else
// waits for a compatible local migration instead of coercing or failing
// mid-import.
func typeCompatible(decl columnDesc, vt codec.ValueType) bool {
	if vt == codec.TypeNull {
		return !decl.notNull
	}
	want, err := schema.ParseColumnType(decl.typ)
	if err != nil {
		return false
	}
	switch vt {
	case codec.TypeInteger:
		return want == schema.ColInteger || want == schema.ColReal
	case codec.TypeReal:
		return want == schema.ColReal
	case codec.TypeText:
		return want == schema.ColText
	case codec.TypeBlob:
		return want == schema.ColBlob
	default:
		return false
	}
}

func valueTypeName(vt codec.ValueType) string {
	switch vt {
	case codec.TypeNull:
		return "NULL"
	case codec.TypeInteger:
		return "INTEGER"
	case codec.TypeReal:
		return "REAL"
	case codec.TypeText:
		return "TEXT"
	case codec.TypeBlob:
		return "BLOB"
	default:
		return fmt.Sprintf("value type %d", int(vt))
	}
}

func bundleHasCRDT(b *Bundle) bool {
	for _, batch := range b.Batches {
		for _, record := range batch.Records {
			for _, column := range record.Columns {
				if column.Policy != schema.LWW {
					return true
				}
			}
		}
	}
	return false
}
func sourceReceiptID(bundle *Bundle, batch Batch) ids.TxID {
	if !bundleHasCRDT(bundle) {
		return batch.TxID
	}
	h := sha256.New()
	h.Write([]byte("murmur/bridge/source-receipt/v2"))
	h.Write(bundle.Manifest.SourceDomain[:])
	h.Write([]byte(bundle.Manifest.Stream))
	h.Write([]byte{0})
	h.Write(batch.TxID[:])
	var id ids.TxID
	copy(id[:], h.Sum(nil))
	return id
}
