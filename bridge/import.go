package bridge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/schema"
)

// Importer applies opened bundles to a High database as ordinary
// transactions, so accepted imports redistribute among High mesh peers as
// regular replicated writes. One bundle maps to exactly one transaction:
// imports are atomic per bundle.
//
// Imported puts apply as UPDATE-then-INSERT by primary key (partial rows
// fill missing columns with NULL), so replaying an applied bundle converges
// to the same state instead of duplicating effects. Two High receivers
// importing the same bundle likewise converge through mesh LWW on identical
// values. Deletes remove by primary key.
type Importer struct {
	db *db.DB

	mu  sync.Mutex
	pks map[string]string
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

// ApplyBundle imports one bundle atomically while preserving source transaction
// boundaries and recording stable source-transaction receipts in authoritative storage.
// The entire bundle is applied in a single atomic database transaction so that partial
// failures roll back completely without leaving partial source effects.
// Batches and bundles whose receipts are already present in authoritative storage
// are skipped to ensure deduplication across replays and concurrent receivers.
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
	// Resolve primary keys before opening the write transaction: PK
	// discovery queries the shared materialization, which the write lock
	// would deadlock against.
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

	// Check if all batches in the bundle are already recorded.
	allRecorded := len(b.Batches) > 0
	for _, batch := range b.Batches {
		if batch.TxID.IsZero() {
			allRecorded = false
			break
		}
		has, err := im.db.HasTransactionReceipt(batch.TxID)
		if err != nil {
			return err
		}
		if !has {
			allRecorded = false
			break
		}
	}
	if allRecorded {
		return im.db.SetBridgeStreamProgress(b.Manifest.Stream, b.Manifest.SeqLast)
	}
	prepared, err := im.prepareBundle(ctx, b, resolution)
	if err != nil {
		return err
	}

	// File records split from row records: rows import through one SQL
	// transaction, files through one metadata commit. Homogeneous bundles
	// (the only kind capture produces) stay atomic; a mixed bundle applies
	// as two idempotent commits (rows first, preserving poison ordering),
	// and quarantine retry replays both to convergence.
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

		tx, err := im.db.BeginBridgeImportTx(ctx, txID, b.Manifest.SourceDomain, b.Manifest.Stream, b.Manifest.BundleID, b.Manifest.SeqFirst, b.Manifest.SeqLast, resolution == "accept-low-delete", &db.TxOptions{})
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		for _, batch := range rowBatches {
			for _, rec := range batch.Records {
				if err := im.applyRecord(ctx, tx, rec); err != nil {
					return err
				}
			}
		}

		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
	}

	if len(filePuts) > 0 || len(fileDeletes) > 0 {
		if err := im.db.BridgeApplyFileMetadata(ctx, b.Manifest.SourceDomain, b.Manifest.Stream, b.Manifest.BundleID, b.Manifest.SeqFirst, b.Manifest.SeqLast, resolution == "accept-low-delete", filePuts, fileDeletes); err != nil {
			return err
		}
	}

	// Record receipts for all constituent batches in authoritative storage.
	if !b.Manifest.BundleID.IsZero() {
		_ = im.db.RecordTransactionReceipt(b.Manifest.BundleID)
	}
	for _, batch := range b.Batches {
		if !batch.TxID.IsZero() {
			_ = im.db.RecordTransactionReceipt(batch.TxID)
		}
	}

	// Update contiguous stream progress in authoritative storage.
	if err := im.db.SetBridgeStreamProgress(b.Manifest.Stream, b.Manifest.SeqLast); err != nil {
		return err
	}
	return nil
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
				if rowPolicy.Owner == db.BridgeOwnerHigh || rowPolicy.SourceDomain != b.Manifest.SourceDomain || rowPolicy.Stream != b.Manifest.Stream {
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
					if ok && policy.Owner == db.BridgeOwnerHigh {
						continue
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
	table, err := quoteIdent(tableName)
	if err != nil {
		return false, err
	}
	pk, err := im.pkColumn(ctx, tableName)
	if err != nil {
		return false, err
	}
	pk, err = quoteIdent(pk)
	if err != nil {
		return false, err
	}
	var found int
	err = im.db.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE %s = ?", table, pk), row[:]).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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
		if applied, ok, err := im.db.BridgeStreamProgress(stream); err == nil && ok {
			_ = in.SyncAuthoritativeProgress(stream, applied)
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
// value-type compatibility against the live local schema without writing
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
				vts[c.Value.Type] = true
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
			// File metadata bypasses the SQL schema: it needs the file
			// subsystem, not a SQL table. Shape validation happens at
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

func (im *Importer) applyRecord(ctx context.Context, tx *db.Tx, rec Record) error {
	table, err := quoteIdent(rec.Table)
	if err != nil {
		return err
	}
	pk, err := im.pkColumn(ctx, rec.Table)
	if err != nil {
		return err
	}
	pkQ, err := quoteIdent(pk)
	if err != nil {
		return err
	}
	switch rec.Op {
	case RecordDelete:
		_, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = ?", table, pkQ), rec.Row[:])
		return err
	case RecordPut:
		if len(rec.Columns) == 0 {
			return fmt.Errorf("bridge: import: upsert of %s has no columns", rec.Table)
		}
		set := make([]string, len(rec.Columns))
		args := make([]any, 0, 2*len(rec.Columns)+2)
		for i, c := range rec.Columns {
			cq, err := quoteIdent(c.Column)
			if err != nil {
				return err
			}
			set[i] = cq + " = ?"
			v, err := valueArg(c.Value)
			if err != nil {
				return err
			}
			args = append(args, v)
		}
		args = append(args, rec.Row[:])
		res, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", table, strings.Join(set, ", "), pkQ), args...)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return nil
		}
		cols := make([]string, len(rec.Columns)+1)
		holders := make([]string, len(rec.Columns)+1)
		insArgs := make([]any, 0, len(rec.Columns)+1)
		cols[0], holders[0], insArgs = pkQ, "?", append(insArgs, rec.Row[:])
		for i, c := range rec.Columns {
			cq, err := quoteIdent(c.Column)
			if err != nil {
				return err
			}
			cols[i+1] = cq
			holders[i+1] = "?"
			v, err := valueArg(c.Value)
			if err != nil {
				return err
			}
			insArgs = append(insArgs, v)
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(holders, ", ")), insArgs...)
		return err
	default:
		return fmt.Errorf("bridge: import: unknown record op %d", int(rec.Op))
	}
}

// pkColumn resolves (and caches) a table's single-column primary key via the
// live High schema.
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

// describeTable reads a table's live definition. A missing table returns
// a nil descriptor without an error; the schema gate must always see
// post-migration truth, so results are never cached here (pkColumn keeps
// its own PK cache).
func (im *Importer) describeTable(ctx context.Context, table string) (*tableDesc, error) {
	tq, err := quoteIdent(table)
	if err != nil {
		return nil, err
	}
	rows, err := im.db.QueryContext(ctx, fmt.Sprintf("SELECT name, type, \"notnull\", pk FROM pragma_table_info(%s)", tq))
	if err != nil {
		return nil, fmt.Errorf("bridge: import: describe %s: %w", table, err)
	}
	defer rows.Close()
	desc := &tableDesc{cols: make(map[string]columnDesc)}
	for rows.Next() {
		var name, typ string
		var notnull, pk int64
		if err := rows.Scan(&name, &typ, &notnull, &pk); err != nil {
			return nil, err
		}
		desc.cols[name] = columnDesc{typ: typ, notNull: notnull != 0}
		if pk != 0 {
			desc.pk = name
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(desc.cols) == 0 {
		return nil, nil
	}
	return desc, nil
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

func quoteIdent(name string) (string, error) {
	if name == "" || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("bridge: import: invalid identifier")
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`, nil
}

func valueArg(v codec.Value) (any, error) {
	switch v.Type {
	case codec.TypeNull:
		return nil, nil
	case codec.TypeInteger:
		return v.I, nil
	case codec.TypeReal:
		return v.F, nil
	case codec.TypeText:
		return v.S, nil
	case codec.TypeBlob:
		return append([]byte(nil), v.B...), nil
	default:
		return nil, fmt.Errorf("bridge: import: unsupported value type %d", int(v.Type))
	}
}
