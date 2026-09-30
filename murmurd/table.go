package murmurd

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	db "github.com/marcgauthier/murmur"
)

// murmurTable is one sql.Table over a murmur materialized table. It
// implements the SQLite-expressible edit set: InsertableTable,
// DeletableTable, UpdatableTable, TruncateableTable. All writes run
// as murmur implicit transactions (autocommit, one statement each).
type murmurTable struct {
	db     *db.DB
	name   string
	schema sql.PrimaryKeySchema
}

func (t *murmurTable) Name() string   { return t.name }
func (t *murmurTable) String() string { return t.name }

func (t *murmurTable) Schema() sql.Schema { return t.schema.Schema }

func (t *murmurTable) Collation() sql.CollationID { return sql.Collation_Default }

// murmurPartition is the single partition: murmur scans run on the
// shared materialization, so there is nothing to split.
type murmurPartition struct{}

func (murmurPartition) Key() []byte { return []byte{0} }

func (t *murmurTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(murmurPartition{}), nil
}

func (t *murmurTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	cols := make([]string, len(t.schema.Schema))
	for i, c := range t.schema.Schema {
		cols[i] = quoteIdent(c.Name)
	}
	rows, err := t.db.QueryContext(ctx.Context, fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ", "), quoteIdent(t.name)))
	if err != nil {
		return nil, fmt.Errorf("murmurd: scan %s: %w", t.name, err)
	}
	// Buffer the whole scan and close before returning: the engine
	// interleaves iteration with writes (UPDATE/DELETE route per row),
	// and an open murmur Rows stalls writers -- streaming would
	// deadlock. The materialization is in-memory, so this copies.
	var out []sql.Row
	for rows.Next() {
		cells := make([]any, len(t.schema.Schema))
		ptrs := make([]any, len(t.schema.Schema))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			rows.Close()
			return nil, fmt.Errorf("murmurd: scan %s: %w", t.name, err)
		}
		row := make(sql.Row, len(cells))
		for i, v := range cells {
			cv, err := convertCell(ctx, t.schema.Schema[i].Type, v)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("murmurd: scan %s column %s: %w", t.name, t.schema.Schema[i].Name, err)
			}
			row[i] = cv
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("murmurd: scan %s: %w", t.name, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("murmurd: scan %s: %w", t.name, err)
	}
	return sql.RowsToRowIter(out...), nil
}

// convertCell coerces one SQLite driver value to the GMS column type.
func convertCell(ctx *sql.Context, typ sql.Type, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	// Normalize driver integers: both SQLite backends return int64
	// for INTEGER affinity, but be liberal in what we accept.
	switch n := v.(type) {
	case int:
		v = int64(n)
	case int32:
		v = int64(n)
	case int16:
		v = int64(n)
	case uint64:
		v = int64(n)
	case float32:
		v = float64(n)
	case bool:
		if n {
			v = int64(1)
		} else {
			v = int64(0)
		}
	}
	cv, _, err := typ.Convert(ctx, v)
	return cv, err
}

// Inserter buffers rows and flushes them as multi-row INSERTs at
// Close (one implicit murmur transaction per chunk).
func (t *murmurTable) Inserter(*sql.Context) sql.RowInserter {
	return &murmurInserter{table: t}
}

type murmurInserter struct {
	table *murmurTable
	rows  []sql.Row
}

func (ins *murmurInserter) StatementBegin(*sql.Context)              {}
func (ins *murmurInserter) DiscardChanges(*sql.Context, error) error { return nil }
func (ins *murmurInserter) StatementComplete(*sql.Context) error     { return nil }
func (ins *murmurInserter) Insert(_ *sql.Context, row sql.Row) error {
	cp := append(sql.Row(nil), row...)
	ins.rows = append(ins.rows, cp)
	return nil
}

func (ins *murmurInserter) Close(ctx *sql.Context) error {
	const chunk = 500
	for len(ins.rows) > 0 {
		n := len(ins.rows)
		if n > chunk {
			n = chunk
		}
		if err := ins.flush(ctx.Context, ins.rows[:n]); err != nil {
			return err
		}
		ins.rows = ins.rows[n:]
	}
	return nil
}

func (ins *murmurInserter) flush(ctx context.Context, rows []sql.Row) error {
	t := ins.table
	cols := make([]string, len(t.schema.Schema))
	for i, c := range t.schema.Schema {
		cols[i] = quoteIdent(c.Name)
	}
	var b strings.Builder
	var args []any
	b.WriteString("INSERT INTO ")
	b.WriteString(quoteIdent(t.name))
	b.WriteString(" (")
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(") VALUES ")
	for i, row := range rows {
		if len(row) != len(cols) {
			return fmt.Errorf("murmurd: insert into %s: row has %d values, table has %d columns", t.name, len(row), len(cols))
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(")
		for j := range cols {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString("?")
			args = append(args, gmsValueToDriver(row[j]))
		}
		b.WriteString(")")
	}
	if _, err := t.db.ExecContext(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("murmurd: insert into %s: %w", t.name, err)
	}
	return nil
}

// gmsValueToDriver renders a GMS value bindable by the SQLite driver.
func gmsValueToDriver(v any) any {
	switch n := v.(type) {
	case uint64:
		return int64(n)
	case uint32:
		return int64(n)
	case uint16:
		return int64(n)
	case uint8:
		return int64(n)
	case int32:
		return int64(n)
	case int16:
		return int64(n)
	case int8:
		return int64(n)
	case int:
		return int64(n)
	case float32:
		return float64(n)
	default:
		return v
	}
}

// Replacer buffers rows and applies them as delete-by-key + insert
// at Close (SQLite has no REPLACE; murmur upserts per row).
func (t *murmurTable) Replacer(*sql.Context) sql.RowReplacer {
	return &murmurReplacer{table: t}
}

type murmurReplacer struct {
	table *murmurTable
	rows  []sql.Row
}

func (r *murmurReplacer) StatementBegin(*sql.Context)              {}
func (r *murmurReplacer) DiscardChanges(*sql.Context, error) error { return nil }
func (r *murmurReplacer) StatementComplete(*sql.Context) error     { return nil }
func (r *murmurReplacer) Insert(_ *sql.Context, row sql.Row) error {
	r.rows = append(r.rows, append(sql.Row(nil), row...))
	return nil
}

// Delete marks one buffered row replaced-away. The engine calls
// Delete for the conflicting old row before Insert supplies the new
// one; with full-row buffering the delete is subsumed by the
// delete-by-key at Close, so this is a no-op gate on row shape.
func (r *murmurReplacer) Delete(_ *sql.Context, row sql.Row) error {
	if len(row) != len(r.table.schema.Schema) {
		return fmt.Errorf("murmurd: replace into %s: row has %d values, table has %d columns", r.table.name, len(row), len(r.table.schema.Schema))
	}
	return nil
}

func (r *murmurReplacer) Close(ctx *sql.Context) error {
	t := r.table
	for _, row := range r.rows {
		where, args := t.matchWhere(row)
		if _, err := t.db.ExecContext(ctx.Context, fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(t.name), where), args...); err != nil {
			return fmt.Errorf("murmurd: replace into %s: %w", t.name, err)
		}
	}
	ins := &murmurInserter{table: t, rows: r.rows}
	if err := ins.Close(ctx); err != nil {
		return fmt.Errorf("murmurd: replace into %s: %w", t.name, err)
	}
	return nil
}

// Deleter routes row deletes by primary key (or full-row match when
// the table has no key).
func (t *murmurTable) Deleter(*sql.Context) sql.RowDeleter {
	return &murmurDeleter{table: t}
}

type murmurDeleter struct {
	table *murmurTable
}

func (d *murmurDeleter) StatementBegin(*sql.Context)              {}
func (d *murmurDeleter) DiscardChanges(*sql.Context, error) error { return nil }
func (d *murmurDeleter) StatementComplete(*sql.Context) error     { return nil }
func (d *murmurDeleter) Close(*sql.Context) error                 { return nil }

func (d *murmurDeleter) Delete(ctx *sql.Context, row sql.Row) error {
	where, args := d.table.matchWhere(row)
	res, err := d.table.db.ExecContext(ctx.Context, fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(d.table.name), where), args...)
	if err != nil {
		return fmt.Errorf("murmurd: delete from %s: %w", d.table.name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("murmurd: delete from %s: %w", d.table.name, err)
	}
	if n == 0 {
		return sql.ErrDeleteRowNotFound.New()
	}
	return nil
}

// Updater routes row updates by primary key (or full-row match on the
// old row when the table has no key).
func (t *murmurTable) Updater(*sql.Context) sql.RowUpdater {
	return &murmurUpdater{table: t}
}

type murmurUpdater struct {
	table *murmurTable
}

func (u *murmurUpdater) StatementBegin(*sql.Context)              {}
func (u *murmurUpdater) DiscardChanges(*sql.Context, error) error { return nil }
func (u *murmurUpdater) StatementComplete(*sql.Context) error     { return nil }
func (u *murmurUpdater) Close(*sql.Context) error                 { return nil }

func (u *murmurUpdater) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	t := u.table
	if len(new) != len(t.schema.Schema) {
		return fmt.Errorf("murmurd: update %s: row has %d values, table has %d columns", t.name, len(new), len(t.schema.Schema))
	}
	var sets []string
	var args []any
	pk := map[int]bool{}
	for _, o := range t.schema.PkOrdinals {
		pk[o] = true
	}
	for i, c := range t.schema.Schema {
		if pk[i] {
			continue
		}
		sets = append(sets, quoteIdent(c.Name)+" = ?")
		args = append(args, gmsValueToDriver(new[i]))
	}
	if len(sets) == 0 {
		return fmt.Errorf("murmurd: update %s: nothing to update (key-only row)", t.name)
	}
	where, wargs := t.matchWhere(old)
	args = append(args, wargs...)
	res, err := t.db.ExecContext(ctx.Context,
		fmt.Sprintf("UPDATE %s SET %s WHERE %s", quoteIdent(t.name), strings.Join(sets, ", "), where), args...)
	if err != nil {
		return fmt.Errorf("murmurd: update %s: %w", t.name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("murmurd: update %s: %w", t.name, err)
	}
	if n == 0 {
		return sql.ErrDeleteRowNotFound.New()
	}
	return nil
}

// matchWhere builds a WHERE clause matching one row: by primary key
// when the table has one, else by full-row equality (NULL-safe).
func (t *murmurTable) matchWhere(row sql.Row) (string, []any) {
	ords := t.schema.PkOrdinals
	if len(ords) == 0 {
		ords = make([]int, len(t.schema.Schema))
		for i := range ords {
			ords[i] = i
		}
	}
	var parts []string
	var args []any
	for _, o := range ords {
		col := quoteIdent(t.schema.Schema[o].Name)
		if row[o] == nil {
			parts = append(parts, col+" IS NULL")
			continue
		}
		parts = append(parts, col+" = ?")
		args = append(args, gmsValueToDriver(row[o]))
	}
	return strings.Join(parts, " AND "), args
}

// Truncate deletes all rows (SQLite has no TRUNCATE; DELETE without
// WHERE is equivalent).
func (t *murmurTable) Truncate(ctx *sql.Context) (int, error) {
	res, err := t.db.ExecContext(ctx.Context, "DELETE FROM "+quoteIdent(t.name))
	if err != nil {
		return 0, fmt.Errorf("murmurd: truncate %s: %w", t.name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("murmurd: truncate %s: %w", t.name, err)
	}
	return int(n), nil
}

// quoteIdent quotes an SQLite identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
