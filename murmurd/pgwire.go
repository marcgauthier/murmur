package murmurd

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"

	db "github.com/marcgauthier/spedsql"
)

// This file is the PostgreSQL wire-protocol frontend (psql-wire). It
// speaks the PG v3 protocol but executes SQLite: every statement is
// classified by the shared filter, $1..$N placeholders translate to
// ?, and the text runs on murmur unchanged. There is no dialect
// translation and no pg_catalog: anything SQLite cannot express is
// rejected with an explicit reason.
//
// Authentication is trust (any user connects); deployments must bind
// a loopback address or firewall the port.

// pgVersionParam is the server_version parameter (numeric: clients
// parse it). The full product string is served by SELECT version().
const pgVersionParam = "15.0"

func pgVersionString() string {
	return fmt.Sprintf("PostgreSQL %s (murmurd %s, SQLite engine)", pgVersionParam, Version)
}

// pgFrontend serves one murmur database over the PG wire.
type pgFrontend struct {
	db       *db.DB
	provider *Provider // shared DDL declarations with the MySQL side
	dbName   string
	logger   *slog.Logger
}

func newPGServer(database *db.DB, provider *Provider, dbName string, logger *slog.Logger) (*wire.Server, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	fe := &pgFrontend{db: database, provider: provider, dbName: dbName, logger: logger}
	return wire.NewServer(fe.parse, wire.Version(pgVersionParam), wire.Logger(logger))
}

// pgAttrTxn tracks the absorbed transaction state per session.
const pgAttrTxn = "murmurd.in_transaction"

func (fe *pgFrontend) parse(ctx context.Context, q wire.Query) (wire.PreparedStatements, error) {
	stmts := SplitStatements(q.Query)
	if !q.SimpleQuery && len(stmts) > 1 {
		return nil, &FilterError{Stmt: q.Query, Reason: "multi-statement strings are only accepted in the simple protocol; the extended protocol takes one statement"}
	}
	var out wire.PreparedStatements
	for _, s := range stmts {
		// Skip comment-only fragments (drivers ping with `-- ping`).
		if stripLeadingComments(strings.TrimSpace(s)) == "" {
			continue
		}
		ps, err := fe.prepareOne(ctx, s, q.ParameterOIDs)
		if err != nil {
			return nil, err
		}
		out = append(out, ps)
	}
	if len(out) == 0 {
		// Comment-only input: EmptyQueryResponse, no completion tag.
		empty := wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
			return writer.Empty()
		})
		out = append(out, empty)
	}
	return out, nil
}

func (fe *pgFrontend) prepareOne(ctx context.Context, q string, paramOIDs []uint32) (*wire.PreparedStatement, error) {
	class, ferr := Classify(q)
	if ferr != nil {
		return nil, ferr
	}
	translated, nparams, err := TranslateParams(q)
	if err != nil {
		return nil, err
	}
	// Blob-bound parameters (for bytea decoding) resolve once at
	// parse against the single-table schema.
	blobParams := fe.resolveBlobParams(ctx, q, class)
	var opts []wire.PreparedOptionFn
	if nparams > 0 {
		// Zero OIDs = unspecified; the client sent the values and
		// decodeParams reads them by declared format.
		opts = append(opts, wire.WithParameters(make([]uint32, nparams)))
	}
	switch class {
	case ClassEmulated:
		return fe.emulatedStatement(q, opts), nil
	case ClassTxnNoop:
		return fe.txnStatement(q, opts), nil
	case ClassSessionNoop:
		return fe.sessionStatement(q, opts), nil
	case ClassWrite:
		head, _ := splitHead(strings.TrimSpace(q))
		tag := strings.ToUpper(head)
		return wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
			args, err := decodeParams(params, paramOIDs, blobParams)
			if err != nil {
				return err
			}
			res, err := fe.db.ExecContext(ctx, translated, args...)
			if err != nil {
				return fmt.Errorf("murmurd: SQLite engine rejected this statement: %w", err)
			}
			n, _ := res.RowsAffected()
			return writer.Complete(writeTag(tag, n))
		}, opts...), nil
	case ClassTruncate:
		table, err := truncateTable(q)
		if err != nil {
			return nil, err
		}
		return wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
			if len(params) != 0 {
				return &FilterError{Stmt: q, Reason: "TRUNCATE takes no parameters"}
			}
			if _, err := fe.db.ExecContext(ctx, "DELETE FROM "+quoteIdent(table)); err != nil {
				return fmt.Errorf("murmurd: SQLite engine rejected this statement: %w", err)
			}
			return writer.Complete("TRUNCATE TABLE")
		}, opts...), nil
	case ClassCreateTable:
		def, err := ParseCreateTable(translated)
		if err != nil {
			return nil, err
		}
		return wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
			if len(params) != 0 {
				return &FilterError{Stmt: q, Reason: "CREATE TABLE takes no parameters"}
			}
			if err := fe.createTable(ctx, def); err != nil {
				return err
			}
			return writer.Complete("CREATE TABLE")
		}, opts...), nil
	case ClassRead:
		return fe.readStatement(ctx, q, translated, nparams, paramOIDs, blobParams)
	default:
		return nil, &FilterError{Stmt: q, Reason: "unsupported statement"}
	}
}

// readStatement prepares a read. Columns must be known at parse time
// (Describe precedes Execute), so simple queries and parameterless
// extended queries execute at parse and replay at Execute; parameterized
// queries probe with NULLs for the shape and re-run at Execute.
func (fe *pgFrontend) readStatement(parseCtx context.Context, orig, translated string, nparams int, paramOIDs []uint32, blobParams map[int]bool) (*wire.PreparedStatement, error) {
	if nparams > 0 {
		nulls := make([]any, nparams)
		shape, err := fe.runRead(parseCtx, translated, nulls)
		if err != nil {
			return nil, err
		}
		cols := shape.columns
		if len(shape.rows) == 0 {
			// Empty probe: resolve plain single-table selects
			// against the schema so OIDs stay exact.
			if resolved, ok := fe.resolveSelectColumns(parseCtx, orig); ok {
				cols = resolved
			}
		}
		return wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
			args, err := decodeParams(params, paramOIDs, blobParams)
			if err != nil {
				return err
			}
			if len(args) != nparams {
				return fmt.Errorf("murmurd: got %d parameters, want %d", len(args), nparams)
			}
			shape, err := fe.runRead(ctx, translated, args)
			if err != nil {
				return err
			}
			return emitRows(writer, cols, shape.rows)
		}, wire.WithColumns(wire.Columns(pgColumns(cols))), wire.WithParameters(make([]uint32, nparams))), nil
	}
	shape, err := fe.runRead(parseCtx, translated, nil)
	if err != nil {
		return nil, err
	}
	cols := shape.columns
	return wire.NewStatement(func(ctx context.Context, writer wire.DataWriter, params []wire.Parameter) error {
		if len(params) != 0 {
			return fmt.Errorf("murmurd: got %d parameters, want 0", len(params))
		}
		return emitRows(writer, cols, shape.rows)
	}, wire.WithColumns(wire.Columns(pgColumns(cols)))), nil
}

// pgColumn is one decided result column.
type pgColumn struct {
	name string
	oid  uint32
}

// readShape is a buffered read result with decided columns.
type readShape struct {
	columns []pgColumn
	rows    [][]any // values coerced to their column OID kind
}

func (fe *pgFrontend) runRead(ctx context.Context, q string, args []any) (*readShape, error) {
	rows, err := fe.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("murmurd: SQLite engine rejected this statement: %w", err)
	}
	defer rows.Close()
	names := rows.Columns()
	var raw [][]any
	for rows.Next() {
		cells := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("murmurd: read rows: %w", err)
		}
		raw = append(raw, cells)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("murmurd: read rows: %w", err)
	}
	cols := decideColumns(names, raw)
	cooked := make([][]any, len(raw))
	for i, row := range raw {
		out := make([]any, len(row))
		for j, v := range row {
			cv, err := coerceValue(cols[j].oid, v)
			if err != nil {
				return nil, fmt.Errorf("murmurd: column %s: %w", names[j], err)
			}
			out[j] = cv
		}
		cooked[i] = out
	}
	return &readShape{columns: cols, rows: cooked}, nil
}

// decideColumns infers one OID per column: unanimous kinds keep
// their type, mixed kinds degrade to TEXT.
func decideColumns(names []string, rows [][]any) []pgColumn {
	cols := make([]pgColumn, len(names))
	for j, name := range names {
		kind := kindNone
		for _, row := range rows {
			k := kindOf(row[j])
			if k == kindNone {
				continue
			}
			if kind == kindNone {
				kind = k
				continue
			}
			if k != kind {
				// int+float mixes stay numeric (ints widen).
				if (kind == kindInt && k == kindFloat) || (kind == kindFloat && k == kindInt) {
					kind = kindFloat
					continue
				}
				kind = kindMixed
				break
			}
		}
		var oid uint32
		switch kind {
		case kindInt:
			oid = pgtype.Int8OID
		case kindFloat:
			oid = pgtype.Float8OID
		case kindBytes:
			oid = pgtype.ByteaOID
		case kindBool:
			oid = pgtype.BoolOID
		case kindTime:
			oid = pgtype.TimestamptzOID
		default:
			oid = pgtype.TextOID
		}
		cols[j] = pgColumn{name: name, oid: oid}
	}
	return cols
}

type valueKind int

const (
	kindNone valueKind = iota
	kindInt
	kindFloat
	kindText
	kindBytes
	kindBool
	kindTime
	kindMixed
)

func kindOf(v any) valueKind {
	switch v.(type) {
	case nil:
		return kindNone
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return kindInt
	case float32, float64:
		return kindFloat
	case string:
		return kindText
	case []byte:
		return kindBytes
	case bool:
		return kindBool
	case time.Time:
		return kindTime
	default:
		return kindMixed
	}
}

// coerceValue renders one value for its decided OID kind.
func coerceValue(oid uint32, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch oid {
	case pgtype.Int8OID:
		return toInt64(v)
	case pgtype.Float8OID:
		return toFloat64(v)
	case pgtype.ByteaOID:
		b, ok := v.([]byte)
		if !ok {
			return nil, fmt.Errorf("cannot render %T as bytea", v)
		}
		return b, nil
	case pgtype.BoolOID:
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("cannot render %T as bool", v)
		}
		return b, nil
	case pgtype.TimestamptzOID:
		t, ok := v.(time.Time)
		if !ok {
			return nil, fmt.Errorf("cannot render %T as timestamptz", v)
		}
		return t, nil
	default:
		return stringifyValue(v), nil
	}
}

func toInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int16:
		return int64(n), nil
	case int8:
		return int64(n), nil
	case uint64:
		return int64(n), nil
	case uint32:
		return int64(n), nil
	case uint16:
		return int64(n), nil
	case uint8:
		return int64(n), nil
	case uint:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("cannot render %T as int8", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	default:
		if i, err := toInt64(v); err == nil {
			return float64(i), nil
		}
		return 0, fmt.Errorf("cannot render %T as float8", v)
	}
}

func stringifyValue(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	case time.Time:
		return t.Format(time.RFC3339Nano)
	default:
		return strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(
			strings.TrimPrefix(strings.TrimPrefix(fmt.Sprintf("%v", v), "["), "]"), " ", ","), ",,", ","))
	}
}

// resolveSelectColumns resolves a plain single-table SELECT's output
// columns against the table schema (used when the NULL probe returns
// no rows to infer from). It reports ok=false for anything shaped
// beyond `SELECT [DISTINCT] collist FROM table [rest...]` with bare
// column references.
func (fe *pgFrontend) resolveSelectColumns(ctx context.Context, q string) ([]pgColumn, bool) {
	t, ok := parseSimpleSelect(q)
	if !ok {
		return nil, false
	}
	ordered, byName, err := fe.tableDecls(ctx, t.table)
	if err != nil || len(ordered) == 0 {
		return nil, false
	}
	if len(t.cols) == 1 && t.cols[0].source == "*" {
		cols := make([]pgColumn, len(ordered))
		for i, d := range ordered {
			cols[i] = pgColumn{name: d.name, oid: sqliteDeclToOID(d.decl)}
		}
		return cols, true
	}
	cols := make([]pgColumn, 0, len(t.cols))
	for _, c := range t.cols {
		decl, ok := byName[strings.ToLower(c.source)]
		if !ok {
			return nil, false
		}
		cols = append(cols, pgColumn{name: c.name, oid: sqliteDeclToOID(decl)})
	}
	return cols, len(cols) > 0
}

type simpleSelect struct {
	table string
	cols  []simpleCol
}

type simpleCol struct {
	name   string // output name (alias or column)
	source string // base column name
}

// parseSimpleSelect accepts SELECT [DISTINCT] a [, b AS c ...] FROM t
// with bare identifiers (optionally table-qualified) or a single *.
// JOINs, subqueries, expressions, and GROUP BY fall out (ok=false);
// WHERE/ORDER BY/LIMIT tails are allowed and ignored.
func parseSimpleSelect(q string) (simpleSelect, bool) {
	var out simpleSelect
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(q), ";"))
	rest, ok := cutKeyword(s, "select")
	if !ok {
		return out, false
	}
	if r, ok := cutKeyword(rest, "distinct"); ok {
		rest = r
	}
	// Split collist FROM rest at the top-level FROM.
	depth := 0
	fromAt := -1
	i := 0
	for i < len(rest) {
		c := rest[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(rest, i, c)
		case c == '$':
			if end, ok := dollarTagEnd(rest, i); ok {
				i = end
			} else {
				i++
			}
		case c == '(':
			depth++
			i++
		case c == ')':
			if depth > 0 {
				depth--
			}
			i++
		default:
			if depth == 0 && isWordStart(rest, i) && hasKeywordAt(rest, i, "from") {
				fromAt = i
				i = len(rest)
				continue
			}
			i++
		}
	}
	if fromAt < 0 {
		return out, false
	}
	collist := strings.TrimSpace(rest[:fromAt])
	tail := strings.TrimSpace(rest[fromAt+4:])
	if collist == "" || tail == "" {
		return out, false
	}
	table, _ := splitHead(tail)
	table = unquoteIdent(table)
	if table == "" || !isBareIdent(table) {
		return out, false
	}
	out.table = table
	if strings.TrimSpace(collist) == "*" {
		out.cols = []simpleCol{{name: "*", source: "*"}}
		return out, len(splitTopCommas(collist)) == 1
	}
	for _, item := range splitTopCommas(collist) {
		item = strings.TrimSpace(item)
		if item == "" || strings.ContainsAny(item, "()") {
			return out, false
		}
		parts := strings.Fields(item)
		var src, alias string
		switch len(parts) {
		case 1:
			src, alias = parts[0], parts[0]
		case 2:
			// bare alias without AS is ambiguous; reject it.
			return out, false
		case 3:
			if !strings.EqualFold(parts[1], "as") {
				return out, false
			}
			src, alias = parts[0], parts[2]
		default:
			return out, false
		}
		if i := strings.LastIndexByte(src, '.'); i >= 0 {
			src = src[i+1:]
		}
		src = unquoteIdent(src)
		alias = unquoteIdent(alias)
		if !isBareIdent(src) || !isBareIdent(alias) {
			return out, false
		}
		out.cols = append(out.cols, simpleCol{name: alias, source: src})
	}
	// Reject tails that change the shape: JOIN/GROUP BY/HAVING and set ops.
	for _, w := range topLevelVerbs(tail) {
		switch w {
		case "join", "group", "having", "union", "intersect", "except":
			return out, false
		}
	}
	return out, len(out.cols) > 0
}

// tableDecl holds one PRAGMA-resolved column declaration.
type tableDecl struct {
	name string
	decl string
}

// tableDecls returns the ordered column declarations plus a
// lowercase-name index for a table.
func (fe *pgFrontend) tableDecls(ctx context.Context, table string) ([]tableDecl, map[string]string, error) {
	rows, err := fe.db.QueryContext(ctx, "SELECT name, type FROM pragma_table_info(?)", table)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var ordered []tableDecl
	byName := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, nil, err
		}
		ordered = append(ordered, tableDecl{name: name, decl: typ})
		byName[strings.ToLower(name)] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return ordered, byName, nil
}

func sqliteDeclToOID(decl string) uint32 {
	u := strings.ToUpper(decl)
	switch {
	case strings.Contains(u, "INT"):
		return pgtype.Int8OID
	case strings.Contains(u, "REAL"), strings.Contains(u, "FLOA"), strings.Contains(u, "DOUB"):
		return pgtype.Float8OID
	case strings.Contains(u, "BLOB"):
		return pgtype.ByteaOID
	default:
		return pgtype.TextOID
	}
}

func isWordStart(s string, i int) bool {
	if i == 0 {
		return true
	}
	return !isWordChar(s[i-1])
}

func hasKeywordAt(s string, i int, kw string) bool {
	if i+len(kw) > len(s) || !strings.EqualFold(s[i:i+len(kw)], kw) {
		return false
	}
	if i+len(kw) < len(s) && isWordChar(s[i+len(kw)]) {
		return false
	}
	return true
}

// pgColumns converts decided columns to wire columns.
func pgColumns(cols []pgColumn) []wire.Column {
	out := make([]wire.Column, len(cols))
	for i, c := range cols {
		out[i] = wire.Column{Name: c.name, Oid: c.oid, Width: -1}
	}
	return out
}

func emitRows(w wire.DataWriter, cols []pgColumn, rows [][]any) error {
	if err := wire.WriteRows(w, rows); err != nil {
		return err
	}
	return w.Complete(fmt.Sprintf("SELECT %d", len(rows)))
}

func writeTag(head string, n int64) string {
	switch head {
	case "INSERT":
		return fmt.Sprintf("INSERT 0 %d", n)
	default:
		return fmt.Sprintf("%s %d", head, n)
	}
}

// truncateTable extracts the single table name from TRUNCATE [TABLE] t.
func truncateTable(q string) (string, error) {
	rest, _ := cutKeyword(strings.TrimSpace(q), "truncate")
	if r, ok := cutKeyword(rest, "table"); ok {
		rest = r
	}
	rest = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rest), ";"))
	name, tail := splitHead(rest)
	name = unquoteIdent(name)
	if name == "" || !isBareIdent(name) {
		return "", &FilterError{Stmt: q, Reason: "TRUNCATE takes exactly one table name"}
	}
	if t := strings.TrimSpace(tail); t != "" {
		// ONLY / RESTART IDENTITY / CASCADE / multi-table: reject.
		return "", &FilterError{Stmt: q, Reason: fmt.Sprintf("TRUNCATE option %q is not supported (single table only, storage is content-addressed so no identity/space clauses apply)", t)}
	}
	return name, nil
}

// createTable runs a parsed CREATE TABLE through the shared DDL path.
func (fe *pgFrontend) createTable(ctx context.Context, def *CreateTableDef) error {
	ts, err := createDefToTableSchema(def)
	if err != nil {
		return &FilterError{Stmt: def.Name, Reason: err.Error()}
	}
	if err := fe.provider.addTable(ctx, ts); err != nil {
		if isTableExists(err) {
			if def.IfNotExist {
				return nil
			}
			return fmt.Errorf("murmurd: relation %q already exists", def.Name)
		}
		return fmt.Errorf("murmurd: CREATE TABLE %s: %w", def.Name, err)
	}
	return nil
}

func isTableExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}

func (fe *pgFrontend) emulatedStatement(q string, opts []wire.PreparedOptionFn) *wire.PreparedStatement {
	switch EmulatedName(q) {
	case "version":
		opts = append(opts, wire.WithColumns(wire.Columns{{Name: "version", Oid: pgtype.TextOID}}))
		return wire.NewStatement(func(ctx context.Context, w wire.DataWriter, p []wire.Parameter) error {
			if err := wire.WriteRows(w, [][]any{{pgVersionString()}}); err != nil {
				return err
			}
			return w.Complete("SELECT 1")
		}, opts...)
	default: // current_database
		opts = append(opts, wire.WithColumns(wire.Columns{{Name: "current_database", Oid: pgtype.NameOID}}))
		return wire.NewStatement(func(ctx context.Context, w wire.DataWriter, p []wire.Parameter) error {
			if err := wire.WriteRows(w, [][]any{{fe.dbName}}); err != nil {
				return err
			}
			return w.Complete("SELECT 1")
		}, opts...)
	}
}

func (fe *pgFrontend) txnStatement(q string, opts []wire.PreparedOptionFn) *wire.PreparedStatement {
	head, _ := splitHead(strings.TrimSpace(q))
	tag := "BEGIN"
	lower := strings.ToLower(head)
	begin := true
	switch lower {
	case "begin", "start", "savepoint":
		tag = strings.ToUpper(head)
		if lower == "start" {
			tag = "START TRANSACTION"
		}
	case "commit", "end":
		tag = "COMMIT"
		begin = false
	case "rollback":
		tag = "ROLLBACK"
		begin = false
	case "release":
		tag = "RELEASE"
		begin = false
	}
	return wire.NewStatement(func(ctx context.Context, w wire.DataWriter, p []wire.Parameter) error {
		wire.SetAttribute(ctx, pgAttrTxn, begin)
		return w.Complete(tag)
	}, opts...)
}

func (fe *pgFrontend) sessionStatement(q string, opts []wire.PreparedOptionFn) *wire.PreparedStatement {
	head, _ := splitHead(strings.TrimSpace(q))
	tag := strings.ToUpper(head)
	if strings.EqualFold(head, "discard") {
		tag = "DISCARD ALL"
	}
	fe.logger.Debug("pg session statement absorbed", "stmt", q)
	return wire.NewStatement(func(ctx context.Context, w wire.DataWriter, p []wire.Parameter) error {
		return w.Complete(tag)
	}, opts...)
}

// decodeTextBytea decodes PG text-format bytea (\xHEX or octal escapes).
func decodeTextBytea(src []byte) ([]byte, error) {
	v, err := pgtype.ByteaCodec{}.DecodeValue(pgtype.NewMap(), pgtype.ByteaOID, int16(wire.TextFormat), src)
	if err != nil {
		return nil, err
	}
	raw, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("got %T", v)
	}
	return raw, nil
}

// decodeParams decodes extended-protocol parameters to murmur bind
// values. Text parameters pass through as strings (SQLite coerces),
// except for blob-bound ones (see resolveBlobParams), which arrive
// PG-escaped (\xHEX or octal) and decode to raw bytes. Binary
// parameters decode by their client-supplied OID.
func decodeParams(params []wire.Parameter, paramOIDs []uint32, blobParams map[int]bool) ([]any, error) {
	args := make([]any, len(params))
	for i, p := range params {
		if p.Format() == wire.TextFormat {
			if i < len(paramOIDs) && paramOIDs[i] == pgtype.ByteaOID {
				raw, err := decodeTextBytea(p.Value())
				if err != nil {
					return nil, fmt.Errorf("murmurd: decode bytea parameter $%d: %w", i+1, err)
				}
				args[i] = raw
				continue
			}
			if blobParams[i] {
				// Unspecified-OID text for a blob column: PG
				// clients hex-escape bytea (\x...) in text mode.
				if raw, err := decodeTextBytea(p.Value()); err == nil {
					args[i] = raw
					continue
				}
			}
			args[i] = string(p.Value())
			continue
		}
		var oid uint32
		if i < len(paramOIDs) {
			oid = paramOIDs[i]
		}
		if oid == 0 {
			return nil, fmt.Errorf("murmurd: binary parameter $%d has no type OID (send text format or specify the type)", i+1)
		}
		v, err := p.Scan(oid)
		if err != nil {
			return nil, fmt.Errorf("murmurd: decode parameter $%d: %w", i+1, err)
		}
		nv, err := normalizeParam(v)
		if err != nil {
			return nil, fmt.Errorf("murmurd: parameter $%d: %w", i+1, err)
		}
		args[i] = nv
	}
	return args, nil
}

// resolveBlobParams maps 0-based parameter indexes to true when the
// parameter binds a blob column of the statement's single table. PG
// clients with unspecified param OIDs send bytea hex-escaped in text
// mode, and only the target column disambiguates that from genuine
// text. Anything unrecognized stays text (SQLite coerces, or the
// engine rejects loudly).
func (fe *pgFrontend) resolveBlobParams(ctx context.Context, q string, class StmtClass) map[int]bool {
	out := map[int]bool{}
	var table string
	var cols []string // INSERT positional columns, nil otherwise
	switch class {
	case ClassWrite:
		head, _ := splitHead(strings.TrimSpace(q))
		switch strings.ToLower(head) {
		case "insert":
			t, c, ok := parseInsertTarget(q)
			if !ok {
				return out
			}
			table = t
			cols = c
		case "update":
			t, sets, ok := parseUpdateTarget(q)
			if !ok {
				return out
			}
			table = t
			cols = sets // resolved below by assignment, not position
			_ = cols
		case "delete":
			t, ok := parseDeleteTarget(q)
			if !ok {
				return out
			}
			table = t
		default:
			return out
		}
	case ClassRead:
		sel, ok := parseSimpleSelect(q)
		if !ok {
			return out
		}
		table = sel.table
	default:
		return out
	}
	ordered, byName, err := fe.tableDecls(ctx, table)
	if err != nil || len(ordered) == 0 {
		return out
	}
	isBlob := func(col string) bool {
		decl, ok := byName[strings.ToLower(col)]
		if !ok {
			return false
		}
		return strings.Contains(strings.ToUpper(decl), "BLOB")
	}
	head, _ := splitHead(strings.TrimSpace(q))
	switch strings.ToLower(head) {
	case "insert":
		if cols == nil {
			// No column list: table order.
			for _, d := range ordered {
				cols = append(cols, d.name)
			}
		}
		if len(cols) == 0 {
			return out
		}
		// Map $n in appearance order onto the column list,
		// honoring DEFAULT slots inside each VALUES group.
		groups := valuesGroups(q)
		for _, g := range groups {
			pos := 0
			for _, item := range splitTopCommas(g) {
				item = strings.TrimSpace(item)
				if strings.HasPrefix(item, "$") {
					if n, err := paramNum(item); err == nil && pos < len(cols) && isBlob(cols[pos]) {
						out[n-1] = true
					}
				}
				pos++
			}
		}
		// Fallback when no VALUES groups parsed (e.g. INSERT..SELECT
		// carries params in its SELECT): positional cycle.
		if len(groups) == 0 {
			for i, n := range paramNumsInOrder(q) {
				if isBlob(cols[i%len(cols)]) {
					out[n-1] = true
				}
			}
		}
	case "update":
		for col, n := range updateSetParams(q) {
			if isBlob(col) {
				out[n-1] = true
			}
		}
		for col, n := range whereComparisons(q) {
			if isBlob(col) {
				out[n-1] = true
			}
		}
	default: // select/delete: WHERE comparisons
		for col, n := range whereComparisons(q) {
			if isBlob(col) {
				out[n-1] = true
			}
		}
	}
	return out
}

// parseInsertTarget extracts the table and column list from
// INSERT INTO [ONLY] t [(cols)].
func parseInsertTarget(q string) (table string, cols []string, ok bool) {
	rest, ok := cutKeyword(q, "insert")
	if !ok {
		return "", nil, false
	}
	rest, ok = cutKeyword(rest, "into")
	if !ok {
		return "", nil, false
	}
	if r, ok := cutKeyword(rest, "only"); ok {
		rest = r
	}
	head, tail := splitHead(rest)
	table = unquoteIdent(head)
	if table == "" || !isBareIdent(table) {
		return "", nil, false
	}
	tail = strings.TrimSpace(tail)
	if !strings.HasPrefix(tail, "(") {
		return table, nil, true // no column list: table order
	}
	end := matchingParen(tail, 0)
	if end < 0 {
		return "", nil, false
	}
	for _, item := range splitTopCommas(tail[1:end]) {
		c := unquoteIdent(strings.TrimSpace(item))
		if c == "" || !isBareIdent(c) {
			return "", nil, false
		}
		cols = append(cols, c)
	}
	return table, cols, len(cols) > 0
}

// parseUpdateTarget extracts the table from UPDATE [ONLY] t.
func parseUpdateTarget(q string) (table string, sets []string, ok bool) {
	rest, ok := cutKeyword(q, "update")
	if !ok {
		return "", nil, false
	}
	if r, ok := cutKeyword(rest, "only"); ok {
		rest = r
	}
	head, _ := splitHead(rest)
	table = unquoteIdent(head)
	if table == "" || !isBareIdent(table) {
		return "", nil, false
	}
	return table, nil, true
}

// parseDeleteTarget extracts the table from DELETE FROM [ONLY] t.
func parseDeleteTarget(q string) (table string, ok bool) {
	rest, ok := cutKeyword(q, "delete")
	if !ok {
		return "", false
	}
	rest, ok = cutKeyword(rest, "from")
	if !ok {
		return "", false
	}
	if r, ok := cutKeyword(rest, "only"); ok {
		rest = r
	}
	head, _ := splitHead(rest)
	table = unquoteIdent(head)
	if table == "" || !isBareIdent(table) {
		return "", false
	}
	return table, true
}

// valuesGroups returns the raw contents of each (...) group after the
// top-level VALUES keyword.
func valuesGroups(q string) []string {
	up := strings.ToUpper(q)
	idx := strings.Index(up, "VALUES")
	if idx < 0 {
		return nil
	}
	rest := q[idx+6:]
	var out []string
	i := 0
	for i < len(rest) {
		c := rest[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(rest, i, c)
		case c == '(':
			end := matchingParen(rest, i)
			if end < 0 {
				return out
			}
			out = append(out, rest[i+1:end])
			i = end + 1
		default:
			i++
		}
	}
	return out
}

func matchingParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '\'':
			i = skipQuoted(s, i, '\'')
			continue
		case '"':
			i = skipQuoted(s, i, '"')
			continue
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// paramNum parses "$n" (exactly, nothing else).
func paramNum(s string) (int, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '$' {
		return 0, fmt.Errorf("not a param")
	}
	for i := 1; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("not a param")
		}
	}
	n, err := strconv.Atoi(s[1:])
	if err != nil || n < 1 {
		return 0, fmt.Errorf("not a param")
	}
	return n, nil
}

// paramNumsInOrder lists $n in appearance order (outside strings).
func paramNumsInOrder(q string) []int {
	var out []int
	i := 0
	for i < len(q) {
		c := q[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(q, i, c)
		case c == '$':
			if end, ok := dollarTagEnd(q, i); ok {
				i = end
				continue
			}
			j := i + 1
			for j < len(q) && q[j] >= '0' && q[j] <= '9' {
				j++
			}
			if j > i+1 {
				if n, err := strconv.Atoi(q[i+1 : j]); err == nil && n >= 1 {
					out = append(out, n)
				}
			}
			i = j
		default:
			i++
		}
	}
	return out
}

// updateSetParams maps SET-assigned columns to their $n when the RHS
// is exactly a parameter: SET a = $1, b = $2.
func updateSetParams(q string) map[string]int {
	out := map[string]int{}
	up := strings.ToUpper(q)
	setIdx := strings.Index(up, " SET ")
	if setIdx < 0 {
		return out
	}
	span := q[setIdx+5:]
	// Cut at the top-level WHERE.
	depth := 0
	cut := len(span)
	for i := 0; i < len(span); i++ {
		switch span[i] {
		case '\'':
			i = skipQuoted(span, i, '\'')
		case '"':
			i = skipQuoted(span, i, '"')
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && isWordStart(span, i) && hasKeywordAt(span, i, "where") {
				cut = i
				i = len(span)
			}
		}
	}
	for _, item := range splitTopCommas(span[:cut]) {
		eq := strings.IndexByte(item, '=')
		if eq < 0 {
			continue
		}
		col := unquoteIdent(strings.TrimSpace(item[:eq]))
		rhs := strings.TrimSpace(item[eq+1:])
		// Skip `==`, `<=`, `>=`, `!=` on the LHS edge.
		if strings.HasSuffix(col, "<") || strings.HasSuffix(col, ">") || strings.HasSuffix(col, "!") {
			continue
		}
		if !isBareIdent(col) {
			continue
		}
		if n, err := paramNum(rhs); err == nil {
			out[col] = n
		}
	}
	return out
}

// whereComparisons maps columns compared directly to a parameter:
// `ident op $n` or `$n op ident`, where ident is a bare (optionally
// quoted/qualified) column that is not a function call.
func whereComparisons(q string) map[string]int {
	out := map[string]int{}
	i := 0
	for i < len(q) {
		c := q[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			// Quoted idents handled inline by scanIdent; skip string
			// literals only when they cannot start an operand. Both
			// cases are covered by trying an operand parse first.
			if col, n, ok, next := tryComparisonOperand(q, i); ok {
				out[col] = n
				i = next
			} else {
				i = skipQuoted(q, i, c)
			}
		case c == '$':
			if col, n, ok, next := tryParamComparison(q, i); ok {
				out[col] = n
				i = next
			} else {
				i++
			}
		default:
			if isWordChar(c) {
				if col, n, ok, next := tryComparisonOperand(q, i); ok {
					out[col] = n
					i = next
					continue
				}
			}
			i++
		}
	}
	return out
}

// tryComparisonOperand tries `operand op $n` at q[i] (operand ident or
// quoted ident). Returns the column, param number, and next index.
func tryComparisonOperand(q string, i int) (string, int, bool, int) {
	col, next := scanOperand(q, i)
	if col == "" {
		return "", 0, false, i
	}
	j := skipSpaces(q, next)
	opLen := comparisonOpLen(q, j)
	if opLen == 0 {
		return "", 0, false, i
	}
	j = skipSpaces(q, j+opLen)
	if j >= len(q) || q[j] != '$' {
		return "", 0, false, i
	}
	n, end, ok := scanParam(q, j)
	if !ok {
		return "", 0, false, i
	}
	return col, n, true, end
}

// tryParamComparison tries `$n op operand` at q[i] == '$'.
func tryParamComparison(q string, i int) (string, int, bool, int) {
	if end, ok := dollarTagEnd(q, i); ok {
		return "", 0, false, end
	}
	n, end, ok := scanParam(q, i)
	if !ok {
		return "", 0, false, i + 1
	}
	j := skipSpaces(q, end)
	opLen := comparisonOpLen(q, j)
	if opLen == 0 {
		return "", 0, false, end
	}
	col, next := scanOperand(q, skipSpaces(q, j+opLen))
	if col == "" {
		return "", 0, false, end
	}
	return col, n, true, next
}

// scanOperand scans a bare, quoted, or qualified column identifier,
// rejecting function calls (ident followed by `(`).
func scanOperand(q string, i int) (string, int) {
	i = skipSpaces(q, i)
	if i >= len(q) {
		return "", i
	}
	var name string
	if q[i] == '"' {
		end := skipQuoted(q, i, '"')
		name = q[i+1 : end-1]
		i = end
	} else {
		j := i
		for j < len(q) && (isWordChar(q[j]) || q[j] == '.') {
			j++
		}
		if j == i {
			return "", i
		}
		name = q[i:j]
		if k := strings.LastIndexByte(name, '.'); k >= 0 {
			name = name[k+1:]
		}
		i = j
	}
	if name == "" || !isBareIdent(name) {
		return "", i
	}
	if skipSpaces(q, i) < len(q) && q[skipSpaces(q, i)] == '(' {
		return "", i // function call, not a column
	}
	return name, i
}

func scanParam(q string, i int) (int, int, bool) {
	j := i + 1
	for j < len(q) && q[j] >= '0' && q[j] <= '9' {
		j++
	}
	if j == i+1 {
		return 0, i, false
	}
	n, err := strconv.Atoi(q[i+1 : j])
	if err != nil || n < 1 {
		return 0, i, false
	}
	return n, j, true
}

func skipSpaces(q string, i int) int {
	for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\r' || q[i] == '\n') {
		i++
	}
	return i
}

func comparisonOpLen(q string, i int) int {
	if i >= len(q) {
		return 0
	}
	two := ""
	if i+1 < len(q) {
		two = q[i : i+2]
	}
	switch two {
	case "<=", ">=", "<>", "!=":
		return 2
	}
	switch q[i] {
	case '=', '<', '>':
		return 1
	}
	return 0
}

// normalizeParam renders a pgtype-decoded value bindable by murmur.
func normalizeParam(v any) (any, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string, []byte, int64, float64, bool, time.Time:
		return v, nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int:
		return int64(t), nil
	case float32:
		return float64(t), nil
	case pgtype.Numeric:
		f, err := t.Float64Value()
		if err != nil {
			return nil, fmt.Errorf("numeric %v out of float64 range", t)
		}
		return f.Float64, nil
	default:
		return nil, fmt.Errorf("unsupported parameter type %T (use text format)", v)
	}
}
