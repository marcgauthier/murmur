package sqlengine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/marcgauthier/murmur/schema"
)

// This file enforces section 6 at the engine boundary: user-supplied
// DDL (Schema.DDL, LocalDDL) is classified before it runs, and the
// resulting materialized schema is validated after it lands, so raw
// DDL cannot bypass the replicated-schema rules. All violations wrap
// schema.ErrUnsupportedSchema, matching declaration validation.

// checkReplicatedDDLStatement allows only the statements that may
// define replicated tables: CREATE TABLE and non-unique CREATE
// INDEX. Everything else belongs in LocalDDL, Migrate, or nowhere.
func checkReplicatedDDLStatement(s string) error {
	kind, obj, err := parseCreateHead(s)
	if err != nil {
		return err
	}
	switch obj {
	case "TABLE":
		if kind == "VIRTUAL" || kind == "TEMP" {
			return fmt.Errorf("sqlengine: replicated tables cannot be %s (use LocalDDL for local-only objects): %w", strings.ToLower(kind), schema.ErrUnsupportedSchema)
		}
		return nil
	case "INDEX":
		if kind == "UNIQUE" {
			return fmt.Errorf("sqlengine: secondary UNIQUE indexes are not allowed on replicated tables (murmur replicates only the primary-key index): %w", schema.ErrUnsupportedSchema)
		}
		return nil
	case "VIEW", "TRIGGER":
		return fmt.Errorf("sqlengine: %s belongs in LocalDDL, not replicated schema DDL: %w", strings.ToLower(obj), schema.ErrUnsupportedSchema)
	default:
		return fmt.Errorf("sqlengine: schema DDL must be CREATE TABLE or CREATE INDEX: %w", schema.ErrUnsupportedSchema)
	}
}

// checkLocalDDLStatement allows only CREATE statements for
// local-only objects. Uniqueness is decided by the resulting-state
// scan, which knows which tables replicate.
func checkLocalDDLStatement(s string) error {
	_, obj, err := parseCreateHead(s)
	if err != nil {
		return err
	}
	switch obj {
	case "TABLE", "INDEX", "VIEW", "TRIGGER", "VIRTUAL":
		return nil
	default:
		return fmt.Errorf("sqlengine: local DDL must be CREATE statements (table, view, index, trigger, virtual): %w", schema.ErrUnsupportedSchema)
	}
}

// parseCreateHead parses the head of a CREATE statement: the
// modifier kind ("" plain, or TEMP, VIRTUAL, UNIQUE) and the object
// (TABLE, INDEX, VIEW, TRIGGER, VIRTUAL). kind "" with nil error
// means "not a CREATE statement".
func parseCreateHead(s string) (kind, obj string, err error) {
	r := &wordReader{q: s}
	w, ok := r.next()
	if !ok || !strings.EqualFold(w, "CREATE") {
		return "", "", ddlVerbError(s)
	}
	w, ok = r.next()
	if !ok {
		return "", "", fmt.Errorf("sqlengine: truncated CREATE statement: %w", schema.ErrUnsupportedSchema)
	}
	up := strings.ToUpper(w)
	switch up {
	case "OR":
		// CREATE OR REPLACE VIEW only.
		w2, ok := r.next()
		if !ok || !strings.EqualFold(w2, "REPLACE") {
			return "", "", fmt.Errorf("sqlengine: malformed CREATE statement: %w", schema.ErrUnsupportedSchema)
		}
		w3, ok := r.next()
		if !ok {
			return "", "", fmt.Errorf("sqlengine: truncated CREATE statement: %w", schema.ErrUnsupportedSchema)
		}
		return "", strings.ToUpper(w3), nil
	case "TEMP", "TEMPORARY":
		kind = "TEMP"
	case "VIRTUAL":
		kind = "VIRTUAL"
	case "UNIQUE":
		kind = "UNIQUE"
	default:
		return "", up, nil
	}
	w2, ok := r.next()
	if !ok {
		return "", "", fmt.Errorf("sqlengine: truncated CREATE statement: %w", schema.ErrUnsupportedSchema)
	}
	return kind, strings.ToUpper(w2), nil
}

// ddlVerbError reports a non-CREATE statement in a DDL list with a
// pointer to the right mechanism.
func ddlVerbError(s string) error {
	switch firstKeyword(s) {
	case "DROP", "ALTER":
		return fmt.Errorf("sqlengine: schema changes in DDL lists are not allowed (use Migrate): %w", schema.ErrUnsupportedSchema)
	case "PRAGMA", "ATTACH", "DETACH", "VACUUM":
		return fmt.Errorf("sqlengine: %s is not allowed in DDL lists: %w", strings.ToLower(firstKeyword(s)), schema.ErrUnsupportedSchema)
	default:
		return fmt.Errorf("sqlengine: only CREATE statements are allowed in DDL lists: %w", schema.ErrUnsupportedSchema)
	}
}

// wordReader yields bare words, skipping whitespace, comments, and
// string literals; quoted identifiers yield without quotes.
type wordReader struct {
	q string
	i int
}

func (r *wordReader) next() (string, bool) {
	s := r.q
	i := r.i
	for {
		for i < len(s) && isSpaceByte(s[i]) {
			i++
		}
		if strings.HasPrefix(s[i:], "--") {
			if j := strings.IndexByte(s[i:], '\n'); j >= 0 {
				i += j + 1
				continue
			}
			r.i = len(s)
			return "", false
		}
		if strings.HasPrefix(s[i:], "/*") {
			if j := strings.Index(s[i:], "*/"); j >= 0 {
				i += j + 2
				continue
			}
			r.i = len(s)
			return "", false
		}
		break
	}
	if i >= len(s) {
		r.i = i
		return "", false
	}
	c := s[i]
	switch c {
	case '\'', '"', '`':
		j := i + 1
		for j < len(s) {
			if s[j] == c {
				if c == '\'' && j+1 < len(s) && s[j+1] == '\'' {
					j += 2
					continue
				}
				r.i = j + 1
				return s[i+1 : j], true
			}
			j++
		}
		r.i = len(s)
		return "", false
	case '[':
		if j := strings.IndexByte(s[i:], ']'); j >= 0 {
			r.i = i + j + 1
			return s[i+1 : i+j], true
		}
		r.i = len(s)
		return "", false
	default:
		if !isKeywordByte(c) {
			r.i = i + 1
			return string(c), true
		}
		j := i
		for j < len(s) && (isKeywordByte(s[j]) || (s[j] >= '0' && s[j] <= '9')) {
			j++
		}
		r.i = j
		return s[i:j], true
	}
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// validateResultingSchema enforces the replicated-schema rules on the
// materialized database: every registry table must be a plain rowid
// table whose shape matches its declaration, with no secondary
// unique indexes and no unsupported checks. Local-only objects are
// unconstrained except that unique indexes may not attach to
// replicated tables.
func (e *Engine) validateResultingSchema(ctx context.Context) error {
	for _, t := range e.reg.Tables {
		if err := e.validateResultingTable(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) validateResultingTable(ctx context.Context, t *schema.TableSchema) error {
	var objType string
	var tableSQL sql.NullString
	err := e.write.QueryRowContext(ctx,
		`SELECT type, sql FROM sqlite_master WHERE name = ?`, t.Name).Scan(&objType, &tableSQL)
	if err != nil {
		return fmt.Errorf("sqlengine: table %s missing from materialization: %w", t.Name, err)
	}
	if objType != "table" {
		return fmt.Errorf("sqlengine: replicated %q must be a table, got %s: %w", t.Name, objType, schema.ErrUnsupportedSchema)
	}
	ddl := ""
	if tableSQL.Valid {
		ddl = tableSQL.String
	}
	cols := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		cols[strings.ToLower(c.Name)] = true
	}
	if err := checkTableKeywords(t.Name, ddl); err != nil {
		return err
	}
	if err := checkTableChecks(t.Name, ddl, cols); err != nil {
		return err
	}
	if err := e.checkNoSecondaryUnique(ctx, t); err != nil {
		return err
	}
	return e.checkTableShape(ctx, t)
}

// checkTableKeywords rejects table options and constraints Murmur
// cannot replicate. UNIQUE is a reserved word so a bare occurrence
// is always a constraint; AUTOINCREMENT outside the name position is
// always a constraint; STRICT and WITHOUT ROWID only count as table
// options past the column-def list.
func checkTableKeywords(table, ddl string) error {
	toks := tokenizeDDL(ddl)
	seenCreate := false
	for _, tok := range toks {
		if tok.quoted {
			continue
		}
		up := strings.ToUpper(tok.text)
		if !seenCreate {
			if up == "CREATE" {
				seenCreate = true
			}
			continue
		}
		if up == "TABLE" || tok.text == "(" {
			break
		}
		if up == "VIRTUAL" {
			return fmt.Errorf("sqlengine: table %q must not be virtual (replicated tables are plain rowid tables): %w", table, schema.ErrUnsupportedSchema)
		}
	}
	depth := 0
	seenBody := false
	for i, tok := range toks {
		if tok.text == "(" {
			depth++
			seenBody = true
			continue
		}
		if tok.text == ")" {
			depth--
			continue
		}
		if tok.quoted || depth < 0 {
			continue
		}
		up := strings.ToUpper(tok.text)
		switch {
		case up == "UNIQUE":
			return fmt.Errorf("sqlengine: table %q: UNIQUE constraints are not allowed on replicated tables (murmur replicates only the primary-key index): %w", table, schema.ErrUnsupportedSchema)
		case up == "AUTOINCREMENT" && !isSegmentFirst(toks, i):
			return fmt.Errorf("sqlengine: table %q: AUTOINCREMENT is not allowed (primary keys are application-generated): %w", table, schema.ErrUnsupportedSchema)
		case seenBody && depth == 0 && up == "STRICT":
			return fmt.Errorf("sqlengine: table %q: STRICT tables are not supported (use plain rowid tables): %w", table, schema.ErrUnsupportedSchema)
		case seenBody && depth == 0 && up == "WITHOUT" && nextWord(toks, i) == "ROWID":
			return fmt.Errorf("sqlengine: table %q: WITHOUT ROWID tables are not supported (use plain rowid tables): %w", table, schema.ErrUnsupportedSchema)
		}
	}
	return nil
}

// isSegmentFirst reports whether toks[i] starts a top-level
// comma-separated segment (a column name or table-constraint
// keyword), used to exempt pathological column names from
// constraint-word matches.
func isSegmentFirst(toks []ddlToken, i int) bool {
	depth := 0
	for j := i - 1; j >= 0; j-- {
		t := toks[j]
		switch {
		case t.text == ")":
			depth++
		case t.text == "(":
			if depth == 0 {
				return true
			}
			depth--
		case t.text == "," && depth == 0:
			return true
		case depth == 0 && isWordToken(t):
			return false
		}
	}
	return false
}

func isWordToken(tok ddlToken) bool {
	if tok.quoted || tok.text == "" {
		return tok.quoted
	}
	return isKeywordByte(tok.text[0])
}

func nextWord(toks []ddlToken, i int) string {
	if i+1 < len(toks) && !toks[i+1].quoted {
		return strings.ToUpper(toks[i+1].text)
	}
	return ""
}

// checkTableChecks enforces the single-column CHECK rule: table-level
// checks are rejected outright, and each column-level CHECK may
// reference only its own column.
func checkTableChecks(table, ddl string, cols map[string]bool) error {
	body, ok := tableBody(ddl)
	if !ok {
		return nil // let SQLite report the malformed DDL
	}
	for _, seg := range splitSegments(body) {
		if len(seg) == 0 {
			continue
		}
		first := ""
		if !seg[0].quoted {
			first = strings.ToUpper(seg[0].text)
		}
		switch first {
		case "CONSTRAINT", "CHECK", "PRIMARY", "FOREIGN", "UNIQUE":
			for _, tok := range seg {
				if !tok.quoted && strings.EqualFold(tok.text, "CHECK") {
					return fmt.Errorf("sqlengine: table %q: table-level CHECK constraints are not allowed (declare single-column checks inline on the column): %w", table, schema.ErrUnsupportedSchema)
				}
			}
			continue
		}
		declared := seg[0].text
		for i, tok := range seg {
			if tok.quoted || !strings.EqualFold(tok.text, "CHECK") {
				continue
			}
			refs := checkRefs(seg, i, cols)
			for ref := range refs {
				if !strings.EqualFold(ref, declared) {
					return fmt.Errorf("sqlengine: table %q: CHECK on column %q references column %q (checks must reference only their own column): %w", table, declared, ref, schema.ErrUnsupportedSchema)
				}
			}
		}
	}
	return nil
}

// checkRefs collects the column names referenced by the CHECK
// expression starting at seg[i].
func checkRefs(seg []ddlToken, i int, cols map[string]bool) map[string]bool {
	refs := map[string]bool{}
	j := i + 1
	if j >= len(seg) || seg[j].text != "(" {
		return refs
	}
	depth := 0
	for ; j < len(seg); j++ {
		tok := seg[j]
		switch tok.text {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return refs
			}
		default:
			if depth > 0 && cols[strings.ToLower(tok.text)] {
				refs[tok.text] = true
			}
		}
	}
	return refs
}

// checkNoSecondaryUnique rejects unique indexes attached to a
// replicated table, which catches column, table, and standalone
// UNIQUE uniformly. Only the unique flag is decisive: UNIQUE
// constraints report origin "u" but a standalone CREATE UNIQUE
// INDEX reports origin "c", so any unique non-PK index is rejected
// and the primary-key autoindex ("pk") is the only one allowed.
func (e *Engine) checkNoSecondaryUnique(ctx context.Context, t *schema.TableSchema) error {
	rows, err := e.write.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(%s)", quoteIdent(t.Name)))
	if err != nil {
		return fmt.Errorf("sqlengine: index_list %s: %w", t.Name, err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name, origin string
		var unique int
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return err
		}
		if unique != 0 && origin != "pk" {
			return fmt.Errorf("sqlengine: table %q: secondary UNIQUE index %q is not allowed (murmur replicates only the primary-key index): %w", t.Name, name, schema.ErrUnsupportedSchema)
		}
	}
	return rows.Err()
}

// checkTableShape verifies the materialized table matches its
// declaration: the registry PK column is the sole PRIMARY KEY and
// NOT NULL, nullability matches on every column, and every decltype
// maps to the declared Murmur type.
func (e *Engine) checkTableShape(ctx context.Context, t *schema.TableSchema) error {
	rows, err := e.write.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(t.Name)))
	if err != nil {
		return fmt.Errorf("sqlengine: table_info %s: %w", t.Name, err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	pkName := ""
	if pk := t.ColumnByID(t.PK); pk != nil {
		pkName = pk.Name
	}
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		seen[strings.ToLower(name)] = true
		var want *schema.ColumnSchema
		for i := range t.Columns {
			if strings.EqualFold(t.Columns[i].Name, name) {
				want = &t.Columns[i]
				break
			}
		}
		if want == nil {
			continue // ordinal validation reports unknown columns
		}
		isPK := strings.EqualFold(name, pkName)
		if isPK && pk == 0 {
			return fmt.Errorf("sqlengine: table %q primary key %q is not declared PRIMARY KEY in DDL: %w", t.Name, name, schema.ErrUnsupportedSchema)
		}
		if !isPK && pk > 0 {
			return fmt.Errorf("sqlengine: table %q column %q is an extra PRIMARY KEY in DDL (murmur keys are single-column): %w", t.Name, name, schema.ErrUnsupportedSchema)
		}
		if wantNotNull := !want.Nullable; (notnull == 1) != wantNotNull {
			return fmt.Errorf("sqlengine: table %q column %q nullability does not match its declaration: %w", t.Name, name, schema.ErrUnsupportedSchema)
		}
		got, ok := murmurTypeForDecltype(ctype)
		if !ok || got != want.Type {
			return fmt.Errorf("sqlengine: table %q column %q decltype %q does not map to declared type %s: %w", t.Name, name, ctype, want.Type, schema.ErrUnsupportedSchema)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range t.Columns {
		if !seen[strings.ToLower(c.Name)] {
			return fmt.Errorf("sqlengine: table %q column %q missing from materialization: %w", t.Name, c.Name, schema.ErrUnsupportedSchema)
		}
	}
	return nil
}

// murmurTypeForDecltype maps a SQLite decltype to a Murmur type
// following SQLite's own affinity rules. NUMERIC and exotic
// affinities have no Murmur equivalent and are rejected.
func murmurTypeForDecltype(decl string) (schema.ColumnType, bool) {
	u := strings.ToUpper(decl)
	switch {
	case strings.Contains(u, "INT"):
		return schema.ColInteger, true
	case strings.Contains(u, "CHAR") || strings.Contains(u, "CLOB") || strings.Contains(u, "TEXT"):
		return schema.ColText, true
	case strings.Contains(u, "BLOB") || strings.TrimSpace(u) == "":
		return schema.ColBlob, true
	case strings.Contains(u, "REAL") || strings.Contains(u, "FLOA") || strings.Contains(u, "DOUB"):
		return schema.ColReal, true
	default:
		return 0, false
	}
}

// ddlToken is one token of table DDL: words, quoted identifiers
// (quotes stripped, quoted set), or single punctuation bytes. String
// literals and comments are dropped.
type ddlToken struct {
	text   string
	quoted bool
}

// tokenizeDDL tokenizes CREATE TABLE text for validation.
func tokenizeDDL(s string) []ddlToken {
	const (
		stNormal = iota
		stString
		stQuoted
		stBacktick
		stBracket
		stLineComment
		stBlockComment
	)
	var out []ddlToken
	st := stNormal
	i := 0
	for i < len(s) {
		c := s[i]
		switch st {
		case stNormal:
			switch {
			case c == '\'':
				st = stString
				i++
			case c == '"':
				j := i + 1
				for j < len(s) && s[j] != '"' {
					j++
				}
				out = append(out, ddlToken{text: s[i+1 : min(j, len(s))], quoted: true})
				if j < len(s) {
					j++
				}
				i = j
			case c == '`':
				j := i + 1
				for j < len(s) && s[j] != '`' {
					j++
				}
				out = append(out, ddlToken{text: s[i+1 : min(j, len(s))], quoted: true})
				if j < len(s) {
					j++
				}
				i = j
			case c == '[':
				j := i + 1
				for j < len(s) && s[j] != ']' {
					j++
				}
				out = append(out, ddlToken{text: s[i+1 : min(j, len(s))], quoted: true})
				if j < len(s) {
					j++
				}
				i = j
			case c == '-' && i+1 < len(s) && s[i+1] == '-':
				st = stLineComment
				i += 2
			case c == '/' && i+1 < len(s) && s[i+1] == '*':
				st = stBlockComment
				i += 2
			case isKeywordByte(c):
				j := i
				for j < len(s) && (isKeywordByte(s[j]) || (s[j] >= '0' && s[j] <= '9')) {
					j++
				}
				out = append(out, ddlToken{text: s[i:j]})
				i = j
			case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' || c == ';':
				i++
			default:
				out = append(out, ddlToken{text: string(c)})
				i++
			}
		case stString:
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i += 2
				} else {
					st = stNormal
					i++
				}
			} else {
				i++
			}
		case stLineComment:
			if c == '\n' {
				st = stNormal
			}
			i++
		case stBlockComment:
			if c == '*' && i+1 < len(s) && s[i+1] == '/' {
				st = stNormal
				i += 2
			} else {
				i++
			}
		default:
			i++
		}
	}
	return out
}

// tableBody returns the tokens of the top-level column-def list.
func tableBody(ddl string) ([]ddlToken, bool) {
	toks := tokenizeDDL(ddl)
	depth := 0
	start := -1
	for i, tok := range toks {
		if tok.quoted {
			continue
		}
		switch tok.text {
		case "(":
			if depth == 0 {
				start = i + 1
			}
			depth++
		case ")":
			depth--
			if depth == 0 && start >= 0 {
				return toks[start:i], true
			}
		}
	}
	return nil, false
}

// splitSegments splits top-level comma-separated definition segments.
func splitSegments(body []ddlToken) [][]ddlToken {
	var out [][]ddlToken
	depth := 0
	start := 0
	flush := func(end int) {
		if end > start {
			out = append(out, body[start:end])
		}
	}
	for i, tok := range body {
		if tok.quoted {
			continue
		}
		switch tok.text {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 0 {
				flush(i)
				start = i + 1
			}
		}
	}
	flush(len(body))
	return out
}
