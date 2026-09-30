package murmurd

import (
	"fmt"
	"strconv"
	"strings"
)

// This file is the SQLite-subset filter shared by both wire frontends.
// murmur's engine is SQLite: only SQLite-expressible statements can
// succeed. The MySQL side enforces this through the provider's narrow
// capability set (reads, single-row-routed writes, CREATE TABLE); the
// PostgreSQL side classifies raw statement text here. Both sides
// reject everything else with an explicit reason instead of a
// confusing engine error.

// StmtClass is the filter verdict for one statement.
type StmtClass int

const (
	// ClassRead passes to the engine read path (SELECT, WITH, VALUES, EXPLAIN).
	ClassRead StmtClass = iota
	// ClassWrite passes to the engine write path (INSERT, UPDATE, DELETE).
	ClassWrite
	// ClassCreateTable is an additive CREATE TABLE for the strict parser.
	ClassCreateTable
	// ClassTruncate rewrites to DELETE FROM (SQLite has no TRUNCATE).
	ClassTruncate
	// ClassTxnNoop is a transaction framing statement absorbed by the
	// session (the engine autocommits every statement).
	ClassTxnNoop
	// ClassSessionNoop is a session statement with no murmur meaning
	// (SET, RESET, DISCARD): accepted and ignored.
	ClassSessionNoop
	// ClassEmulated is answered by the frontend itself (version()).
	ClassEmulated
	// ClassReject is refused with FilterError.
	ClassReject
)

// FilterError explains why a statement is outside the SQLite subset.
type FilterError struct {
	Stmt   string
	Reason string
}

func (e *FilterError) Error() string {
	stmt := strings.TrimSpace(e.Stmt)
	if len(stmt) > 120 {
		stmt = stmt[:120] + "..."
	}
	return fmt.Sprintf("murmurd: only SQLite-expressible statements are supported: %s (in %q)", e.Reason, stmt)
}

// Classify returns the filter verdict for one statement. Multi-
// statement strings must be split first (see SplitStatements).
func Classify(q string) (StmtClass, *FilterError) {
	stmt := stripLeadingComments(strings.TrimSpace(q))
	stmt = strings.TrimSuffix(strings.TrimSpace(stmt), ";")
	if stmt == "" {
		return ClassReject, &FilterError{Stmt: q, Reason: "empty statement"}
	}
	head, rest := splitHead(stmt)
	switch strings.ToLower(head) {
	case "select", "values", "explain", "pragma", "table":
		if isEmulatedSelect(stmt) {
			return ClassEmulated, nil
		}
		return ClassRead, nil
	case "with":
		// A WITH whose outer statement writes must take the write
		// path (capture), never the read path: scan the top-level
		// verbs outside all parens. SQLite CTE bodies are
		// SELECT-only, so a top-level write verb is the outer op.
		for _, w := range topLevelVerbs(stmt) {
			if w == "insert" || w == "update" || w == "delete" {
				if hasReturning(stmt) {
					return ClassReject, &FilterError{Stmt: q, Reason: "RETURNING is not supported (writes return no rows)"}
				}
				return ClassWrite, nil
			}
		}
		return ClassRead, nil
	case "insert", "update", "delete":
		if hasReturning(stmt) {
			return ClassReject, &FilterError{Stmt: q, Reason: head + " ... RETURNING is not supported (writes return no rows)"}
		}
		return ClassWrite, nil
	case "create":
		return classifyCreate(q, rest)
	case "truncate":
		return ClassTruncate, nil
	case "begin", "start", "commit", "rollback", "end", "savepoint", "release":
		return ClassTxnNoop, nil
	case "set", "reset", "discard":
		return ClassSessionNoop, nil
	case "show", "copy", "listen", "notify", "vacuum", "analyze",
		"grant", "revoke", "prepare", "deallocate", "execute",
		"checkpoint", "reindex", "cluster":
		return ClassReject, &FilterError{Stmt: q, Reason: head + " has no SQLite equivalent in murmur"}
	case "drop", "alter":
		return ClassReject, &FilterError{Stmt: q, Reason: head + " is destructive: murmur schema changes are additive-only (CREATE TABLE, ADD COLUMN via migration)"}
	default:
		return ClassReject, &FilterError{Stmt: q, Reason: fmt.Sprintf("unrecognized statement %q", head)}
	}
}

func classifyCreate(q, rest string) (StmtClass, *FilterError) {
	head, _ := splitHead(rest)
	switch strings.ToLower(head) {
	case "table":
		return ClassCreateTable, nil
	case "unique":
		return ClassReject, &FilterError{Stmt: q, Reason: "secondary UNIQUE indexes are not supported (murmur replicates only the primary-key index)"}
	case "index":
		return ClassReject, &FilterError{Stmt: q, Reason: "explicit indexes are local-only in murmur and not managed over the wire (see LocalDDL)"}
	case "view", "trigger", "function", "procedure", "sequence",
		"schema", "database", "user", "role", "extension", "type":
		return ClassReject, &FilterError{Stmt: q, Reason: "CREATE " + strings.ToUpper(head) + " has no murmur equivalent"}
	default:
		if strings.EqualFold(head, "temporary") || strings.EqualFold(head, "temp") {
			return ClassReject, &FilterError{Stmt: q, Reason: "temporary tables are not supported (single shared materialization)"}
		}
		return ClassReject, &FilterError{Stmt: q, Reason: fmt.Sprintf("unrecognized %q", "CREATE "+head)}
	}
}

// splitHead splits the first whitespace-delimited word (lowercased by
// the caller as needed) from the rest.
func splitHead(s string) (head, rest string) {
	s = strings.TrimLeft(s, " \t\r\n")
	i := strings.IndexAny(s, " \t\r\n(")
	if i < 0 {
		return s, ""
	}
	if s[i] == '(' {
		return s[:i], s[i:]
	}
	return s[:i], strings.TrimLeft(s[i:], " \t\r\n")
}

// topLevelVerbs returns the lowercase words outside all parens,
// quotes, and dollar-quoted runs (used to find a WITH's outer verb).
func topLevelVerbs(s string) []string {
	var words []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			words = append(words, strings.ToLower(cur.String()))
			cur.Reset()
		}
	}
	depth := 0
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			flush()
			i = skipQuoted(s, i, c)
		case c == '$':
			if end, ok := dollarTagEnd(s, i); ok {
				flush()
				i = end
			} else {
				flush()
				i++
			}
		case c == '(':
			flush()
			depth++
			i++
		case c == ')':
			flush()
			if depth > 0 {
				depth--
			}
			i++
		case depth == 0 && isWordChar(c):
			cur.WriteByte(c)
			i++
		default:
			flush()
			i++
		}
	}
	flush()
	return words
}

// stripLeadingComments removes leading -- and /* */ comments for
// classification. The engine sees the original text.
func stripLeadingComments(s string) string {
	for {
		t := strings.TrimLeft(s, " \t\r\n")
		if strings.HasPrefix(t, "--") {
			if i := strings.IndexByte(t, '\n'); i >= 0 {
				s = t[i+1:]
				continue
			}
			return ""
		}
		if strings.HasPrefix(t, "/*") {
			if i := strings.Index(t, "*/"); i >= 0 {
				s = t[i+2:]
				continue
			}
			return s
		}
		return t
	}
}

// hasReturning reports whether the statement carries a RETURNING
// clause (string literals excluded).
func hasReturning(q string) bool {
	return hasWordOutsideStrings(q, "returning")
}

// isEmulatedSelect matches the frontend-emulated introspection
// selects: version() and current_database(), any case/spacing.
func isEmulatedSelect(q string) bool {
	t := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(q), ";"))
	t = strings.Join(strings.Fields(t), " ")
	return strings.EqualFold(t, "select version()") ||
		strings.EqualFold(t, "select current_database()")
}

// EmulatedName returns "version" or "current_database" for an
// emulated select, or "" otherwise.
func EmulatedName(q string) string {
	t := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(q), ";"))
	t = strings.Join(strings.Fields(t), " ")
	switch {
	case strings.EqualFold(t, "select version()"):
		return "version"
	case strings.EqualFold(t, "select current_database()"):
		return "current_database"
	default:
		return ""
	}
}

func hasWordOutsideStrings(q, word string) bool {
	words := splitWordsOutsideStrings(strings.ToLower(q))
	for _, w := range words {
		if w == word {
			return true
		}
	}
	return false
}

// splitWordsOutsideStrings splits on non-letter/digit/underscore,
// skipping '...', "...", `...`, and $tag$...$tag$ runs.
func splitWordsOutsideStrings(s string) []string {
	var words []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			words = append(words, cur.String())
			cur.Reset()
		}
	}
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			flush()
			i = skipQuoted(s, i, c)
		case c == '$':
			if end, ok := dollarTagEnd(s, i); ok {
				flush()
				i = end
			} else {
				flush()
				i++
			}
		case isWordChar(c):
			cur.WriteByte(c)
			i++
		default:
			flush()
			i++
		}
	}
	flush()
	return words
}

func isWordChar(c byte) bool {
	return c == '_' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9'
}

// skipQuoted skips a quoted run starting at the quote in s[i].
func skipQuoted(s string, i int, quote byte) int {
	i++
	for i < len(s) {
		if s[i] == quote {
			if quote == '\'' && i+1 < len(s) && s[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

// dollarTagEnd returns the index just past a $tag$...$tag$ run
// starting at s[i] == '$', or ok=false when s[i] is not a tag open.
func dollarTagEnd(s string, i int) (end int, ok bool) {
	j := i + 1
	for j < len(s) && (isWordChar(s[j])) {
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return 0, false
	}
	tag := s[i : j+1]
	k := strings.Index(s[j+1:], tag)
	if k < 0 {
		return len(s), true
	}
	return j + 1 + k + len(tag), true
}

// SplitStatements splits q on semicolons outside strings, quotes, and
// dollar-quoted runs. Empty fragments are dropped.
func SplitStatements(q string) []string {
	var out []string
	start := 0
	flush := func(end int) {
		if frag := strings.TrimSpace(q[start:end]); frag != "" {
			out = append(out, frag)
		}
		start = end + 1
	}
	i := 0
	for i < len(q) {
		c := q[i]
		switch {
		case c == ';':
			flush(i)
			i++
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(q, i, c)
		case c == '$':
			if end, ok := dollarTagEnd(q, i); ok {
				i = end
			} else {
				i++
			}
		default:
			i++
		}
	}
	flush(len(q))
	return out
}

// TranslateParams rewrites PostgreSQL $1..$N placeholders to SQLite
// ? placeholders and returns the parameter count. Placeholders must
// be sequential from 1; anything else is rejected. Dollar-quoted
// runs are left alone.
func TranslateParams(q string) (string, int, error) {
	var b strings.Builder
	maxSeen := 0
	seen := map[int]bool{}
	i := 0
	for i < len(q) {
		c := q[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			end := skipQuoted(q, i, c)
			b.WriteString(q[i:end])
			i = end
		case c == '$':
			if end, ok := dollarTagEnd(q, i); ok {
				b.WriteString(q[i:end])
				i = end
				continue
			}
			j := i + 1
			for j < len(q) && '0' <= q[j] && q[j] <= '9' {
				j++
			}
			if j == i+1 {
				return "", 0, fmt.Errorf("murmurd: bare $ in query (placeholders are $1..$N)")
			}
			n, err := strconv.Atoi(q[i+1 : j])
			if err != nil || n < 1 {
				return "", 0, fmt.Errorf("murmurd: invalid placeholder %q (placeholders are $1..$N)", q[i:j])
			}
			seen[n] = true
			if n > maxSeen {
				maxSeen = n
			}
			b.WriteByte('?')
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	for n := 1; n <= maxSeen; n++ {
		if !seen[n] {
			return "", 0, fmt.Errorf("murmurd: placeholder $%d missing (placeholders must run $1..$N)", n)
		}
	}
	return b.String(), maxSeen, nil
}

// CreateTableDef is a parsed strict-subset CREATE TABLE.
type CreateTableDef struct {
	Name       string
	IfNotExist bool
	Columns    []CreateColumnDef
}

// CreateColumnDef is one parsed column.
type CreateColumnDef struct {
	Name     string
	Type     string // murmur column type word: integer, real, text, blob
	PK       bool
	Nullable bool
}

// ParseCreateTable parses the strict DDL subset:
//
//	CREATE TABLE [IF NOT EXISTS] name (col TYPE [PRIMARY KEY] [NOT NULL | NULL], ...)
//
// Types accept SQLite, PostgreSQL, and common MySQL spellings and map
// to murmur's four column types. Everything else (constraints,
// defaults, indexes, foreign keys, composite keys, table options) is
// rejected with a reason: murmur models none of it.
func ParseCreateTable(q string) (*CreateTableDef, error) {
	t := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(q), ";"))
	rest, ok := cutKeyword(t, "create")
	if !ok {
		return nil, fmt.Errorf("murmurd: not a CREATE TABLE statement")
	}
	rest, ok = cutKeyword(rest, "table")
	if !ok {
		return nil, fmt.Errorf("murmurd: only CREATE TABLE is supported (got CREATE %.20s...)", rest)
	}
	def := &CreateTableDef{}
	if r, ok := cutKeyword(rest, "if"); ok {
		rest = r
		var ok2 bool
		if rest, ok2 = cutKeyword(rest, "not"); !ok2 {
			return nil, fmt.Errorf("murmurd: malformed IF NOT EXISTS")
		}
		if rest, ok2 = cutKeyword(rest, "exists"); !ok2 {
			return nil, fmt.Errorf("murmurd: malformed IF NOT EXISTS")
		}
		def.IfNotExist = true
	}
	open := strings.IndexByte(rest, '(')
	if open < 0 {
		return nil, fmt.Errorf("murmurd: CREATE TABLE needs a (column, ...) list")
	}
	def.Name = unquoteIdent(strings.TrimSpace(rest[:open]))
	if def.Name == "" || !isBareIdent(def.Name) {
		return nil, fmt.Errorf("murmurd: invalid table name %q", strings.TrimSpace(rest[:open]))
	}
	colsPart := strings.TrimSpace(rest[open+1:])
	if !strings.HasSuffix(colsPart, ")") {
		return nil, fmt.Errorf("murmurd: malformed column list (trailing table options are not supported)")
	}
	colsPart = colsPart[:len(colsPart)-1]
	for _, item := range splitTopCommas(colsPart) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		head, _ := splitHead(item)
		if isConstraintHead(head) {
			pk, err := parseTablePK(item)
			if err != nil {
				return nil, err
			}
			if err := def.markPK(pk); err != nil {
				return nil, err
			}
			continue
		}
		col, err := parseColumnDef(item)
		if err != nil {
			return nil, err
		}
		def.Columns = append(def.Columns, col)
	}
	if len(def.Columns) == 0 {
		return nil, fmt.Errorf("murmurd: CREATE TABLE needs at least one column")
	}
	pks := 0
	for _, c := range def.Columns {
		if c.PK {
			pks++
		}
	}
	if pks == 0 {
		return nil, fmt.Errorf("murmurd: CREATE TABLE needs exactly one PRIMARY KEY column (murmur keys are single-column)")
	}
	if pks > 1 {
		return nil, fmt.Errorf("murmurd: composite PRIMARY KEY is not supported (murmur keys are single-column)")
	}
	seen := map[string]bool{}
	for _, c := range def.Columns {
		l := strings.ToLower(c.Name)
		if seen[l] {
			return nil, fmt.Errorf("murmurd: duplicate column %q", c.Name)
		}
		seen[l] = true
	}
	return def, nil
}

func cutKeyword(s, kw string) (string, bool) {
	t := strings.TrimLeft(s, " \t\r\n")
	if len(t) < len(kw) || !strings.EqualFold(t[:len(kw)], kw) {
		return "", false
	}
	if len(t) > len(kw) {
		if c := t[len(kw)]; isWordChar(c) {
			return "", false
		}
	}
	return strings.TrimLeft(t[len(kw):], " \t\r\n"), true
}

func unquoteIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') ||
			(s[0] == '`' && s[len(s)-1] == '`') ||
			(s[0] == '[' && s[len(s)-1] == ']') {
			return s[1 : len(s)-1]
		}
	}
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		return unquoteIdent(s[i+1:])
	}
	return s
}

func isBareIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isWordChar(s[i]) {
			return false
		}
	}
	return true
}

// splitTopCommas splits on commas outside parens and quotes.
func splitTopCommas(s string) []string {
	var out []string
	depth := 0
	start := 0
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipQuoted(s, i, c)
			continue
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			out = append(out, s[start:i])
			start = i + 1
		}
		i++
	}
	out = append(out, s[start:])
	return out
}

func isConstraintHead(head string) bool {
	switch strings.ToLower(head) {
	case "primary", "unique", "constraint", "foreign", "check", "like":
		return true
	}
	return false
}

func parseTablePK(item string) (string, error) {
	rest, ok := cutKeyword(item, "primary")
	if !ok {
		return "", &FilterError{Stmt: item, Reason: "table constraints other than PRIMARY KEY are not supported"}
	}
	var ok2 bool
	if rest, ok2 = cutKeyword(rest, "key"); !ok2 {
		return "", &FilterError{Stmt: item, Reason: "malformed PRIMARY KEY constraint"}
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
		return "", &FilterError{Stmt: item, Reason: "malformed PRIMARY KEY constraint"}
	}
	inner := splitTopCommas(rest[1 : len(rest)-1])
	if len(inner) != 1 {
		return "", &FilterError{Stmt: item, Reason: "composite PRIMARY KEY is not supported (murmur keys are single-column)"}
	}
	return unquoteIdent(strings.TrimSpace(inner[0])), nil
}

func (d *CreateTableDef) markPK(name string) error {
	found := false
	for i := range d.Columns {
		if strings.EqualFold(d.Columns[i].Name, name) {
			if d.Columns[i].PK {
				return &FilterError{Stmt: name, Reason: "PRIMARY KEY declared twice"}
			}
			d.Columns[i].PK = true
			found = true
		}
	}
	if !found {
		return &FilterError{Stmt: name, Reason: fmt.Sprintf("PRIMARY KEY on unknown column %q (declare columns before the constraint)", name)}
	}
	return nil
}

func parseColumnDef(item string) (CreateColumnDef, error) {
	var col CreateColumnDef
	rest := strings.TrimSpace(item)
	head, tail := splitHead(rest)
	col.Name = unquoteIdent(head)
	if col.Name == "" || !isBareIdent(col.Name) {
		return col, &FilterError{Stmt: item, Reason: fmt.Sprintf("invalid column definition %q", item)}
	}
	rest = tail
	typeHead, tail := splitHead(rest)
	typeName := strings.ToLower(typeHead)
	// Fold size/precision suffixes: VARCHAR(32), CHAR(16), NUMERIC(10,2).
	if strings.HasPrefix(strings.TrimLeft(tail, " "), "(") {
		t := strings.TrimLeft(tail, " ")
		depth := 0
		i := 0
		for ; i < len(t); i++ {
			switch t[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					i++
					goto sized
				}
			}
		}
	sized:
		rest = strings.TrimSpace(t[i:])
	} else {
		rest = tail
	}
	mapped, err := mapDDLType(typeName)
	if err != nil {
		return col, &FilterError{Stmt: item, Reason: err.Error()}
	}
	col.Type = mapped
	col.Nullable = true
	for rest != "" {
		head, tail = splitHead(rest)
		switch strings.ToLower(head) {
		case "primary":
			var ok bool
			if tail, ok = cutKeyword(tail, "key"); !ok {
				return col, &FilterError{Stmt: item, Reason: "malformed PRIMARY KEY"}
			}
			col.PK = true
			col.Nullable = false
			rest = tail
		case "not":
			var ok bool
			if tail, ok = cutKeyword(tail, "null"); !ok {
				return col, &FilterError{Stmt: item, Reason: fmt.Sprintf("unsupported column option in %q", item)}
			}
			col.Nullable = false
			rest = tail
		case "null":
			col.Nullable = true
			rest = tail
		default:
			return col, &FilterError{Stmt: item, Reason: fmt.Sprintf("unsupported column option %q (murmur models no defaults, checks, references, or generation)", head)}
		}
	}
	return col, nil
}

// mapDDLType maps SQLite, PostgreSQL, and common MySQL type spellings
// to murmur's four column types.
func mapDDLType(name string) (string, error) {
	switch strings.ToLower(name) {
	case "integer", "int", "int2", "int4", "int8", "bigint", "smallint",
		"tinyint", "mediumint", "year":
		return "integer", nil
	case "real", "float", "float4", "float8", "double", "doubleprecision":
		return "real", nil
	case "text", "varchar", "char", "character", "citext", "clob", "string",
		"tinytext", "mediumtext", "longtext", "nvarchar", "nchar", "name":
		return "text", nil
	case "blob", "bytea", "binary", "varbinary", "tinyblob", "mediumblob",
		"longblob", "bit", "varbit":
		return "blob", nil
	case "serial", "bigserial", "smallserial":
		return "", fmt.Errorf("SERIAL is not supported (murmur forbids autoincrement; generate key values client-side)")
	case "numeric", "decimal", "money":
		return "", fmt.Errorf("type %s has no murmur equivalent (use integer, real, text, or blob)", name)
	case "boolean", "bool":
		return "", fmt.Errorf("type %s has no murmur equivalent (use integer 0/1)", name)
	case "date", "time", "timestamp", "timestamptz", "timetz", "interval",
		"datetime", "uuid", "json", "jsonb", "xml", "inet", "cidr", "macaddr",
		"array", "enum", "set", "point", "geometry":
		return "", fmt.Errorf("type %s has no murmur equivalent (use integer, real, text, or blob)", name)
	default:
		return "", fmt.Errorf("unknown column type %q (use integer, real, text, or blob)", name)
	}
}
