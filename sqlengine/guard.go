package sqlengine

import (
	"errors"
	"fmt"
	"strings"
)

// ErrStatementNotAllowed is returned when SQL text outside the
// engine's permitted surface reaches a public entry point. Messages
// stay marker-free (no echoed SQL, no source positions) so they are
// safe to surface through service APIs.
var ErrStatementNotAllowed = errors.New("sqlengine: statement not allowed")

// firstKeyword returns the uppercased first keyword of q, skipping
// whitespace and SQL comments. It returns "" when no keyword is
// present; callers defer unrecognized text to the SQLite parser.
func firstKeyword(q string) string {
	s := q
	for {
		s = strings.TrimLeft(s, " \t\n\r\f\v")
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s, "*/"); i >= 0 {
				s = s[i+2:]
				continue
			}
			return ""
		}
		break
	}
	i := 0
	for i < len(s) && isKeywordByte(s[i]) {
		i++
	}
	return strings.ToUpper(s[:i])
}

func isKeywordByte(c byte) bool {
	return c == '_' || c == '$' ||
		(c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

// checkExecAllowed rejects statements that must never run through
// the write path: schema changes (use Migrate), database attach,
// pragmas, and transaction control (use the Tx API). Writes, reads,
// and analysis statements pass through.
func checkExecAllowed(query string) error {
	switch firstKeyword(query) {
	case "CREATE", "DROP", "ALTER":
		return fmt.Errorf("%w: schema changes are not allowed through Exec (use Migrate)", ErrStatementNotAllowed)
	case "ATTACH", "DETACH":
		return fmt.Errorf("%w: database attach is not allowed (single-database engine)", ErrStatementNotAllowed)
	case "PRAGMA":
		return fmt.Errorf("%w: PRAGMA is not allowed through Exec (use Query for read-only introspection)", ErrStatementNotAllowed)
	case "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE":
		return fmt.Errorf("%w: transaction control is not allowed in SQL text (use the Tx API)", ErrStatementNotAllowed)
	case "VACUUM":
		return fmt.Errorf("%w: VACUUM is not allowed (in-memory engine)", ErrStatementNotAllowed)
	default:
		return nil
	}
}

// checkPoolQueryAllowed gates standalone reads, which run on pool
// connections without capture: anything that could write or change
// state is rejected so it can never diverge silently. WITH reads
// stay allowed; WITH writes are detected by verb scan.
func checkPoolQueryAllowed(query string) error {
	switch kw := firstKeyword(query); kw {
	case "SELECT", "EXPLAIN", "VALUES", "TABLE":
		return nil
	case "PRAGMA":
		if pragmaAssigns(query) {
			return fmt.Errorf("%w: PRAGMA assignments are not allowed (read-only introspection only)", ErrStatementNotAllowed)
		}
		return nil
	case "WITH":
		if containsWriteVerb(query) {
			return fmt.Errorf("%w: writes are not allowed through Query (use Exec or a transaction)", ErrStatementNotAllowed)
		}
		return nil
	case "INSERT", "UPDATE", "DELETE", "REPLACE":
		return fmt.Errorf("%w: writes are not allowed through Query (use Exec or a transaction)", ErrStatementNotAllowed)
	case "":
		return nil
	default:
		// Deny-by-default would break future read forms; SQLite
		// reports genuine syntax errors itself. The dangerous verbs
		// are enumerated explicitly below.
		switch kw {
		case "CREATE", "DROP", "ALTER", "ATTACH", "DETACH",
			"BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE",
			"VACUUM":
			return fmt.Errorf("%w: %s is not allowed through Query", ErrStatementNotAllowed, kw)
		default:
			return nil
		}
	}
}

// checkTxQueryAllowed gates in-transaction reads, which run on the
// write connection with capture: writes are fine (they replicate on
// commit), but schema changes, attach, PRAGMA assignment, and
// transaction control stay rejected.
func checkTxQueryAllowed(query string) error {
	switch kw := firstKeyword(query); kw {
	case "CREATE", "DROP", "ALTER":
		return fmt.Errorf("%w: schema changes are not allowed through Query (use Migrate)", ErrStatementNotAllowed)
	case "ATTACH", "DETACH":
		return fmt.Errorf("%w: database attach is not allowed (single-database engine)", ErrStatementNotAllowed)
	case "PRAGMA":
		if pragmaAssigns(query) {
			return fmt.Errorf("%w: PRAGMA assignments are not allowed (read-only introspection only)", ErrStatementNotAllowed)
		}
		return nil
	case "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE":
		return fmt.Errorf("%w: transaction control is not allowed in SQL text (use the Tx API)", ErrStatementNotAllowed)
	case "VACUUM":
		return fmt.Errorf("%w: VACUUM is not allowed (in-memory engine)", ErrStatementNotAllowed)
	default:
		return nil
	}
}

// pragmaAssigns reports whether a PRAGMA statement assigns a value
// (an equals sign outside any literal or quoted identifier).
func pragmaAssigns(q string) bool {
	const (
		stNormal = iota
		stString
		stQuoted
		stBacktick
		stBracket
		stLineComment
		stBlockComment
	)
	st := stNormal
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch st {
		case stNormal:
			switch {
			case c == '=':
				return true
			case c == '\'':
				st = stString
			case c == '"':
				st = stQuoted
			case c == '`':
				st = stBacktick
			case c == '[':
				st = stBracket
			case c == '-' && i+1 < len(q) && q[i+1] == '-':
				st = stLineComment
			case c == '/' && i+1 < len(q) && q[i+1] == '*':
				st = stBlockComment
				i++
			}
		case stString:
			if c == '\'' {
				if i+1 < len(q) && q[i+1] == '\'' {
					i++
				} else {
					st = stNormal
				}
			}
		case stQuoted:
			if c == '"' {
				st = stNormal
			}
		case stBacktick:
			if c == '`' {
				st = stNormal
			}
		case stBracket:
			if c == ']' {
				st = stNormal
			}
		case stLineComment:
			if c == '\n' {
				st = stNormal
			}
		case stBlockComment:
			if c == '*' && i+1 < len(q) && q[i+1] == '/' {
				st = stNormal
				i++
			}
		}
	}
	return false
}

// containsWriteVerb reports whether bare INSERT/UPDATE/DELETE (or a
// non-function REPLACE) appears outside any literal, quoted
// identifier, or comment. SQLite subqueries cannot contain writes,
// so any occurrence means the statement writes.
func containsWriteVerb(q string) bool {
	for _, tok := range scanWords(q) {
		switch strings.ToUpper(tok.word) {
		case "INSERT", "UPDATE", "DELETE":
			return true
		case "REPLACE":
			// replace(...) is the string function; anything else is
			// the REPLACE write.
			if tok.follow != '(' {
				return true
			}
		}
	}
	return false
}

type scannedWord struct {
	word   string
	follow byte // first non-blank byte after the word (0 when none)
}

// scanWords extracts bare words outside literals, quoted
// identifiers, and comments.
func scanWords(q string) []scannedWord {
	const (
		stNormal = iota
		stString
		stQuoted
		stBacktick
		stBracket
		stLineComment
		stBlockComment
	)
	var out []scannedWord
	st := stNormal
	i := 0
	for i < len(q) {
		c := q[i]
		switch st {
		case stNormal:
			switch {
			case c == '\'':
				st = stString
				i++
			case c == '"':
				st = stQuoted
				i++
			case c == '`':
				st = stBacktick
				i++
			case c == '[':
				st = stBracket
				i++
			case c == '-' && i+1 < len(q) && q[i+1] == '-':
				st = stLineComment
				i += 2
			case c == '/' && i+1 < len(q) && q[i+1] == '*':
				st = stBlockComment
				i += 2
			case isKeywordByte(c):
				j := i
				for j < len(q) && (isKeywordByte(q[j]) || (q[j] >= '0' && q[j] <= '9')) {
					j++
				}
				k := j
				for k < len(q) && (q[k] == ' ' || q[k] == '\t' || q[k] == '\n' || q[k] == '\r') {
					k++
				}
				var follow byte
				if k < len(q) {
					follow = q[k]
				}
				out = append(out, scannedWord{word: q[i:j], follow: follow})
				i = j
			default:
				i++
			}
		case stString:
			if c == '\'' {
				if i+1 < len(q) && q[i+1] == '\'' {
					i += 2
				} else {
					st = stNormal
					i++
				}
			} else {
				i++
			}
		case stQuoted:
			if c == '"' {
				st = stNormal
			}
			i++
		case stBacktick:
			if c == '`' {
				st = stNormal
			}
			i++
		case stBracket:
			if c == ']' {
				st = stNormal
			}
			i++
		case stLineComment:
			if c == '\n' {
				st = stNormal
			}
			i++
		case stBlockComment:
			if c == '*' && i+1 < len(q) && q[i+1] == '/' {
				st = stNormal
				i += 2
			} else {
				i++
			}
		}
	}
	return out
}
