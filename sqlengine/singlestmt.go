package sqlengine

import (
	"errors"
	"strings"
)

// ErrMultiStatement is returned when one query string carries more than one
// SQL statement. The SQLite backends disagree about stacked statements (one
// driver runs only the first, the other runs them all), so the engine
// rejects them outright: a single trailing semicolon is allowed, anything
// after it is not. This keeps the API's one-query-one-statement contract
// identical on every backend.
var ErrMultiStatement = errors.New("sqlengine: multiple statements in one query are not allowed")

// checkSingleStatement rejects query strings that carry a second statement
// after a semicolon. Semicolons inside string literals, quoted identifiers,
// and comments do not split statements; a lone trailing semicolon (plus
// optional whitespace/comments) is accepted. Unterminated literals or
// comments defer to the SQLite parser, which reports the syntax error.
func checkSingleStatement(query string) error {
	split := splitOffset(query)
	if split < 0 {
		return nil
	}
	if onlyWhitespaceAndComments(query[split+1:]) {
		return nil
	}
	return ErrMultiStatement
}

// splitOffset returns the offset of the first semicolon outside any string
// literal, quoted identifier, or comment, or -1 when there is none.
func splitOffset(s string) int {
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
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch st {
		case stNormal:
			switch {
			case c == ';':
				return i
			case c == '\'':
				st = stString
			case c == '"':
				st = stQuoted
			case c == '`':
				st = stBacktick
			case c == '[':
				st = stBracket
			case c == '-' && i+1 < len(s) && s[i+1] == '-':
				st = stLineComment
				i++
			case c == '/' && i+1 < len(s) && s[i+1] == '*':
				st = stBlockComment
				i++
			}
		case stString:
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
				} else {
					st = stNormal
				}
			}
		case stQuoted:
			if c == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
				} else {
					st = stNormal
				}
			}
		case stBacktick:
			if c == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
				} else {
					st = stNormal
				}
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
			if c == '*' && i+1 < len(s) && s[i+1] == '/' {
				st = stNormal
				i++
			}
		}
	}
	return -1
}

// onlyWhitespaceAndComments reports whether s holds nothing but whitespace
// and SQL comments (used to accept a lone trailing semicolon).
func onlyWhitespaceAndComments(s string) bool {
	for i := 0; i < len(s); {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' {
			i++
			continue
		}
		if c == '-' && i+1 < len(s) && s[i+1] == '-' {
			j := strings.IndexByte(s[i:], '\n')
			if j < 0 {
				return true
			}
			i += j + 1
			continue
		}
		if c == '/' && i+1 < len(s) && s[i+1] == '*' {
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return true
			}
			i += end + 4
			continue
		}
		return false
	}
	return true
}
