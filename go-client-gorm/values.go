package murmurgorm

import (
	"database/sql/driver"
	"time"

	murmur "github.com/marcgauthier/murmur/go-client"
)

// timestampWriteFormat is the TEXT representation used for time.Time arguments.
// It matches the format family emitted by common SQLite drivers (space
// separator, nanosecond fraction, numeric zone), so values stay readable and
// comparable with stock SQLite tooling.
const timestampWriteFormat = "2006-01-02 15:04:05.999999999Z07:00"

// timestampReadFormats are the TEXT layouts recognized as timestamps on read.
// database/sql cannot convert strings to time.Time, and Murmur's row protocol
// carries no declared column types, so parsing is purely format-driven: any
// TEXT value that fully matches one of these layouts is returned as time.Time.
// This makes idiomatic GORM timestamp columns (CreatedAt/UpdatedAt, "datetime"
// DDL) work. A plain TEXT value that happens to match one of these layouts
// will also come back as time.Time; scanning that into a *string still works
// (database/sql formats it as RFC3339Nano) but is not byte-identical.
var timestampReadFormats = []string{
	"2006-01-02 15:04:05.999999999Z07:00",
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

// formatTimestamp renders a time.Time for SQLite TEXT storage.
func formatTimestamp(t time.Time) string {
	return t.Format(timestampWriteFormat)
}

// parseTimestamp reports whether s is a canonical SQLite datetime and, if so,
// its parsed value. The cheap shape precheck keeps non-date strings fast.
func parseTimestamp(s string) (time.Time, bool) {
	if len(s) < len("2006-01-02") || len(s) > len("2006-01-02 15:04:05.999999999Z07:00") {
		return time.Time{}, false
	}
	if s[4] != '-' || s[7] != '-' {
		return time.Time{}, false
	}
	for _, layout := range timestampReadFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// toDriverValue converts one Murmur cell to a database/sql driver value.
// Blob bytes are cloned so the caller's buffer stays immutable.
func toDriverValue(v murmur.Value) driver.Value {
	switch v.Kind {
	case murmur.NullKind:
		return nil
	case murmur.IntegerKind:
		return v.Integer
	case murmur.RealKind:
		return v.Real
	case murmur.BlobKind:
		return append([]byte(nil), v.Blob...)
	case murmur.TextKind:
		if t, ok := parseTimestamp(v.Text); ok {
			return t
		}
		return v.Text
	}
	return nil
}

// namedArgsToPositional flattens driver.NamedValue args to positional values.
// The query text keeps SQLite "?" placeholders, which Murmur binds natively.
func namedArgsToPositional(args []driver.NamedValue) ([]any, error) {
	out := make([]any, len(args))
	for i, arg := range args {
		v, err := convertArg(arg.Value)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// convertArg maps one driver argument to a Murmur bind value. time.Time has no
// native SQLite binding, so it travels as canonical TEXT (see formatTimestamp).
func convertArg(v driver.Value) (any, error) {
	switch t := v.(type) {
	case nil, int64, float64, bool, string, []byte:
		return v, nil
	case time.Time:
		return formatTimestamp(t), nil
	default:
		return nil, errUnsupportedArg(v)
	}
}
