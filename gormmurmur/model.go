package murmur

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	replicateddb "github.com/marcgauthier/murmur"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// Model is the Murmur replacement for gorm.Model. Embed it in every
// replicated model:
//
//	type User struct {
//		murmur.Model
//		Name string
//	}
//
// The primary key is a client-generated 16-byte blob; unlike
// gorm.Model there is no autoincrement integer key, and the
// soft-delete column carries no index tag (Murmur keeps no secondary
// indexes).
type Model struct {
	ID        ID        `gorm:"primaryKey;column:id"`
	CreatedAt Time      `gorm:"column:created_at"`
	UpdatedAt Time      `gorm:"column:updated_at"`
	DeletedAt DeletedAt `gorm:"column:deleted_at"`
}

// ID is a 16-byte Murmur primary key. It binds as a blob, compares
// by value, and renders as a UUID string in JSON. Raw [16]byte
// cannot bind through database/sql, so key columns must use this
// type (or []byte); the migrator rejects anything else loudly.
type ID [16]byte

// NewID returns a random ID.
func NewID() ID {
	return ID(replicateddb.NewRowID())
}

// String renders the UUID text form.
func (id ID) String() string {
	return uuid.UUID(id).String()
}

// IsZero reports whether this is the zero ID.
func (id ID) IsZero() bool {
	return id == ID{}
}

// Value implements driver.Valuer.
func (id ID) Value() (driver.Value, error) {
	cp := id
	return cp[:], nil
}

// Scan implements sql.Scanner, accepting raw 16-byte blobs (and
// their string form) as well as UUID text.
func (id *ID) Scan(v interface{}) error {
	switch t := v.(type) {
	case nil:
		*id = ID{}
		return nil
	case []byte:
		return id.scanBytes(t)
	case string:
		if len(t) == 16 {
			return id.scanBytes([]byte(t))
		}
		u, err := uuid.Parse(t)
		if err != nil {
			return fmt.Errorf("murmur: cannot scan %q into murmur.ID (want 16 bytes or UUID text)", t)
		}
		*id = ID(u)
		return nil
	default:
		return fmt.Errorf("murmur: cannot scan %T into murmur.ID (want 16 bytes or UUID text)", v)
	}
}

func (id *ID) scanBytes(b []byte) error {
	if len(b) != 16 {
		return fmt.Errorf("murmur: cannot scan %d bytes into murmur.ID (want 16)", len(b))
	}
	copy(id[:], b)
	return nil
}

// MarshalJSON renders the UUID text form.
func (id ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.String())
}

// UnmarshalJSON parses the UUID text form.
func (id *ID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return fmt.Errorf("murmur: cannot parse %q as murmur.ID: %w", s, err)
	}
	*id = ID(u)
	return nil
}

// timeStoreFormat is the canonical UTC timestamp representation used
// for every backend, so text comparisons have the same chronological
// ordering across nodes and SQLite implementations.
const timeStoreFormat = "2006-01-02 15:04:05.000000000Z"

// Time is a Murmur-mapped timestamp. Values bind as canonical UTC
// datetime text and reads parse that text back. Legacy SQLite datetime
// spellings are also accepted when scanning. It round-trips through
// CreatedAt/UpdatedAt auto-tracking like time.Time does on the
// sqlite driver. Plain time.Time struct fields are rejected at
// migrate time: the engine returns TEXT, which database/sql cannot
// scan into *time.Time.
type Time struct {
	time.Time
}

// GormDataType reports Time as a time so auto-tracking applies.
func (Time) GormDataType() string {
	return "time"
}

// Value implements driver.Valuer. It normalizes to UTC and uses one
// fixed-width representation, keeping text comparisons chronological
// and consistent across backends and node time zones.
func (t Time) Value() (driver.Value, error) {
	return t.Time.UTC().Format(timeStoreFormat), nil
}

// Scan implements sql.Scanner, accepting the datetime spellings the
// bundled drivers produce as well as RFC 3339 and time.Time itself.
func (t *Time) Scan(v interface{}) error {
	tm, err := parseMurmurTime(v)
	if err != nil {
		return err
	}
	t.Time = tm
	return nil
}

// MarshalJSON implements json.Marshaler.
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time)
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *Time) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &t.Time)
}

// DeletedAt is the Murmur soft-delete timestamp (NULL = alive). It
// plugs into GORM's soft-delete query/update/delete clauses exactly
// like gorm.DeletedAt; gorm.DeletedAt itself cannot work here because
// sql.NullTime cannot scan the TEXT storage.
type DeletedAt struct {
	Time  Time
	Valid bool
}

// GormDataType reports DeletedAt as a time.
func (DeletedAt) GormDataType() string {
	return "time"
}

// Value implements driver.Valuer.
func (n DeletedAt) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Time.Value()
}

// Scan implements sql.Scanner.
func (n *DeletedAt) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		return nil
	}
	if err := n.Time.Scan(v); err != nil {
		return err
	}
	n.Valid = true
	return nil
}

// MarshalJSON implements json.Marshaler.
func (n DeletedAt) MarshalJSON() ([]byte, error) {
	if n.Valid {
		return json.Marshal(n.Time.Time)
	}
	return json.Marshal(nil)
}

// UnmarshalJSON implements json.Unmarshaler.
func (n *DeletedAt) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		n.Valid = false
		return nil
	}
	if err := json.Unmarshal(b, &n.Time.Time); err != nil {
		return err
	}
	n.Valid = true
	return nil
}

// QueryClauses implements schema.QueryClausesInterface.
func (DeletedAt) QueryClauses(f *schema.Field) []clause.Interface {
	return []clause.Interface{gorm.SoftDeleteQueryClause{Field: f}}
}

// UpdateClauses implements schema.UpdateClausesInterface.
func (DeletedAt) UpdateClauses(f *schema.Field) []clause.Interface {
	return []clause.Interface{gorm.SoftDeleteUpdateClause{Field: f}}
}

// DeleteClauses implements schema.DeleteClausesInterface.
func (DeletedAt) DeleteClauses(f *schema.Field) []clause.Interface {
	return []clause.Interface{gorm.SoftDeleteDeleteClause{Field: f}}
}

// parseMurmurTime converts stored or bound values to time.Time. TEXT
// storage means every spelling the drivers produce must parse; unit
// scalars are refused loudly rather than guessed.
func parseMurmurTime(v interface{}) (time.Time, error) {
	switch t := v.(type) {
	case nil:
		return time.Time{}, nil
	case time.Time:
		return t, nil
	case []byte:
		return parseMurmurTimeString(string(t))
	case string:
		return parseMurmurTimeString(t)
	default:
		return time.Time{}, fmt.Errorf("murmur: cannot scan %T into murmur.Time (store RFC 3339 or SQLite datetime text)", v)
	}
}

func parseMurmurTimeString(s string) (time.Time, error) {
	for _, layout := range []string{
		timeStoreFormat,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999 -0700 MST", // modernc/raw Go form
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02T15:04:05.999999999Z07:00",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	} {
		if tm, err := time.Parse(layout, s); err == nil {
			return tm, nil
		}
	}
	return time.Time{}, fmt.Errorf("murmur: cannot parse %q as murmur.Time", s)
}
