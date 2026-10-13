package murmurgorm

import (
	"database/sql"
	"errors"

	murmur "github.com/marcgauthier/murmur/go-client"
	"gorm.io/gorm"
)

// Dialector is a GORM dialect for Murmur. The SQLite SQL generation
// (quoting, binding, clause builders, migrator, savepoints) is vendored from
// gorm.io/driver/sqlite v1.6.0 (see dialect.go); the connection pool is the
// Murmur bridge from this package, and error translation is Murmur-native.
type Dialector struct {
	db    *murmur.DB
	sqlDB *sql.DB
}

// Open returns a GORM dialect over an open Murmur database. The caller keeps
// owning db: GORM pool connections are clones, admin operations stay on the
// native handle, and closing the GORM or *sql.DB never closes db.
func Open(db *murmur.DB) gorm.Dialector {
	return &Dialector{db: db}
}

// Name implements gorm.Dialector.
func (d Dialector) Name() string {
	return "murmur"
}

// Initialize implements gorm.Dialector. It builds the bridged pool over the
// caller's database, then runs the vendored SQLite setup (version probe,
// default callbacks, clause builders).
func (d *Dialector) Initialize(db *gorm.DB) error {
	if d.db == nil {
		return errors.New("murmurgorm: nil *murmur.DB")
	}
	// Murmur's schema validation rejects the REFERENCES keyword, so
	// database-level foreign key constraints can never migrate. Force them
	// off: GORM-level association handling (Preload, joins) is unaffected.
	db.Config.DisableForeignKeyConstraintWhenMigrating = true
	d.sqlDB = OpenDB(d.db)
	return d.initialize(db, d.sqlDB)
}

// Translate implements gorm.ErrorTranslator, mapping Murmur SQL errors with
// SQLite extended result codes to GORM sentinel errors.
func (d Dialector) Translate(err error) error {
	if code, ok := sqliteCodeOf(err); ok {
		switch code {
		case sqliteConstraintUnique, sqliteConstraintPrimaryKey:
			return gorm.ErrDuplicatedKey
		case sqliteConstraintForeignKey:
			return gorm.ErrForeignKeyViolated
		}
	}
	return err
}

// Native returns the *murmur.DB behind a GORM handle opened with Open. It is
// the escape hatch for admin operations (Backup, RotateDataKey, Status,
// Replication, ...); keeping your own reference to the handle passed to Open
// works equally well since clones share the one native database.
func Native(db *gorm.DB) (*murmur.DB, bool) {
	if db == nil {
		return nil, false
	}
	d, ok := db.Dialector.(*Dialector)
	if !ok || d.db == nil {
		return nil, false
	}
	return d.db, true
}

// SQLDB returns the *sql.DB behind a GORM handle opened with Open, for pool
// tuning (SetMaxOpenConns, SetConnMaxLifetime, Stats). It reports false for
// handles from other dialects.
func SQLDB(db *gorm.DB) (*sql.DB, bool) {
	if db == nil {
		return nil, false
	}
	d, ok := db.Dialector.(*Dialector)
	if !ok || d.sqlDB == nil {
		return nil, false
	}
	return d.sqlDB, true
}
