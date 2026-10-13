package murmurgorm

import (
	"database/sql/driver"
	"errors"
	"fmt"

	murmur "github.com/marcgauthier/murmur/go-client"
)

// errUnsupportedArg reports a driver argument of unexpected type. In practice
// database/sql plus CheckNamedValue only deliver the types convertArg accepts.
func errUnsupportedArg(v driver.Value) error {
	return fmt.Errorf("murmurgorm: unsupported argument type %T", v)
}

// ErrConstraintsNotImplemented is returned by migrator paths SQLite cannot
// express. (Vendored from gorm.io/driver/sqlite v1.6.0, MIT; see
// LICENSE.UPSTREAM.)
var ErrConstraintsNotImplemented = errors.New("constraints not implemented on sqlite, consider using DisableForeignKeyConstraintWhenMigrating, more details https://github.com/go-gorm/gorm/wiki/GORM-V2-Release-Note-Draft#all-new-migrator")

// errNoDSN reports use of the Driver.Open path, which cannot work: opening a
// Murmur database needs a key and options, not a DSN string.
func errNoDSN(name string) error {
	return fmt.Errorf("murmurgorm: no DSN form (got %q); use sql.OpenDB(NewConnector(db))", name)
}

// SQLite extended result codes mapped to GORM errors. Same codes as the
// upstream SQLite driver's error map.
const (
	sqliteConstraintUnique     = 2067 // SQLITE_CONSTRAINT_UNIQUE
	sqliteConstraintPrimaryKey = 1555 // SQLITE_CONSTRAINT_PRIMARYKEY
	sqliteConstraintForeignKey = 787  // SQLITE_CONSTRAINT_FOREIGNKEY
)

// errTxPanicked reports a panic caught while running one shuttled statement.
func errTxPanicked(p any) error {
	return fmt.Errorf("murmurgorm: transaction statement panicked: %v", p)
}

// errTxBadOp reports an impossible pump opcode (defensive; no caller sends one).
func errTxBadOp(op txOp) error {
	return fmt.Errorf("murmurgorm: invalid transaction opcode %d", int(op))
}

// errDDLWithArgs reports DDL carrying bind arguments. Murmur migrations take
// raw SQL only; inline values into the statement instead.
func errDDLWithArgs() error {
	return fmt.Errorf("murmurgorm: DDL with bind arguments is unsupported; inline values into the SQL")
}

// ErrDDLInTransaction reports DDL issued on a transaction. Murmur's authorizer
// allows schema changes only through Migrate (migration mode), which cannot
// run inside an open driver transaction; run migrations outside transactions.
var ErrDDLInTransaction = errors.New("murmurgorm: DDL is not allowed inside transactions; run migrations outside tx")

// ErrSavepointsUnsupported reports SAVEPOINT/RELEASE/ROLLBACK TO usage.
// Murmur's authorizer denies savepoint statements in every access mode, so
// nested transactions are unsupported; use single-level transactions.
var ErrSavepointsUnsupported = errors.New("murmurgorm: savepoints are not supported by Murmur; use single-level transactions")

// ErrTxControlSQL reports raw transaction-control SQL (BEGIN/COMMIT/ROLLBACK).
// Use the database/sql Tx API instead; Murmur denies these statements.
var ErrTxControlSQL = errors.New("murmurgorm: raw transaction-control SQL is denied; use Begin/Commit/Rollback")

// sqliteCodeOf unwraps err to a Murmur SQL error and returns its SQLite result
// code, if present. Murmur errors travel wrapped (%w) so errors.As applies.
func sqliteCodeOf(err error) (int, bool) {
	var merr *murmur.Error
	if !errors.As(err, &merr) {
		return 0, false
	}
	if merr.Code != "Sql" || merr.SQLiteCode == nil {
		return 0, false
	}
	return *merr.SQLiteCode, true
}
