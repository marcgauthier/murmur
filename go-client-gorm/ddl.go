package murmurgorm

import (
	"database/sql"
	"database/sql/driver"
	"strings"

	murmur "github.com/marcgauthier/murmur/go-client"
)

// firstKeyword returns the upper-cased first word of a statement, skipping
// leading whitespace, semicolons, and -- and /* */ comments. It reports false
// when no keyword is present.
func firstKeyword(query string) (string, bool) {
	s := query
	for {
		s = strings.TrimLeft(s, " \t\r\n;")
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
				continue
			}
			return "", false
		}
		if strings.HasPrefix(s, "/*") {
			end := strings.Index(s, "*/")
			if end < 0 {
				return "", false
			}
			s = s[end+2:]
			continue
		}
		break
	}
	if s == "" {
		return "", false
	}
	end := strings.IndexAny(s, " \t\r\n(;")
	if end < 0 {
		end = len(s)
	}
	return strings.ToUpper(s[:end]), true
}

// isDDL reports whether the statement is schema DDL. Murmur's authorizer
// allows these verbs only in migration mode, so the shim routes them to
// DB.Migrate instead of an ordinary transaction. All other statements flow to
// Read/Transaction and any authorizer denial surfaces natively.
func isDDL(query string) bool {
	kw, ok := firstKeyword(query)
	if !ok {
		return false
	}
	switch kw {
	case "CREATE", "DROP", "ALTER", "REINDEX":
		return true
	}
	return false
}

// isReadQuery reports statements that never write. Query calls for anything
// else (INSERT/UPDATE/DELETE ... RETURNING, REPLACE, WITH carrying writes)
// run in a Transaction instead of Read, since Murmur's authorizer denies
// writes in read mode. WITH always takes the Transaction branch: reads work
// identically there, and only that branch can serve write-CTEs.
func isReadQuery(query string) bool {
	kw, ok := firstKeyword(query)
	if !ok {
		return true
	}
	switch kw {
	case "SELECT", "VALUES", "TABLE", "PRAGMA", "EXPLAIN":
		return true
	}
	return false
}

// txControlKind classifies transaction-control statements, which Murmur denies
// in every mode, into clear client-side errors (nil means "not tx-control").
func txControlKind(query string) error {
	kw, ok := firstKeyword(query)
	if !ok {
		return nil
	}
	switch kw {
	case "SAVEPOINT", "RELEASE":
		return ErrSavepointsUnsupported
	case "ROLLBACK":
		// ROLLBACK TO SAVEPOINT is a savepoint operation; bare ROLLBACK is
		// raw transaction control.
		if isRollbackTo(query) {
			return ErrSavepointsUnsupported
		}
		return ErrTxControlSQL
	case "BEGIN", "COMMIT", "END":
		return ErrTxControlSQL
	}
	return nil
}

// isRollbackTo reports a ROLLBACK TO [SAVEPOINT] statement.
func isRollbackTo(query string) bool {
	fields := strings.Fields(query)
	return len(fields) >= 2 && strings.EqualFold(fields[0], "ROLLBACK") &&
		strings.EqualFold(fields[1], "TO")
}

// migrateExec runs one DDL statement through Murmur's migration path. Counts
// are unavailable there, so the result reports zeros.
func migrateExec(db *murmur.DB, query string) (driver.Result, error) {
	if err := db.Migrate(murmur.NewMigration(query)); err != nil {
		return nil, err
	}
	return result{}, nil
}

// migrateQuery runs one DDL statement issued through Query and returns an
// empty result set. DDL belongs in Exec; this path exists so Query callers get
// defined behavior instead of an authorizer denial.
func migrateQuery(db *murmur.DB, query string) (driver.Rows, error) {
	if err := db.Migrate(murmur.NewMigration(query)); err != nil {
		return nil, err
	}
	return &bufferedRows{}, nil
}

// checkConnUsable returns the clone or an error when the conn is closed.
func (c *conn) checkConnUsable() (*murmur.DB, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.db == nil {
		return nil, sql.ErrConnDone
	}
	return c.db, nil
}
