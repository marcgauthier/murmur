//go:build cgo

// Blank-imports the CGO SQLite driver (mattn/go-sqlite3, driver name
// "sqlite3"). Excluded from pure-Go (CGO_ENABLED=0) builds; the suite
// skips the mattn leg when the driver is not registered.
package sqlitebench_test

import _ "github.com/mattn/go-sqlite3"
