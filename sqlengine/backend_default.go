//go:build !modernc

package sqlengine

import _ "github.com/mattn/go-sqlite3"

const sqlDriverName = "sqlite3"
