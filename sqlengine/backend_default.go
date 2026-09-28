//go:build !lumosql

package sqlengine

import _ "modernc.org/sqlite"

const (
	sqlDriverName         = "sqlite"
	backendConcurrentMVCC = false
)
