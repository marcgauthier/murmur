// Package murmurgorm is a GORM-compatible SQL layer over Murmur.
//
// It bridges go-client's callback-scoped API (DB.Read/DB.Transaction) to
// database/sql/driver, then reuses the upstream gorm.io/driver/sqlite dialect
// with an injected connection pool. Murmur's SQLite is a private in-memory
// instance inside the Rust core, so stock GORM+SQLite drivers cannot reach it;
// this package is the only GORM entry point.
//
// Ownership: the caller opens and owns the *murmur.DB handle (including the
// encryption key and Options) and keeps it for admin operations (Backup,
// RotateDataKey, Status, Replication, ...), which stay on the native API and
// are intentionally not exposed here. Pool connections are murmur clones;
// closing the *sql.DB or *gorm.DB never closes the caller's database.
//
// Limitations (by design of the callback API):
//
//   - Query rows are buffered in memory per call; there is no streaming cursor.
//   - Prepared statements are re-prepared on every call.
//   - TEXT values that exactly match canonical SQLite datetime formats are
//     returned as time.Time; see the timestamp notes in values.go.
//   - database/sql transaction options (isolation level, read-only) are
//     accepted and ignored: Murmur serializes writers internally.
package murmurgorm
