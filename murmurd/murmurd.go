// Package murmurd is the assembled Murmur-SQL database daemon: the same
// embedded engine (Pebble + encrypted storage + QUIC replication) with
// MySQL- and PostgreSQL-protocol frontends instead of the Go API.
//
// Both frontends funnel into the one SQLite engine, so only
// SQLite-expressible statements succeed; anything else is rejected
// with an explicit error (see filter.go). There is no query
// translation: the wire protocols are access paths, not dialects.
package murmurd

// Version is the daemon version reported to clients. The main package
// may override it at link time (-X).
var Version = "0.1.0"
