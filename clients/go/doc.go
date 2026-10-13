// Package murmur embeds the Murmur Rust database through cgo. Managed databases
// keep one SQLite database in memory and persist/replicate through native workers.
//
// Obtain Options with DefaultOptions, open a database, migrate its schema, and
// execute application SQL with DB.Read or DB.Transaction. SQL, statements and rows
// are scoped to the callback's OS thread. Return errors from callbacks; do not call
// runtime.Goexit. Call Flush for a durability barrier and explicitly close owned
// handles. Native binaries must first be built with build.sh or build.ps1.
package murmur
