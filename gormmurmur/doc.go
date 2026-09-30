// Package murmur is the GORM dialect for Murmur-SQL.
//
// It speaks the engine's SQLite flavor through the embedded
// database/sql driver, so ordinary GORM code (queries, preloading,
// associations, hooks, soft delete, transactions) works unchanged.
// Schema management differs: Murmur tables replicate, so every table
// needs a single application-generated `id` BLOB(16) primary key and
// nothing Murmur cannot replicate (no secondary indexes, defaults,
// autoincrement, checks, or foreign-key constraints). The migrator
// publishes schema through the engine's additive Migrate API — never
// through SQL DDL — and rejects anything outside the contract loudly
// instead of silently dropping it.
//
// Open with an already-open engine (which needs at least one table;
// use GenesisTables to derive genesis declarations from models):
//
//	tables, err := murmur.GenesisTables(&User{}, &Order{})
//	db, err := replicateddb.Open(ctx, replicateddb.Config{... Schema: ...Tables: tables})
//	gdb, err := gorm.Open(murmur.Open(db))
//	gdb.AutoMigrate(&User{}, &Order{}) // idempotent for genesis tables
package murmur
