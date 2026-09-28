# LumoSQL query backend

The query engine has two build backends. The default build remains pure Go and
uses `modernc.org/sqlite`. The optional `lumosql` build tag uses
`mattn/go-sqlite3` linked to an externally built LumoSQL library with an LMDB
backend and pre-update support. `go-sqlite3` links `-lsqlite3` directly; its
`libsqlite3` tag does not read `sqlite3.pc`, so set the CGO include and linker
flags explicitly as shown below. A pkg-config file remains useful to inspect
the external library's version and flags.

## Build and test

Build LumoSQL for the LMDB or LMDBv1 backend, enabling SQLite's
`SQLITE_ENABLE_PREUPDATE_HOOK` in the LumoSQL library itself. Its amalgamation
instructions describe the generated source files, LMDB object files, defines,
and link dependencies: [LumoSQL amalgamation guide](https://lumosql.org/src/lumosql/doc/trunk/LUMOSQL-AMALGAMATION.md).
LumoSQL's build requires a C compiler, GNU make, Tcl/Tclx, Perl's `Text::Glob`,
`not-forking`, SQLite build dependencies, and `pkg-config`; see the
[LumoSQL build documentation](https://lumosql.org/src/lumosql/doc/trunk/README.md).

Generate the LumoSQL LMDB amalgamation with `KEEP_SOURCES=1`. Build LMDB's
objects as PIC, then link the amalgamation and objects into a shared library.
For the `lmdb` variant, the defines below enable LMDB backend selection and
SQLite's pre-update API (the latter is required by the Go capture hook):

```sh
SRC=/path/to/lumosql/build/<target>/sources/sqlite3
LM=/path/to/lumosql/build/<target>/sources/lmdb
LB=/path/to/lumosql/build/<target>/lumo/build
DST=/path/to/lumosql-install
mkdir -p "$DST/include" "$DST/lib/pkgconfig"
cp "$SRC/sqlite3.h" "$DST/include/"
cp -r "$SRC/src" "$DST/include/"
cp -r "$LB/.lumosql/backend" "$DST/include/"
cp "$LB/lmdb.h" "$LB/midl.h" "$DST/include/"
(cd "$LM" && \
  cc -fPIC -O2 -I"$DST/include" -c mdb.c -o "$DST/lib/mdb_pic.o" && \
  cc -fPIC -O2 -I"$DST/include" -c midl.c -o "$DST/lib/midl_pic.o")
cc -fPIC -shared -O2 -I"$DST/include" \
  -DLUMO_LMDB_FIXED_ROWID -DLUMO_BACKEND_PRAGMA \
  -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_RTREE \
  -DSQLITE_ENABLE_FTS5 -DSQLITE_THREADSAFE=1 \
  "$SRC/sqlite3.c" "$DST/lib/mdb_pic.o" "$DST/lib/midl_pic.o" \
  -o "$DST/lib/libsqlite3.so" -lpthread -lm -ldl
```

Write `$DST/lib/pkgconfig/sqlite3.pc` with paths matching the install prefix:

```pkgconfig
prefix=/path/to/lumosql-install
exec_prefix=${prefix}
libdir=${exec_prefix}/lib
includedir=${prefix}/include

Name: sqlite3
Description: LumoSQL with LMDB backend and pre-update capture
Version: 3.53.2
Libs: -L${libdir} -lsqlite3 -lpthread -lm -ldl
Cflags: -I${includedir}
```

Set `PKG_CONFIG_PATH` for metadata inspection, `LD_LIBRARY_PATH` for runtime
dependencies, and `LUMO_LMDB_MAPSIZE` for the target's data size. Export CGO
flags so `go-sqlite3` finds this header and library (the LumoSQL dependency
may also require additional runtime library directories):

```sh
pkg-config --modversion sqlite3
pkg-config --cflags --libs sqlite3
export CGO_CFLAGS="-I$DST/include"
export CGO_LDFLAGS="-L$DST/lib -Wl,-rpath,$DST/lib"
export LD_LIBRARY_PATH="$DST/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PKG_CONFIG_PATH="$DST/lib/pkgconfig"
export LUMO_LMDB_MAPSIZE=1073741824
CGO_ENABLED=1 go test -tags 'lumosql libsqlite3 sqlite_preupdate_hook lumosql_mvcc' ./sqlengine -run '^TestLumoSQLReaderSnapshotDoesNotBlockWriter$' -count=1
```

The `libsqlite3` tag makes `go-sqlite3` link against the system `sqlite3`
library rather than its bundled SQLite. The targeted acceptance test is
required for LumoSQL's file-backed MVCC path. Shared in-memory LumoSQL mode
uses one reserved connection because independent LMDB memory connections do
not share an environment. Run the sqlengine suite and then broader checks:

```sh
export CGO_ENABLED=1
export PKG_CONFIG_PATH=/path/to/lumosql-install/lib/pkgconfig
export LD_LIBRARY_PATH=/path/to/lumosql-install/lib
go test -tags 'lumosql libsqlite3 sqlite_preupdate_hook' ./sqlengine
go test -tags 'lumosql libsqlite3 sqlite_preupdate_hook lumosql_mvcc' ./sqlengine
go test -tags 'lumosql libsqlite3 sqlite_preupdate_hook' ./...
```

`lumosql` selects the external driver and its MVCC connection policy.
`libsqlite3` tells `go-sqlite3` to link `-lsqlite3` instead of its bundled
SQLite; `CGO_CFLAGS` and `CGO_LDFLAGS` above supply the nonstandard paths.
`sqlite_preupdate_hook` enables the hook bridge in the
driver. The external LumoSQL library must also be compiled with
`SQLITE_ENABLE_PREUPDATE_HOOK`; the Go build tag cannot add that capability to
an already-built library. This repository does not vendor or automatically
build LumoSQL. An external LMDBv1 build passes the focused MVCC acceptance
test; broader package/repository tests still depend on the backend build and
platform libraries.

The tag can be compiled and exercised against the driver's bundled SQLite for
the pre-update bridge alone:

```sh
go test -tags 'lumosql sqlite_preupdate_hook' ./sqlengine -run 'TestLumoSQLPreUpdatePreservesTextAndBlobTypes|TestPreUpdateCaptureInsertUpdateDelete'
```

That check does not validate LMDB or non-blocking MVCC. Enable `lumosql_mvcc`
with the external library to include the open-reader/write-progress acceptance
test. The
LumoSQL project documentation currently describes its release as a preview
and says production use is not yet recommended.

## Pinned versions

Reproduce the validated backend build with these exact versions:

- Go toolchain 1.26.0 (`go.mod`; CI uses `1.26.x`).
- `github.com/mattn/go-sqlite3 v1.14.32` (`go.mod`) for the `lumosql` tag.
- LumoSQL 3.53.2 LMDBv1 amalgamation generated with `KEEP_SOURCES=1`,
  compiled with `SQLITE_ENABLE_PREUPDATE_HOOK` (plus the defines above).
- LMDB as vendored by that LumoSQL revision, built PIC from its sources.
- Default backend: `modernc.org/sqlite v1.44.3` (pure Go, no CGO).

## Supported platforms

| Backend | Linux amd64 | Linux arm64 | Windows amd64 | Other |
|---|---|---|---|---|
| Default (pure Go) | CI-tested | CI compile-checked | CI-tested | Follows the main CI matrix |
| `lumosql` + external LMDB | Validated 2026-09-27 (MVCC acceptance) | Not validated | Not supported | Not supported |
| `lumosql` + bundled SQLite (pre-update bridge only) | CI tag-build check | Untested | Untested | Untested; never validates LMDB/MVCC |

The `lumosql` backend is not released. Do not add platforms to a
supported cell without running the external-LMDB acceptance command
above on them; the bundled-SQLite tag check alone is never sufficient.

## Query connections and MVCC

With the `lumosql` build tag and `QueryStoreMMap`, every standalone query uses
its own pooled SQL connection. An open `Rows` keeps its connection and LMDB
read snapshot until `Close` or end-of-results. A concurrent write transaction
uses a separate connection and does not wait on the reader lock; new reads can
also proceed while it writes. The pool is capped at 32 connections, including
the dedicated write and startup/read connections. Closing the engine waits for
outstanding query rows, then closes the pool and deletes the disposable query
directory. Pebble remains the sole authoritative store; startup rebuilds this
query materialization.

The LumoSQL build sets per-connection `synchronous=OFF` and foreign keys off;
durable acknowledgement remains the responsibility of Pebble. LMDB reserves
virtual address space per environment; consult LumoSQL's
[mmap guidance](https://lumosql.org/src/lumosql/doc/trunk/README.md#about-lmdb-and-mmap)
and choose its `LUMO_LMDB_MAPSIZE` setting for the supported data size and
number of simultaneously open databases.
