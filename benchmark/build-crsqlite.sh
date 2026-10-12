#!/bin/sh
set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CRSQL="$ROOT/third_party/cr-sqlite"
OUT="$(cd "$(dirname "$0")" && pwd)"

make -C "$CRSQL" loadable
gcc -O2 -I"$CRSQL/src" -I"$CRSQL/src/sqlite" \
  -std=c99 -fPIC -shared -Wall \
  "$CRSQL/src/crsqlite.c" \
  "$CRSQL/src/changes-vtab.c" \
  "$CRSQL/src/ext-data.c" \
  "$CRSQL/dist/libcrsql_bundle_static-loadable.a" \
  "$OUT/crsql_driver_shim.c" \
  -o "$OUT/libcrsqlite.so"
