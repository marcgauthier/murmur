# FTS crash rebuilds exact search results

Run with `go test -count=1 ./tests-live/fts-crash`. FTS5 indexes are
local-only derived state, so this single-node suite seeds
`MURMUR_FTS_CRASH_SEED` docs (default 200) with dual base+FTS
writes, proves MATCH equals the base table exactly pre-crash, then
SIGKILLs mid-index-write (the kill only counts when a write was in
flight). After restart it proves the in-memory index is gone,
rebuilds from the durable base table, and requires every MATCH to
equal the base set exactly with a clean integrity check. Skips
honestly when the backend lacks FTS5.
