# views (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/views/` (`views_test.go`,
`TestTypedViewTracksReplicatedRowsAndRebuilds`).

## What it proves

Three nodes form an encrypted mesh. Each inserts one contact (`ann`, `bob`,
`hidden`); `ann` and `bob` are enabled via a counter add. The two qualifying
rows must appear in the managed view on all replicas while `hidden` stays
out. Node 3 then restarts and its rebuilt view must contain the same two rows.

## Pass criteria (same as Go)

- View on nodes 1-3 converges to exactly `[ann, bob]` within 20 s.
- After the node 3 restart + re-mesh, its view again converges to `[ann, bob]`
  within 20 s.

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; restart is
  `close()` + reopen of the same directory with the same key, followed by an
  explicit re-mesh (fresh ephemeral port).
- Plain SQL replaces the typed-RIME managed query: base table
  `contacts(name TEXT PRIMARY KEY, score INTEGER NOT NULL)` plus
  `VIEW enabled_names AS SELECT name FROM contacts WHERE score > 0 ORDER BY
  name`. `score` plays the role of the enabling PN counter.
- Same schema is migrated locally on every node (no shared schema manifest).

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/views/Cargo.toml
```
