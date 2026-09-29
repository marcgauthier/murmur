# release-upgrade

Previous-release binaries interoperate with the current build: the
suite checks out `SPEDSQL_PREV_REF` (default: the pinned previous
release commit) into a scratch worktree, builds its daemon, and proves
three upgrade paths.

Tests:

- `TestRollingUpgradeLosesNoWrites`: three nodes start on the previous
  release and flip to the current build one at a time under continuous
  writes. Zero failed writes outside upgrade windows, a mixed-version
  old-writes/new-reads replication proof mid-roll, and identical
  digests at the end.
- `TestNewBinaryOpensOldStore`: the current binary opens a store
  written by the previous release (same identity, same directory) with
  a byte-identical digest.
- `TestOldBackupRestoresOnNewBinary`: a backup taken by the previous
  release's writer (via a helper linked against the old tree)
  restores under the current library with a fresh identity, and the
  current binary serves the restored data with no lost rows.

Requirements: a git checkout containing the previous ref (CI uses
`fetch-depth: 0`; a shallow clone fails with an actionable message).
Without a git checkout at all the suite skips. Bump `defaultPrevRef`
in `release_upgrade_test.go` to the last release commit on every
release.

Knobs: `SPEDSQL_PREV_REF` (previous revision under test),
`SPEDSQL_TAGS` (backend tags for the old build, same default as the
harness).
