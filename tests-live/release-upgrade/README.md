# release-upgrade

Previous-release peers cannot interoperate with the current signed protocol.
The suite checks out `MURMUR_PREV_REF` into a scratch worktree and builds its
daemon. The current binary must reject stores and backups from the legacy
Pebble format without treating them as current RIME data. Legacy rows are
seeded through the previous binary only; migration into the new RIME schema is
not performed in place.

Tests:

- `TestNewBinaryRejectsOldPebbleStore`: seed with the previous daemon and verify
  the current storage engine refuses the legacy directory.
- `TestOldBackupRejectedOnNewBinary`: verify restore refuses a backup from the
  old storage format.

Requirements: a git checkout containing the previous ref (CI uses
`fetch-depth: 0`; a shallow clone fails with an actionable message).
Without a git checkout at all the suite skips. Bump `defaultPrevRef`
in `release_upgrade_test.go` to the last release commit on every
release.

Knobs: `MURMUR_PREV_REF` (previous revision under test),
`MURMUR_TAGS` (backend tags for the old build; defaults to
`sqlite_preupdate_hook` for the pinned release). Because that historical
binary uses SQLite, run this compatibility fixture with CGO enabled. Current
typed RIME live scenarios are separately compiled with CGO disabled.
