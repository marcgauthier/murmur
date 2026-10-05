# release-upgrade

Unsigned previous-release peers cannot interoperate with signed protocol v4.
The suite checks out `MURMUR_PREV_REF` into a scratch worktree, builds its daemon,
and verifies a coordinated offline cutover, explicit legacy-store migration,
and legacy-backup restore followed by migration. All current nodes use separate
Ed25519 origin identities; historical public keys remain available after restore.

Tests:

- `TestCoordinatedSignedCutoverLosesNoWrites`: legacy writers converge, all stop,
  and every store migrates explicitly before the signed cluster resumes. Baseline
  digests remain identical and new signed writes replicate from every node.
- `TestNewBinaryOpensOldStore`: migrate a stopped legacy store with the fixture's
  offline `migrate-origin-baseline` command, then verify identical data.
- `TestOldBackupRestoresOnNewBinary`: restore an old backup under a fresh identity,
  provision its signing key, migrate its trusted baseline, and verify all rows.

Requirements: a git checkout containing the previous ref (CI uses
`fetch-depth: 0`; a shallow clone fails with an actionable message).
Without a git checkout at all the suite skips. Bump `defaultPrevRef`
in `release_upgrade_test.go` to the last release commit on every
release.

Knobs: `MURMUR_PREV_REF` (previous revision under test),
`MURMUR_TAGS` (backend tags for the old build, same default as the
harness).
