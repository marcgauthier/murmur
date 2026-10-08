RIME: (muse)
    Keep adding tests and hunting for bugs

SPOOL: (agy)
    Keep adding tests and hunting for bugs

MURMUR: (migration implementation landed; release qualification remains)
- Production SQL APIs, SQLite engine/driver, SQL CLI commands, and SQLite build
  tags are removed; format-5 and legacy Pebble stores fail closed.
- Keep SQL/SQLite references only where they explain the breaking cutover,
  previous-release compatibility fixtures, removed-API rejection tests, or
  isolated historical benchmark modules.
- Remaining migration gates: complete all-package acceptance, fixed-host
  rich-record performance cells, scheduled long-duration soaks, and the
  remaining storage-fault and replication-interleaving qualification in
  `MIGRATION_PLAN.md`.
- The sequential live release gate and CGO-disabled Murmur root test suite pass
  on the current worktree; see `MIGRATION_PLAN.md` for exact evidence.


OVERWATCH:
-  Migrate Overwatch toward using Murmur without GORM or SQL and instead use new go Murmur golang commands 
- Check All of Overwatch dependencies.




