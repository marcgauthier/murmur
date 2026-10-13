# diskfull-live (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/diskfull-live/`
(`diskfull_live_test.go`, `TestDiskFullFailsClosedThenRecovers`).

## What it proves

One node is stopped, its directory is moved onto a tiny tmpfs, and it is
restarted there; writes past ENOSPC must fail closed (error, no partial
row, node keeps answering reads) with pre-full data intact, and freeing
space plus a restart must recover to exact convergence.

## Pass criteria (same as Go)

- 20-row baseline converges within 30 s; survive the tmpfs move and
  converge again within 30 s.
- The fs proves tiny (< 5 MiB free) before filling (else vacuous).
- Fixed filler lands first; the fill loop records every acked/errored
  outcome and stops after 5 errors; at least one error is required
  (ENOSPC must actually hit).
- While still full: reads answer; a probe write either acks (tracked) or
  errors cleanly.
- After freeing space + restart: every acked row intact everywhere,
  every errored row absent-on-all or whole-on-all (never torn).
- Post-recovery writes on both nodes converge within 30 s.
- Knobs: `MURMUR_DISKFULL_TMPFS_SIZE_MB` (8),
  `MURMUR_DISKFULL_FILLER_SIZE_MB` (1), `MURMUR_DISKFULL_VALUE_SIZE_KB`
  (128). SKIPs (restoring the dir) when the mount fails; SKIPs on
  non-Unix. Teardown never leaks the mount (unmount failure is loud).

## Deviations from the Go original

- In-process two-`Database` cluster instead of daemon processes; plain
  SQL replaces the typed-records HTTP API
  (`live_typed_records(name, value)`).
- Go's daemon-log panic scan has no target: an in-process panic fails
  the test by construction.
- Free space via libc `statvfs` (new scenario-only dep `libc`, same
  version as the harness); mount/copy/unmount via `mount`/`cp`/`umount`.
- On failure the tmpfs content is copied aside before the unmount so the
  `finish` snapshot still captures it.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/diskfull-live/Cargo.toml
```
