# Resource exhaustion

"Can I make Murmur fail under realistic abuse?" — the resource leg.
Five live subtests squeeze real daemons and demand the same contract
every time: fail closed under pressure, stay exactly converged after.

## Run

```sh
# Whole suite (~5-10 minutes at defaults)
bash tests-live/run.sh resource-exhaustion

# One leg, e.g. the slow peer with a tighter throttle
MURMUR_RX_SLOW_RATE=8192 go test -tags "sqlite_preupdate_hook sqlite_fts5" \
  ./tests-live/resource-exhaustion/ -run TestSlowPeer -v -count=1 -timeout=10m
```

## Legs

| Test | Pressure | Verdict |
|---|---|---|
| `TestNearOOMSurvivesAndConverges` | 16 MiB block cache + `GOMEMLIMIT` (default 128 MiB) + 2 KiB churn + 8 MiB burst | Victim stays alive, responsive, exactly converged |
| `TestFDExhaustionFailClosed` | `prlimit` RLIMIT_NOFILE squeeze + held idle TLS connections until a probe write observably fails | Process alive, restart recovers to exact convergence (linux-only, skips without `prlimit`) |
| `TestStalledCompactionStaysCorrect` | Automatic compactions disabled + 1 MiB memtables | SST pile-up proves the stall; exact convergence under stall; debt drains after re-enable |
| `TestSlowPeerConvergesWithoutWedge` | Userspace UDP throttle proxies, both directions (default 256 KiB/s, no privileges), byte-count proof the data crossed them | Slow node makes steady progress (90 s without progress = wedge = FAIL), then converges; direct re-peer heals |
| `TestHugeTransactionsAtDefaultBudgets` | 8 MiB accept (byte-exact both nodes), 20 MiB reject (`too large`, no partial row), 20 x 512 KiB load | Exact convergence, writer healthy after rejection |

Knobs: `MURMUR_RX_OOM_MEMLIMIT` / `MURMUR_RX_OOM_ROWS` /
`MURMUR_RX_OOM_BURST` / `MURMUR_RX_OOM_CONC`, `MURMUR_RX_FD_NOFILE` /
`MURMUR_RX_FD_MAXHOLD`, `MURMUR_RX_COMPACT_ROWS`,
`MURMUR_RX_SLOW_RATE` / `MURMUR_RX_SLOW_ROWS` /
`MURMUR_RX_SLOW_VALBYTES` / `MURMUR_RX_SLOW_BOUND_SECONDS`.

## Observed limits

- Full-table scans over hundreds of MB fail while a tight RSS cap
  binds (writes and point reads keep serving). The OOM leg therefore
  verifies exact convergence after lifting the cap; per-row forensics
  confirmed the bytes were identical on both nodes throughout.
- Slow-peer envelope: replication frames carry up to 128 rows and must
  clear the 30 s framed-write deadline (`SendTimeout`), or the session
  recycles by design. 1 KiB rows at 1000+ through 1 Mbit/s or less per
  direction churn sessions faster than they drain and wedge on the
  90 s-no-progress trip; the defaults (2 Mbit/s, 600 x 1 KiB rows)
  converge with margin and byte-count proof.

## Elsewhere

These vectors already have suites and are not duplicated here:
out-of-disk (`diskfull-live`), oversized/malicious frames
(`hostile-peer`), connection floods and slow-loris (`dos-client`),
tc-impaired networks (`impaired-network`), small-budget rejection
(`overload-budgets`).

## Plumbing

The suite needed two small harness additions (`PebbleOptions` /
`PebbleByNode` storage overrides and per-node `NodeEnv`), a `pebble`
section in the test daemon config, and one product knob:
`PebbleConfig.DisableAutomaticCompactions` (production default false),
mapped to Pebble's native option.
