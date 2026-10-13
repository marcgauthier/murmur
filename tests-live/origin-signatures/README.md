# origin-signatures (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/origin-signatures/`
(`origin_signatures_test.go`:
`TestForwardedOriginSurvivesOfflineOriginAndHostileRelay`).

## What it proves

Three encrypted nodes share one database id. A commits a transaction,
B receives it, A stops, and C receives A's transaction exclusively
through B. The fixture checks the actual stored origin proof, then a
malicious relay authenticated as B sends modified, fabricated,
unsigned, cross-DBID and chunk-identity attacks at C. Every attack must
be rejected while rows, generation and receipts stay unchanged.
Restart plus signed-duplicate replay check durability and idempotency,
and an untrusted snapshot manifest must be refused.

## Pass criteria

- C converges on A's row while A is stopped (forwarded through B only);
  B's and C's row sets are equal.
- A's served bytes verify under A's origin key with the origin intact.
- Modified / fabricated / unsigned / cross-DBID batches are rejected;
  the tampered chunk fails before durable staging (availability index
  empty); row set and staged/applied counters never move.
- After a C restart the honest row is still present; replaying the
  signed original is idempotent (no error, session survives, counters
  and rows unchanged).
- A well-formed snapshot manifest from the (untrusted) relay is
  refused; rows and counters unchanged.

## Env knobs

None beyond the shared `MURMUR_LIVE_RUNTIME` / `MURMUR_LIVE_FAILURES` /
`MURMUR_LIVE_UNLOCK_*` harness knobs.

## Deviations from the Go original

- Plain SQL replaces the typed-records HTTP API; there is no daemon,
  no HTTPS API, and no Prometheus endpoint (in-process cluster).
- Go replays all four batch attacks over one relay session. The Rust
  engine kills a session on `Error::Auth`, so each signature-failure
  leg uses its own session and asserts stream death; only the
  cross-DBID leg (`Error::Protocol`) shares Go's surviving session.
- Go asserts rejection counters move. The Rust engine keeps no
  invalid-batch counters, so each leg asserts the wire/session outcome
  plus the matching `last_error` latch cause instead
  (`payload digest mismatch`, `signature error`,
  `transaction identity`, `snapshot identity, trust or expiry`).
- Go reads A's log by reopening A's directory. The port fetches A's
  genuine batches from C's stored log over a `Need` relay session and
  verifies them locally against A's origin public key.
- Go reuses B's TLS key file and origin key for the relay. The port
  uses a fresh harness-CA leaf issued for B's NodeID (same transport
  identity, still fully authenticated) and a dedicated attacker
  signing key for the fabricated leg (node seeds are harness-private);
  the verification exercised is identical.
- Go asserts HLC, generation and per-TxID receipts. Rust exposes no
  HLC clock or receipt read API; the port asserts the staged/applied
  counters plus the row set instead (receipts commit atomically with
  the apply, and clock observation happens only inside staging, after
  verification).
- Go's duplicate replay reaches `batches_received_total == 1`. The
  Rust engine dedups before counting, so the port asserts `received`
  stays 0 across the replay (session alive, no error frame).
- C's `trusted_snapshot_sources` is empty by default in the harness,
  matching Go's explicit `{2: {}}` overlay (no overlay needed).
