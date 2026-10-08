# Origin signature acceptance

Run `bash tests-live/run.sh origin-signatures`. This release-gate scenario uses
the managed typed API and a no-tag child binary with CGO disabled.

Three independent encrypted nodes use separate Ed25519 keys and QUIC/mTLS.
A commits a transaction, B receives it, A stops, and C receives A's transaction
exclusively through B. The fixture checks the actual stored origin proof.
A malicious client then authenticates as B and sends modified, fabricated,
unsigned, cross-DBID and chunk-identity attacks. Rejection counters must move
while typed RIME state, HLC, generation and receipts remain unchanged. Restart
and signed duplicate replay check durability and idempotency.

Snapshot trust, migration and proof boundaries are specified in
[origin signatures](../../architecture/origin-signatures.md).
