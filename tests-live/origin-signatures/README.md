# Origin signature acceptance

Run `MURMUR_TAGS=modernc bash tests-live/run.sh origin-signatures` or use the
runner's default CGO build tags. This scenario is part of the release gate.

Three independent encrypted nodes use separate Ed25519 keys and QUIC/mTLS.
A commits a transaction, B receives it, A stops, and C receives A's transaction
exclusively through B. The fixture checks the actual stored origin proof.
A malicious client then authenticates as B and sends modified, fabricated,
unsigned, cross-DBID and chunk-identity attacks. Rejection counters must move
while SQL state, HLC, generation and receipts remain unchanged. Restart and
signed duplicate replay check durability and idempotency.

Snapshot trust, migration and proof boundaries are specified in
[origin signatures](../../architecture/origin-signatures.md).
