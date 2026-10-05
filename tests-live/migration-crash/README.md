# Crash mid-migration lands in a defined state

Run with `go test -count=1 ./tests-live/migration-crash`. A node is
SIGKILLed mid-migration (`MURMUR_MIGRATION_CRASH_KILL_DELAY_MS`);
on restart it must land cleanly in exactly one defined state (old
schema at epoch 1 with pre-crash rows intact, or new schema at epoch
2), never a mix. Post-migration writes replicate and all nodes finish
with equal digests.
