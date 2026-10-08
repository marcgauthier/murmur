# Spool storage-engine live checks

Run with `bash tests-live/run.sh spool` (or `go test -count=1
./spool/tests-live`). No daemon processes are involved: the tests
exercise the `spool` package against real disk.

`TestSpoolCrashRecovery` starts the test binary re-executed as a child
writer process, lets it append checksummed rounds with Sync durability
(each round acknowledged in a synced progress marker), then SIGKILLs
it mid-stream. The parent reopens the store and requires every
acknowledged round to reload byte-identical, then runs key rotation,
deletion, and reclamation on the recovered store and verifies a clean
reopen shows no resurrection. Tune with
`MURMUR_SPOOL_WRITE_SECONDS` (default 3) and
`MURMUR_SPOOL_ROUND_KEYS` (default 200).

`TestSpoolLiveSoak` hammers a default-configured store from 64
goroutines with interleaved key rotation and reclamation, then
verifies the full dataset reloads intact and reports write
throughput. All tests shrink in `-short` mode.

`TestSpoolGroupCrashAtomicity` SIGKILLs a child committing Sync
groups and requires per-group atomicity: acknowledged groups fully
present, the in-flight group fully present or fully absent, never
partial. Tune with `MURMUR_SPOOL_WRITE_SECONDS` (default 3) and
`MURMUR_SPOOL_GROUP_KEYS` (default 20).

`TestSpoolRebindKillRecovery` SIGKILLs a child blocked in
`RebindContext` on a large store. The parent waits for provable
mid-staging state (a `.rew-*` temp past 1MB) before killing, then
requires resume to complete the rebind: context-B hint, no intent
left, every byte intact, the old context rejected, and the store
writable. Tune with `MURMUR_SPOOL_REBIND_KEYS` (default 1500),
`MURMUR_SPOOL_REBIND_VALKB` (default 100), and
`MURMUR_SPOOL_REBIND_KILL_MS` (default 1000).

`TestSpoolBackupUnderWrite` captures a checkpoint to a caller
destination and streams its files to a backup directory while
writers hammer the live store under concurrent key rotation and
reclamation, then requires the streamed backup to open with a
consistent cut: the quiesced anchor byte-exact, the full fixed key
set present, and every value parseable with an impossible value
failing the test. Tune with `MURMUR_SPOOL_BACKUP_SECONDS`
(default 6), `MURMUR_SPOOL_BACKUP_WRITERS` (default 8), and
`MURMUR_SPOOL_BACKUP_SLOTS` (default 50).

`TestSpoolLockExclusionAndCrashUnlock` tests real OS-level multi-process
exclusion. A child process opens the store, holding the exclusive
`flock`. A concurrent `spool.Open` by the parent must immediately fail
with `spool.ErrLocked`. When the child process is terminated with
`SIGKILL`, the lock is automatically reclaimed by the OS, and the
parent can open the store and continue writing.

`TestSpoolCompactionCrashRecovery` runs a child process performing
aggressive segment rotations, tombstone generation, and compaction
rewriting. The child is killed abruptly mid-compaction. On reopen,
`sweepUnlisted` sweeps uncommitted staging files (`.cmp-*.tmp-*`), the
manifest remains consistent, all live keeper and volatile keys reload
without data loss or tombstone resurrection, and subsequent compaction
passes succeed.

`TestSpoolTornTailTruncationLive` simulates a partial torn write at
the tail of the active segment file (e.g. abrupt power outage). On
reopen, Spool detects the torn tail, truncates the file back to the
last complete block boundary, reports `TruncatedTails >= 1`, verifies
all previously acknowledged keys are intact, and cleanly resumes
appends.

`TestSpoolMidBlockCorruptionLive` verifies that bit-rot or byte
tampering in the middle of a sealed block fails closed: authenticated
encryption detects the digest/payload mismatch and returns an error,
preventing silent corruption.

`TestSpoolMasterKeyRotationLive` verifies live master-key rotation.
Data written before rotation and data written after rotation under the
new master key are verified. Reopening with the old master key fails
with `spool.ErrWrongKey`, while the new master key decrypts both eras
seamlessly.

`TestSpoolStorageFaultTerminalIsolationLive` verifies that when an
underlying storage write fails, `OnStorageError` is invoked and the store
immediately latches into terminal error, rejecting subsequent mutations
with `spool.ErrStorageFailed`. Once reopened without fault hooks, all
pre-fault data is recovered intact and mutations resume.

`TestSpoolContextIDIsolationAndRebindLive` verifies that a store bound to
Context A strictly rejects opening under Context B with
`spool.ErrContextMismatch`. It verifies that `RebindContext` migrates
the store to Context B, after which Context A is rejected and Context B
opens all records.

`TestSpoolCheckpointOnlineIsolationAndForkLive` captures an online point-in-time
checkpoint, streams its files into an independent fork directory, and verifies
that subsequent deletions and appends in the original store do not affect the
fork, while new writes in the fork store do not bleed into the original store.

`TestSpoolDataKeyAgingAndPruningLive` verifies multiple internal data key
rotations, inspects key inventory across eras, verifies that `PruneDataKeys`
retains active keys, and proves that all records across all key eras decrypt
cleanly.

`TestSpoolBackpressureHighConcurrencyLive` stresses the store under high
concurrency with a constrained `MaxPendingBytes` budget, proving that `TryPut`
returns `spool.ErrBackpressure` gracefully without panics, `Put` blocks and
flushes safely, and no accepted data is lost.

`TestSpoolAtomicBatchMixedMutationsCrashLive` runs a child writer committing
atomic multi-mutation batches containing mixed inserts, overwrites, and
tombstones, and SIGKILLs the child mid-stream. On reopen, it verifies that all
acknowledged rounds have applied updates and deletions without resurrection,
and the in-flight round maintains all-or-nothing atomicity.
