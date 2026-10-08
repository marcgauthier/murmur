# Fetch from a corrupt source fails verification

Run with `CGO_ENABLED=0 bash tests-live/run.sh files-corrupt-source`. This
uses managed typed records without SQLite or CGO. A
victim object (`MURMUR_FILES_CORRUPT_VICTIM_KB`, default 256) is
published honestly, then its stored bytes are corrupted on the source
node while stopped (same length, flipped chunk ciphertext). A peer
fetch must fail with the exact `objectstore: invalid encrypted
object` sentinel, advance the fetch-failed counter, leave no partial
install (no verified file, no staging residue, metadata untouched),
and fail identically on retry. Honest objects must still fetch
afterwards.
