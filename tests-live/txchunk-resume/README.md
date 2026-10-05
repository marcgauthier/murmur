# Chunked transaction resume after receiver SIGKILL

Run with `go test -count=1 ./tests-live/txchunk-resume`. A large
multi-chunk transaction (`MURMUR_TXCHUNK_ROWS`,
`MURMUR_TXCHUNK_VALUE_BYTES`) streams to a peer that is SIGKILLed
mid-receipt; the kill only counts when chunks were in flight
(`MURMUR_TXCHUNK_ATTEMPTS` bounds the too-late retries, else the test
fails loudly). After restart the transfer resumes and the full row set
lands exactly once with equal digests.
