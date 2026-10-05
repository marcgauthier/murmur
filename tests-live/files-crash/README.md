# File crash leaves no partials or phantoms

Run with `go test -count=1 ./tests-live/files-crash`. The uploader is
SIGKILLed mid-upload and the fetcher mid-fetch (a kill only counts
when the op was in flight, else the round retries and the test fails
loudly if no valid kill lands). Afterwards every listed object must
download byte-complete and digest-verified, metadata must converge
exactly, and every on-disk `.spfo`/`.stage-*`/`.part` must be
attributable to a known attempt. `MURMUR_FILES_CRASH_BIG_MB` sizes
the interrupted fetch.
