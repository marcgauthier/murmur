# File crash leaves no partials or phantoms

Run with `CGO_ENABLED=0 bash tests-live/run.sh files-crash`. The test uses
managed typed records without SQLite or CGO. The uploader is
SIGKILLed mid-upload and the fetcher mid-fetch (a kill only counts
when the op was in flight, else the round retries and the test fails
loudly if no valid kill lands). Afterwards every listed object must
download byte-complete and digest-verified, metadata must converge
exactly, and every on-disk `.spfo`/`.stage-*`/`.part` must be
attributable to a known attempt. `MURMUR_FILES_CRASH_BIG_MB` sizes
the interrupted fetch.
