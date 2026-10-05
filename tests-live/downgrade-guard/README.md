# Downgrade guard refuses newer stores

Run with `go test -count=1 ./tests-live/downgrade-guard`. Builds the
previous-release daemon from git history (`MURMUR_PREV_REF`, same
`MURMUR_TAGS`), creates a store with it, then requires the current
build to refuse the older/newer store combination loudly instead of
opening it into a mixed state. Needs a git checkout with history.
