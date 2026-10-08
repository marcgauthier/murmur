# Two typed Low domains into one High mesh

Run with `CGO_ENABLED=0 go test -count=1 ./tests-live/bridge-two-streams` or
`bash tests-live/run.sh bridge-two-streams`. Two independent Low domains export
managed typed records into separate streams; a two-node High cluster imports
them and replicates the results. The gate checks stream progress isolation,
duplicate delivery, convergence, Low-owned provenance, and fail-closed
cross-domain row-ID collisions.
