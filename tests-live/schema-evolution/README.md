# Rolling additive schema migration (multi-process)

Run with `go test -count=1 ./tests-live/schema-evolution`. Three Murmur-SQL
daemons mesh on a two-column table. Node1 migrates first over
`POST /v1/admin/migrate` (adds a nullable `score` column) and writes
scores while node2/node3 stay behind: the mixed-version mesh must keep
replicating in both directions. Then the laggards migrate and all three
converge on the full three-column state with equal digests.
