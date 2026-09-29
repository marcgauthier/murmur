# Managed views Go integration test

Run from the repository root with:

```sh
go test -count=1 ./tests-live/views
```

Three Murmur-SQL daemon processes in discrete node directories form an
encrypted mutual-TLS mesh with the same local view over the replicated
`contacts` table. Each node inserts a row; the two qualifying
rows must appear in the view on all replicas, while a non-qualifying row stays
out. It then reopens one replica and verifies that startup rebuild restores
both the replicated rows and the local view. This ports the managed-view
scenario from `../GALVANIZE/tests-live/views`.
