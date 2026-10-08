# Encrypted storage live test

Run with `go test -count=1 ./tests-live/encryption`. The typed scenario writes
through `Config.Tables`, stops the node, confirms its record marker does not
occur in durable files, restarts it, rejects a wrong key, then unlocks with the
correct key and reads the row through the typed API. Encrypted replication of
sustained concurrent writes remains covered by the replication integration
scenarios.
