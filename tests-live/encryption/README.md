# Encrypted storage live test

Run with `go test -count=1 ./tests-live/encryption`. The test writes a unique
plaintext marker using the public API, closes the database, confirms the marker
does not occur in durable storage files, rejects a wrong key, and reopens with
the correct key to verify the row. This covers storage confidentiality and
restart behavior; encrypted replication of sustained concurrent writes remains
covered by the replication integration scenarios.
