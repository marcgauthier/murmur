# Hostile schema manifests are quarantined

Run with `go test -count=1 ./tests-live/hostile-schema`. A malicious
fixture peer (`attacker.go`) serves hostile schema manifests (bad
epochs, forged hashes, oversized declarations). The honest mesh must
quarantine them without adopting or wedging: schema epoch stays put,
honest writes converge, and digests stay equal.
