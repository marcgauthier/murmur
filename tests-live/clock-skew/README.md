# Wall-clock skew keeps LWW deterministic

Run with `go test -count=1 ./tests-live/clock-skew`. Nodes run under
libfaketime with skewed clocks; concurrent writes to shared rows must
still resolve to identical LWW winners on every node. Requires
libfaketime on PATH (`LD_PRELOAD`); the suite skips with a message
when it is absent. CI installs libfaketime and runs this suite
privileged; see `.github/workflows/ci.yml`.
