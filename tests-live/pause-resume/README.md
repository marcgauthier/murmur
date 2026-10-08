# SIGSTOP pause and resume without loss

Run with `go test -count=1 ./tests-live/pause-resume`. One node is
SIGSTOP-frozen for `MURMUR_PAUSE_RESUME_STOP_SECONDS` (default 20)
while survivors keep writing. Freeze detection is asserted (alive-view
dip or failed SWIM probes on survivors); after SIGCONT every
acknowledged write must be present on all nodes with equal digests,
and the rejoin path (range repair vs snapshot) is recorded from
counters. Rows are written and verified through managed typed RIME
records; the runner builds without SQLite tags and with CGO disabled.
