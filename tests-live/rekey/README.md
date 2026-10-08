# Storage-key rotation

Run `bash tests-live/run.sh rekey` to exercise application-key rotation,
restart recovery, and rejection of the retired key. Both normal restart and
await-unlock/SIGKILL recovery use managed typed records; the runner builds the
fixture with CGO disabled. The tests verify records written before and after
rotation after reopening with the new key and confirm that the retired key is
rejected.
