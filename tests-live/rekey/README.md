# Storage-key rekey Go integration test

Run from the repository root with:

```sh
go test -count=1 ./tests-live/rekey
```

One Murmur-SQL daemon process writes 25 rows, rotates its storage key over
HTTP without downtime (writes keep flowing, status reports the new key),
restarts on the new key with state intact, accepts another write, and then
rejects the retired key at startup. It ports the rekey/restart verification
from `../GALVANIZE/tests-live/rekey`, driving the application process rather
than the embedded API.
