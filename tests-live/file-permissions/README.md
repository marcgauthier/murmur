# Product-owned secret paths stay locked down

Run with `go test -count=1 ./tests-live/file-permissions`. After fresh
init and again after backup plus restore, every product-owned secret
path (key-registry directory and files, backup/restore artifacts)
must grant nothing to group/other (0600 files, 0700 dirs).
Harness-minted material (`node.key`) and operator-owned paths are
audit-logged, never asserted, since no product change can alter
fixture-chosen modes.
