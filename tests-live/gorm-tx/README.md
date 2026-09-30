# GORM transaction live test

Run with `go test -count=1 ./tests-live/gorm-tx`. Two nodes prove
GORM transactions replicate atomically: commits converge fully on
the peer, while a transaction failed midway (duplicate key) and an
explicitly rolled-back transaction leave zero rows behind locally
and never expose partial counts on either node. Concurrent commits
from both nodes converge to the union.

Nodes run in-process, because GORM requires an embedded engine
handle, and replicate over real QUIC with on-disk encrypted state.
