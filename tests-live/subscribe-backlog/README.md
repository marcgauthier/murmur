# Subscription backlog delivers fully after heal

Run with `go test -count=1 ./tests-live/subscribe-backlog`. A live
subscription is held across a partition while
`MURMUR_SUBSCRIBE_BACKLOG_ROWS` rows land on the far side; healing
must deliver the complete backlog exactly once (no reset, no gaps, no
duplicates) and the subscriber must observe the converged row set.
