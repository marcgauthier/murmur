# Distributed merge policies

Run `go test -tags modernc -v ./tests-live/merge-policies` or
`bash tests-live/run.sh merge-policies`.

Three encrypted daemons start with manual peer connections. They write a
counter beyond int64, observed-remove sets and MAX/MIN while disconnected.
After two nodes connect, the original writer stops; another node must forward
its signed causal operations to the third. The scenario checks projections,
retained causal-record hashes, restart recovery and observed removal after
restart. Communication uses QUIC/mTLS and separately provisioned origin keys.
