# Distributed merge policies

Run `bash tests-live/run.sh merge-policies`. This scenario uses the managed
typed API and builds its node fixture with CGO disabled.

Three encrypted daemons start with manual peer connections. They write
PN_COUNTER values, OR_SET values and MAX/MIN fields while disconnected. After
two nodes connect, the original writer stops; the second node forwards its
causal operations to the third. The scenario checks typed projections,
unobserved versus observed set removal, and restart recovery. Communication
uses QUIC/mTLS and separately provisioned origin keys.
