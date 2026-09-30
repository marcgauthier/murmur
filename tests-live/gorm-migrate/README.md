# GORM migration live test

Run with `go test -count=1 ./tests-live/gorm-migrate`. Two nodes
replicate while one of them runs `AutoMigrate` to add a column: the
test proves the peer adopts the new epoch, both nodes read and write
the new column, a rolling restart of the whole cluster preserves
data, and a repeated `AutoMigrate` advances no epoch.

Nodes run in-process, because GORM requires an embedded engine
handle, and replicate over real QUIC with on-disk encrypted state.
