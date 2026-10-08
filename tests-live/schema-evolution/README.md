# Rolling additive schema migration (multi-process)

Run with `CGO_ENABLED=0 bash tests-live/run.sh schema-evolution`. Three native
typed-record nodes replicate an additive Note-field migration. An older
application binding updates a known field after adoption, and the test checks
that it preserves the added field, late inserts, CRDT counters, and restart
reconstruction on all three encrypted nodes.
