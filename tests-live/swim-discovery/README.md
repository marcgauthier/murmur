# Seed-only SWIM discovery and rediscovery

Run with `go test -count=1 ./tests-live/swim-discovery`. Nodes start
with seed addresses only (no static full mesh) and must form a
connected mesh (`MURMUR_SWIM_DISCOVERY_SECONDS` bounds discovery);
then the seed is killed and the survivors must rediscover each other
and reconverge writes with equal digests.
