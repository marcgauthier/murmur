# Heal order does not affect the final digest

Run with `CGO_ENABLED=0 bash tests-live/run.sh three-way-heal`. Four daemons
use managed typed records without SQLite or CGO, then
split into pairs, diverge with disjoint inserts plus conflicting
updates to the same base rows, then heal link-by-link in two different
orders (`MURMUR_THREE_WAY_HEAL_ORDER`, `MURMUR_THREE_WAY_HEAL_ORDER_B`;
each step waits for real cross-side traffic). Both orders must reach
the identical digest, including identical LWW conflict winners.
