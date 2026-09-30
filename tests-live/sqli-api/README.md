# SQL injection via the API is contained

Run with `go test -count=1 ./tests-live/sqli-api`. Classic SQLi
payloads (tautologies, stacked queries, comment truncation, UNION
probes) are sent through the service API's exec/query paths. Every
payload must be contained (parameterized or rejected, never executed
as SQL): no out-of-scope rows appear, honest data is untouched, and
digests stay equal.
