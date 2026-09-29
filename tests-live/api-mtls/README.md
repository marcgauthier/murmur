# API mTLS

Starts a real Murmur-SQL daemon and verifies that its API is HTTPS-only. A
trusted client certificate is required for every route except public
`GET /healthz`; `/metrics` and `/v1/*` reject requests without a valid
cluster-CA-issued client certificate. A certificate from another CA and
plaintext HTTP are rejected.

Run with `bash tests-live/run.sh api-mtls`.
