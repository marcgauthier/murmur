# Removed SQL application routes

This live test starts two native typed nodes and verifies that the retired
`/v1/query` and `/v1/exec` routes return HTTP 410 with typed RIME migration
guidance, including for stacked statements and data-exfiltration payloads.
It then proves typed writes still replicate after those requests.

Run with `CGO_ENABLED=0 bash tests-live/run.sh sqli-api`.
