# Peer exclusion, restart persistence, and retention release (multi-process)

Run with `bash tests-live/run.sh churn-retirement`. Three typed RIME
daemons mesh; node3 is excluded on the survivors via
`POST /v1/admin/remove_peer`. Sessions to node3 must drop, its writes
must stop arriving, and it must release its GC retention pin
(`spedsql_gating_members` drops). The exclusion must survive a survivor
restart with no rediscovery re-mesh, and explicit re-adding must heal
replication. SWIM suspect/dead/refutation is not covered: memberlist
suspicion is not wired into the runtime.
