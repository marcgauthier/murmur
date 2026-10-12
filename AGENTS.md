# Architecture guidance

The design is an SQLITE in memory database that has background functions that provide data persistance with compression and encryption and data replication via masterless peer-to-peer. Keep this in mind when developping, it's SQLITE in memory (with some limitation) first and the replication and persistance is done in the background.


Keep the SQLite crate lightweight and focused on local database operations.
Replication and conflict resolution belong in a separate background task;
they must not add blocking work or heavyweight dependencies to the SQLite
operation path. Keep the boundary between SQLite and that task small.


Use exactly one in-memory SQLite database. Application writes must execute only
application SQL, transaction control, and SQLite session capture. Do not add CRDT
metadata tables, generated triggers, or metadata SQL to that database.

Spool is the authoritative store for conflict versions and tombstones. Background
workers read and update those Spool records directly; do not maintain a separate
Rust metadata database or a second SQLite replica. Transient batch buffers and
Spool's existing key/location index are permitted.

Assign a batch's writing time when its changeset is collected from SQLite, before
queueing it. Preserve that timestamp across worker delays and storage retries.

Maintain a file VENDORS.md that list all of the external package/crate that are dependencies.  I want to see the name of the company and author and country of orgin for the company if there is one otherwise the author. 
Keep Spool point reads small: default to 64 KiB independently compressed and
authenticated blocks, with oversized individual records isolated. Large atomic
commits must span blocks. Keep a byte-bounded hot block cache inside Spool, with
configurable capacity, eviction and decoded-buffer wiping. Cache data is disposable
and must never become a separate authoritative metadata store.

Table replication is controlled by `Database::set_replication` / `SetReplication`.
Persist the node-local map keyed by stable table IDs at `00/schema/replication`,
atomically with schema events. Missing entries mean enabled. Preserve settings
through migrations and recovery. Filter row data, tombstones and table CRDT
records in background outgoing and incoming paths; keep local persistence intact.
Do not check these settings on ordinary application SQL writes.

Replication retention runs in background workers and persists peer ACKs, peer
retirement, persistence-time ages, snapshot coverage and duplicate floors in
Spool. Enforce both minimum age and minimum log count, protect unretired peers
within the offline window and pending High/Low exports. Publish a durable
snapshot boundary before atomically deleting logs/receipts and advancing floors.
Keep CRDT tombstones. Recheck retained heads on the serialized Spool worker so
cleanup cannot overwrite newer appends. Never add retention SQL to SQLite.
