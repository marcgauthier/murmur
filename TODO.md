
RUN BENCHMARK for Insert, Update and Select while replication is running.
With medium speed and burst, during burst I want to know the insert /s average. Then let the replication/persistance catch up. At the end I would like to know the database size in RAM and on disk to compare how well the compression work.

Look at the murmur live-tests and bring them in this crate as equivalent rust test.

| High/Low replication | One-way cross-domain transfer, recipient-sealed bundles, encrypted outbox/inbox journals, delivery receipts, replay protection, schema holds, and directory/HTTP(S)/FTP(S) transport adapters. |

| High ownership and provenance | Source tracking, High-owned field protection against subsequent Low updates, protected deletes, and explicit ownership resolution. |

| Key rotation | Online data-key rotation, application-key replacement through keyring rewrapping, compaction-based re-encryption, and safe retirement of old keys. |

| Reactive subscriptions | Query snapshots and change notifications, resume cursors, slow-consumer handling, and reset events. |

| Build Sqlite with FTS and Vector search/storage. Find a way to make virtual table works extension session does not work with virtual tables. |

| Operational tooling | Storage inspect/verify/repair/doctor, key inventory, cluster topology, replication lag, backup commands, and manual maintenance controls. V3 only specifies serve and check-config. |

| Large replication transfers | V3 includes bounded batches and full-state reconciliation, but does not specify Murmur’s durable chunk staging, resumable chunk bitmaps, cross-peer repair, or paginated progress exchange. |

| Snapshot synchronization | V3 needs full-state synchronization, but lacks the detailed durable-resume and source-retention protections implemented in Murmur’s snapshot subsystem. |


Those next 3 are the last things to implement:

    Build a Go Client 

    Build a Go Gorm Client
    
    Murmur Daemon Build a full application with users, Allow user to connect to murmur via HTTPS, MySql and PostgreSql wire's (HTTPS and mTLS require for production); toml file configuration with settings, Replication Nodes, etc.






>>>
| Work scheduling | Bounded queues are included, but configurable local-versus-replication writer shares, idle borrowing, and maintenance fairness are not. |
>>>

  Here is the breakdown of why it existed, why Murmur’s current architecture gets away without it for now, and the specific
  failure modes that determine whether you need to implement it.
  ──────
  ### 1. Where this comes from (The Go/SpedSQL Precedent)
  
  In the original Go implementation (scheduler.go), writer scheduling was an admission coordinator guarding state mutations
  across three classes:
  
  • WriterLocal: Local user transactions (BeginTx / commit).
  • WriterRemote: Incoming replication apply (ApplyRemote, snapshot chunks, schema sync).
  • WriterMaintenance: Migrations, log retention/GC, and file key rotation.
  
  It enforced a 90% local / 10% remote virtual-time fair queue over measured execution time:
  
  • Idle borrowing: If no remote transactions were waiting, local writes used 100% of the capacity with zero artificial    
  delays.
  • Bounded debt (1s): Prevented a long idle period from giving one class unfair priority once contention started.         
  • Maintenance reserve (1 in 100 grants): Ensured background log GC and compaction weren't starved indefinitely if user   
  writes or replication were constantly busy.
  ──────
  ### 2. Why V3 does not strictly need it right now
  
  In Murmur V3's current Rust architecture:
  
  • Application writes run directly against the single in-memory SQLite connection and only record changesets via SQLite   
  session capture.
  • Persistence, compression, Spool commits, and replication run asynchronously in the background.
  • Overload protection currently relies on bounded queues (capture_bytes = 8 MiB, queue_bytes = 64 MiB). If background    
  workers fall behind, the queue fills up and rejects writes with Error::Busy.
  
  For unit tests, benchmarks, single-node use, or low-to-moderate replication traffic, bounded queues are sufficient.      
  ──────
  ### 3. When you will need it (Failure modes without it)
  
  Without writer shares and maintenance fairness, you run into three concrete failure modes under heavy load:              
  
  1. Replication catch-up stalling local transactions:
  When a node reconnects after being offline and receives a flood of remote transactions or snapshot chunks, remote apply  
  needs access to the SQLite database. Without a writer share (e.g., 90/10), applying remote batches can monopolize the    
  SQLite connection or Spool writer, causing severe local transaction latency spikes.
  2. Maintenance starvation leading to disk bloat:
  If log retention, snapshot compaction, and tombstone cleanup only run "when idle", a system under continuous 24/7 write  
  traffic will never be idle. Without the maintenance reserve (e.g., 1% guarantee), Spool append logs and tombstones never 
  get cleaned up, causing unbounded disk growth.
  3. Local write starvation of incoming replication:
  Under sustained 100% local write saturation, remote replication applies can be starved completely, causing the node to   
  fall progressively further behind the rest of the peer mesh until it falls out of the retention window.
  ──────
  ### Recommendation
  
  • If your focus is completing the v3 core / benchmarks / Go client: Skip / defer it. Bounded queues and coarse           
  mutexes/channels are enough to validate correctness, replication, and basic throughput.
  • If you are hardening for multi-node production deployment with high write concurrency: Keep it on the roadmap. You will
  need at least:
      • An admission coordinator before acquiring write access to the in-memory SQLite instance (prioritizing local writes 
      over remote apply).
      • A minimum starvation floor (e.g., 1-in-100 reserve) so background log/receipt GC can make forward progress even    
      under continuous load.
  

──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
>
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
? for shortcuts                                                                                      Gemini 3.8 Flash · medium


