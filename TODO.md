build a gorm client for MURMUR-SQL?



Version 2.0

### 2. Custom conflict resolution callbacks                                                                                                    
                                  
  Per-cell LWW is clean and deterministic, but it's also lossy. If two nodes increment a counter, one write disappears. If you allowed           
  registering per-column or per-table merge functions (e.g., MergeMax, MergeSum, MergeAppendSet, or arbitrary func(local, remote Cell) Cell),    
  applications could model CRDTs beyond LWW — counters, grow-only sets, merge-friendly JSON — without leaving the replication framework. The     
  Pebble merge operator could potentially be leveraged here.                                                                                     
 