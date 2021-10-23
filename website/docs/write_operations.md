---
title: Write Operations
summary: "In this page, we describe the different write operations in Hudi."
toc: true
last_modified_at:
---

It may be helpful to understand the 3 different write operations provided by Hudi datasource or the delta streamer tool and how best to leverage them. These operations
can be chosen/changed across each commit/deltacommit issued against the table.


- **UPSERT** : This is the default operation where the input records are first tagged as inserts or updates by looking up the index.
  The records are ultimately written after heuristics are run to determine how best to pack them on storage to optimize for things like file sizing.
  This operation is recommended for use-cases like database change capture where the input almost certainly contains updates. The target table will never show duplicates.
- **INSERT** : This operation is very similar to upsert in terms of heuristics/file sizing but completely skips the index lookup step. Thus, it can be a lot faster than upserts
  for use-cases like log de-duplication (in conjunction with options to filter duplicates mentioned below). This is also suitable for use-cases where the table can tolerate duplicates, but just
  need the transactional writes/incremental pull/storage management capabilities of Hudi.
- **BULK_INSERT** : Both upsert and insert operations keep input records in memory to speed up storage heuristics computations faster (among other things) and thus can be cumbersome for
  initial loading/bootstrapping a Hudi table at first. Bulk insert provides the same semantics as insert, while implementing a sort-based data writing algorithm, which can scale very well for several hundred TBs
  of initial load. However, this just does a best-effort job at sizing files vs guaranteeing file sizes like inserts/upserts do.
