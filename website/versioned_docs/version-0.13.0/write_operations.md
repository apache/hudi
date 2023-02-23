---
title: Write Operations
summary: "In this page, we describe the different write operations in Hudi."
toc: true
last_modified_at:
---

It may be helpful to understand the different write operations of Hudi and how best to leverage them. These operations
can be chosen/changed across each commit/deltacommit issued against the table. See the [How To docs on Writing Data](/docs/writing_data) 
to see more examples.

## Operation Types
### UPSERT 
This is the default operation where the input records are first tagged as inserts or updates by looking up the index.
The records are ultimately written after heuristics are run to determine how best to pack them on storage to optimize for things like file sizing.
This operation is recommended for use-cases like database change capture where the input almost certainly contains updates. The target table will never show duplicates. 

### INSERT
This operation is very similar to upsert in terms of heuristics/file sizing but completely skips the index lookup step. Thus, it can be a lot faster than upserts
for use-cases like log de-duplication (in conjunction with options to filter duplicates mentioned below). This is also suitable for use-cases where the table can tolerate duplicates, but just
need the transactional writes/incremental pull/storage management capabilities of Hudi.

### BULK_INSERT
Both upsert and insert operations keep input records in memory to speed up storage heuristics computations faster (among other things) and thus can be cumbersome for
initial loading/bootstrapping a Hudi table at first. Bulk insert provides the same semantics as insert, while implementing a sort-based data writing algorithm, which can scale very well for several hundred TBs
of initial load. However, this just does a best-effort job at sizing files vs guaranteeing file sizes like inserts/upserts do.

### DELETE
Hudi supports implementing two types of deletes on data stored in Hudi tables, by enabling the user to specify a different record payload implementation.
- **Soft Deletes** : Retain the record key and just null out the values for all the other fields.
  This can be achieved by ensuring the appropriate fields are nullable in the table schema and simply upserting the table after setting these fields to null.
- **Hard Deletes** : A stronger form of deletion is to physically remove any trace of the record from the table. This can be achieved in 3 different ways. 
  - Using DataSource, set `OPERATION_OPT_KEY` to `DELETE_OPERATION_OPT_VAL`. This will remove all the records in the DataSet being submitted. 
  - Using DataSource, set `PAYLOAD_CLASS_OPT_KEY` to `"org.apache.hudi.EmptyHoodieRecordPayload"`. This will remove all the records in the DataSet being submitted. 
  - Using DataSource or DeltaStreamer, add a column named `_hoodie_is_deleted` to DataSet. The value of this column must be set to `true` for all the records to be deleted and either `false` or left null for any records which are to be upserted.

## Writing path
The following is an inside look on the Hudi write path and the sequence of events that occur during a write.

1. [Deduping](/docs/configurations#hoodiecombinebeforeinsert)
   1. First your input records may have duplicate keys within the same batch and duplicates need to be combined or reduced by key.
2. [Index Lookup](/docs/next/indexing)
   1. Next, an index lookup is performed to try and match the input records to identify which file groups they belong to.
3. [File Sizing](/docs/next/file_sizing)
   1. Then, based on the average size of previous commits, Hudi will make a plan to add enough records to a small file to get it close to the configured maximum limit.
4. [Partitioning](/docs/next/file_layouts)
   1. We now arrive at partitioning where we decide what file groups certain updates and inserts will be placed in or if new file groups will be created
5. Write I/O
   1. Now we actually do the write operations which is either creating a new base file, appending to the log file,
   or versioning an existing base file.
6. Update [Index](/docs/next/indexing)
   1. Now that the write is performed, we will go back and update the index.
7. Commit
   1. Finally we commit all of these changes atomically. (A [callback notification](/docs/next/writing_data#commit-notifications) is exposed)
8. [Clean](/docs/next/hoodie_cleaner) (if needed)
   1. Following the commit, cleaning is invoked if needed.
9. [Compaction](/docs/next/compaction)
   1. If you are using MOR tables, compaction will either run inline, or be scheduled asynchronously
10. Archive
    1. Lastly, we perform an archival step which moves old [timeline](/docs/next/timeline) items to an archive folder.
