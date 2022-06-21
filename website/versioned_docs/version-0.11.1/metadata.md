---
title: Metadata Table
keywords: [ hudi, metadata, S3 file listings]
---

## Motivation for a Metadata Table

The Apache Hudi Metadata Table can significantly improve read/write performance of your queries. The main purpose of the
Metadata Table is to eliminate the requirement for the "list files" operation.

When reading and writing data, file listing operations are performed to get the current view of the file system.
When data sets are large, listing all the files may be a performance bottleneck, but more importantly in the case of cloud storage systems
like AWS S3, the large number of file listing requests sometimes causes throttling due to certain request limits.
The Metadata Table will instead proactively maintain the list of files and remove the need for recursive file listing operations

### Some numbers from a study:
Running a TPCDS benchmark the p50 list latencies for a single folder scales ~linearly with the amount of files/objects:

|Number of files/objects|100|1K|10K|100K|
|---|---|---|---|---|
|P50 list latency|50ms|131ms|1062ms|9932ms|

Whereas listings from the Metadata Table will not scale linearly with file/object count and instead take about 100-500ms per read even for very large tables.
Even better, the timeline server caches portions of the metadata (currently only for writers), and provides ~10ms performance for listings.

### Supporting Multi-Modal Index

Multi-modal index can drastically improve the lookup performance in file index and query latency with data skipping.
Bloom filter index containing the file-level bloom filter facilitates the key lookup and file pruning.  The column stats
index containing the statistics of all columns improves file pruning based on key and column value range in both the
writer and the reader, in query planning in Spark for example.  Multi-modal index is implemented as independent partitions
containing the indices in the metadata table.

## Enable Hudi Metadata Table and Multi-Modal Index
Since 0.11.0, the metadata table with synchronous updates and metadata-table-based file listing are enabled by default.
There are prerequisite configurations and steps in [Deployment considerations](#deployment-considerations) to
safely use this feature.  The metadata table and related file listing functionality can still be turned off by setting
[`hoodie.metadata.enable`](/docs/configurations#hoodiemetadataenable) to `false`.  For 0.10.1 and prior releases, metadata
table is disabled by default, and you can turn it on by setting the same config to `true`.

If you turn off the metadata table after enabling, be sure to wait for a few commits so that the metadata table is fully
cleaned up, before re-enabling the metadata table again.

The multi-modal index is introduced in 0.11.0 release.  They are disabled by default.  You can choose to enable bloom
filter index by setting `hoodie.metadata.index.bloom.filter.enable` to `true` and enable column stats index by setting
`hoodie.metadata.index.column.stats.enable` to `true`, when metadata table is enabled.  In 0.11.0 release, data skipping
to improve queries in Spark now relies on the column stats index in metadata table.  The enabling of metadata table and
column stats index is prerequisite to enabling data skipping with `hoodie.enable.data.skipping`.

## Deployment considerations
To ensure that Metadata Table stays up to date, all write operations on the same Hudi table need additional configurations
besides the above in different deployment models.  Before enabling metadata table, all writers on the same table must
be stopped.

### Deployment Model A: Single writer with inline table services

If your current deployment model is single writer and all table services (cleaning, clustering, compaction) are configured
to be inline, such as Deltastreamer sync-once mode and Spark Datasource with default configs, there is no additional configuration
required.  After setting [`hoodie.metadata.enable`](/docs/configurations#hoodiemetadataenable) to `true`, restarting
the single writer is sufficient to safely enable metadata table.

### Deployment Model B: Single writer with async table services

If your current deployment model is single writer along with async table services (such as cleaning, clustering, compaction)
running in the same process, such as Deltastreamer continuous mode writing MOR table, Spark streaming (where compaction is async by default),
and your own job setup enabling async table services inside the same writer, it is a must to have the optimistic concurrency control,
the lock provider, and lazy failed write clean policy configured before enabling metadata table as follows.  This is to guarantee
the proper behavior of [optimistic concurrency control](/docs/concurrency_control#enabling-multi-writing) when enabling
metadata table. Failing to follow the configuration guide leads to loss of data.  Note that these configurations are
required only if metadata table is enabled in this deployment model.

```properties
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.cleaner.policy.failed.writes=LAZY
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider
```

If multiple writers in different processes are present, including one writer with async table services, please refer to
[Deployment Model C: Multi-writer](#deployment-model-c-multi-writer) for configs, with the difference of using a
distributed lock provider.  Note that running a separate compaction (`HoodieCompactor`) or clustering (`HoodieClusteringJob`)
job apart from the ingestion writer is considered as multi-writer deployment, as they are not running in the same
process which cannot rely on the in-process lock provider.

### Deployment Model C: Multi-writer

If your current deployment model is multi-writer along with a lock provider and other required configs set for every writer
as follows, there is no additional configuration required.  You can bring up the writers sequentially after stopping the
writers for enabling metadata table.  Applying the proper configurations to only partial writers leads to loss of data
from the inconsistent writer. So, ensure you enable metadata table across all writers.

```properties
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.cleaner.policy.failed.writes=LAZY
hoodie.write.lock.provider=<distributed-lock-provider-classname>
```

Note that there are 3 different distributed [lock providers available](/docs/concurrency_control#enabling-multi-writing)
to choose from: `ZookeeperBasedLockProvider`, `HiveMetastoreBasedLockProvider`, and `DynamoDBBasedLockProvider`.